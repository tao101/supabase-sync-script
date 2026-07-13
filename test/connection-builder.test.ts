import assert from 'node:assert/strict';
import test from 'node:test';
import { createPostgresPool } from '../src/clients/postgres-client.js';
import { ConnectionBuilder } from '../src/config/connection-builder.js';
import { baseConfig } from './fixture.js';

test('builds credential-free verified libpq URLs for remote databases', () => {
  const builder = new ConnectionBuilder();
  const connection = {
    ...baseConfig.source,
    dbUrl: 'postgresql://user:p%40ss@db.example.com:5432/postgres?application_name=sync',
  };

  const url = new URL(builder.buildDbUrl(connection));
  assert.equal(url.password, '');
  assert.equal(url.searchParams.get('sslmode'), 'verify-full');
  assert.equal(url.searchParams.get('sslrootcert'), 'system');
  assert.equal(url.searchParams.get('application_name'), 'sync');
  assert.equal(builder.buildPgEnv(connection).PGPASSWORD, 'p@ss');
});

test('uses the system trust store sentinel only for libpq clients', () => {
  const builder = new ConnectionBuilder();
  const connection = {
    ...baseConfig.source,
    dbUrl: 'postgresql://user@db.example.com/postgres?sslmode=verify-full&sslrootcert=system',
  };

  assert.equal(new URL(builder.buildDbUrl(connection)).searchParams.get('sslrootcert'), 'system');
  assert.equal(new URL(builder.buildNodeDbUrl(connection)).searchParams.has('sslrootcert'), false);
});

test('applies legacy dbPassword consistently to node and libpq clients', () => {
  const builder = new ConnectionBuilder();
  const connection = {
    ...baseConfig.source,
    dbUrl: 'postgresql://user@db.example.com/postgres',
    dbPassword: 'legacy secret',
  };

  assert.equal(new URL(builder.buildNodeDbUrl(connection)).password, 'legacy%20secret');
  assert.equal(new URL(builder.buildDbUrl(connection)).password, '');
  assert.equal(builder.buildPgEnv(connection).PGPASSWORD, 'legacy secret');
});

test('lets an explicit empty query password clear other password sources', () => {
  const builder = new ConnectionBuilder();
  const connection = {
    ...baseConfig.source,
    dbUrl: 'postgresql://user:old@db.example.com/postgres?password=',
    dbPassword: 'legacy',
  };

  assert.equal(new URL(builder.buildNodeDbUrl(connection)).password, '');
  assert.equal(builder.buildPgEnv(connection).PGPASSWORD, '');
});

test('rejects endpoint overrides hidden in database URL query parameters', () => {
  const builder = new ConnectionBuilder();
  for (const parameter of ['host', 'hostaddr', 'port', 'dbname', 'service']) {
    const errors = builder.validateConnection({
      ...baseConfig.source,
      dbUrl: `postgresql://user@db.example.com/postgres?${parameter}=other`,
    });
    assert.match(errors.join('\n'), /query parameter/);
  }
});

test('requires deterministic host, user, and database URL fields', () => {
  const builder = new ConnectionBuilder();
  for (const [dbUrl, message] of [
    ['postgresql:///postgres', /explicit host/],
    ['postgresql://db.example.com/postgres', /explicit user/],
    ['postgresql://user@db.example.com', /explicit database name/],
  ] as const) {
    assert.match(builder.validateConnection({ ...baseConfig.source, dbUrl }).join('\n'), message);
  }
});

test('removes inherited libpq endpoint overrides from child environments', () => {
  const previous = process.env.PGHOSTADDR;
  process.env.PGHOSTADDR = 'other.example.com';
  try {
    assert.equal(new ConnectionBuilder().buildPgEnv(baseConfig.source).PGHOSTADDR, undefined);
  } finally {
    if (previous === undefined) delete process.env.PGHOSTADDR;
    else process.env.PGHOSTADDR = previous;
  }
});

test('uses plaintext only for exact loopback or explicit disable', () => {
  const builder = new ConnectionBuilder();
  for (const host of ['localhost', 'LOCALHOST.', '127.42.0.1', '[::1]']) {
    const url = builder.buildDbUrl({
      ...baseConfig.source,
      dbUrl: `postgresql://user@${host}/postgres`,
    });
    assert.equal(new URL(url).searchParams.get('sslmode'), 'disable');
  }

  for (const host of ['localhost.example', 'evil-localhost.example', '127.0.0.1.example']) {
    const url = builder.buildDbUrl({
      ...baseConfig.source,
      dbUrl: `postgresql://user@${host}/postgres`,
    });
    assert.equal(new URL(url).searchParams.get('sslmode'), 'verify-full');
  }

  const explicit = builder.buildDbUrl({
    ...baseConfig.source,
    dbUrl: 'postgresql://user@db.example.com/postgres?sslmode=disable',
  });
  assert.equal(new URL(explicit).searchParams.get('sslmode'), 'disable');
});

test('legacy forceNoSsl cannot silently downgrade a remote connection', async () => {
  assert.throws(() => createPostgresPool(baseConfig.source, true), /explicit sslmode=disable/);

  const pool = createPostgresPool({
    ...baseConfig.source,
    dbUrl: 'postgresql://user@localhost/postgres',
  }, true);
  await pool.end();
});

test('rejects downgrade and certificate-bypass modes', () => {
  const builder = new ConnectionBuilder();
  for (const sslmode of ['allow', 'prefer', 'require', 'verify-ca', 'no-verify']) {
    const errors = builder.validateConnection({
      ...baseConfig.source,
      dbUrl: `postgresql://user@db.example.com/postgres?sslmode=${sslmode}`,
    });
    assert.match(errors.join('\n'), /sslmode/);
  }
});

test('rejects ambiguous duplicate SSL modes', () => {
  const builder = new ConnectionBuilder();
  for (const query of [
    'sslmode=verify-full&sslmode=disable',
    'sslmode=disable&sslmode=verify-full',
    'sslmode=verify-full&SSLMODE=disable',
  ]) {
    const errors = builder.validateConnection({
      ...baseConfig.source,
      dbUrl: `postgresql://user@db.example.com/postgres?${query}`,
    });
    assert.match(errors.join('\n'), /sslmode/);
  }
});

test('rejects client-key passphrases in URLs', () => {
  const errors = new ConnectionBuilder().validateConnection({
    ...baseConfig.source,
    dbUrl: 'postgresql://user@db.example.com/postgres?sslpassword=secret',
  });
  assert.match(errors.join('\n'), /sslpassword/);

  const oauthErrors = new ConnectionBuilder().validateConnection({
    ...baseConfig.source,
    dbUrl: 'postgresql://user@db.example.com/postgres?oauth_client_secret=secret',
  });
  assert.match(oauthErrors.join('\n'), /OAuth/);

  for (const query of ['PASSWORD=secret', 'SSLPASSWORD=secret', 'password=used&PASSWORD=leaked']) {
    const uppercaseErrors = new ConnectionBuilder().validateConnection({
      ...baseConfig.source,
      dbUrl: `postgresql://user@db.example.com/postgres?${query}`,
    });
    assert.notEqual(uppercaseErrors.length, 0);
  }
});

test('safe display never exposes database credentials', () => {
  const display = new ConnectionBuilder().getSafeDisplay({
    ...baseConfig.source,
    dbUrl: 'postgresql://user:pa:ss@db.example.com/postgres?password=query-secret',
  });

  assert.doesNotMatch(JSON.stringify(display), /pa:ss|query-secret/);
});

test('rejects and hides credentials in Supabase API URLs', () => {
  const builder = new ConnectionBuilder();
  const connection = {
    ...baseConfig.source,
    apiUrl: 'https://user:api-secret@source.example.com',
  };

  assert.match(builder.validateConnection(connection).join('\n'), /must not contain credentials/);
  assert.equal(builder.getSafeDisplay(connection).apiUrl, 'invalid API URL');
});

test('requires HTTPS for remote Supabase APIs but permits loopback HTTP', () => {
  const builder = new ConnectionBuilder();
  assert.match(builder.validateConnection({
    ...baseConfig.source,
    apiUrl: 'http://api.example.com',
  }).join('\n'), /must use https/);

  for (const host of ['localhost', '127.0.0.1', '[::1]']) {
    assert.deepEqual(builder.validateConnection({
      ...baseConfig.source,
      apiUrl: `http://${host}:54321`,
    }), []);
  }
});
