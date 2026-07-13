import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { after, before, test } from 'node:test';
import pg from 'pg';
import { ConfigSchema, type Config } from '../src/types/config.js';
import { DataSync } from '../src/sync/database/data-sync.js';
import { SchemaSync } from '../src/sync/database/schema-sync.js';
import { AuthSync } from '../src/sync/auth/auth-sync.js';
import { TempFileManager } from '../src/utils/temp-files.js';

const rootUrl = process.env.TEST_POSTGRES_URL;
const skip = !rootUrl;
const suffix = `${process.pid}_${randomUUID().replace(/-/g, '').slice(0, 8)}`;
const sourceDatabase = `sync_source_${suffix}`;
const targetDatabase = `sync_target_${suffix}`;
let adminPool: pg.Pool;
let sourcePool: pg.Pool;
let targetPool: pg.Pool;
let config: Config;

function databaseUrl(name: string): string {
  const url = new URL(rootUrl!);
  url.pathname = `/${name}`;
  return url.toString();
}

async function resetSchemas(): Promise<void> {
  await sourcePool.query('DROP SCHEMA public CASCADE; CREATE SCHEMA public');
  await targetPool.query('DROP SCHEMA public CASCADE; CREATE SCHEMA public');
}

async function resetAuthSchemas(targetUserConstraint: string = ''): Promise<void> {
  await sourcePool.query(`
    DROP SCHEMA IF EXISTS auth CASCADE;
    CREATE SCHEMA auth;
    CREATE TABLE auth.users (id uuid PRIMARY KEY, email text NOT NULL);
  `);
  await targetPool.query(`
    DROP SCHEMA IF EXISTS auth CASCADE;
    CREATE SCHEMA auth;
    CREATE TABLE auth.users (id uuid PRIMARY KEY, email text NOT NULL ${targetUserConstraint});
    CREATE TABLE auth.sessions (id uuid PRIMARY KEY, user_id uuid REFERENCES auth.users(id));
  `);
}

function authOnlyConfig(): Config {
  const authConfig = structuredClone(config);
  authConfig.options.components = {
    schema: false,
    data: false,
    auth: true,
    storage: false,
    roles: false,
  };
  authConfig.options.auth.migrateIdentities = false;
  return authConfig;
}

before(async () => {
  if (skip) return;
  adminPool = new pg.Pool({ connectionString: rootUrl });
  await adminPool.query(`CREATE DATABASE "${sourceDatabase}"`);
  await adminPool.query(`CREATE DATABASE "${targetDatabase}"`);
  sourcePool = new pg.Pool({ connectionString: databaseUrl(sourceDatabase) });
  targetPool = new pg.Pool({ connectionString: databaseUrl(targetDatabase) });
  config = ConfigSchema.parse({
    source: { dbUrl: databaseUrl(sourceDatabase) },
    target: { dbUrl: databaseUrl(targetDatabase) },
    options: { components: { storage: false } },
  });
});

after(async () => {
  if (skip) return;
  await sourcePool.end();
  await targetPool.end();
  await adminPool.query(`DROP DATABASE "${sourceDatabase}"`);
  await adminPool.query(`DROP DATABASE "${targetDatabase}"`);
  await adminPool.end();
});

test('schema import rollback preserves the original target', { skip }, async () => {
  await resetSchemas();
  await sourcePool.query('CREATE TABLE public.items (id integer PRIMARY KEY)');
  await targetPool.query('CREATE TABLE public.old_items (id integer PRIMARY KEY)');
  await targetPool.query('INSERT INTO public.old_items VALUES (7)');
  const files = new TempFileManager();
  await files.init();

  try {
    const sync = new SchemaSync(config, files, targetPool, sourcePool);
    const dump = await sync.exportSchema();
    const prepared = await sync.prepareTargetSchemas();
    await assert.rejects(
      sync.importSchema(dump, prepared.resetSql, 'SELECT missing_finalize_function();')
    );
    assert.equal((await targetPool.query('SELECT id FROM public.old_items')).rows[0].id, 7);
    assert.equal((await targetPool.query("SELECT to_regclass('public.items') AS table_name")).rows[0].table_name, null);
  } finally {
    await files.cleanup();
  }
});

test('schema sync atomically replaces the target schema', { skip }, async () => {
  await resetSchemas();
  await sourcePool.query('CREATE TABLE public.items (id integer PRIMARY KEY)');
  await targetPool.query('CREATE TABLE public.old_items (id integer PRIMARY KEY)');
  const files = new TempFileManager();
  await files.init();

  try {
    await new SchemaSync(config, files, targetPool, sourcePool).sync();
    assert.equal((await targetPool.query("SELECT to_regclass('public.old_items') AS table_name")).rows[0].table_name, null);
    assert.equal((await targetPool.query("SELECT to_regclass('public.items') AS table_name")).rows[0].table_name, 'items');
  } finally {
    await files.cleanup();
  }
});

test('data import rollback preserves rows cleared in the same transaction', { skip }, async () => {
  await resetSchemas();
  await targetPool.query('CREATE TABLE public.items (id integer PRIMARY KEY)');
  await targetPool.query('INSERT INTO public.items VALUES (99)');
  const files = new TempFileManager();
  await files.init();

  try {
    const dump = await files.createFile('broken-data');
    await files.writeFile(dump, [
      'COPY "public"."items" ("id") FROM stdin;',
      '1',
      '\\.',
      'SELECT missing_import_function();',
      '',
    ].join('\n'));
    const sync = new DataSync(config, files, targetPool);
    await assert.rejects(
      sync.importData(dump, 'TRUNCATE TABLE "public"."items";')
    );
    assert.deepEqual((await targetPool.query('SELECT id FROM public.items')).rows, [{ id: 99 }]);
  } finally {
    await files.cleanup();
  }
});

test('auth sync rolls back user and session changes when a user import fails', { skip }, async () => {
  await resetSchemas();
  await resetAuthSchemas("CHECK (email <> 'blocked@example.com')");
  await sourcePool.query(`
    INSERT INTO auth.users VALUES
      ('00000000-0000-0000-0000-000000000001', 'updated@example.com'),
      ('00000000-0000-0000-0000-000000000002', 'blocked@example.com');
  `);
  await targetPool.query(`
    INSERT INTO auth.users VALUES
      ('00000000-0000-0000-0000-000000000001', 'original@example.com');
    INSERT INTO auth.sessions VALUES
      ('00000000-0000-0000-0000-000000000010', '00000000-0000-0000-0000-000000000001');
  `);

  await assert.rejects(
    new AuthSync(authOnlyConfig(), sourcePool, targetPool).sync(),
    /Auth sync failed/
  );

  assert.deepEqual(
    (await targetPool.query('SELECT id, email FROM auth.users ORDER BY id')).rows,
    [{ id: '00000000-0000-0000-0000-000000000001', email: 'original@example.com' }]
  );
  assert.deepEqual(
    (await targetPool.query('SELECT id, user_id FROM auth.sessions ORDER BY id')).rows,
    [{
      id: '00000000-0000-0000-0000-000000000010',
      user_id: '00000000-0000-0000-0000-000000000001',
    }]
  );
});

test('auth-only sync replaces users and clears login sessions', { skip }, async () => {
  await resetSchemas();
  await resetAuthSchemas();
  await sourcePool.query(`
    INSERT INTO auth.users VALUES
      ('00000000-0000-0000-0000-000000000001', 'source@example.com');
  `);
  await targetPool.query(`
    INSERT INTO auth.users VALUES
      ('00000000-0000-0000-0000-000000000001', 'stale@example.com'),
      ('00000000-0000-0000-0000-000000000002', 'target-only@example.com');
    INSERT INTO auth.sessions VALUES
      ('00000000-0000-0000-0000-000000000010', '00000000-0000-0000-0000-000000000001'),
      ('00000000-0000-0000-0000-000000000011', '00000000-0000-0000-0000-000000000002');
  `);

  const result = await new AuthSync(authOnlyConfig(), sourcePool, targetPool).sync();

  assert.deepEqual(result, { usersImported: 1, identitiesImported: 0, errors: [] });
  assert.deepEqual(
    (await targetPool.query('SELECT id, email FROM auth.users ORDER BY id')).rows,
    [{ id: '00000000-0000-0000-0000-000000000001', email: 'source@example.com' }]
  );
  assert.equal(
    (await targetPool.query('SELECT count(*)::int AS count FROM auth.sessions')).rows[0].count,
    0
  );
});

test('auth cleanup removes login state without deleting application references', { skip }, async () => {
  await resetSchemas();
  await targetPool.query(`
    DROP SCHEMA IF EXISTS auth CASCADE;
    CREATE SCHEMA auth;
    CREATE TABLE auth.users (id uuid PRIMARY KEY);
    CREATE TABLE auth.identities (id uuid PRIMARY KEY, user_id uuid REFERENCES auth.users(id));
    CREATE TABLE auth.sessions (id uuid PRIMARY KEY, user_id uuid REFERENCES auth.users(id));
    CREATE TABLE auth.refresh_tokens (id bigint PRIMARY KEY, session_id uuid REFERENCES auth.sessions(id));
    CREATE TABLE public.profiles (user_id uuid REFERENCES auth.users(id));
    INSERT INTO auth.users VALUES ('00000000-0000-0000-0000-000000000001');
    INSERT INTO auth.identities VALUES ('00000000-0000-0000-0000-000000000002', '00000000-0000-0000-0000-000000000001');
    INSERT INTO auth.sessions VALUES ('00000000-0000-0000-0000-000000000003', '00000000-0000-0000-0000-000000000001');
    INSERT INTO auth.refresh_tokens VALUES (1, '00000000-0000-0000-0000-000000000003');
    INSERT INTO public.profiles VALUES ('00000000-0000-0000-0000-000000000001');
  `);

  const client = await targetPool.connect();
  let transactionStarted = false;
  try {
    await client.query('BEGIN');
    transactionStarted = true;
    await client.query('SET LOCAL session_replication_role = replica');
    await (new AuthSync(config, sourcePool, targetPool) as unknown as {
      clearTargetAuth(client: pg.PoolClient): Promise<void>;
    }).clearTargetAuth(client);

    for (const table of ['identities', 'sessions', 'refresh_tokens']) {
      assert.equal((await client.query(`SELECT count(*)::int AS count FROM auth."${table}"`)).rows[0].count, 0);
    }
    assert.equal((await client.query('SELECT count(*)::int AS count FROM auth.users')).rows[0].count, 1);
    assert.equal((await client.query('SELECT count(*)::int AS count FROM public.profiles')).rows[0].count, 1);

    await client.query('SET LOCAL session_replication_role = DEFAULT');
    const sync = new AuthSync(config, sourcePool, targetPool) as unknown as {
      sourceUserIds: string[];
      cleanupTargetOnlyUsers(client: pg.PoolClient): Promise<number>;
    };
    sync.sourceUserIds = [];
    await assert.rejects(sync.cleanupTargetOnlyUsers(client), /still references them/);
    assert.equal((await client.query('SELECT count(*)::int AS count FROM auth.users')).rows[0].count, 1);
    assert.equal((await client.query('SELECT count(*)::int AS count FROM public.profiles')).rows[0].count, 1);

    await client.query('DELETE FROM public.profiles');
    assert.equal(await sync.cleanupTargetOnlyUsers(client), 1);
    assert.equal((await client.query('SELECT count(*)::int AS count FROM auth.users')).rows[0].count, 0);
    await client.query('ROLLBACK');
    transactionStarted = false;
  } finally {
    if (transactionStarted) await client.query('ROLLBACK').catch(() => {});
    client.release();
  }
});
