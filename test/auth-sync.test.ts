import assert from 'node:assert/strict';
import test from 'node:test';
import { AuthSync } from '../src/sync/auth/auth-sync.js';
import { baseConfig } from './fixture.js';

test('clears auth rows without cascading into application tables', async () => {
  const queries: string[] = [];
  const client = {
    async query(text: string) {
      queries.push(text);
      return text.includes('information_schema.tables')
        ? {
          rows: [
            'refresh_tokens',
            'sessions',
            'oauth_authorizations',
            'oauth_consents',
            'identities',
            'users',
          ].map(table_name => ({ table_name })),
        }
        : { rows: [] };
    },
  };
  const sync = new AuthSync(baseConfig, {} as never, {} as never);

  await (sync as unknown as { clearTargetAuth(client: unknown): Promise<void> })
    .clearTargetAuth(client);

  assert.match(queries[0], /information_schema\.tables/);
  assert.match(queries[0], /pg_constraint/);
  assert.match(queries[0], /'auth\.users'::regclass/);
  assert.deepEqual(queries.slice(1), [
    'DELETE FROM auth."refresh_tokens"',
    'DELETE FROM auth."sessions"',
    'DELETE FROM auth."oauth_authorizations"',
    'DELETE FROM auth."oauth_consents"',
    'DELETE FROM auth."identities"',
  ]);
});

test('refuses to remove target-only users while application rows reference them', async () => {
  const queries: string[] = [];
  const client = {
    async query(text: string) {
      queries.push(text);
      if (text.includes('FROM pg_constraint')) {
        return {
          rows: [{
            constraint_name: 'profiles_user_id_fkey',
            schema_name: 'public',
            table_name: 'profiles',
            column_name: 'user_id',
          }],
        };
      }
      if (text.includes('has_target_only_reference')) {
        return { rows: [{ has_target_only_reference: true }] };
      }
      return { rows: [], rowCount: 0 };
    },
    release() {},
  };
  const sync = new AuthSync(baseConfig, {} as never, {} as never) as unknown as {
    sourceUserIds: string[];
    cleanupTargetOnlyUsers(client: unknown): Promise<number>;
  };
  sync.sourceUserIds = ['00000000-0000-0000-0000-000000000001'];

  await assert.rejects(sync.cleanupTargetOnlyUsers(client), /still references them/);
  assert.equal(queries.some(query => query.startsWith('DELETE FROM auth.users')), false);
});

test('prepares target users to omit hashes and clear target-only unique conflicts', async () => {
  const calls: { text: string; values?: unknown[] }[] = [];
  const client = {
    async query(text: string, values?: unknown[]) {
      calls.push({ text, values });
      return { rows: [], rowCount: 0 };
    },
  };
  const config = structuredClone(baseConfig);
  config.options.auth.preservePasswordHashes = false;
  const sync = new AuthSync(config, {} as never, {} as never) as unknown as {
    prepareTargetUsers(
      users: Record<string, unknown>[],
      commonColumns: string[],
      client: unknown
    ): Promise<void>;
  };

  await sync.prepareTargetUsers(
    [{
      id: '00000000-0000-0000-0000-000000000001',
      email: 'user@example.com',
      phone: '+15555550123',
    }],
    ['id', 'email', 'phone', 'encrypted_password'],
    client
  );

  assert.equal(calls.length, 3);
  assert.match(calls[0].text, /SET encrypted_password = NULL/);
  assert.deepEqual(calls[0].values, [['00000000-0000-0000-0000-000000000001']]);
  assert.match(calls[1].text, /SET "email" = NULL/);
  assert.deepEqual(calls[1].values, [
    ['00000000-0000-0000-0000-000000000001'],
    ['user@example.com'],
  ]);
  assert.match(calls[2].text, /SET "phone" = NULL/);
});
