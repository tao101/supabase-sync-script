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
        ? { rows: ['refresh_tokens', 'sessions', 'identities', 'users'].map(table_name => ({ table_name })) }
        : { rows: [] };
    },
  };
  const sync = new AuthSync(baseConfig, {} as never, {} as never);

  await (sync as unknown as { clearTargetAuth(client: unknown): Promise<void> })
    .clearTargetAuth(client);

  assert.match(queries[0], /information_schema\.tables/);
  assert.deepEqual(queries.slice(1), [
    'DELETE FROM auth."refresh_tokens"',
    'DELETE FROM auth."sessions"',
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
