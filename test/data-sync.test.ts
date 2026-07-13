import assert from 'node:assert/strict';
import test from 'node:test';
import { DataSync } from '../src/sync/database/data-sync.js';
import { baseConfig } from './fixture.js';

test('clears only included target tables without cascading into excluded tables', async () => {
  const config = structuredClone(baseConfig);
  config.options.database.excludeTables = ['public.keep_me'];
  const queries: string[] = [];
  const client = {
    async query(text: string) {
      queries.push(text);
      if (text.includes('FROM pg_tables')) {
        return {
          rows: [
            { schemaname: 'public', tablename: 'replace"me' },
            { schemaname: 'public', tablename: 'keep_me' },
          ],
        };
      }
      return { rows: [] };
    },
    release() {},
  };
  const pool = { async connect() { return client; } };
  const sync = new DataSync(config, {} as never, pool as never);

  await sync.clearTargetData();

  const truncateQueries = queries.filter(query => query.startsWith('TRUNCATE TABLE'));
  assert.deepEqual(truncateQueries, ['TRUNCATE TABLE "public"."replace""me"']);
  assert.doesNotMatch(truncateQueries[0], /CASCADE/);
});

test('checks partial-null MATCH FULL foreign keys as violations', async () => {
  let orphanQuery = '';
  const client = {
    async query(text: string) {
      if (text.includes('FROM pg_constraint')) {
        return { rows: [{
          constraint_name: 'items_parent_fkey',
          child_schema: 'public',
          child_table: 'items',
          child_columns: ['parent_a', 'parent_b'],
          parent_schema: 'public',
          parent_table: 'parents',
          parent_columns: ['id_a', 'id_b'],
          match_type: 'f',
        }] };
      }
      orphanQuery = text;
      return { rows: [{ orphan_count: '1' }] };
    },
    release() {},
  };
  const sync = new DataSync(baseConfig, {} as never, {
    async connect() { return client; },
  } as never);

  assert.equal(await sync.verifyForeignKeys(), false);
  assert.match(orphanQuery, /"parent_a" IS NULL/);
  assert.match(orphanQuery, /"parent_b" IS NULL/);
});
