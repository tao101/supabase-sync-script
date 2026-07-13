import assert from 'node:assert/strict';
import { chmod, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
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
            { schemaname: 'public', tablename: 'schema_migrations' },
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
  assert.deepEqual(truncateQueries, [
    'TRUNCATE TABLE "public"."replace""me", "public"."schema_migrations"',
  ]);
  assert.doesNotMatch(truncateQueries[0], /CASCADE/);
});

test('exports same-named application migration tables', async () => {
  const dir = await mkdtemp(path.join(tmpdir(), 'supabase-sync-data-export-'));
  const script = path.join(dir, 'pg_dump');
  const argsFile = path.join(dir, 'args.txt');
  const originalPath = process.env.PATH || '';
  try {
    await writeFile(script, '#!/bin/sh\nprintf "%s\\n" "$@" > "$DATA_ARGS_FILE"\n');
    await chmod(script, 0o755);
    process.env.PATH = `${dir}:${originalPath}`;
    process.env.DATA_ARGS_FILE = argsFile;
    const sync = new DataSync(baseConfig, {
      async createFile() { return '/tmp/data.sql'; },
    } as never, {} as never);

    await sync.exportData();

    const args = await readFile(argsFile, 'utf8');
    assert.match(args, /--schema=public/);
    assert.doesNotMatch(args, /--exclude-table=\*\.(schema_migrations|migrations)/);
  } finally {
    process.env.PATH = originalPath;
    delete process.env.DATA_ARGS_FILE;
    await rm(dir, { recursive: true, force: true });
  }
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
