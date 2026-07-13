import assert from 'node:assert/strict';
import { chmod, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { SchemaSync } from '../src/sync/database/schema-sync.js';
import { ErrorCategory, SyncError } from '../src/types/sync.js';
import { TempFileManager } from '../src/utils/temp-files.js';
import { baseConfig } from './fixture.js';

test('fails schema import when psql exits nonzero', async () => {
  const dir = await mkdtemp(path.join(tmpdir(), 'supabase-sync-schema-'));
  const script = path.join(dir, 'psql');
  const originalPath = process.env.PATH || '';
  const files = new TempFileManager(dir);
  try {
    await writeFile(script, '#!/bin/sh\nexit 2\n');
    await chmod(script, 0o755);
    process.env.PATH = `${dir}:${originalPath}`;
    await files.init();
    const dumpFile = await files.createFile('schema-dump');
    await files.writeFile(dumpFile, 'CREATE TABLE "public"."items" ("id" integer);\n');
    const sync = new SchemaSync(baseConfig, files, {} as never);

    await assert.rejects(
      sync.importSchema(dumpFile),
      (error: unknown) => error instanceof SyncError && error.category === ErrorCategory.IMPORT
    );
  } finally {
    process.env.PATH = originalPath;
    await files.cleanup();
    await rm(dir, { recursive: true, force: true });
  }
});

test('prepares schema reset and privilege SQL without mutating the target', async () => {
  const queries: string[] = [];
  const client = {
    async query(text: string) {
      queries.push(text);
      if (text.includes('FROM pg_extension')) return { rows: [] };
      if (text.includes('dependency_schemas')) return { rows: [] };
      if (text.includes('JOIN pg_roles r')) return { rows: [{ owner: 'app_owner' }] };
      return { rows: [] };
    },
    release() {},
  };
  const sync = new SchemaSync(baseConfig, {} as never, {
    async connect() { return client; },
  } as never);

  const prepared = await sync.prepareTargetSchemas();

  assert.match(prepared.resetSql, /DROP SCHEMA IF EXISTS "public" CASCADE;/);
  assert.match(prepared.resetSql, /CREATE SCHEMA "public";/);
  assert.match(prepared.finalizeSql, /ALTER SCHEMA "public" OWNER TO "app_owner";/);
  assert.equal(queries.some(query => /^\s*(DROP|CREATE|BEGIN|COMMIT)/.test(query)), false);
});

test('runs reset, dump, and finalization files in one schema transaction', async () => {
  const dir = await mkdtemp(path.join(tmpdir(), 'supabase-sync-schema-args-'));
  const script = path.join(dir, 'psql');
  const argsFile = path.join(dir, 'args.txt');
  const originalPath = process.env.PATH || '';
  const files = new TempFileManager(dir);
  try {
    await writeFile(script, '#!/bin/sh\nprintf "%s\\n" "$@" > "$SCHEMA_ARGS_FILE"\n');
    await chmod(script, 0o755);
    process.env.PATH = `${dir}:${originalPath}`;
    process.env.SCHEMA_ARGS_FILE = argsFile;
    await files.init();
    const dumpFile = await files.createFile('schema-dump');
    await files.writeFile(dumpFile, 'CREATE TABLE "public"."items" ("id" integer);\n');
    const sync = new SchemaSync(baseConfig, files, {} as never);

    await sync.importSchema(
      dumpFile,
      'DROP SCHEMA IF EXISTS "public" CASCADE; CREATE SCHEMA "public";',
      'ALTER SCHEMA "public" OWNER TO "app_owner";'
    );

    const args = (await readFile(argsFile, 'utf8')).trim().split('\n');
    assert.ok(args.includes('--single-transaction'));
    assert.equal(args.filter(arg => arg === '-f').length, 3);
  } finally {
    process.env.PATH = originalPath;
    delete process.env.SCHEMA_ARGS_FILE;
    await files.cleanup();
    await rm(dir, { recursive: true, force: true });
  }
});
