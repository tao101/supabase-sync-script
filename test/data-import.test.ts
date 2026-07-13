import assert from 'node:assert/strict';
import { chmod, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { afterEach, beforeEach, test } from 'node:test';
import { DataSync } from '../src/sync/database/data-sync.js';
import { ErrorCategory, SyncError } from '../src/types/sync.js';
import { TempFileManager } from '../src/utils/temp-files.js';
import { baseConfig } from './fixture.js';

let root = '';
let originalPath = '';

beforeEach(async () => {
  root = await mkdtemp(path.join(tmpdir(), 'supabase-sync-psql-'));
  originalPath = process.env.PATH || '';
  const script = path.join(root, 'psql');
  await writeFile(script, [
    '#!/bin/sh',
    'printf "%s\\n" "$@" > "$PSQL_ARGS_FILE"',
    'exit "${PSQL_EXIT_CODE:-0}"',
    '',
  ].join('\n'));
  await chmod(script, 0o755);
  process.env.PATH = `${root}:${originalPath}`;
  process.env.PSQL_ARGS_FILE = path.join(root, 'args.txt');
});

afterEach(async () => {
  process.env.PATH = originalPath;
  delete process.env.PSQL_ARGS_FILE;
  delete process.env.PSQL_EXIT_CODE;
  await rm(root, { recursive: true, force: true });
});

async function createSync(): Promise<{ sync: DataSync; dumpFile: string; files: TempFileManager }> {
  const files = new TempFileManager(root);
  await files.init();
  const dumpFile = await files.createFile('data-dump');
  await files.writeFile(dumpFile, 'COPY "public"."items" ("id") FROM stdin;\n1\n\\.\n');
  return {
    sync: new DataSync(baseConfig, files, {} as never),
    dumpFile,
    files,
  };
}

test('runs target clearing and data import in one error-stopping transaction', async () => {
  const { sync, dumpFile, files } = await createSync();
  try {
    await sync.importData(dumpFile, 'TRUNCATE TABLE "public"."items";');
    const rawArgs = await readFile(process.env.PSQL_ARGS_FILE!, 'utf8');
    assert.doesNotMatch(rawArgs, /target:password/);
    const args = rawArgs.trim().split('\n');
    assert.ok(args.includes('-X'));
    assert.ok(args.includes('--single-transaction'));
    assert.equal(args[args.indexOf('-v') + 1], 'ON_ERROR_STOP=1');
    assert.match(args[args.indexOf('-c') + 1], /SET LOCAL session_replication_role = replica/);
    assert.match(rawArgs, /TRUNCATE TABLE "public"\."items"/);
    assert.ok(args.indexOf('-c') < args.indexOf('-f'));
  } finally {
    await files.cleanup();
  }
});

test('fails the import when psql exits nonzero', async () => {
  process.env.PSQL_EXIT_CODE = '2';
  const { sync, dumpFile, files } = await createSync();
  try {
    await assert.rejects(
      sync.importData(dumpFile, 'TRUNCATE TABLE "public"."items";'),
      (error: unknown) => error instanceof SyncError && error.category === ErrorCategory.IMPORT
    );
  } finally {
    await files.cleanup();
  }
});
