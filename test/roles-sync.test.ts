import assert from 'node:assert/strict';
import { chmod, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { RolesSync } from '../src/sync/database/roles-sync.js';
import { SyncError } from '../src/types/sync.js';
import { baseConfig } from './fixture.js';

test('removes system-role statements without swallowing the next application role', async () => {
  const dump = [
    'CREATE ROLE postgres;',
    'ALTER ROLE postgres WITH SUPERUSER;',
    '',
    'CREATE ROLE app_user;',
    'ALTER ROLE app_user WITH LOGIN;',
    'CREATE ROLE reporting;',
    '',
  ].join('\n');
  let written = '';
  const files = {
    async readFile() { return dump; },
    async createFile() { return '/tmp/roles-filtered.sql'; },
    async writeFile(_path: string, content: string) { written = content; },
  };
  const sync = new RolesSync(baseConfig, files as never);

  await sync.filterRoles('/tmp/roles.sql');

  assert.doesNotMatch(written, /ROLE postgres/);
  assert.match(written, /CREATE ROLE app_user;/);
  assert.match(written, /ALTER ROLE app_user WITH LOGIN;/);
  assert.match(written, /CREATE ROLE reporting;/);
});

test('fails role import when psql fails', async () => {
  const dir = await mkdtemp(path.join(tmpdir(), 'supabase-sync-role-'));
  const script = path.join(dir, 'psql');
  const argsFile = path.join(dir, 'args.txt');
  const originalPath = process.env.PATH || '';
  try {
    await writeFile(script, '#!/bin/sh\nprintf "%s\\n" "$@" > "$ROLE_ARGS_FILE"\nexit 2\n');
    await chmod(script, 0o755);
    process.env.PATH = `${dir}:${originalPath}`;
    process.env.ROLE_ARGS_FILE = argsFile;
    const sync = new RolesSync(baseConfig, {} as never);

    await assert.rejects(sync.importRoles('/tmp/roles.sql'), SyncError);
    assert.match(await readFile(argsFile, 'utf8'), /--single-transaction/);
  } finally {
    process.env.PATH = originalPath;
    delete process.env.ROLE_ARGS_FILE;
    await rm(dir, { recursive: true, force: true });
  }
});
