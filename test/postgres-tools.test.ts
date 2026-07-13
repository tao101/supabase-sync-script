import assert from 'node:assert/strict';
import { chmod, mkdtemp, readFile, readdir, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { afterEach, test } from 'node:test';
import { testPostgresTools } from '../src/utils/postgres-tools.js';
import { baseConfig } from './fixture.js';

const originalPath = process.env.PATH;
const originalRecordDir = process.env.TOOL_RECORD_DIR;
const originalExitCode = process.env.PSQL_EXIT_CODE;
const tempDirs: string[] = [];

afterEach(async () => {
  process.env.PATH = originalPath;
  if (originalRecordDir === undefined) delete process.env.TOOL_RECORD_DIR;
  else process.env.TOOL_RECORD_DIR = originalRecordDir;
  if (originalExitCode === undefined) delete process.env.PSQL_EXIT_CODE;
  else process.env.PSQL_EXIT_CODE = originalExitCode;
  await Promise.all(tempDirs.splice(0).map(dir => rm(dir, { recursive: true, force: true })));
});

async function installFakeTools(version: number): Promise<string> {
  const dir = await mkdtemp(path.join(tmpdir(), 'supabase-sync-tools-'));
  tempDirs.push(dir);
  for (const tool of ['psql', 'pg_dump', 'pg_dumpall']) {
    const file = path.join(dir, tool);
    await writeFile(file, [
      '#!/bin/sh',
      `{ printf 'PGPASSWORD=%s\\n' "\${PGPASSWORD-}"; for arg in "$@"; do printf 'ARG=%s\\n' "$arg"; done; } > "$TOOL_RECORD_DIR/${tool}-$$"`,
      `if [ "$1" = "--version" ]; then echo "${tool} (PostgreSQL) ${version}.0"; exit 0; fi`,
      `if [ "${tool}" = "psql" ] && [ "\${PSQL_EXIT_CODE:-0}" -ne 0 ]; then printf 'failed PGPASSWORD=%s postgresql://user:%s@db.example.com/postgres?password=%s\\n' "$PGPASSWORD" "$PGPASSWORD" "$PGPASSWORD" >&2; fi`,
      'exit "${PSQL_EXIT_CODE:-0}"',
      '',
    ].join('\n'));
    await chmod(file, 0o755);
  }
  process.env.TOOL_RECORD_DIR = dir;
  process.env.PATH = `${dir}:${originalPath || ''}`;
  return dir;
}

async function readToolCalls(dir: string, tool: string): Promise<Array<{ password: string; args: string[] }>> {
  const files = (await readdir(dir)).filter(file => file.startsWith(`${tool}-`));
  return Promise.all(files.map(async file => {
    const [passwordLine, ...argLines] = (await readFile(path.join(dir, file), 'utf8')).trim().split('\n');
    return {
      password: passwordLine.slice('PGPASSWORD='.length),
      args: argLines.map(line => line.slice('ARG='.length)),
    };
  }));
}

function configWithDistinctPasswords() {
  const config = structuredClone(baseConfig);
  config.source.dbUrl = 'postgresql://source:source-secret@source.example.com/postgres';
  config.target.dbUrl = 'postgresql://target:target-secret@target.example.com/postgres';
  return config;
}

test('accepts PostgreSQL 16+ tools and libpq connection checks', async () => {
  const dir = await installFakeTools(17);
  assert.deepEqual(await testPostgresTools(configWithDistinctPasswords()), { success: true });

  for (const tool of ['pg_dump', 'pg_dumpall']) {
    assert.deepEqual((await readToolCalls(dir, tool)).map(call => call.args), [['--version']]);
  }

  const psqlCalls = await readToolCalls(dir, 'psql');
  assert.deepEqual(psqlCalls.filter(call => call.args[0] === '--version').map(call => call.args), [['--version']]);
  const calls = psqlCalls.filter(call => call.args[0] !== '--version');
  assert.equal(calls.length, 2);
  const expectedPasswords = new Map([
    ['source', 'source-secret'],
    ['target', 'target-secret'],
  ]);
  const users = new Set<string>();
  for (const call of calls) {
    const url = new URL(call.args[0]);
    users.add(url.username);
    assert.equal(url.password, '');
    assert.equal(url.searchParams.has('password'), false);
    assert.equal(url.searchParams.get('connect_timeout'), '10');
    assert.equal(url.searchParams.get('sslmode'), 'verify-full');
    assert.equal(url.searchParams.get('sslrootcert'), 'system');
    assert.deepEqual(call.args.slice(1), ['-X', '-w', '-v', 'ON_ERROR_STOP=1', '-c', 'SELECT 1']);
    assert.equal(call.password, expectedPasswords.get(url.username));
  }
  assert.deepEqual([...users].sort(), ['source', 'target']);
});

test('rejects old PostgreSQL client tools', async () => {
  await installFakeTools(15);
  const result = await testPostgresTools(baseConfig);
  assert.equal(result.success, false);
  assert.match(result.error || '', /16 or newer/);
});

test('sanitizes nonzero psql connection failures', async () => {
  await installFakeTools(17);
  process.env.PSQL_EXIT_CODE = '2';

  const result = await testPostgresTools(configWithDistinctPasswords());

  assert.equal(result.success, false);
  assert.match(result.error || '', /psql could not connect to (source|target)/);
  assert.match(result.error || '', /REDACTED/);
  assert.doesNotMatch(result.error || '', /source-secret|target-secret/);
});
