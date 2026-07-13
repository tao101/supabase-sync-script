import assert from 'node:assert/strict';
import { execFile } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { promisify } from 'node:util';
import test from 'node:test';

const execFileAsync = promisify(execFile);
const projectRoot = fileURLToPath(new URL('..', import.meta.url));
const tsx = fileURLToPath(new URL('../node_modules/.bin/tsx', import.meta.url));

test('CI prints a sanitized effective plan before connection attempts', async () => {
  await assert.rejects(
    execFileAsync(tsx, ['src/cli.ts', 'sync', '--ci', '--dry-run'], {
      cwd: projectRoot,
      timeout: 10_000,
      env: {
        ...process.env,
        CI: 'false',
        GITHUB_ACTIONS: 'false',
        SOURCE_DB_URL: 'postgresql://user:source-secret@127.0.0.1:1/source',
        TARGET_DB_URL: 'postgresql://user:target-secret@127.0.0.1:1/target',
        SYNC_SCHEMA: 'false',
        SYNC_DATA: 'false',
        SYNC_AUTH: 'true',
        SYNC_STORAGE: 'false',
        SYNC_ROLES: 'false',
      },
    }),
    error => {
      const failure = error as Error & { code: number; stdout: string; stderr: string };
      const output = `${failure.stdout}\n${failure.stderr}`;
      assert.equal(failure.code, 1);
      assert.match(failure.stdout, /Plan: DRY RUN/);
      assert.match(failure.stdout, /Target database: postgresql:\/\/user@127\.0\.0\.1:1\/target/);
      assert.match(failure.stdout, /Components: auth/);
      assert.doesNotMatch(output, /source-secret|target-secret/);
      return true;
    }
  );
});
