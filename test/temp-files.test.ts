import assert from 'node:assert/strict';
import { access, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { test } from 'node:test';
import { TempFileManager } from '../src/utils/temp-files.js';

test('cleanup removes only its unique run directory', async () => {
  const parent = await mkdtemp(path.join(tmpdir(), 'supabase-sync-parent-'));
  const sentinel = path.join(parent, 'keep-me.txt');
  await writeFile(sentinel, 'safe');

  try {
    const first = new TempFileManager(parent);
    const second = new TempFileManager(parent);
    await first.init();
    await second.init();
    const firstRun = first.getBasePath();
    const secondRun = second.getBasePath();
    assert.notEqual(firstRun, secondRun);

    await first.createFile('dump');
    await second.createFile('dump');
    await first.cleanup();

    assert.equal(await readFile(sentinel, 'utf8'), 'safe');
    await access(secondRun);
    await assert.rejects(access(firstRun));
    await second.cleanup();
  } finally {
    await rm(parent, { recursive: true, force: true });
  }
});

test('cleanup reports deletion failures instead of claiming success', async () => {
  const parent = await mkdtemp(path.join(tmpdir(), 'supabase-sync-parent-'));
  try {
    const manager = new TempFileManager(parent) as unknown as {
      init(): Promise<void>;
      createFile(prefix: string): Promise<string>;
      cleanup(): Promise<void>;
      secureDelete(path: string): Promise<void>;
    };
    await manager.init();
    await manager.createFile('dump');
    manager.secureDelete = async () => { throw new Error('blocked'); };

    await assert.rejects(manager.cleanup(), /Failed to clean up 1 temporary path/);
  } finally {
    await rm(parent, { recursive: true, force: true });
  }
});
