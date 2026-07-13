import assert from 'node:assert/strict';
import test from 'node:test';
import { StorageSync } from '../src/sync/storage/storage-sync.js';
import type { StorageBucket, StorageFile } from '../src/types/sync.js';
import { baseConfig } from './fixture.js';

const bucket: StorageBucket = {
  id: 'avatars',
  name: 'avatars',
  public: true,
  file_size_limit: null,
  allowed_mime_types: null,
  created_at: '',
  updated_at: '',
};

test('fails oversized files before attempting a download', async () => {
  const config = structuredClone(baseConfig);
  config.options.storage.maxFileSizeMB = 50;
  const files: StorageFile[] = [{
    name: 'too-large.bin',
    id: '1',
    bucket_id: 'avatars',
    metadata: {},
    size: 51 * 1024 * 1024,
  }];
  const sync = new StorageSync(config, {} as never, {} as never);
  let downloads = 0;
  sync.createBucket = async () => {};
  sync.listAllFiles = async () => files;
  sync.syncFile = async () => { downloads++; };

  await assert.rejects(sync.syncBucket(bucket), /maxFileSizeMB/);
  assert.equal(downloads, 0);
});

test('preflights every bucket before creating or downloading anything', async () => {
  const config = structuredClone(baseConfig);
  config.options.storage.maxFileSizeMB = 50;
  const sync = new StorageSync(config, {} as never, {} as never);
  let creations = 0;
  let downloads = 0;
  sync.listBuckets = async () => [
    { ...bucket, id: 'first', name: 'first' },
    { ...bucket, id: 'second', name: 'second' },
  ];
  sync.listAllFiles = async bucketName => [{
    name: `${bucketName}.bin`,
    id: bucketName,
    bucket_id: bucketName,
    metadata: {},
    size: bucketName === 'first' ? 1 : 51 * 1024 * 1024,
  }];
  sync.createBucket = async () => { creations++; };
  sync.syncFile = async () => { downloads++; };

  await assert.rejects(sync.sync(), /maxFileSizeMB/);
  assert.equal(creations, 0);
  assert.equal(downloads, 0);
});

test('rejects files whose size metadata cannot enforce the configured limit', async () => {
  const sync = new StorageSync(baseConfig, {} as never, {} as never);
  sync.createBucket = async () => {};
  sync.listAllFiles = async () => [{
    name: 'unknown.bin',
    id: '1',
    bucket_id: 'avatars',
    metadata: {},
    size: Number.NaN,
  }];

  await assert.rejects(sync.syncBucket(bucket), /size metadata/);
});

test('bounds upload workers and reports per-file failures', async () => {
  const config = structuredClone(baseConfig);
  config.options.storage.concurrency = 2;
  const files: StorageFile[] = Array.from({ length: 5 }, (_, index) => ({
    name: `file-${index}`,
    id: String(index),
    bucket_id: bucket.name,
    metadata: {},
    size: 1,
  }));
  const sync = new StorageSync(config, {} as never, {} as never);
  let active = 0;
  let maxActive = 0;
  sync.createBucket = async () => {};
  sync.syncFile = async (_bucket, file) => {
    active++;
    maxActive = Math.max(maxActive, active);
    await new Promise(resolve => setImmediate(resolve));
    active--;
    if (file === 'file-3') throw new Error('failed');
  };

  const result = await sync.syncBucket(bucket, files);

  assert.equal(maxActive, 2);
  assert.deepEqual(result, { bucket: 'avatars', total: 5, uploaded: 4, failed: 1 });
});

test('rewrites only public Storage object URL prefixes', async () => {
  const calls: { text: string; values?: unknown[] }[] = [];
  const client = {
    async query(text: string, values?: unknown[]) {
      calls.push({ text, values });
      if (text.includes('information_schema.columns')) return { rows: [] };
      return { rows: [], rowCount: 0 };
    },
    release() {},
  };
  const pool = { async connect() { return client; } };
  const sync = new StorageSync(baseConfig, {} as never, {} as never, pool as never);

  await sync.rewriteStorageUrls();

  const rewriteCall = calls.find(call => call.values?.length === 2);
  const columnsCall = calls.find(call => call.text.includes('information_schema.columns'));
  assert.deepEqual(rewriteCall?.values, [
    'https://source.example.com/storage/v1/object/public/',
    'https://target.example.com/storage/v1/object/public/',
  ]);
  assert.match(rewriteCall!.text, /left\(/);
  assert.doesNotMatch(rewriteCall!.text, /LIKE/);
  assert.match(columnsCall!.text, /table_type = 'BASE TABLE'/);
  assert.match(columnsCall!.text, /is_generated = 'NEVER'/);
});
