import assert from 'node:assert/strict';
import test from 'node:test';
import { SyncOrchestrator } from '../src/core/sync-orchestrator.js';
import { ErrorCategory, SyncError } from '../src/types/sync.js';
import { baseConfig } from './fixture.js';

test('preserves structured sync errors in the public result', () => {
  const orchestrator = new SyncOrchestrator(baseConfig);
  const error = new SyncError('upload failed', ErrorCategory.STORAGE, 'storage-sync', false);
  const result = (orchestrator as unknown as {
    buildResult(success: boolean, error: Error): { errors: SyncError[] };
  }).buildResult(false, error);

  assert.equal(result.errors[0], error);
  assert.equal(result.errors[0].category, ErrorCategory.STORAGE);
  assert.equal(result.errors[0].step, 'storage-sync');
});

test('marks dry-run mutation steps as planned', async () => {
  const config = structuredClone(baseConfig);
  config.dryRun = true;
  const orchestrator = new SyncOrchestrator(config) as unknown as {
    stepResults: Array<{ status?: string }>;
    runStep(name: string, fn: () => Promise<void>): Promise<void>;
  };

  await orchestrator.runStep('sync-data', async () => {});

  assert.equal(orchestrator.stepResults[0].status, 'planned');
});

test('cleanup closes each pool at most once and does not mask close failures', async () => {
  const orchestrator = new SyncOrchestrator(baseConfig) as unknown as {
    sourcePool: { end(): Promise<void> } | null;
    targetPool: { end(): Promise<void> } | null;
    tempFileManager: { cleanup(): Promise<void> };
    stepResults: Array<{ status?: string }>;
    runStep(name: string, fn: () => Promise<void>): Promise<void>;
    cleanup(): Promise<void>;
  };
  let sourceEnds = 0;
  let targetEnds = 0;
  let tempCleanups = 0;
  orchestrator.sourcePool = { async end() { sourceEnds++; } };
  orchestrator.targetPool = { async end() { targetEnds++; throw new Error('close failed'); } };
  orchestrator.tempFileManager = { async cleanup() { tempCleanups++; } };

  await orchestrator.runStep('cleanup', () => orchestrator.cleanup());
  await orchestrator.cleanup();

  assert.equal(sourceEnds, 1);
  assert.equal(targetEnds, 1);
  assert.equal(tempCleanups, 2);
  assert.equal(orchestrator.stepResults[0].status, 'warning');
});

test('treats temporary-file cleanup failure as fatal', async () => {
  const orchestrator = new SyncOrchestrator(baseConfig) as unknown as {
    tempFileManager: { cleanup(): Promise<void> };
    cleanup(): Promise<void>;
  };
  orchestrator.tempFileManager = {
    async cleanup() { throw new Error('delete failed'); },
  };

  await assert.rejects(orchestrator.cleanup(), (error: SyncError) => {
    assert.equal(error.step, 'cleanup');
    return true;
  });
});

test('returns a failed result when final temporary-file cleanup fails', async () => {
  const config = structuredClone(baseConfig);
  config.options.components = {
    schema: false,
    data: false,
    auth: true,
    storage: false,
    roles: false,
  };
  const orchestrator = new SyncOrchestrator(config) as unknown as {
    tempFileManager: { init(): Promise<void>; cleanup(): Promise<void> };
    validateConnections(): Promise<void>;
    syncAuth(): Promise<void>;
    execute(): Promise<{ success: boolean; errors: SyncError[] }>;
  };
  orchestrator.tempFileManager = {
    async init() {},
    async cleanup() { throw new Error('delete failed'); },
  };
  orchestrator.validateConnections = async () => {};
  orchestrator.syncAuth = async () => {};

  const result = await orchestrator.execute();

  assert.equal(result.success, false);
  assert.equal(result.errors[0].step, 'cleanup');
});

test('rejects invalid programmatic configs before initializing resources', async () => {
  const config = structuredClone(baseConfig);
  config.target.dbUrl = config.source.dbUrl;
  const orchestrator = new SyncOrchestrator(config) as unknown as {
    tempFileManager: { init(): Promise<void>; cleanup(): Promise<void> };
    execute(): Promise<{ success: boolean; errors: SyncError[] }>;
  };
  let initialized = false;
  orchestrator.tempFileManager = {
    async init() { initialized = true; },
    async cleanup() {},
  };

  const result = await orchestrator.execute();

  assert.equal(initialized, false);
  assert.equal(result.success, false);
  assert.equal(result.errors[0].category, ErrorCategory.VALIDATION);
  assert.equal(result.errors[0].step, 'validate-config');
});
