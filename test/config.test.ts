import assert from 'node:assert/strict';
import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { afterEach, beforeEach, test } from 'node:test';
import type { Config } from '../src/types/config.js';
import { ConfigSchema } from '../src/types/config.js';
import { loadConfig, validateConfig } from '../src/config/index.js';
import { loadCIConfig } from '../src/modes/ci-mode.js';
import { resolveCIMode } from '../src/config/env.js';
import { baseConfig } from './fixture.js';

const envNames = [
  'CI', 'GITHUB_ACTIONS', 'GITLAB_CI', 'JENKINS_URL', 'CIRCLECI', 'TRAVIS',
  'SYNC_MODE', 'SYNC_DRY_RUN', 'SYNC_VERBOSE', 'SYNC_TEMP_DIR',
  'SYNC_SCHEMA', 'SYNC_DATA', 'SYNC_AUTH', 'SYNC_STORAGE', 'SYNC_ROLES',
  'STORAGE_CONCURRENCY', 'STORAGE_MAX_FILE_SIZE_MB', 'STORAGE_EXCLUDE_BUCKETS',
  'DB_INCLUDE_SCHEMAS', 'DB_EXCLUDE_SCHEMAS', 'DB_EXCLUDE_TABLES',
  'SOURCE_DB_URL', 'SOURCE_API_URL', 'SOURCE_SERVICE_ROLE_KEY', 'SOURCE_ANON_KEY',
  'SOURCE_SECRET_KEY', 'SOURCE_PUBLISHABLE_KEY', 'TARGET_DB_URL', 'TARGET_API_URL',
  'TARGET_SERVICE_ROLE_KEY', 'TARGET_ANON_KEY', 'TARGET_SECRET_KEY', 'TARGET_PUBLISHABLE_KEY',
];

const originalEnv = new Map<string, string | undefined>();
const tempDirs: string[] = [];

beforeEach(() => {
  for (const name of envNames) {
    originalEnv.set(name, process.env[name]);
    delete process.env[name];
  }
});

afterEach(async () => {
  for (const [name, value] of originalEnv) {
    if (value === undefined) delete process.env[name];
    else process.env[name] = value;
  }
  originalEnv.clear();
  await Promise.all(tempDirs.splice(0).map(dir => rm(dir, { recursive: true, force: true })));
});

async function writeConfig(config: unknown): Promise<string> {
  const dir = await mkdtemp(path.join(tmpdir(), 'supabase-sync-test-'));
  tempDirs.push(dir);
  const configPath = path.join(dir, 'config.json');
  await writeFile(configPath, JSON.stringify(config));
  return configPath;
}

test('preserves config-file values when matching environment variables are absent', async () => {
  const fileConfig = structuredClone(baseConfig);
  fileConfig.mode = 'ci';
  fileConfig.dryRun = true;
  fileConfig.verbose = true;
  fileConfig.tempDir = '/custom/temp';
  fileConfig.options.components.schema = false;
  fileConfig.options.database.includeSchemas = ['app'];
  fileConfig.options.storage.concurrency = 9;

  const config = await loadConfig({ configPath: await writeConfig(fileConfig) });

  assert.equal(config.mode, 'ci');
  assert.equal(config.dryRun, true);
  assert.equal(config.verbose, true);
  assert.equal(config.tempDir, '/custom/temp');
  assert.equal(config.options.components.schema, false);
  assert.deepEqual(config.options.database.includeSchemas, ['app']);
  assert.equal(config.options.storage.concurrency, 9);
});

test('auto-detects CI only when mode is not configured', async () => {
  process.env.CI = 'true';
  const fileConfig: Partial<Config> = structuredClone(baseConfig);
  delete fileConfig.mode;

  const config = await loadConfig({ configPath: await writeConfig(fileConfig) });

  assert.equal(config.mode, 'ci');
});

test('resolves execution mode by explicit precedence and rejects invalid values', () => {
  process.env.CI = 'true';
  process.env.SYNC_MODE = 'interactive';
  assert.equal(resolveCIMode(undefined), false);
  assert.equal(resolveCIMode(true), true);
  assert.equal(resolveCIMode(undefined, 'ci'), true);

  process.env.SYNC_MODE = 'invalid';
  assert.throws(() => resolveCIMode(undefined), /SYNC_MODE must be ci or interactive/);
});

test('lets an explicit CLI mode override an invalid environment mode', async () => {
  process.env.SYNC_MODE = 'invalid';
  const config = await loadConfig({
    configPath: await writeConfig(baseConfig),
    overrides: { mode: 'ci' },
  });
  assert.equal(config.mode, 'ci');
});

test('does not replace configured booleans with undefined CLI overrides', async () => {
  const fileConfig = structuredClone(baseConfig);
  fileConfig.dryRun = true;
  fileConfig.verbose = true;

  const config = await loadCIConfig({
    configPath: await writeConfig(fileConfig),
    overrides: { dryRun: undefined, verbose: undefined },
  });

  assert.equal(config.dryRun, true);
  assert.equal(config.verbose, true);
});

test('rejects source and target endpoints that identify the same Supabase instance', () => {
  const sameDatabase = structuredClone(baseConfig);
  sameDatabase.target.dbUrl = 'postgresql://other-user:other-password@source.example.com:5432/postgres?sslmode=verify-full';
  assert.match(validateConfig(sameDatabase).join('\n'), /same database/);

  const sameApi = structuredClone(baseConfig);
  sameApi.target.apiUrl = 'https://source.example.com/';
  assert.match(validateConfig(sameApi).join('\n'), /same API/);

  const loopbackAliases = structuredClone(baseConfig);
  loopbackAliases.source.dbUrl = 'postgresql://user@localhost/same';
  loopbackAliases.target.dbUrl = 'postgresql://user@127.0.0.42:5432/same';
  assert.match(validateConfig(loopbackAliases).join('\n'), /same database/);

  const trailingDot = structuredClone(baseConfig);
  trailingDot.source.dbUrl = 'postgresql://user@db.example.com./same';
  trailingDot.target.dbUrl = 'postgresql://user@db.example.com/same';
  assert.match(validateConfig(trailingDot).join('\n'), /same database/);
});

test('rejects managed or empty application schema selections', () => {
  const managed = structuredClone(baseConfig);
  managed.options.database.includeSchemas = ['auth'];
  assert.match(validateConfig(managed).join('\n'), /Supabase-managed schemas/);
  assert.match(validateConfig(managed).join('\n'), /At least one application schema/);

  const empty = structuredClone(baseConfig);
  empty.options.database.includeSchemas = [];
  assert.match(validateConfig(empty).join('\n'), /At least one application schema/);
});

test('rejects configurations with no enabled sync components', () => {
  const config = structuredClone(baseConfig);
  for (const component of Object.keys(config.options.components) as Array<keyof typeof config.options.components>) {
    config.options.components[component] = false;
  }
  assert.match(validateConfig(config).join('\n'), /At least one sync component/);
});

test('rejects unknown config keys and invalid storage limits', () => {
  const typo = structuredClone(baseConfig) as Config & { dryrun: boolean };
  typo.dryrun = true;
  assert.equal(ConfigSchema.safeParse(typo).success, false);

  for (const concurrency of [0, -1, 1.5]) {
    const config = structuredClone(baseConfig);
    config.options.storage.concurrency = concurrency;
    assert.equal(ConfigSchema.safeParse(config).success, false);
  }

  const config = structuredClone(baseConfig);
  config.options.storage.maxFileSizeMB = 0;
  assert.equal(ConfigSchema.safeParse(config).success, false);
});

test('loads the previous published template metadata without relaxing strict validation', async () => {
  const legacyTemplate = {
    ...structuredClone(baseConfig),
    _comment: 'Use EITHER legacy keys (serviceRoleKey) OR new keys (secretKey), not both',
    source: {
      ...baseConfig.source,
      _keys_option_a: 'Legacy JWT keys',
      _keys_option_b: 'New API keys',
      _secretKey: 'sb_secret_...',
    },
    target: {
      ...baseConfig.target,
      _keys_option_a: 'Legacy JWT keys',
      _keys_option_b: 'New API keys',
      _secretKey: 'sb_secret_...',
    },
  };

  const config = await loadConfig({ configPath: await writeConfig(legacyTemplate) });
  assert.equal(config.source.apiUrl, baseConfig.source.apiUrl);
  assert.equal(config.target.apiUrl, baseConfig.target.apiUrl);

  await assert.rejects(
    loadConfig({
      configPath: await writeConfig({ ...legacyTemplate, _commment: 'typo' }),
    }),
    /Unrecognized key.*_commment/
  );
});

test('database-only configs do not require unused Supabase API credentials', () => {
  const config = structuredClone(baseConfig) as unknown as {
    source: Partial<Config['source']>;
    target: Partial<Config['target']>;
    options: Config['options'];
  };
  config.options.components.storage = false;
  delete config.source.apiUrl;
  delete config.source.serviceRoleKey;
  delete config.target.apiUrl;
  delete config.target.serviceRoleKey;

  const parsed = ConfigSchema.parse(config);
  assert.equal(parsed.source.apiUrl, '');
  assert.equal(parsed.target.apiUrl, '');
  assert.deepEqual(validateConfig(parsed), []);
});

test('rejects excluded-table preservation when schema reset is enabled', () => {
  const config = structuredClone(baseConfig);
  config.options.database.excludeTables = ['public.audit_log'];
  assert.match(validateConfig(config).join('\n'), /requires schema sync to be disabled/);

  config.options.components.data = false;
  assert.match(validateConfig(config).join('\n'), /requires schema sync to be disabled/);

  config.options.components.schema = false;
  assert.doesNotMatch(validateConfig(config).join('\n'), /excludeTables/);
});

test('rejects fractional and malformed numeric environment settings', async () => {
  process.env.STORAGE_CONCURRENCY = '1.5';
  await assert.rejects(loadConfig({ configPath: await writeConfig(baseConfig) }));

  process.env.STORAGE_CONCURRENCY = 'not-a-number';
  await assert.rejects(loadConfig({ configPath: await writeConfig(baseConfig) }));
});

test('rejects malformed boolean environment settings', async () => {
  process.env.SYNC_DRY_RUN = 'treu';
  await assert.rejects(
    loadConfig({ configPath: await writeConfig(baseConfig) }),
    /SYNC_DRY_RUN must be true, false, 1, or 0/
  );
});

test('reports a missing explicitly requested config file', async () => {
  await assert.rejects(
    loadConfig({ configPath: path.join(tmpdir(), 'missing-supabase-sync-config.json') }),
    /Config file not found/
  );
});

test('ships a config template that passes structural and semantic validation', async () => {
  const template = JSON.parse(
    await readFile(new URL('../templates/config.example.json', import.meta.url), 'utf8')
  );
  const config = ConfigSchema.parse(template);
  assert.deepEqual(validateConfig(config), []);
});
