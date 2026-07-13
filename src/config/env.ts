import { config as dotenvConfig } from 'dotenv';
import type { Config, SupabaseConnection } from '../types/config.js';

// Load .env file
dotenvConfig();

function getEnvVar(name: string): string | undefined {
  return process.env[name];
}

function getEnvVarAsNumber(name: string): number | undefined {
  const value = process.env[name];
  if (!value) return undefined;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : Number.NaN;
}

function getEnvVarAsBoolean(name: string): boolean | undefined {
  const value = process.env[name];
  if (!value) return undefined;
  if (value.toLowerCase() === 'true' || value === '1') return true;
  if (value.toLowerCase() === 'false' || value === '0') return false;
  throw new Error(`Environment variable ${name} must be true, false, 1, or 0`);
}

export function loadSourceFromEnv(): Partial<SupabaseConnection> {
  return {
    dbUrl: getEnvVar('SOURCE_DB_URL'),
    apiUrl: getEnvVar('SOURCE_API_URL'),
    // Legacy keys (JWT format)
    serviceRoleKey: getEnvVar('SOURCE_SERVICE_ROLE_KEY'),
    anonKey: getEnvVar('SOURCE_ANON_KEY'),
    // New keys (sb_secret_/sb_publishable_ format)
    secretKey: getEnvVar('SOURCE_SECRET_KEY'),
    publishableKey: getEnvVar('SOURCE_PUBLISHABLE_KEY'),
  };
}

export function loadTargetFromEnv(): Partial<SupabaseConnection> {
  return {
    dbUrl: getEnvVar('TARGET_DB_URL'),
    apiUrl: getEnvVar('TARGET_API_URL'),
    // Legacy keys (JWT format)
    serviceRoleKey: getEnvVar('TARGET_SERVICE_ROLE_KEY'),
    anonKey: getEnvVar('TARGET_ANON_KEY'),
    // New keys (sb_secret_/sb_publishable_ format)
    secretKey: getEnvVar('TARGET_SECRET_KEY'),
    publishableKey: getEnvVar('TARGET_PUBLISHABLE_KEY'),
  };
}

export function loadOptionsFromEnv(): Partial<Config['options']> {
  return {
    components: {
      schema: getEnvVarAsBoolean('SYNC_SCHEMA'),
      data: getEnvVarAsBoolean('SYNC_DATA'),
      auth: getEnvVarAsBoolean('SYNC_AUTH'),
      storage: getEnvVarAsBoolean('SYNC_STORAGE'),
      roles: getEnvVarAsBoolean('SYNC_ROLES'),
    },
    storage: {
      concurrency: getEnvVarAsNumber('STORAGE_CONCURRENCY'),
      maxFileSizeMB: getEnvVarAsNumber('STORAGE_MAX_FILE_SIZE_MB'),
      excludeBuckets: getEnvVar('STORAGE_EXCLUDE_BUCKETS')?.split(',').filter(Boolean),
    },
    database: {
      includeSchemas: getEnvVar('DB_INCLUDE_SCHEMAS')?.split(',').filter(Boolean),
      excludeTables: getEnvVar('DB_EXCLUDE_TABLES')?.split(',').filter(Boolean),
      excludeSchemas: getEnvVar('DB_EXCLUDE_SCHEMAS')?.split(',').filter(Boolean),
    },
  } as Partial<Config['options']>;
}

export function loadConfigFromEnv(): Partial<Config> {
  return {
    mode: getEnvVar('SYNC_MODE') as 'ci' | 'interactive' | undefined,
    dryRun: getEnvVarAsBoolean('SYNC_DRY_RUN'),
    verbose: getEnvVarAsBoolean('SYNC_VERBOSE'),
    tempDir: getEnvVar('SYNC_TEMP_DIR'),
  };
}

export function detectCIEnvironment(): boolean {
  const ciEnvVars = ['CI', 'GITHUB_ACTIONS', 'GITLAB_CI', 'JENKINS_URL', 'CIRCLECI', 'TRAVIS'];
  return ciEnvVars.some(v => {
    const value = process.env[v]?.toLowerCase();
    return value !== undefined && value !== '' && value !== 'false' && value !== '0';
  });
}

export function resolveCIMode(
  cliFlag: boolean | undefined,
  configuredMode?: 'ci' | 'interactive'
): boolean {
  if (cliFlag) return true;
  if (configuredMode) return configuredMode === 'ci';

  const envMode = process.env.SYNC_MODE?.trim().toLowerCase();
  if (envMode && envMode !== 'ci' && envMode !== 'interactive') {
    throw new Error('Environment variable SYNC_MODE must be ci or interactive');
  }

  return envMode ? envMode === 'ci' : detectCIEnvironment();
}
