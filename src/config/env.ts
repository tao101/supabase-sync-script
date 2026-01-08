import { config as dotenvConfig } from 'dotenv';
import type { Config, SupabaseConnection, ConnectionType } from '../types/config.js';

// Load .env file
dotenvConfig();

function getEnvVar(name: string, required: boolean = false): string | undefined {
  const value = process.env[name];
  if (required && !value) {
    throw new Error(`Required environment variable ${name} is not set`);
  }
  return value;
}

function getEnvVarAsNumber(name: string): number | undefined;
function getEnvVarAsNumber(name: string, defaultValue: number): number;
function getEnvVarAsNumber(name: string, defaultValue?: number): number | undefined {
  const value = process.env[name];
  if (!value) return defaultValue;
  const parsed = parseInt(value, 10);
  if (isNaN(parsed)) return defaultValue;
  return parsed;
}

function getEnvVarAsBoolean(name: string, defaultValue: boolean = false): boolean {
  const value = process.env[name];
  if (!value) return defaultValue;
  return value.toLowerCase() === 'true' || value === '1';
}

export function loadSourceFromEnv(): Partial<SupabaseConnection> {
  return {
    type: getEnvVar('SOURCE_TYPE') as ConnectionType | undefined,
    projectRef: getEnvVar('SOURCE_PROJECT_REF'),
    host: getEnvVar('SOURCE_HOST'),
    port: getEnvVarAsNumber('SOURCE_PORT', 5432),
    dbPassword: getEnvVar('SOURCE_DB_PASSWORD'),
    serviceRoleKey: getEnvVar('SOURCE_SERVICE_ROLE_KEY'),
    anonKey: getEnvVar('SOURCE_ANON_KEY'),
    apiUrl: getEnvVar('SOURCE_API_URL'),
    dbUrl: getEnvVar('SOURCE_DB_URL'),
  };
}

export function loadTargetFromEnv(): Partial<SupabaseConnection> {
  return {
    type: getEnvVar('TARGET_TYPE') as ConnectionType | undefined,
    projectRef: getEnvVar('TARGET_PROJECT_REF'),
    host: getEnvVar('TARGET_HOST'),
    port: getEnvVarAsNumber('TARGET_PORT', 5432),
    dbPassword: getEnvVar('TARGET_DB_PASSWORD'),
    serviceRoleKey: getEnvVar('TARGET_SERVICE_ROLE_KEY'),
    anonKey: getEnvVar('TARGET_ANON_KEY'),
    apiUrl: getEnvVar('TARGET_API_URL'),
    dbUrl: getEnvVar('TARGET_DB_URL'),
  };
}

export function loadOptionsFromEnv(): Partial<Config['options']> {
  return {
    components: {
      schema: getEnvVarAsBoolean('SYNC_SCHEMA', true),
      data: getEnvVarAsBoolean('SYNC_DATA', true),
      auth: getEnvVarAsBoolean('SYNC_AUTH', true),
      storage: getEnvVarAsBoolean('SYNC_STORAGE', true),
      roles: getEnvVarAsBoolean('SYNC_ROLES', true),
    },
    storage: {
      concurrency: getEnvVarAsNumber('STORAGE_CONCURRENCY', 5),
      maxFileSizeMB: getEnvVarAsNumber('STORAGE_MAX_FILE_SIZE_MB', 50),
      excludeBuckets: getEnvVar('STORAGE_EXCLUDE_BUCKETS')?.split(',').filter(Boolean) || [],
    },
    database: {
      includeSchemas: getEnvVar('DB_INCLUDE_SCHEMAS')?.split(',').filter(Boolean) || ['public', 'auth', 'storage'],
      excludeTables: getEnvVar('DB_EXCLUDE_TABLES')?.split(',').filter(Boolean) || [],
      excludeSchemas: getEnvVar('DB_EXCLUDE_SCHEMAS')?.split(',').filter(Boolean) || ['pg_catalog', 'information_schema', 'pg_toast'],
    },
  };
}

export function loadConfigFromEnv(): Partial<Config> {
  return {
    mode: (getEnvVar('SYNC_MODE') as 'ci' | 'interactive') || 'interactive',
    dryRun: getEnvVarAsBoolean('SYNC_DRY_RUN', false),
    verbose: getEnvVarAsBoolean('SYNC_VERBOSE', false),
    tempDir: getEnvVar('SYNC_TEMP_DIR') || '/tmp/supabase-sync',
  };
}

export function detectCIEnvironment(): boolean {
  const ciEnvVars = ['CI', 'GITHUB_ACTIONS', 'GITLAB_CI', 'JENKINS_URL', 'CIRCLECI', 'TRAVIS'];
  return ciEnvVars.some(v => process.env[v]);
}
