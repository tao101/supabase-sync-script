import { promises as fs } from 'fs';
import path from 'path';
import { ConfigSchema, type Config, type SupabaseConnection } from '../types/config.js';
import {
  loadSourceFromEnv,
  loadTargetFromEnv,
  loadOptionsFromEnv,
  loadConfigFromEnv,
  detectCIEnvironment,
} from './env.js';
import { ConnectionBuilder } from './connection-builder.js';
import { logger, sanitizeConfig } from '../utils/logger.js';
import { getApplicationSchemas, getManagedSchemas } from '../sync/database/schemas.js';

export { ConnectionBuilder } from './connection-builder.js';

function deepMerge<T extends Record<string, unknown>>(target: T, source: Partial<T>): T {
  const result = { ...target };

  for (const key of Object.keys(source) as (keyof T)[]) {
    const sourceValue = source[key];
    const targetValue = target[key];

    if (sourceValue === undefined) continue;

    if (
      sourceValue &&
      typeof sourceValue === 'object' &&
      !Array.isArray(sourceValue) &&
      targetValue &&
      typeof targetValue === 'object' &&
      !Array.isArray(targetValue)
    ) {
      result[key] = deepMerge(
        targetValue as Record<string, unknown>,
        sourceValue as Record<string, unknown>
      ) as T[keyof T];
    } else {
      result[key] = sourceValue as T[keyof T];
    }
  }

  return result;
}

function removeUndefined<T extends Record<string, unknown>>(obj: T): Partial<T> {
  const result: Partial<T> = {};
  for (const [key, value] of Object.entries(obj)) {
    if (value === undefined) continue;
    if (value && typeof value === 'object' && !Array.isArray(value)) {
      const cleaned = removeUndefined(value as Record<string, unknown>);
      if (Object.keys(cleaned).length > 0) {
        result[key as keyof T] = cleaned as T[keyof T];
      }
    } else {
      result[key as keyof T] = value as T[keyof T];
    }
  }
  return result;
}

function normalizeLegacyTemplateMetadata(config: Partial<Config>): Partial<Config> {
  const normalized = { ...config } as Record<string, unknown>;
  delete normalized._comment;

  for (const key of ['source', 'target']) {
    const connection = normalized[key];
    if (!connection || typeof connection !== 'object' || Array.isArray(connection)) continue;

    const cleaned = { ...connection } as Record<string, unknown>;
    delete cleaned._keys_option_a;
    delete cleaned._keys_option_b;
    delete cleaned._secretKey;
    normalized[key] = cleaned;
  }

  return normalized as Partial<Config>;
}

export async function loadConfigFromFile(
  configPath: string,
  ignoreMissing: boolean = false
): Promise<Partial<Config>> {
  try {
    const absolutePath = path.resolve(configPath);
    const content = await fs.readFile(absolutePath, 'utf-8');
    return JSON.parse(content);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === 'ENOENT') {
      if (ignoreMissing) {
        logger.debug(`Config file not found: ${configPath}`);
        return {};
      }
      throw new Error(`Config file not found: ${path.resolve(configPath)}`);
    }
    throw error;
  }
}

export async function loadConfig(options: {
  configPath?: string;
  overrides?: Partial<Config>;
}): Promise<Config> {
  const { configPath, overrides = {} } = options;

  // Start with env vars
  const envSource = removeUndefined(loadSourceFromEnv());
  const envTarget = removeUndefined(loadTargetFromEnv());
  const envOptions = removeUndefined(loadOptionsFromEnv());
  const envConfig = removeUndefined(loadConfigFromEnv());
  const cleanedOverrides = removeUndefined(
    overrides as unknown as Record<string, unknown>
  ) as Partial<Config>;

  // Load from file if provided
  let fileConfig: Partial<Config> = {};
  if (configPath) {
    fileConfig = await loadConfigFromFile(configPath);
  } else {
    // Try default locations
    const defaultPaths = ['./sync-config.json', './config.json', './.supabase-sync.json'];
    for (const defaultPath of defaultPaths) {
      fileConfig = await loadConfigFromFile(defaultPath, true);
      if (Object.keys(fileConfig).length > 0) {
        logger.info(`Loaded config from ${defaultPath}`);
        break;
      }
    }
  }

  // Merge configs: env vars override file config, CLI overrides override all
  const mergedConfig: Partial<Config> = {
    ...fileConfig,
    ...envConfig,
    ...cleanedOverrides,
    source: deepMerge(
      deepMerge(
        (fileConfig.source || {}) as SupabaseConnection,
        envSource as Partial<SupabaseConnection>
      ),
      (cleanedOverrides.source || {}) as Partial<SupabaseConnection>
    ),
    target: deepMerge(
      deepMerge(
        (fileConfig.target || {}) as SupabaseConnection,
        envTarget as Partial<SupabaseConnection>
      ),
      (cleanedOverrides.target || {}) as Partial<SupabaseConnection>
    ),
    options: deepMerge(
      deepMerge(
        (fileConfig.options || {}) as Config['options'],
        envOptions as Partial<Config['options']>
      ),
      (cleanedOverrides.options || {}) as Partial<Config['options']>
    ),
  };

  // Auto-detect CI mode if not explicitly set
  if (!mergedConfig.mode && detectCIEnvironment()) {
    mergedConfig.mode = 'ci';
    logger.info('CI environment detected, running in CI mode');
  }

  // Validate with Zod
  const result = ConfigSchema.safeParse(normalizeLegacyTemplateMetadata(mergedConfig));

  if (!result.success) {
    const errors = result.error.errors.map(e => `${e.path.join('.')}: ${e.message}`);
    throw new Error(`Configuration validation failed:\n${errors.join('\n')}`);
  }

  logger.debug('Loaded configuration:', sanitizeConfig(result.data as Record<string, unknown>));

  return result.data;
}

// Key validation helpers - exported for use in other modules
export function isLegacyJwtKey(key: string): boolean {
  return key.split('.').length === 3;
}

export function isNewSecretKey(key: string): boolean {
  return key.startsWith('sb_secret_');
}

export function isNewPublishableKey(key: string): boolean {
  return key.startsWith('sb_publishable_');
}

export function isValidApiKey(key: string): boolean {
  return isLegacyJwtKey(key) || isNewSecretKey(key);
}

type KeyType = 'legacy' | 'new' | 'mixed' | 'missing';

function detectKeyType(connection: SupabaseConnection): KeyType {
  const hasLegacyServiceKey = !!connection.serviceRoleKey;
  const hasLegacyAnonKey = !!connection.anonKey;
  const hasNewSecretKey = !!connection.secretKey;
  const hasNewPublishableKey = !!connection.publishableKey;

  const hasLegacy = hasLegacyServiceKey || hasLegacyAnonKey;
  const hasNew = hasNewSecretKey || hasNewPublishableKey;

  if (hasLegacy && hasNew) {
    return 'mixed';
  }
  if (hasLegacyServiceKey) {
    return 'legacy';
  }
  if (hasNewSecretKey) {
    return 'new';
  }
  return 'missing';
}

function validateKeyPair(connection: SupabaseConnection, prefix: string): string[] {
  const errors: string[] = [];
  const keyType = detectKeyType(connection);

  switch (keyType) {
    case 'mixed':
      errors.push(`${prefix}: Cannot mix legacy keys (serviceRoleKey/anonKey) with new keys (secretKey/publishableKey). Use one pair or the other.`);
      break;

    case 'missing':
      errors.push(`${prefix}: Either serviceRoleKey (legacy) or secretKey (new) is required`);
      break;

    case 'legacy':
      // Validate legacy JWT format
      if (connection.serviceRoleKey && !isLegacyJwtKey(connection.serviceRoleKey)) {
        errors.push(`${prefix}: Service role key appears invalid (not a JWT format)`);
      }
      if (connection.anonKey && !isLegacyJwtKey(connection.anonKey)) {
        errors.push(`${prefix}: Anon key appears invalid (not a JWT format)`);
      }
      break;

    case 'new':
      // Validate new key format
      if (connection.secretKey && !isNewSecretKey(connection.secretKey)) {
        errors.push(`${prefix}: Secret key must start with 'sb_secret_'`);
      }
      if (connection.publishableKey && !isNewPublishableKey(connection.publishableKey)) {
        errors.push(`${prefix}: Publishable key must start with 'sb_publishable_'`);
      }
      break;
  }

  return errors;
}

export function validateConfig(config: Config): string[] {
  const errors: string[] = [];
  const builder = new ConnectionBuilder();
  const requiresApi = config.options.components.storage;
  const syncsApplicationDatabase = config.options.components.schema || config.options.components.data;

  if (!Object.values(config.options.components).some(Boolean)) {
    errors.push('At least one sync component must be enabled');
  }

  // Validate source connection
  const sourceErrors = builder.validateConnection(config.source, requiresApi);
  errors.push(...sourceErrors.map(e => `Source: ${e}`));

  // Validate target connection
  const targetErrors = builder.validateConnection(config.target, requiresApi);
  errors.push(...targetErrors.map(e => `Target: ${e}`));

  // Validate key pairs for source and target
  if (requiresApi) {
    errors.push(...validateKeyPair(config.source, 'Source'));
    errors.push(...validateKeyPair(config.target, 'Target'));
  }

  if (!config.options.auth.skipSessions) {
    errors.push('Auth session migration is not supported; options.auth.skipSessions must remain true');
  }

  if (
    config.options.components.schema &&
    config.options.database.excludeTables.length > 0
  ) {
    errors.push('database.excludeTables requires schema sync to be disabled so excluded target tables can be preserved');
  }

  if (syncsApplicationDatabase) {
    const managedSchemas = new Set(getManagedSchemas());
    const explicitlyManaged = config.options.database.includeSchemas
      .filter(schema => managedSchemas.has(schema.trim().toLowerCase()));
    if (explicitlyManaged.length > 0) {
      errors.push(`database.includeSchemas cannot include Supabase-managed schemas: ${explicitlyManaged.join(', ')}`);
    }
    if (getApplicationSchemas(config).length === 0) {
      errors.push('At least one application schema is required when schema or data sync is enabled');
    }
  }

  const sourceDatabase = builder.getDatabaseEndpoint(config.source.dbUrl);
  const targetDatabase = builder.getDatabaseEndpoint(config.target.dbUrl);
  if (sourceDatabase && sourceDatabase === targetDatabase) {
    errors.push('Source and target resolve to the same database; refusing a destructive self-sync');
  }

  const sourceApi = config.source.apiUrl
    ? builder.getApiEndpoint(config.source.apiUrl)
    : null;
  const targetApi = config.target.apiUrl
    ? builder.getApiEndpoint(config.target.apiUrl)
    : null;
  if (requiresApi && sourceApi && sourceApi === targetApi) {
    errors.push('Source and target resolve to the same API; refusing a destructive self-sync');
  }

  return errors;
}
