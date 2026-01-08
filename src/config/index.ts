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

export async function loadConfigFromFile(configPath: string): Promise<Partial<Config>> {
  try {
    const absolutePath = path.resolve(configPath);
    const content = await fs.readFile(absolutePath, 'utf-8');
    return JSON.parse(content);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === 'ENOENT') {
      logger.debug(`Config file not found: ${configPath}`);
      return {};
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

  // Load from file if provided
  let fileConfig: Partial<Config> = {};
  if (configPath) {
    fileConfig = await loadConfigFromFile(configPath);
  } else {
    // Try default locations
    const defaultPaths = ['./sync-config.json', './config.json', './.supabase-sync.json'];
    for (const defaultPath of defaultPaths) {
      fileConfig = await loadConfigFromFile(defaultPath);
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
    source: deepMerge(
      (fileConfig.source || {}) as SupabaseConnection,
      envSource as Partial<SupabaseConnection>
    ),
    target: deepMerge(
      (fileConfig.target || {}) as SupabaseConnection,
      envTarget as Partial<SupabaseConnection>
    ),
    options: deepMerge(
      (fileConfig.options || {}) as Config['options'],
      envOptions as Partial<Config['options']>
    ),
    ...overrides,
  };

  // Auto-detect CI mode if not explicitly set
  if (!mergedConfig.mode && detectCIEnvironment()) {
    mergedConfig.mode = 'ci';
    logger.info('CI environment detected, running in CI mode');
  }

  // Validate with Zod
  const result = ConfigSchema.safeParse(mergedConfig);

  if (!result.success) {
    const errors = result.error.errors.map(e => `${e.path.join('.')}: ${e.message}`);
    throw new Error(`Configuration validation failed:\n${errors.join('\n')}`);
  }

  logger.debug('Loaded configuration:', sanitizeConfig(result.data as Record<string, unknown>));

  return result.data;
}

export function validateConfig(config: Config): string[] {
  const errors: string[] = [];
  const builder = new ConnectionBuilder();

  // Validate source connection
  const sourceErrors = builder.validateConnection(config.source);
  errors.push(...sourceErrors.map(e => `Source: ${e}`));

  // Validate target connection
  const targetErrors = builder.validateConnection(config.target);
  errors.push(...targetErrors.map(e => `Target: ${e}`));

  // Validate service role key format
  if (config.source.serviceRoleKey) {
    try {
      const parts = config.source.serviceRoleKey.split('.');
      if (parts.length !== 3) {
        errors.push('Source: Service role key appears invalid (not a JWT)');
      }
    } catch {
      errors.push('Source: Service role key appears invalid');
    }
  }

  if (config.target.serviceRoleKey) {
    try {
      const parts = config.target.serviceRoleKey.split('.');
      if (parts.length !== 3) {
        errors.push('Target: Service role key appears invalid (not a JWT)');
      }
    } catch {
      errors.push('Target: Service role key appears invalid');
    }
  }

  return errors;
}
