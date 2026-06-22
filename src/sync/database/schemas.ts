import type { Config } from '../../types/config.js';

const MANAGED_SCHEMAS = new Set([
  'auth',
  'storage',
  'realtime',
  'extensions',
  'vault',
  'graphql',
  'graphql_public',
  'net',
  'pgsodium',
  'supabase_functions',
  'supabase_migrations',
]);

export function getApplicationSchemas(config: Config): string[] {
  return config.options.database.includeSchemas.filter(schema => !MANAGED_SCHEMAS.has(schema.toLowerCase()));
}

export function quoteIdentifier(identifier: string): string {
  return `"${identifier.replace(/"/g, '""')}"`;
}

export function stripUnsupportedDumpSettings(content: string): string {
  return content.replace(/^SET transaction_timeout = [^;]+;\r?\n?/gim, '');
}
