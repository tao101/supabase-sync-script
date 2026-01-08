import { z } from 'zod';

export const ConnectionTypeSchema = z.enum(['saas', 'self-hosted', 'local']);
export type ConnectionType = z.infer<typeof ConnectionTypeSchema>;

export const SupabaseConnectionSchema = z.object({
  type: ConnectionTypeSchema.optional(),
  // Database URL (primary connection method)
  dbUrl: z.string(),
  // Supabase API
  apiUrl: z.string().url(),
  serviceRoleKey: z.string(),
  anonKey: z.string().optional(),
  // Legacy fields (optional, for backwards compatibility)
  projectRef: z.string().optional(),
  host: z.string().optional(),
  port: z.number().default(5432),
  dbPassword: z.string().optional(),
});

export type SupabaseConnection = z.infer<typeof SupabaseConnectionSchema>;

export const SyncOptionsSchema = z.object({
  components: z.object({
    schema: z.boolean().default(true),
    data: z.boolean().default(true),
    auth: z.boolean().default(true),
    storage: z.boolean().default(true),
    roles: z.boolean().default(true),
  }).default({}),
  database: z.object({
    excludeSchemas: z.array(z.string()).default(['pg_catalog', 'information_schema', 'pg_toast']),
    excludeTables: z.array(z.string()).default([]),
    includeSchemas: z.array(z.string()).default(['public', 'auth', 'storage']),
  }).default({}),
  storage: z.object({
    excludeBuckets: z.array(z.string()).default([]),
    maxFileSizeMB: z.number().default(50),
    concurrency: z.number().default(5),
  }).default({}),
  auth: z.object({
    preservePasswordHashes: z.boolean().default(true),
    migrateIdentities: z.boolean().default(true),
    skipSessions: z.boolean().default(true),
  }).default({}),
}).default({});

export type SyncOptions = z.infer<typeof SyncOptionsSchema>;

export const ConfigSchema = z.object({
  source: SupabaseConnectionSchema,
  target: SupabaseConnectionSchema,
  options: SyncOptionsSchema,
  mode: z.enum(['ci', 'interactive']).default('interactive'),
  dryRun: z.boolean().default(false),
  verbose: z.boolean().default(false),
  tempDir: z.string().default('/tmp/supabase-sync'),
});

export type Config = z.infer<typeof ConfigSchema>;
