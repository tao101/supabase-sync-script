import { z } from 'zod';

export const ConnectionTypeSchema = z.enum(['saas', 'self-hosted', 'local']);
export type ConnectionType = z.infer<typeof ConnectionTypeSchema>;

export const SupabaseConnectionSchema = z.object({
  type: ConnectionTypeSchema.optional(),
  // Database URL (primary connection method)
  dbUrl: z.string(),
  // Supabase API
  apiUrl: z.string().url().optional().transform(value => value ?? ''),
  // Legacy keys (JWT format) - use either legacy OR new keys, not both
  serviceRoleKey: z.string().optional(),
  anonKey: z.string().optional(),
  // New keys (sb_secret_/sb_publishable_ format) - use either legacy OR new keys, not both
  secretKey: z.string().optional(),
  publishableKey: z.string().optional(),
  // Legacy fields (optional, for backwards compatibility)
  projectRef: z.string().optional(),
  host: z.string().optional(),
  port: z.number().int().positive().max(65535).default(5432),
  dbPassword: z.string().optional(),
}).strict();

export type SupabaseConnection = z.infer<typeof SupabaseConnectionSchema>;

export const SyncOptionsSchema = z.object({
  components: z.object({
    schema: z.boolean().default(true),
    data: z.boolean().default(true),
    auth: z.boolean().default(true),
    storage: z.boolean().default(true),
    roles: z.boolean().default(true),
  }).strict().default({}),
  database: z.object({
    excludeSchemas: z.array(z.string()).default(['pg_catalog', 'information_schema', 'pg_toast']),
    excludeTables: z.array(z.string()).default([]),
    includeSchemas: z.array(z.string()).default(['public']),
  }).strict().default({}),
  storage: z.object({
    excludeBuckets: z.array(z.string()).default([]),
    maxFileSizeMB: z.number().finite().positive().default(50),
    concurrency: z.number().int().positive().default(5),
  }).strict().default({}),
  auth: z.object({
    preservePasswordHashes: z.boolean().default(true),
    migrateIdentities: z.boolean().default(true),
    skipSessions: z.boolean().default(true),
  }).strict().default({}),
}).strict().default({});

export type SyncOptions = z.infer<typeof SyncOptionsSchema>;

export const ConfigSchema = z.object({
  source: SupabaseConnectionSchema,
  target: SupabaseConnectionSchema,
  options: SyncOptionsSchema,
  mode: z.enum(['ci', 'interactive']).default('interactive'),
  dryRun: z.boolean().default(false),
  verbose: z.boolean().default(false),
  tempDir: z.string().default('/tmp/supabase-sync'),
}).strict();

export type Config = z.infer<typeof ConfigSchema>;
