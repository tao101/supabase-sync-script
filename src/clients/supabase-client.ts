import { createClient, SupabaseClient } from '@supabase/supabase-js';
import type { SupabaseConnection } from '../types/config.js';
import { ConnectionBuilder } from '../config/connection-builder.js';
import { logger } from '../utils/logger.js';

export function createSupabaseClient(
  connection: SupabaseConnection,
  useServiceRole: boolean = true
): SupabaseClient {
  const builder = new ConnectionBuilder();
  const apiUrl = builder.buildApiUrl(connection);
  const key = useServiceRole ? connection.serviceRoleKey : (connection.anonKey || connection.serviceRoleKey);

  logger.debug(`Creating Supabase client for ${apiUrl}`);

  return createClient(apiUrl, key, {
    auth: {
      autoRefreshToken: false,
      persistSession: false,
    },
  });
}

export async function testSupabaseConnection(client: SupabaseClient): Promise<boolean> {
  try {
    // Try to list buckets as a simple connectivity test
    const { error } = await client.storage.listBuckets();
    if (error) {
      logger.error('Supabase connection test failed:', { error: error.message });
      return false;
    }
    return true;
  } catch (error) {
    logger.error('Supabase connection test failed:', { error });
    return false;
  }
}

export async function testAuthAdminAccess(client: SupabaseClient): Promise<boolean> {
  try {
    // Try to list users (requires service role)
    const { error } = await client.auth.admin.listUsers({ perPage: 1 });
    if (error) {
      logger.error('Auth admin access test failed:', { error: error.message });
      return false;
    }
    return true;
  } catch (error) {
    logger.error('Auth admin access test failed:', { error });
    return false;
  }
}
