import { createClient, SupabaseClient } from '@supabase/supabase-js';
import type { SupabaseConnection } from '../types/config.js';
import { logger } from '../utils/logger.js';

// Get the effective API key (new secretKey or legacy serviceRoleKey)
function getEffectiveApiKey(connection: SupabaseConnection): string {
  return connection.secretKey || connection.serviceRoleKey || '';
}

export function createSupabaseClient(connection: SupabaseConnection): SupabaseClient {
  const apiKey = getEffectiveApiKey(connection);
  const keyType = connection.secretKey ? 'secret key' : 'service role key';
  logger.debug(`Creating Supabase client for ${connection.apiUrl} using ${keyType}`);

  return createClient(connection.apiUrl, apiKey, {
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
