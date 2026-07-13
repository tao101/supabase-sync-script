import { createClient, SupabaseClient } from '@supabase/supabase-js';
import type { SupabaseConnection } from '../types/config.js';
import { logger } from '../utils/logger.js';
import { ConnectionBuilder } from '../config/connection-builder.js';

// Get the effective API key (new secretKey or legacy serviceRoleKey)
function getEffectiveApiKey(connection: SupabaseConnection): string {
  return connection.secretKey || connection.serviceRoleKey || '';
}

function timeoutFetch(timeoutMs: number): typeof fetch {
  return async (input, init) => {
    const controller = new AbortController();
    const sourceSignal = init?.signal ?? (input instanceof Request ? input.signal : undefined);
    let timer: NodeJS.Timeout;
    const cleanup = () => {
      clearTimeout(timer);
      sourceSignal?.removeEventListener('abort', abortFromSource);
    };
    const abortFromSource = () => {
      cleanup();
      controller.abort(sourceSignal?.reason);
    };
    timer = setTimeout(() => {
      cleanup();
      controller.abort(new Error(`Supabase request timed out after ${timeoutMs}ms`));
    }, timeoutMs);
    timer.unref();

    if (sourceSignal?.aborted) abortFromSource();
    else sourceSignal?.addEventListener('abort', abortFromSource, { once: true });

    try {
      const response = await fetch(input, { ...init, signal: controller.signal });
      if (!response.body) {
        cleanup();
        return response;
      }

      const reader = response.body.getReader();
      const body = new ReadableStream<Uint8Array>({
        async pull(streamController) {
          try {
            const { done, value } = await reader.read();
            if (done) {
              cleanup();
              streamController.close();
            } else {
              streamController.enqueue(value);
            }
          } catch (error) {
            cleanup();
            streamController.error(error);
          }
        },
        async cancel(reason) {
          cleanup();
          await reader.cancel(reason);
        },
      });
      return new Response(body, {
        headers: response.headers,
        status: response.status,
        statusText: response.statusText,
      });
    } catch (error) {
      cleanup();
      throw error;
    }
  };
}

export function createSupabaseClient(
  connection: SupabaseConnection,
  requestTimeoutMs?: number
): SupabaseClient {
  const apiKey = getEffectiveApiKey(connection);
  if (!connection.apiUrl || !apiKey) {
    throw new Error('Supabase API URL and admin key are required for storage sync');
  }
  const apiUrl = new ConnectionBuilder().buildApiUrl(connection);
  const keyType = connection.secretKey ? 'secret key' : 'service role key';
  logger.debug(`Creating Supabase client for ${apiUrl} using ${keyType}`);

  return createClient(apiUrl, apiKey, {
    global: requestTimeoutMs ? { fetch: timeoutFetch(requestTimeoutMs) } : undefined,
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
