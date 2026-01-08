import type { SupabaseConnection } from '../types/config.js';

export class ConnectionBuilder {
  /**
   * Get the database connection URL
   */
  buildDbUrl(config: SupabaseConnection): string {
    return config.dbUrl;
  }

  /**
   * Get direct database connection URL (same as buildDbUrl now)
   */
  buildDirectDbUrl(config: SupabaseConnection): string {
    return config.dbUrl;
  }

  /**
   * Get the Supabase API URL
   */
  buildApiUrl(config: SupabaseConnection): string {
    return config.apiUrl;
  }

  /**
   * Validate that required fields are present
   */
  validateConnection(config: SupabaseConnection): string[] {
    const errors: string[] = [];

    if (!config.dbUrl) {
      errors.push('Database URL is required');
    } else if (!config.dbUrl.startsWith('postgresql://') && !config.dbUrl.startsWith('postgres://')) {
      errors.push('Database URL must start with postgresql:// or postgres://');
    }

    if (!config.apiUrl) {
      errors.push('API URL is required');
    } else if (!config.apiUrl.startsWith('http://') && !config.apiUrl.startsWith('https://')) {
      errors.push('API URL must start with http:// or https://');
    }

    if (!config.serviceRoleKey) {
      errors.push('Service role key is required');
    }

    return errors;
  }

  /**
   * Get a display-safe version of the connection (no passwords)
   */
  getSafeDisplay(config: SupabaseConnection): Record<string, string> {
    // Parse the database URL to extract host
    let host = 'unknown';
    try {
      const url = new URL(config.dbUrl);
      host = url.hostname;
    } catch {
      // If URL parsing fails, try to extract host manually
      const match = config.dbUrl.match(/@([^:\/]+)/);
      if (match) {
        host = match[1];
      }
    }

    return {
      host,
      apiUrl: config.apiUrl,
      dbUrl: config.dbUrl.replace(/:[^:@]+@/, ':***@'),
    };
  }

  /**
   * Extract host from database URL
   */
  getHostFromDbUrl(dbUrl: string): string {
    try {
      const url = new URL(dbUrl);
      return url.hostname;
    } catch {
      const match = dbUrl.match(/@([^:\/]+)/);
      return match ? match[1] : 'unknown';
    }
  }
}
