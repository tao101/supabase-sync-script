import type { SupabaseConnection, ConnectionType } from '../types/config.js';

export class ConnectionBuilder {
  /**
   * Build PostgreSQL connection URL based on connection type
   */
  buildDbUrl(config: SupabaseConnection): string {
    // If explicit dbUrl is provided, use it
    if (config.dbUrl) {
      return config.dbUrl;
    }

    switch (config.type) {
      case 'saas':
        if (!config.projectRef) {
          throw new Error('projectRef is required for SaaS connection');
        }
        // SaaS uses pooler connection
        // Format: postgresql://postgres.[project-ref]:[password]@aws-0-[region].pooler.supabase.com:6543/postgres
        return `postgresql://postgres.${config.projectRef}:${encodeURIComponent(config.dbPassword)}@aws-0-us-east-1.pooler.supabase.com:6543/postgres`;

      case 'self-hosted':
        if (!config.host) {
          throw new Error('host is required for self-hosted connection');
        }
        return `postgresql://postgres:${encodeURIComponent(config.dbPassword)}@${config.host}:${config.port}/postgres`;

      case 'local':
        // Local Supabase CLI default
        const host = config.host || 'localhost';
        const port = config.port || 54322;
        return `postgresql://postgres:${encodeURIComponent(config.dbPassword)}@${host}:${port}/postgres`;

      default:
        throw new Error(`Unknown connection type: ${config.type}`);
    }
  }

  /**
   * Build Supabase API URL based on connection type
   */
  buildApiUrl(config: SupabaseConnection): string {
    // If explicit apiUrl is provided, use it
    if (config.apiUrl) {
      return config.apiUrl;
    }

    switch (config.type) {
      case 'saas':
        if (!config.projectRef) {
          throw new Error('projectRef is required for SaaS connection');
        }
        return `https://${config.projectRef}.supabase.co`;

      case 'self-hosted':
        if (!config.host) {
          throw new Error('host is required for self-hosted connection');
        }
        // Self-hosted typically uses Kong on port 8000 or direct API
        return `https://${config.host}`;

      case 'local':
        return 'http://localhost:54321';

      default:
        throw new Error(`Unknown connection type: ${config.type}`);
    }
  }

  /**
   * Build direct database connection URL (bypassing pooler for operations that need it)
   */
  buildDirectDbUrl(config: SupabaseConnection): string {
    switch (config.type) {
      case 'saas':
        if (!config.projectRef) {
          throw new Error('projectRef is required for SaaS connection');
        }
        // Direct connection for SaaS (port 5432)
        return `postgresql://postgres.${config.projectRef}:${encodeURIComponent(config.dbPassword)}@db.${config.projectRef}.supabase.co:5432/postgres`;

      case 'self-hosted':
      case 'local':
        // Same as regular for self-hosted/local
        return this.buildDbUrl(config);

      default:
        throw new Error(`Unknown connection type: ${config.type}`);
    }
  }

  /**
   * Validate that required fields are present for the connection type
   */
  validateConnection(config: SupabaseConnection): string[] {
    const errors: string[] = [];

    if (!config.type) {
      errors.push('Connection type is required');
      return errors;
    }

    if (!config.dbPassword) {
      errors.push('Database password is required');
    }

    if (!config.serviceRoleKey) {
      errors.push('Service role key is required');
    }

    switch (config.type) {
      case 'saas':
        if (!config.projectRef) {
          errors.push('Project reference is required for SaaS connection');
        }
        break;

      case 'self-hosted':
        if (!config.host) {
          errors.push('Host is required for self-hosted connection');
        }
        break;

      case 'local':
        // Local has defaults, nothing strictly required beyond password
        break;
    }

    return errors;
  }

  /**
   * Get a display-safe version of the connection (no passwords)
   */
  getSafeDisplay(config: SupabaseConnection): Record<string, string> {
    return {
      type: config.type,
      ...(config.projectRef && { projectRef: config.projectRef }),
      ...(config.host && { host: config.host }),
      port: String(config.port || 5432),
      apiUrl: this.buildApiUrl(config),
      dbUrl: this.buildDbUrl(config).replace(/:[^:@]+@/, ':***@'),
    };
  }
}
