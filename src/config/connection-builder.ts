import { isIP } from 'net';
import type { SupabaseConnection } from '../types/config.js';

const SAFE_SSL_MODES = new Set(['disable', 'verify-full']);
const ENDPOINT_QUERY_PARAMETERS = ['host', 'hostaddr', 'port', 'dbname', 'user', 'service', 'servicefile'];

export class ConnectionBuilder {
  private normalizeHostname(hostname: string): string {
    return hostname.toLowerCase().replace(/^\[|\]$/g, '').replace(/\.$/, '');
  }

  private isLoopback(hostname: string): boolean {
    const host = this.normalizeHostname(hostname);
    if (host === 'localhost' || host === '::1') return true;
    return isIP(host) === 4 && host.split('.')[0] === '127';
  }

  private normalizeDbUrl(dbUrl: string): URL {
    const url = new URL(dbUrl);
    if (url.protocol !== 'postgresql:' && url.protocol !== 'postgres:') {
      throw new Error('Database URL must start with postgresql:// or postgres://');
    }
    if (!url.hostname) throw new Error('Database URL must include an explicit host');
    if (!url.username) throw new Error('Database URL must include an explicit user');
    if (!url.pathname || url.pathname === '/') {
      throw new Error('Database URL must include an explicit database name');
    }
    try {
      decodeURIComponent(url.username);
      decodeURIComponent(url.password);
      decodeURIComponent(url.pathname);
    } catch {
      throw new Error('Database URL contains invalid percent encoding');
    }

    const queryParameters = [...url.searchParams.keys()];
    if (queryParameters.some(parameter => parameter.toLowerCase() === 'ssl')) {
      throw new Error('Use sslmode=verify-full or sslmode=disable instead of the ambiguous ssl parameter');
    }
    if (queryParameters.some(parameter => parameter.toLowerCase() === 'sslpassword')) {
      throw new Error('Database URL query parameter "sslpassword" is not supported because it can expose a client-key passphrase');
    }
    if (queryParameters.some(parameter => parameter.toLowerCase().startsWith('oauth_'))) {
      throw new Error('Database URL OAuth query parameters are not supported');
    }
    if (queryParameters.some(parameter => parameter.toLowerCase() === 'password' && parameter !== 'password')) {
      throw new Error('Database URL query parameter "password" must be lowercase');
    }

    const endpointParameter = queryParameters.find(parameter =>
      ENDPOINT_QUERY_PARAMETERS.includes(parameter.toLowerCase())
    );
    if (endpointParameter) {
      throw new Error(`Database URL query parameter "${endpointParameter}" is not supported; put the endpoint in the URL host, port, and path`);
    }

    if (queryParameters.some(parameter => parameter.toLowerCase() === 'sslmode' && parameter !== 'sslmode')) {
      throw new Error('Database URL query parameter "sslmode" must be lowercase');
    }
    if (url.searchParams.getAll('sslmode').length > 1) {
      throw new Error('Database URL must not contain duplicate sslmode parameters');
    }
    const sslMode = url.searchParams.get('sslmode');
    if (sslMode && !SAFE_SSL_MODES.has(sslMode)) {
      throw new Error(`Unsafe sslmode "${sslMode}"; use verify-full, or disable only for intentional plaintext`);
    }
    if (!sslMode) {
      url.searchParams.set('sslmode', this.isLoopback(url.hostname) ? 'disable' : 'verify-full');
    }

    return url;
  }

  private normalizeApiUrl(apiUrl: string): URL {
    const url = new URL(apiUrl);
    if (url.protocol !== 'http:' && url.protocol !== 'https:') {
      throw new Error('API URL must start with http:// or https://');
    }
    if (url.protocol === 'http:' && !this.isLoopback(url.hostname)) {
      throw new Error('Remote API URLs must use https://');
    }
    if (url.username || url.password) throw new Error('API URL must not contain credentials');
    if (url.search || url.hash) throw new Error('API URL must not contain a query string or fragment');
    return url;
  }

  private getDbPassword(config: SupabaseConnection, url: URL): string | undefined {
    const queryPassword = url.searchParams.get('password');
    if (queryPassword !== null) return queryPassword;
    if (url.password) return decodeURIComponent(url.password);
    return config.dbPassword;
  }

  /** URL for node-postgres. Credentials are retained but TLS policy is explicit. */
  buildNodeDbUrl(config: SupabaseConnection): string {
    const url = this.normalizeDbUrl(config.dbUrl);
    const password = this.getDbPassword(config, url);
    url.searchParams.delete('password');
    if (url.searchParams.get('sslrootcert') === 'system') {
      url.searchParams.delete('sslrootcert');
    }
    if (password !== undefined) url.password = password;
    return url.toString();
  }

  /** Credential-free URL for libpq command-line tools. */
  buildDbUrl(config: SupabaseConnection): string {
    const url = this.normalizeDbUrl(config.dbUrl);
    if (url.searchParams.get('sslmode') === 'verify-full' && !url.searchParams.has('sslrootcert')) {
      url.searchParams.set('sslrootcert', 'system');
    }
    url.password = '';
    url.searchParams.delete('password');
    return url.toString();
  }

  buildDirectDbUrl(config: SupabaseConnection): string {
    return this.buildDbUrl(config);
  }

  buildPgEnv(config: SupabaseConnection): NodeJS.ProcessEnv {
    const url = this.normalizeDbUrl(config.dbUrl);
    const password = this.getDbPassword(config, url);
    const env = { ...process.env };
    for (const name of [
      'PGHOST',
      'PGHOSTADDR',
      'PGPORT',
      'PGDATABASE',
      'PGUSER',
      'PGSERVICE',
      'PGSERVICEFILE',
      'PGSSLMODE',
      'PGSSLROOTCERT',
    ]) {
      delete env[name];
    }
    // Match node-postgres: preserve ambient PGPASSWORD only when no explicit password is configured.
    if (password !== undefined) env.PGPASSWORD = password;
    return env;
  }

  buildApiUrl(config: SupabaseConnection): string {
    if (!config.apiUrl) throw new Error('API URL is required');
    return this.normalizeApiUrl(config.apiUrl).toString();
  }

  validateConnection(config: SupabaseConnection, requireApi: boolean = true): string[] {
    const errors: string[] = [];

    if (!config.dbUrl) {
      errors.push('Database URL is required');
    } else {
      try {
        this.normalizeDbUrl(config.dbUrl);
      } catch (error) {
        errors.push((error as Error).message);
      }
    }

    if (requireApi) {
      if (!config.apiUrl) {
        errors.push('API URL is required for storage sync');
      } else {
        try {
          this.normalizeApiUrl(config.apiUrl);
        } catch (error) {
          errors.push((error as Error).message);
        }
      }
    }

    return errors;
  }

  getSafeDisplay(config: SupabaseConnection): Record<string, string> {
    const host = this.getHostFromDbUrl(config.dbUrl);
    let dbUrl = 'invalid database URL';
    let apiUrl = config.apiUrl ? 'invalid API URL' : 'not configured';
    try {
      dbUrl = this.buildDbUrl(config);
    } catch {
      // Never echo an unparseable credential-bearing value.
    }
    try {
      if (config.apiUrl) apiUrl = this.normalizeApiUrl(config.apiUrl).toString();
    } catch {
      // Never echo an unparseable credential-bearing value.
    }

    return {
      host,
      apiUrl,
      dbUrl,
    };
  }

  getDatabaseEndpoint(dbUrl: string): string | null {
    try {
      const url = this.normalizeDbUrl(dbUrl);
      const normalizedHost = this.normalizeHostname(url.hostname);
      const host = this.isLoopback(normalizedHost) ? 'loopback' : normalizedHost;
      return `${host}:${url.port || '5432'}${decodeURIComponent(url.pathname)}`;
    } catch {
      return null;
    }
  }

  getApiEndpoint(apiUrl: string): string | null {
    try {
      const url = this.normalizeApiUrl(apiUrl);
      const host = this.normalizeHostname(url.hostname);
      return `${url.protocol}//${host}${url.port ? `:${url.port}` : ''}${url.pathname.replace(/\/+$/, '')}`;
    } catch {
      return null;
    }
  }

  getHostFromDbUrl(dbUrl: string): string {
    try {
      return new URL(dbUrl).hostname;
    } catch {
      return 'unknown';
    }
  }
}
