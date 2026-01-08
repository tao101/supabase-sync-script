import pg from 'pg';
import type { SupabaseConnection } from '../types/config.js';
import { logger } from '../utils/logger.js';

const { Pool, Client } = pg;

export type PostgresPool = pg.Pool;
export type PostgresClient = pg.Client;

// Track SSL preference per connection URL
const sslPreference = new Map<string, boolean>();

function getHostFromUrl(dbUrl: string): string {
  try {
    const url = new URL(dbUrl);
    return url.hostname;
  } catch {
    return dbUrl;
  }
}

function isLocalConnection(dbUrl: string): boolean {
  return dbUrl.includes('localhost') || dbUrl.includes('127.0.0.1');
}

export function createPostgresPool(connection: SupabaseConnection, forceNoSsl?: boolean): PostgresPool {
  logger.debug(`Creating Postgres pool`);

  const isLocalhost = isLocalConnection(connection.dbUrl);
  const host = getHostFromUrl(connection.dbUrl);

  // Check if we've already determined SSL preference for this host
  const useNoSsl = forceNoSsl || sslPreference.get(host) === false;
  const useSsl = !isLocalhost && !useNoSsl;

  return new Pool({
    connectionString: connection.dbUrl,
    ssl: useSsl ? { rejectUnauthorized: false } : false,
    max: 5,
    idleTimeoutMillis: 30000,
    connectionTimeoutMillis: 10000,
  });
}

export function createPostgresClient(connection: SupabaseConnection, forceNoSsl?: boolean): PostgresClient {
  logger.debug(`Creating Postgres client`);

  const isLocalhost = isLocalConnection(connection.dbUrl);
  const host = getHostFromUrl(connection.dbUrl);

  // Check if we've already determined SSL preference for this host
  const useNoSsl = forceNoSsl || sslPreference.get(host) === false;
  const useSsl = !isLocalhost && !useNoSsl;

  return new Client({
    connectionString: connection.dbUrl,
    ssl: useSsl ? { rejectUnauthorized: false } : false,
    connectionTimeoutMillis: 10000,
  });
}

export function setSslPreference(dbUrl: string, useSsl: boolean): void {
  const host = getHostFromUrl(dbUrl);
  sslPreference.set(host, useSsl);
}

export function getSslPreference(dbUrl: string): boolean | undefined {
  const host = getHostFromUrl(dbUrl);
  return sslPreference.get(host);
}

export function shouldUseSsl(dbUrl: string): boolean {
  const isLocalhost = isLocalConnection(dbUrl);
  if (isLocalhost) return false;

  const preference = getSslPreference(dbUrl);
  // Default to true (SSL) unless explicitly set to false
  return preference !== false;
}

export async function testPostgresConnection(pool: PostgresPool): Promise<{ success: boolean; error?: string }> {
  let client: pg.PoolClient | null = null;
  try {
    client = await pool.connect();
    const result = await client.query('SELECT 1 as test');
    return { success: result.rows[0]?.test === 1 };
  } catch (error) {
    const err = error as Error & { code?: string };
    const errorMessage = err.message || 'Unknown error';
    const errorCode = err.code || '';

    // Provide helpful messages for common errors
    let helpfulMessage = errorMessage;
    if (errorCode === 'ECONNREFUSED') {
      helpfulMessage = `Connection refused - check if the database server is running and accessible at the specified host/port`;
    } else if (errorCode === 'ENOTFOUND') {
      helpfulMessage = `Host not found - check the hostname in your database URL`;
    } else if (errorCode === '28P01' || errorMessage.includes('password authentication failed')) {
      helpfulMessage = `Authentication failed - check your username and password`;
    } else if (errorCode === '3D000' || errorMessage.includes('does not exist')) {
      helpfulMessage = `Database not found - check the database name in your URL`;
    } else if (errorCode === 'ETIMEDOUT') {
      helpfulMessage = `Connection timed out - the server may be unreachable or behind a firewall`;
    } else if (errorMessage.includes('SSL')) {
      helpfulMessage = `SSL error - ${errorMessage}`;
    }

    logger.error('Postgres connection test failed:', { message: helpfulMessage, code: errorCode });
    return { success: false, error: helpfulMessage };
  } finally {
    if (client) {
      client.release();
    }
  }
}

export async function getTableCount(pool: PostgresPool, schema: string, table: string): Promise<number> {
  const client = await pool.connect();
  try {
    const result = await client.query(
      `SELECT COUNT(*) as count FROM "${schema}"."${table}"`
    );
    return parseInt(result.rows[0]?.count || '0', 10);
  } finally {
    client.release();
  }
}

export async function getTables(pool: PostgresPool, schemas: string[]): Promise<{ schema: string; table: string }[]> {
  const client = await pool.connect();
  try {
    const schemaList = schemas.map(s => `'${s}'`).join(',');
    const result = await client.query(`
      SELECT schemaname as schema, tablename as table
      FROM pg_tables
      WHERE schemaname IN (${schemaList})
      ORDER BY schemaname, tablename
    `);
    return result.rows;
  } finally {
    client.release();
  }
}
