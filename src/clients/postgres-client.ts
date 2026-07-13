import pg from 'pg';
import type { SupabaseConnection } from '../types/config.js';
import { logger } from '../utils/logger.js';
import { ConnectionBuilder } from '../config/connection-builder.js';

const { Pool, Client } = pg;
const quoteIdentifier = (identifier: string) => `"${identifier.replace(/"/g, '""')}"`;

export type PostgresPool = pg.Pool;
export type PostgresClient = pg.Client;

function rejectImplicitPlaintext(connection: SupabaseConnection, forceNoSsl?: boolean): void {
  if (
    forceNoSsl &&
    new URL(new ConnectionBuilder().buildNodeDbUrl(connection)).searchParams.get('sslmode') !== 'disable'
  ) {
    throw new Error('forceNoSsl requires an explicit sslmode=disable database URL');
  }
}

export function createPostgresPool(connection: SupabaseConnection, forceNoSsl?: boolean): PostgresPool {
  logger.debug(`Creating Postgres pool`);
  rejectImplicitPlaintext(connection, forceNoSsl);

  return new Pool({
    connectionString: new ConnectionBuilder().buildNodeDbUrl(connection),
    max: 5,
    idleTimeoutMillis: 30000,
    connectionTimeoutMillis: 10000,
  });
}

export function createPostgresClient(connection: SupabaseConnection, forceNoSsl?: boolean): PostgresClient {
  logger.debug(`Creating Postgres client`);
  rejectImplicitPlaintext(connection, forceNoSsl);

  return new Client({
    connectionString: new ConnectionBuilder().buildNodeDbUrl(connection),
    connectionTimeoutMillis: 10000,
  });
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
      `SELECT COUNT(*) as count FROM ${quoteIdentifier(schema)}.${quoteIdentifier(table)}`
    );
    return parseInt(result.rows[0]?.count || '0', 10);
  } finally {
    client.release();
  }
}

export async function getTables(pool: PostgresPool, schemas: string[]): Promise<{ schema: string; table: string }[]> {
  const client = await pool.connect();
  try {
    const result = await client.query(`
      SELECT schemaname as schema, tablename as table
      FROM pg_tables
      WHERE schemaname = ANY($1)
      ORDER BY schemaname, tablename
    `, [schemas]);
    return result.rows;
  } finally {
    client.release();
  }
}
