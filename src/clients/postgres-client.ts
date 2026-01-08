import pg from 'pg';
import type { SupabaseConnection } from '../types/config.js';
import { ConnectionBuilder } from '../config/connection-builder.js';
import { logger } from '../utils/logger.js';

const { Pool, Client } = pg;

export type PostgresPool = pg.Pool;
export type PostgresClient = pg.Client;

export function createPostgresPool(
  connection: SupabaseConnection,
  useDirect: boolean = false
): PostgresPool {
  const builder = new ConnectionBuilder();
  const connectionString = useDirect
    ? builder.buildDirectDbUrl(connection)
    : builder.buildDbUrl(connection);

  logger.debug(`Creating Postgres pool for ${connection.type} connection`);

  return new Pool({
    connectionString,
    ssl: connection.type === 'local' ? false : { rejectUnauthorized: false },
    max: 5,
    idleTimeoutMillis: 30000,
    connectionTimeoutMillis: 10000,
  });
}

export function createPostgresClient(
  connection: SupabaseConnection,
  useDirect: boolean = false
): PostgresClient {
  const builder = new ConnectionBuilder();
  const connectionString = useDirect
    ? builder.buildDirectDbUrl(connection)
    : builder.buildDbUrl(connection);

  logger.debug(`Creating Postgres client for ${connection.type} connection`);

  return new Client({
    connectionString,
    ssl: connection.type === 'local' ? false : { rejectUnauthorized: false },
    connectionTimeoutMillis: 10000,
  });
}

export async function testPostgresConnection(pool: PostgresPool): Promise<boolean> {
  let client: pg.PoolClient | null = null;
  try {
    client = await pool.connect();
    const result = await client.query('SELECT 1 as test');
    return result.rows[0]?.test === 1;
  } catch (error) {
    logger.error('Postgres connection test failed:', { error });
    return false;
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
