import { execa } from 'execa';
import type { Config } from '../types/config.js';
import { ConnectionBuilder } from '../config/connection-builder.js';
import { sanitizeErrorMessage } from './logger.js';

type PostgresTool = 'psql' | 'pg_dump' | 'pg_dumpall';

export interface PostgresToolsResult {
  success: boolean;
  error?: string;
}

export function requiresPostgresTools(config: Config): boolean {
  const { roles, schema, data } = config.options.components;
  return roles || schema || data;
}

function requiredTools(config: Config): PostgresTool[] {
  const tools = new Set<PostgresTool>(['psql']);
  if (config.options.components.schema || config.options.components.data) tools.add('pg_dump');
  if (config.options.components.roles) tools.add('pg_dumpall');
  return [...tools];
}

export async function testPostgresTools(config: Config): Promise<PostgresToolsResult> {
  if (!requiresPostgresTools(config)) return { success: true };

  try {
    await Promise.all(requiredTools(config).map(async tool => {
      const { stdout, stderr } = await execa(tool, ['--version'], { timeout: 10_000 });
      const majorVersion = Number(`${stdout} ${stderr}`.match(/\b(\d+)(?:\.\d+)?\b/)?.[1]);
      if (!Number.isInteger(majorVersion) || majorVersion < 16) {
        throw new Error(`${tool} 16 or newer is required`);
      }
    }));

    const builder = new ConnectionBuilder();
    await Promise.all([
      ['source', config.source] as const,
      ['target', config.target] as const,
    ].map(async ([label, connection]) => {
      try {
        const connectionUrl = new URL(builder.buildDbUrl(connection));
        if (!connectionUrl.searchParams.has('connect_timeout')) {
          connectionUrl.searchParams.set('connect_timeout', '10');
        }
        await execa('psql', [
          connectionUrl.toString(),
          '-X',
          '-w',
          '-v', 'ON_ERROR_STOP=1',
          '-c', 'SELECT 1',
        ], { env: builder.buildPgEnv(connection), timeout: 15_000 });
      } catch (error) {
        throw new Error(`psql could not connect to ${label}: ${(error as Error).message}`);
      }
    }));

    return { success: true };
  } catch (error) {
    return { success: false, error: sanitizeErrorMessage((error as Error).message) };
  }
}
