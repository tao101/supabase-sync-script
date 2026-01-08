import { execa } from 'execa';
import type { Config } from '../../types/config.js';
import { ConnectionBuilder } from '../../config/connection-builder.js';
import { TempFileManager } from '../../utils/temp-files.js';
import { logger } from '../../utils/logger.js';
import { SyncError, ErrorCategory } from '../../types/sync.js';

// Built-in Supabase roles that should not be recreated
const SYSTEM_ROLES = [
  'postgres',
  'supabase_admin',
  'supabase_auth_admin',
  'supabase_storage_admin',
  'supabase_realtime_admin',
  'supabase_replication_admin',
  'supabase_read_only_user',
  'authenticator',
  'anon',
  'authenticated',
  'service_role',
  'dashboard_user',
  'pgbouncer',
  'pgsodium_keyholder',
  'pgsodium_keyiduser',
  'pgsodium_keymaker',
];

export class RolesSync {
  private connectionBuilder: ConnectionBuilder;

  constructor(
    private config: Config,
    private tempFileManager: TempFileManager
  ) {
    this.connectionBuilder = new ConnectionBuilder();
  }

  async exportRoles(): Promise<string> {
    logger.info('Exporting database roles from source...');

    const sourceDbUrl = this.connectionBuilder.buildDirectDbUrl(this.config.source);
    const dumpFile = await this.tempFileManager.createFile('roles_dump', '.sql');

    try {
      // pg_dumpall for roles only
      await execa('pg_dumpall', [
        '-d', sourceDbUrl,
        '--roles-only',
        '-f', dumpFile,
      ], {
        env: { ...process.env, PGPASSWORD: this.config.source.dbPassword },
      });

      logger.info(`Roles exported to ${dumpFile}`);
      return dumpFile;
    } catch (error) {
      throw new SyncError(
        `Failed to export roles: ${(error as Error).message}`,
        ErrorCategory.EXPORT,
        'roles-export',
        false,
        error as Error
      );
    }
  }

  async filterRoles(dumpFile: string): Promise<string> {
    logger.info('Filtering system roles...');

    const content = await this.tempFileManager.readFile(dumpFile);

    // Filter out system roles
    const lines = content.split('\n');
    const filteredLines: string[] = [];
    let skipBlock = false;

    for (const line of lines) {
      // Check if this line creates or alters a system role
      const isSystemRole = SYSTEM_ROLES.some(role => {
        const rolePattern = new RegExp(`(CREATE ROLE|ALTER ROLE)\\s+["']?${role}["']?`, 'i');
        return rolePattern.test(line);
      });

      if (isSystemRole) {
        skipBlock = true;
        continue;
      }

      // End of statement
      if (skipBlock && line.trim().endsWith(';')) {
        skipBlock = false;
        continue;
      }

      if (!skipBlock) {
        filteredLines.push(line);
      }
    }

    const filteredContent = filteredLines.join('\n');
    const filteredFile = await this.tempFileManager.createFile('roles_filtered', '.sql');
    await this.tempFileManager.writeFile(filteredFile, filteredContent);

    logger.info(`Filtered roles saved to ${filteredFile}`);
    return filteredFile;
  }

  async importRoles(dumpFile: string): Promise<void> {
    logger.info('Importing database roles to target...');

    const targetDbUrl = this.connectionBuilder.buildDbUrl(this.config.target);

    try {
      await execa('psql', [
        targetDbUrl,
        '-f', dumpFile,
        '-v', 'ON_ERROR_STOP=0',
      ], {
        env: { ...process.env, PGPASSWORD: this.config.target.dbPassword },
      });

      logger.info('Roles imported successfully');
    } catch (error) {
      // Log but don't fail - some role errors are expected
      logger.warn(`Roles import completed with warnings: ${(error as Error).message}`);
    }
  }

  async sync(): Promise<void> {
    if (this.config.dryRun) {
      logger.info('[DRY RUN] Would export and import database roles');
      return;
    }

    const dumpFile = await this.exportRoles();
    const filteredFile = await this.filterRoles(dumpFile);
    await this.importRoles(filteredFile);
  }
}
