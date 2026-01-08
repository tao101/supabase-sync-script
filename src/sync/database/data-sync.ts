import { execa } from 'execa';
import type { Config } from '../../types/config.js';
import { ConnectionBuilder } from '../../config/connection-builder.js';
import { TempFileManager } from '../../utils/temp-files.js';
import { logger } from '../../utils/logger.js';
import { SyncError, ErrorCategory } from '../../types/sync.js';
import type { PostgresPool } from '../../clients/postgres-client.js';

export class DataSync {
  private connectionBuilder: ConnectionBuilder;

  constructor(
    private config: Config,
    private tempFileManager: TempFileManager,
    private targetPool: PostgresPool
  ) {
    this.connectionBuilder = new ConnectionBuilder();
  }

  async exportData(): Promise<string> {
    logger.info('Exporting database data from source...');

    const sourceDbUrl = this.connectionBuilder.buildDirectDbUrl(this.config.source);
    const dumpFile = await this.tempFileManager.createFile('data_dump', '.sql');

    const args = [
      sourceDbUrl,
      '--data-only',
      '--quote-all-identifiers',
      '--no-owner',
      '--no-privileges',
      '--disable-triggers',
      '--use-copy',
      '-f', dumpFile,
    ];

    // Include specific schemas
    for (const schema of this.config.options.database.includeSchemas) {
      args.push(`--schema=${schema}`);
    }

    // Exclude specific tables
    for (const table of this.config.options.database.excludeTables) {
      args.push(`--exclude-table=${table}`);
    }

    // Always exclude session-related tables
    args.push('--exclude-table=auth.sessions');
    args.push('--exclude-table=auth.refresh_tokens');

    try {
      await execa('pg_dump', args, {
        env: { ...process.env, PGPASSWORD: this.config.source.dbPassword },
      });

      logger.info(`Data exported to ${dumpFile}`);
      return dumpFile;
    } catch (error) {
      throw new SyncError(
        `Failed to export data: ${(error as Error).message}`,
        ErrorCategory.EXPORT,
        'data-export',
        false,
        error as Error
      );
    }
  }

  async disableConstraints(): Promise<void> {
    logger.info('Disabling triggers and deferring constraints on target...');

    const client = await this.targetPool.connect();
    try {
      // Disable trigger-based constraints (including FK triggers)
      await client.query('SET session_replication_role = replica;');

      // Set all deferrable constraints to deferred
      await client.query('SET CONSTRAINTS ALL DEFERRED;');

      logger.debug('Triggers disabled and constraints deferred');
    } finally {
      client.release();
    }
  }

  async enableConstraints(): Promise<void> {
    logger.info('Re-enabling triggers and constraints on target...');

    const client = await this.targetPool.connect();
    try {
      // Re-enable triggers
      await client.query('SET session_replication_role = DEFAULT;');

      // Constraints will be checked at transaction commit
      await client.query('SET CONSTRAINTS ALL IMMEDIATE;');

      logger.debug('Triggers and constraints enabled');
    } finally {
      client.release();
    }
  }

  async clearTargetData(): Promise<void> {
    logger.info('Clearing existing data on target...');

    const client = await this.targetPool.connect();
    try {
      // Get all tables in included schemas
      const tablesResult = await client.query(`
        SELECT schemaname, tablename
        FROM pg_tables
        WHERE schemaname = ANY($1)
        AND tablename NOT IN ('schema_migrations', 'migrations')
        ORDER BY schemaname, tablename
      `, [this.config.options.database.includeSchemas]);

      // Truncate in reverse dependency order (simplified approach)
      await client.query('SET session_replication_role = replica;');

      for (const row of tablesResult.rows) {
        try {
          await client.query(`TRUNCATE TABLE "${row.schemaname}"."${row.tablename}" CASCADE`);
          logger.debug(`Truncated ${row.schemaname}.${row.tablename}`);
        } catch (error) {
          logger.warn(`Failed to truncate ${row.schemaname}.${row.tablename}: ${(error as Error).message}`);
        }
      }

      await client.query('SET session_replication_role = DEFAULT;');
    } finally {
      client.release();
    }
  }

  async importData(dumpFile: string): Promise<void> {
    logger.info('Importing database data to target...');

    const targetDbUrl = this.connectionBuilder.buildDbUrl(this.config.target);

    try {
      await execa('psql', [
        targetDbUrl,
        '-f', dumpFile,
        '-v', 'ON_ERROR_STOP=0',
      ], {
        env: { ...process.env, PGPASSWORD: this.config.target.dbPassword },
      });

      logger.info('Data imported successfully');
    } catch (error) {
      logger.warn(`Data import completed with warnings: ${(error as Error).message}`);
    }
  }

  async sync(): Promise<void> {
    if (this.config.dryRun) {
      logger.info('[DRY RUN] Would export and import database data');
      return;
    }

    // Export data from source
    const dumpFile = await this.exportData();

    // Disable constraints, clear data, import, re-enable constraints
    await this.disableConstraints();
    try {
      await this.clearTargetData();
      await this.importData(dumpFile);
    } finally {
      await this.enableConstraints();
    }
  }
}
