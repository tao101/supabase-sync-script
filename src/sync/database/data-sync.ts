import { execa } from 'execa';
import type { Config } from '../../types/config.js';
import { ConnectionBuilder } from '../../config/connection-builder.js';
import { TempFileManager } from '../../utils/temp-files.js';
import { logger } from '../../utils/logger.js';
import { SyncError, ErrorCategory } from '../../types/sync.js';
import type { PostgresPool } from '../../clients/postgres-client.js';

// Supabase internal auth tables that are version-dependent and should not be
// synced via pg_dump. Auth users and identities are handled by AuthSync.
const EXCLUDED_AUTH_SYSTEM_TABLES = [
  'auth.sessions',
  'auth.refresh_tokens',
  'auth.mfa_factors',
  'auth.mfa_challenges',
  'auth.mfa_amr_claims',
  'auth.saml_relay_states',
  'auth.saml_providers',
  'auth.sso_providers',
  'auth.sso_domains',
  'auth.flow_state',
  'auth.one_time_tokens',
  'auth.oauth_clients',
  'auth.oauth_authorizations',
  'auth.oauth_client_states',
];

// Storage tables managed by StorageSync via the Supabase Storage API
const EXCLUDED_STORAGE_SYSTEM_TABLES = [
  'storage.buckets',
  'storage.objects',
  'storage.s3_multipart_uploads',
  'storage.s3_multipart_uploads_parts',
];

const ALL_EXCLUDED_SYSTEM_TABLES = [
  ...EXCLUDED_AUTH_SYSTEM_TABLES,
  ...EXCLUDED_STORAGE_SYSTEM_TABLES,
];

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

    // Exclude Supabase system tables (auth internals + storage tables managed by API)
    for (const table of ALL_EXCLUDED_SYSTEM_TABLES) {
      args.push(`--exclude-table=${table}`);
    }

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
        AND tablename NOT IN (
          'schema_migrations',
          'migrations',
          'buckets_vectors',
          'vector_indexes'
        )
        ORDER BY schemaname, tablename
      `, [this.config.options.database.includeSchemas]);

      // Truncate in reverse dependency order (simplified approach)
      await client.query('SET session_replication_role = replica;');

      for (const row of tablesResult.rows) {
        const tableName = `${row.schemaname}.${row.tablename}`;

        // Skip system tables managed separately (auth internals, storage via API)
        if (ALL_EXCLUDED_SYSTEM_TABLES.includes(tableName)) {
          logger.debug(`Skipping truncation of ${tableName} (managed separately)`);
          continue;
        }

        try {
          await client.query(`TRUNCATE TABLE "${row.schemaname}"."${row.tablename}" CASCADE`);
          logger.debug(`Truncated ${row.schemaname}.${row.tablename}`);
        } catch (error) {
          logger.warn(`Failed to truncate ${row.schemaname}.${row.tablename}: ${(error as Error).message}`);
        }
      }

      // Don't reset session_replication_role here - it will be done after import
    } finally {
      client.release();
    }
  }

  async importData(dumpFile: string): Promise<string[]> {
    logger.info('Importing database data to target...');

    const targetDbUrl = this.connectionBuilder.buildDbUrl(this.config.target);
    const warnings: string[] = [];

    try {
      // Use -c to set session_replication_role before importing
      // This disables triggers and allows data import without constraint checks
      const result = await execa('psql', [
        targetDbUrl,
        '-c', 'SET session_replication_role = replica;',
        '-f', dumpFile,
      ], {
        env: { ...process.env, PGPASSWORD: this.config.target.dbPassword },
        reject: false, // Don't throw on non-zero exit
      });

      if (result.stderr && result.stderr.trim()) {
        const errorLines = result.stderr.split('\n').filter(line => {
          if (!line.includes('ERROR')) return false;
          // Filter out expected errors for system tables
          if (line.includes('must be owner of')) return false; // System tables owned by supabase_admin
          if (line.includes('permission denied')) return false; // System table permissions
          if (line.includes('current transaction is aborted')) return false; // Cascading from other errors
          return true;
        });
        if (errorLines.length > 0) {
          logger.warn(`Data import had ${errorLines.length} errors:`);
          errorLines.slice(0, 10).forEach(line => logger.warn(`  ${line.trim()}`));
          if (errorLines.length > 10) {
            logger.warn(`  ... and ${errorLines.length - 10} more errors`);
          }
          warnings.push(...errorLines.map(line => line.trim()));
        }
      }

      if (result.exitCode !== 0) {
        logger.warn(`Data import completed with exit code ${result.exitCode}`);
        // Log some of the output for debugging
        if (result.stderr) {
          logger.debug(`psql stderr: ${result.stderr.slice(0, 1000)}`);
        }
      } else {
        logger.info('Data imported successfully');
      }
    } catch (error) {
      logger.warn(`Data import completed with warnings: ${(error as Error).message}`);
      warnings.push((error as Error).message);
    }

    return warnings;
  }

  async verifyDataCounts(sourcePool: PostgresPool): Promise<{ table: string; source: number; target: number }[]> {
    logger.info('Verifying data counts between source and target...');

    const sourceClient = await sourcePool.connect();
    const targetClient = await this.targetPool.connect();

    try {
      // Get table counts from source
      const tablesResult = await sourceClient.query(`
        SELECT schemaname, tablename
        FROM pg_tables
        WHERE schemaname = ANY($1)
        AND tablename NOT IN (
          'schema_migrations',
          'migrations',
          'buckets_vectors',
          'vector_indexes'
        )
        ORDER BY schemaname, tablename
      `, [this.config.options.database.includeSchemas]);

      const mismatches: { table: string; source: number; target: number }[] = [];

      for (const row of tablesResult.rows) {
        const tableName = `${row.schemaname}.${row.tablename}`;

        // Skip excluded tables and system tables managed separately
        if (this.config.options.database.excludeTables.includes(tableName) ||
            ALL_EXCLUDED_SYSTEM_TABLES.includes(tableName)) {
          continue;
        }

        try {
          const sourceCount = await sourceClient.query(
            `SELECT COUNT(*) as count FROM "${row.schemaname}"."${row.tablename}"`
          );
          const targetCount = await targetClient.query(
            `SELECT COUNT(*) as count FROM "${row.schemaname}"."${row.tablename}"`
          );

          const srcCount = parseInt(sourceCount.rows[0]?.count || '0', 10);
          const tgtCount = parseInt(targetCount.rows[0]?.count || '0', 10);

          if (srcCount !== tgtCount) {
            mismatches.push({
              table: tableName,
              source: srcCount,
              target: tgtCount,
            });
          }
        } catch (error) {
          logger.warn(`Could not verify ${tableName}: ${(error as Error).message}`);
          // Track as a mismatch with -1 to signal verification failure
          mismatches.push({
            table: `${tableName} (VERIFICATION FAILED)`,
            source: -1,
            target: -1,
          });
        }
      }

      if (mismatches.length > 0) {
        logger.warn(`Found ${mismatches.length} tables with row count mismatches or verification failures:`);
        for (const m of mismatches.slice(0, 20)) {
          if (m.source === -1) {
            logger.warn(`  ${m.table}`);
          } else {
            logger.warn(`  ${m.table}: source=${m.source}, target=${m.target}`);
          }
        }
        if (mismatches.length > 20) {
          logger.warn(`  ... and ${mismatches.length - 20} more`);
        }
      } else {
        logger.info('All table row counts match between source and target');
      }

      return mismatches;
    } finally {
      sourceClient.release();
      targetClient.release();
    }
  }

  async sync(sourcePool?: PostgresPool): Promise<{ importWarnings: string[]; mismatches: { table: string; source: number; target: number }[] }> {
    if (this.config.dryRun) {
      logger.info('[DRY RUN] Would export and import database data');
      return { importWarnings: [], mismatches: [] };
    }

    // Export data from source
    const dumpFile = await this.exportData();

    // Disable constraints, clear data, import, re-enable constraints
    let importWarnings: string[] = [];
    await this.disableConstraints();
    try {
      await this.clearTargetData();
      importWarnings = await this.importData(dumpFile);
    } finally {
      await this.enableConstraints();
    }

    // Verify data counts if source pool is provided
    let mismatches: { table: string; source: number; target: number }[] = [];
    if (sourcePool) {
      mismatches = await this.verifyDataCounts(sourcePool);
    }

    return { importWarnings, mismatches };
  }
}
