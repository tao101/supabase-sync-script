import { execa } from 'execa';
import pLimit from 'p-limit';
import type { Config } from '../../types/config.js';
import { ConnectionBuilder } from '../../config/connection-builder.js';
import { TempFileManager } from '../../utils/temp-files.js';
import { logger } from '../../utils/logger.js';
import { SyncError, ErrorCategory } from '../../types/sync.js';
import type { PostgresPool } from '../../clients/postgres-client.js';

/**
 * System tables that should be excluded from data sync operations.
 * These tables are managed by Supabase or contain system metadata.
 */
const EXCLUDED_SYSTEM_TABLES = [
  'schema_migrations',
  'migrations',
  'buckets_vectors',
  'vector_indexes',
] as const;

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

  async clearTargetData(): Promise<void> {
    logger.info('Clearing existing data on target...');

    const client = await this.targetPool.connect();
    try {
      // Get all tables in included schemas
      const tablesResult = await client.query(`
        SELECT schemaname, tablename
        FROM pg_tables
        WHERE schemaname = ANY($1)
        AND tablename NOT IN (${EXCLUDED_SYSTEM_TABLES.map((_, i) => `$${i + 2}`).join(', ')})
        ORDER BY schemaname, tablename
      `, [this.config.options.database.includeSchemas, ...EXCLUDED_SYSTEM_TABLES]);

      // Truncate in reverse dependency order (simplified approach)
      await client.query('SET session_replication_role = replica;');

      for (const row of tablesResult.rows) {
        try {
          await client.query(`TRUNCATE TABLE "${row.schemaname}"."${row.tablename}" CASCADE`);
          logger.debug(`Truncated ${row.schemaname}.${row.tablename}`);
        } catch (error) {
          const tableName = `${row.schemaname}.${row.tablename}`;
          throw new SyncError(
            `Failed to truncate table ${tableName}: ${(error as Error).message}`,
            ErrorCategory.IMPORT,
            'clear-target-data',
            false,
            error as Error
          );
        }
      }

      // Don't reset session_replication_role here - it will be done after import
    } finally {
      client.release();
    }
  }

  async importData(dumpFile: string): Promise<void> {
    logger.info('Importing database data to target...');

    const targetDbUrl = this.connectionBuilder.buildDbUrl(this.config.target);

    try {
      // Use -c to set session_replication_role before importing
      // This disables triggers and allows data import without constraint checks
      // Use --single-transaction to ensure atomicity - if import fails, all changes are rolled back
      const result = await execa('psql', [
        targetDbUrl,
        '--single-transaction',
        '-c', 'SET session_replication_role = replica;',
        '-f', dumpFile,
      ], {
        env: { ...process.env, PGPASSWORD: this.config.target.dbPassword },
        reject: false, // Don't throw on non-zero exit
      });

      // Filter out expected errors from stderr
      let criticalErrorLines: string[] = [];
      if (result.stderr && result.stderr.trim()) {
        criticalErrorLines = result.stderr.split('\n').filter(line => {
          if (!line.includes('ERROR')) return false;
          // Filter out expected errors for system tables
          if (line.includes('must be owner of')) return false; // System tables owned by supabase_admin
          if (line.includes('permission denied')) return false; // System table permissions
          if (line.includes('current transaction is aborted')) return false; // Cascading from other errors
          return true;
        });
        if (criticalErrorLines.length > 0) {
          logger.warn(`Data import had ${criticalErrorLines.length} errors:`);
          criticalErrorLines.slice(0, 10).forEach(line => logger.warn(`  ${line.trim()}`));
          if (criticalErrorLines.length > 10) {
            logger.warn(`  ... and ${criticalErrorLines.length - 10} more errors`);
          }
        }
      }

      // Throw error if there are critical errors or non-zero exit code
      if (criticalErrorLines.length > 0 || result.exitCode !== 0) {
        const errorSummary = criticalErrorLines.length > 0
          ? `${criticalErrorLines.length} critical error(s): ${criticalErrorLines[0]?.trim() || 'unknown error'}`
          : `psql exited with code ${result.exitCode}`;

        if (result.stderr) {
          logger.debug(`psql stderr: ${result.stderr.slice(0, 1000)}`);
        }

        throw new SyncError(
          `Data import failed: ${errorSummary}`,
          ErrorCategory.IMPORT,
          'data-import',
          false,
          undefined
        );
      }

      logger.info('Data imported successfully');
    } catch (error) {
      // Re-throw SyncErrors as-is
      if (error instanceof SyncError) {
        throw error;
      }
      // Wrap unexpected errors
      throw new SyncError(
        `Data import failed: ${(error as Error).message}`,
        ErrorCategory.IMPORT,
        'data-import',
        false,
        error as Error
      );
    }
  }

  async verifyDataCounts(sourcePool: PostgresPool): Promise<boolean> {
    logger.info('Verifying data counts between source and target...');

    const sourceClient = await sourcePool.connect();
    const targetClient = await this.targetPool.connect();

    try {
      // Get table counts from source
      const tablesResult = await sourceClient.query(`
        SELECT schemaname, tablename
        FROM pg_tables
        WHERE schemaname = ANY($1)
        AND tablename NOT IN (${EXCLUDED_SYSTEM_TABLES.map((_, i) => `$${i + 2}`).join(', ')})
        ORDER BY schemaname, tablename
      `, [this.config.options.database.includeSchemas, ...EXCLUDED_SYSTEM_TABLES]);

      // Tables that are intentionally excluded from sync (always empty on target)
      const alwaysExcludedTables = ['auth.sessions', 'auth.refresh_tokens'];

      // Filter tables to verify
      const tablesToVerify = tablesResult.rows.filter(row => {
        const tableName = `${row.schemaname}.${row.tablename}`;
        return !this.config.options.database.excludeTables.includes(tableName) &&
               !alwaysExcludedTables.includes(tableName);
      });

      // Use p-limit to run COUNT queries in parallel with concurrency limit of 5
      const limit = pLimit(5);

      const countResults = await Promise.all(
        tablesToVerify.map(row =>
          limit(async () => {
            const tableName = `${row.schemaname}.${row.tablename}`;
            try {
              const [sourceCount, targetCount] = await Promise.all([
                sourceClient.query(
                  `SELECT COUNT(*) as count FROM "${row.schemaname}"."${row.tablename}"`
                ),
                targetClient.query(
                  `SELECT COUNT(*) as count FROM "${row.schemaname}"."${row.tablename}"`
                ),
              ]);

              const srcCount = parseInt(sourceCount.rows[0]?.count || '0', 10);
              const tgtCount = parseInt(targetCount.rows[0]?.count || '0', 10);

              return {
                table: tableName,
                source: srcCount,
                target: tgtCount,
                success: true,
              };
            } catch (error) {
              logger.debug(`Could not verify ${tableName}: ${(error as Error).message}`);
              return {
                table: tableName,
                source: 0,
                target: 0,
                success: false,
              };
            }
          })
        )
      );

      // Check for failed count queries — if any failed, verification cannot be trusted
      const failures = countResults.filter(result => !result.success);
      if (failures.length > 0) {
        logger.warn(`${failures.length}/${countResults.length} table count verifications failed:`);
        for (const f of failures.slice(0, 10)) {
          logger.warn(`  ${f.table}: count query failed`);
        }
        if (failures.length > 10) {
          logger.warn(`  ... and ${failures.length - 10} more failures`);
        }
        return false;
      }

      // Filter for mismatches (only from successful queries)
      const mismatches = countResults.filter(
        result => result.success && result.source !== result.target
      );

      if (mismatches.length > 0) {
        logger.warn(`Found ${mismatches.length} tables with row count mismatches:`);
        for (const m of mismatches.slice(0, 20)) {
          logger.warn(`  ${m.table}: source=${m.source}, target=${m.target}`);
        }
        if (mismatches.length > 20) {
          logger.warn(`  ... and ${mismatches.length - 20} more`);
        }
        return false;
      } else {
        logger.info('All table row counts match between source and target');
        return true;
      }
    } finally {
      sourceClient.release();
      targetClient.release();
    }
  }

  async verifyForeignKeys(): Promise<boolean> {
    logger.info('Verifying foreign key integrity on target...');

    const client = await this.targetPool.connect();

    try {
      // Query all foreign key constraints using pg_catalog tables
      // which correctly handle composite foreign keys via conkey/confkey arrays
      const fkResult = await client.query(`
        SELECT
          c.conname AS constraint_name,
          child_ns.nspname AS child_schema,
          child_rel.relname AS child_table,
          ARRAY(
            SELECT a.attname
            FROM pg_attribute a
            WHERE a.attrelid = c.conrelid
              AND a.attnum = ANY(c.conkey)
            ORDER BY array_position(c.conkey, a.attnum)
          ) AS child_columns,
          parent_ns.nspname AS parent_schema,
          parent_rel.relname AS parent_table,
          ARRAY(
            SELECT a.attname
            FROM pg_attribute a
            WHERE a.attrelid = c.confrelid
              AND a.attnum = ANY(c.confkey)
            ORDER BY array_position(c.confkey, a.attnum)
          ) AS parent_columns
        FROM pg_constraint c
        JOIN pg_class child_rel ON c.conrelid = child_rel.oid
        JOIN pg_namespace child_ns ON child_rel.relnamespace = child_ns.oid
        JOIN pg_class parent_rel ON c.confrelid = parent_rel.oid
        JOIN pg_namespace parent_ns ON parent_rel.relnamespace = parent_ns.oid
        WHERE c.contype = 'f'
          AND child_ns.nspname = ANY($1)
        ORDER BY child_ns.nspname, child_rel.relname, c.conname
      `, [this.config.options.database.includeSchemas]);

      const violations: {
        constraint: string;
        childTable: string;
        parentTable: string;
        orphanCount: number;
      }[] = [];

      for (const fk of fkResult.rows) {
        const childTable = `"${fk.child_schema}"."${fk.child_table}"`;
        const parentTable = `"${fk.parent_schema}"."${fk.parent_table}"`;
        const childColumns: string[] = fk.child_columns;
        const parentColumns: string[] = fk.parent_columns;

        try {
          // Build composite column references for the orphan check
          const nullChecks = childColumns
            .map(col => `c."${col}" IS NOT NULL`)
            .join(' AND ');

          const joinConditions = childColumns
            .map((col, i) => `p."${parentColumns[i]}" = c."${col}"`)
            .join(' AND ');

          // Check for orphaned records: child records pointing to non-existent parent records
          const orphanResult = await client.query(`
            SELECT COUNT(*) as orphan_count
            FROM ${childTable} c
            WHERE ${nullChecks}
              AND NOT EXISTS (
                SELECT 1 FROM ${parentTable} p
                WHERE ${joinConditions}
              )
          `);

          const orphanCount = parseInt(orphanResult.rows[0]?.orphan_count || '0', 10);

          if (orphanCount > 0) {
            violations.push({
              constraint: fk.constraint_name,
              childTable: `${fk.child_schema}.${fk.child_table}`,
              parentTable: `${fk.parent_schema}.${fk.parent_table}`,
              orphanCount,
            });
          }
        } catch (error) {
          logger.debug(
            `Could not verify FK ${fk.constraint_name}: ${(error as Error).message}`
          );
        }
      }

      if (violations.length > 0) {
        logger.warn(`Found ${violations.length} foreign key violations:`);
        for (const v of violations.slice(0, 20)) {
          logger.warn(
            `  ${v.constraint}: ${v.childTable} -> ${v.parentTable} (${v.orphanCount} orphaned records)`
          );
        }
        if (violations.length > 20) {
          logger.warn(`  ... and ${violations.length - 20} more violations`);
        }
        return false;
      } else {
        logger.info('All foreign key constraints verified - no orphaned records found');
        return true;
      }
    } finally {
      client.release();
    }
  }

  async sync(sourcePool: PostgresPool): Promise<void> {
    if (this.config.dryRun) {
      logger.info('[DRY RUN] Would export and import database data');
      return;
    }

    // Export data from source
    const dumpFile = await this.exportData();

    // Clear existing data and import new data
    // Note: importData() uses psql with --single-transaction and sets session_replication_role = replica
    // within that transaction, which correctly disables triggers during the import
    await this.clearTargetData();
    await this.importData(dumpFile);

    // Verify data counts match between source and target
    const countsMatch = await this.verifyDataCounts(sourcePool);
    if (!countsMatch) {
      throw new SyncError(
        'Data sync verification failed: row counts do not match between source and target',
        ErrorCategory.VALIDATION,
        'data-sync',
        false
      );
    }
  }
}
