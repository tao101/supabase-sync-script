import { execa } from 'execa';
import { once } from 'events';
import { createReadStream, createWriteStream } from 'fs';
import { createInterface } from 'readline';
import { finished } from 'stream/promises';
import type { Config } from '../../types/config.js';
import { ConnectionBuilder } from '../../config/connection-builder.js';
import { TempFileManager } from '../../utils/temp-files.js';
import { logger } from '../../utils/logger.js';
import { SyncError, ErrorCategory } from '../../types/sync.js';
import type { PostgresPool } from '../../clients/postgres-client.js';
import { getApplicationSchemas, quoteIdentifier } from './schemas.js';

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

  private isExcludedTable(schema: string, table: string): boolean {
    const exclusions = this.config.options.database.excludeTables;
    return exclusions.includes(table) || exclusions.includes(`${schema}.${table}`);
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

    const schemas = getApplicationSchemas(this.config);
    if (schemas.length === 0) {
      throw new SyncError(
        'No application schemas configured for data sync',
        ErrorCategory.VALIDATION,
        'data-export',
        false
      );
    }

    for (const schema of schemas) {
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
        env: this.connectionBuilder.buildPgEnv(this.config.source),
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

  private async buildClearTargetSql(): Promise<string> {
    const client = await this.targetPool.connect();
    try {
      const tablesResult = await client.query(`
        SELECT schemaname, tablename
        FROM pg_tables
        WHERE schemaname = ANY($1)
        ORDER BY schemaname, tablename
      `, [getApplicationSchemas(this.config)]);

      const tables = tablesResult.rows.filter(row => {
        const tableName = `${row.schemaname}.${row.tablename}`;
        return !ALL_EXCLUDED_SYSTEM_TABLES.includes(tableName) &&
          !this.isExcludedTable(row.schemaname, row.tablename);
      });

      if (tables.length === 0) return '';

      const tableList = tables
        .map(row => `${quoteIdentifier(row.schemaname)}.${quoteIdentifier(row.tablename)}`)
        .join(', ');
      return `TRUNCATE TABLE ${tableList};`;
    } finally {
      client.release();
    }
  }

  async clearTargetData(): Promise<void> {
    logger.info('Clearing existing data on target...');
    const clearSql = await this.buildClearTargetSql();
    if (!clearSql) return;

    const client = await this.targetPool.connect();
    try {
      // Listing all included tables together satisfies their mutual foreign keys.
      // RESTRICT (the default) prevents excluded dependent tables from being erased.
      await client.query(clearSql.replace(/;$/, ''));
    } catch (error) {
      throw new SyncError(
        `Failed to truncate target tables: ${(error as Error).message}`,
        ErrorCategory.IMPORT,
        'clear-target-data',
        false,
        error as Error
      );
    } finally {
      client.release();
    }
  }

  async importData(dumpFile: string, beforeImportSql: string = ''): Promise<void> {
    logger.info('Importing database data to target...');

    const targetDbUrl = this.connectionBuilder.buildDbUrl(this.config.target);

    try {
      const processedFile = await this.preprocessDumpFile(dumpFile);
      const prelude = [
        'SET LOCAL session_replication_role = replica;',
        beforeImportSql,
      ].filter(Boolean).join('\n');
      await execa('psql', [
        targetDbUrl,
        '-X',
        '--single-transaction',
        '-v', 'ON_ERROR_STOP=1',
        '-c', prelude,
        '-f', processedFile,
      ], {
        env: this.connectionBuilder.buildPgEnv(this.config.target),
      });
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
        ORDER BY schemaname, tablename
      `, [getApplicationSchemas(this.config)]);

      // Filter tables to verify
      const tablesToVerify = tablesResult.rows.filter(row => {
        const tableName = `${row.schemaname}.${row.tablename}`;
        return !this.isExcludedTable(row.schemaname, row.tablename) &&
               !ALL_EXCLUDED_SYSTEM_TABLES.includes(tableName);
      });

      const countResults: Array<{
        table: string;
        source: number;
        target: number;
        success: boolean;
      }> = [];
      for (const row of tablesToVerify) {
        const tableName = `${row.schemaname}.${row.tablename}`;
        try {
          const [sourceCount, targetCount] = await Promise.all([
            sourceClient.query(
              `SELECT COUNT(*) as count FROM ${quoteIdentifier(row.schemaname)}.${quoteIdentifier(row.tablename)}`
            ),
            targetClient.query(
              `SELECT COUNT(*) as count FROM ${quoteIdentifier(row.schemaname)}.${quoteIdentifier(row.tablename)}`
            ),
          ]);
          countResults.push({
            table: tableName,
            source: parseInt(sourceCount.rows[0]?.count || '0', 10),
            target: parseInt(targetCount.rows[0]?.count || '0', 10),
            success: true,
          });
        } catch (error) {
          logger.debug(`Could not verify ${tableName}: ${(error as Error).message}`);
          countResults.push({ table: tableName, source: 0, target: 0, success: false });
        }
      }

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
          ) AS parent_columns,
          c.confmatchtype AS match_type
        FROM pg_constraint c
        JOIN pg_class child_rel ON c.conrelid = child_rel.oid
        JOIN pg_namespace child_ns ON child_rel.relnamespace = child_ns.oid
        JOIN pg_class parent_rel ON c.confrelid = parent_rel.oid
        JOIN pg_namespace parent_ns ON parent_rel.relnamespace = parent_ns.oid
        WHERE c.contype = 'f'
          AND child_ns.nspname = ANY($1)
        ORDER BY child_ns.nspname, child_rel.relname, c.conname
      `, [getApplicationSchemas(this.config)]);

      const violations: {
        constraint: string;
        childTable: string;
        parentTable: string;
        orphanCount: number;
      }[] = [];
      let queryFailures = 0;

      for (const fk of fkResult.rows) {
        const childTable = `${quoteIdentifier(fk.child_schema)}.${quoteIdentifier(fk.child_table)}`;
        const parentTable = `${quoteIdentifier(fk.parent_schema)}.${quoteIdentifier(fk.parent_table)}`;
        const childColumns: string[] = fk.child_columns;
        const parentColumns: string[] = fk.parent_columns;

        try {
          // Build composite column references for the orphan check
          const nullChecks = childColumns
            .map(col => `c.${quoteIdentifier(col)} IS NOT NULL`)
            .join(' AND ');

          const joinConditions = childColumns
            .map((col, i) => `p.${quoteIdentifier(parentColumns[i])} = c.${quoteIdentifier(col)}`)
            .join(' AND ');
          const partialNullCheck = fk.match_type === 'f'
            ? ` OR ((${childColumns.map(col => `c.${quoteIdentifier(col)} IS NULL`).join(' OR ')}) AND (${childColumns.map(col => `c.${quoteIdentifier(col)} IS NOT NULL`).join(' OR ')}))`
            : '';

          // Check for orphaned records: child records pointing to non-existent parent records
          const orphanResult = await client.query(`
            SELECT COUNT(*) as orphan_count
            FROM ${childTable} c
            WHERE (${nullChecks}
              AND NOT EXISTS (
                SELECT 1 FROM ${parentTable} p
                WHERE ${joinConditions}
              ))${partialNullCheck}
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
          queryFailures++;
          logger.debug(
            `Could not verify FK ${fk.constraint_name}: ${(error as Error).message}`
          );
        }
      }

      // If any FK check queries failed, verification cannot be trusted
      if (queryFailures > 0) {
        logger.warn(`${queryFailures}/${fkResult.rows.length} FK verification queries failed`);
        return false;
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

    // Clear and import in one psql transaction so failures restore target data.
    const clearSql = await this.buildClearTargetSql();
    await this.importData(dumpFile, clearSql);

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

  private async preprocessDumpFile(dumpFile: string): Promise<string> {
    const processedFile = await this.tempFileManager.createFile('data_processed', '.sql');
    const input = createReadStream(dumpFile, { encoding: 'utf8' });
    const output = createWriteStream(processedFile, { mode: 0o600 });
    const lines = createInterface({ input, crlfDelay: Infinity });
    const outputFinished = finished(output);
    void outputFinished.catch(() => undefined);
    let inCopyBlock = false;

    try {
      for await (const line of lines) {
        if (!inCopyBlock && /^SET transaction_timeout = [^;]+;$/i.test(line.trim())) continue;
        if (!output.write(`${line}\n`)) {
          await Promise.race([once(output, 'drain'), outputFinished]);
        }
        if (inCopyBlock && line === '\\.') {
          inCopyBlock = false;
        } else if (!inCopyBlock && /^COPY .* FROM stdin;$/i.test(line.trim())) {
          inCopyBlock = true;
        }
      }
    } catch (error) {
      output.destroy();
      try {
        await outputFinished;
      } catch {
        // Preserve the original read/write failure.
      }
      throw error;
    } finally {
      lines.close();
    }

    output.end();
    await outputFinished;

    return processedFile;
  }
}
