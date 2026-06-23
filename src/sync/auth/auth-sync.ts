import { randomUUID } from 'crypto';
import type pg from 'pg';
import type { Config } from '../../types/config.js';
import { logger } from '../../utils/logger.js';
import { AuthSyncResult, SyncError, ErrorCategory } from '../../types/sync.js';
import type { PostgresPool } from '../../clients/postgres-client.js';

const BATCH_SIZE = 500;
type AuthRow = Record<string, unknown>;

interface IdentityColumnMapping {
  exportColumns: string[];
  importColumns: string[];
  sourceHasProviderId: boolean;
  targetHasProviderId: boolean;
}

export class AuthSync {
  constructor(
    private config: Config,
    private sourcePool: PostgresPool,
    private targetPool: PostgresPool
  ) {}

  private quoteIdentifier(identifier: string): string {
    return `"${identifier.replace(/"/g, '""')}"`;
  }

  private async getInsertableColumns(pool: PostgresPool, tableName: string): Promise<string[]> {
    const client = await pool.connect();
    try {
      const result = await client.query(`
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'auth'
          AND table_name = $1
          AND is_generated = 'NEVER'
          AND COALESCE(identity_generation, '') <> 'ALWAYS'
        ORDER BY ordinal_position
      `, [tableName]);

      return result.rows.map(row => row.column_name);
    } finally {
      client.release();
    }
  }

  private async getCommonColumns(tableName: string): Promise<string[]> {
    const [sourceColumns, targetColumns] = await Promise.all([
      this.getInsertableColumns(this.sourcePool, tableName),
      this.getInsertableColumns(this.targetPool, tableName),
    ]);
    const targetColumnSet = new Set(targetColumns);
    const commonColumns = sourceColumns.filter(column => targetColumnSet.has(column));

    if (!commonColumns.includes('id')) {
      throw new SyncError(
        `Cannot sync auth.${tableName}: source and target do not share an id column`,
        ErrorCategory.VALIDATION,
        'auth-sync',
        false
      );
    }

    return commonColumns;
  }

  private async getIdentityColumnMapping(): Promise<IdentityColumnMapping> {
    const [sourceColumns, targetColumns] = await Promise.all([
      this.getInsertableColumns(this.sourcePool, 'identities'),
      this.getInsertableColumns(this.targetPool, 'identities'),
    ]);
    const sourceColumnSet = new Set(sourceColumns);
    const targetColumnSet = new Set(targetColumns);
    const sourceHasProviderId = sourceColumnSet.has('provider_id');
    const targetHasProviderId = targetColumnSet.has('provider_id');
    const importColumnSet = new Set(sourceColumns.filter(column => targetColumnSet.has(column)));

    if (targetHasProviderId && !sourceHasProviderId) {
      if (!sourceColumnSet.has('id')) {
        throw new SyncError(
          'Cannot map auth.identities provider_id: source has neither provider_id nor id',
          ErrorCategory.VALIDATION,
          'auth-sync',
          false
        );
      }
      importColumnSet.add('provider_id');
    }

    if (!targetHasProviderId && sourceHasProviderId) {
      if (!targetColumnSet.has('id')) {
        throw new SyncError(
          'Cannot map auth.identities provider_id: target has neither provider_id nor id',
          ErrorCategory.VALIDATION,
          'auth-sync',
          false
        );
      }
      importColumnSet.add('id');
    }

    const importColumns = targetColumns.filter(column => importColumnSet.has(column));
    const exportColumnSet = new Set(importColumns.filter(column => sourceColumnSet.has(column)));
    if (targetHasProviderId && !sourceHasProviderId) {
      exportColumnSet.add('id');
    }
    if (!targetHasProviderId && sourceHasProviderId) {
      exportColumnSet.add('provider_id');
    }

    if (!importColumns.includes('id') && !targetHasProviderId) {
      throw new SyncError(
        'Cannot sync auth.identities: source and target do not share an id column',
        ErrorCategory.VALIDATION,
        'auth-sync',
        false
      );
    }

    return {
      exportColumns: sourceColumns.filter(column => exportColumnSet.has(column)),
      importColumns,
      sourceHasProviderId,
      targetHasProviderId,
    };
  }

  private mapIdentityRows(rows: AuthRow[], mapping: IdentityColumnMapping): AuthRow[] {
    if (mapping.sourceHasProviderId === mapping.targetHasProviderId) return rows;

    return rows.map(row => {
      const mapped = { ...row };
      if (mapping.targetHasProviderId) {
        mapped.provider_id = row.id;
        if (mapping.importColumns.includes('id')) {
          mapped.id = randomUUID();
        }
      } else {
        mapped.id = row.provider_id ?? row.id;
      }
      return mapped;
    });
  }

  private async exportAuthRows(tableName: string, columns: string[], label: string): Promise<AuthRow[]> {
    logger.info(`Exporting auth ${label} from source...`);

    const client = await this.sourcePool.connect();
    try {
      const columnList = columns.map(column => this.quoteIdentifier(column)).join(', ');
      const orderColumn = columns.includes('created_at') ? 'created_at' : columns[0];
      const result = await client.query(`
        SELECT ${columnList}
        FROM auth.${this.quoteIdentifier(tableName)}
        ORDER BY ${this.quoteIdentifier(orderColumn)}
      `);

      logger.info(`Exported ${result.rows.length} ${label}`);
      return result.rows;
    } finally {
      client.release();
    }
  }

  async exportUsers(columns?: string[]): Promise<AuthRow[]> {
    const selectedColumns = columns ?? await this.getInsertableColumns(this.sourcePool, 'users');
    return this.exportAuthRows('users', selectedColumns, 'users');
  }

  async exportIdentities(columns?: string[]): Promise<AuthRow[]> {
    const selectedColumns = columns ?? await this.getInsertableColumns(this.sourcePool, 'identities');
    return this.exportAuthRows('identities', selectedColumns, 'identities');
  }

  /**
   * Clear target auth data using a provided client connection.
   * This ensures the operation uses the same connection where
   * session_replication_role = replica has been set.
   */
  private async clearTargetAuth(client: pg.PoolClient): Promise<void> {
    logger.info('Clearing existing auth data on target...');
    // Clear in correct order due to foreign keys
    await client.query('TRUNCATE auth.identities CASCADE');
    await client.query('TRUNCATE auth.users CASCADE');
    logger.info('Target auth data cleared');
  }

  private async importRowsBatch(
    tableName: string,
    rows: AuthRow[],
    columns: string[],
    conflictColumns: string[],
    client: pg.PoolClient
  ): Promise<void> {
    if (rows.length === 0) return;

    const values: unknown[] = [];
    const valuePlaceholders = rows.map(row => {
      const placeholders = columns.map(column => {
        values.push(row[column] ?? null);
        return `$${values.length}`;
      });
      return `(${placeholders.join(', ')})`;
    });
    const quotedColumns = columns.map(column => this.quoteIdentifier(column)).join(', ');
    const quotedConflictColumns = conflictColumns.map(column => this.quoteIdentifier(column)).join(', ');
    const updateColumns = columns.filter(column => !conflictColumns.includes(column));
    const conflictClause = conflictColumns.length > 0
      ? `ON CONFLICT (${quotedConflictColumns}) ${updateColumns.length > 0
        ? `DO UPDATE SET ${updateColumns
          .map(column => `${this.quoteIdentifier(column)} = EXCLUDED.${this.quoteIdentifier(column)}`)
          .join(', ')}`
        : 'DO NOTHING'}`
      : '';

    await client.query(`
      INSERT INTO auth.${this.quoteIdentifier(tableName)} (${quotedColumns})
      VALUES ${valuePlaceholders.join(', ')}
      ${conflictClause}
    `, values);
  }

  private async importUsersBatch(
    users: AuthRow[],
    columns: string[],
    client: pg.PoolClient
  ): Promise<void> {
    await this.importRowsBatch('users', users, columns, ['id'], client);
  }

  private async importIdentitiesBatch(
    identities: AuthRow[],
    columns: string[],
    conflictColumns: string[],
    client: pg.PoolClient
  ): Promise<void> {
    await this.importRowsBatch('identities', identities, columns, conflictColumns, client);
  }

  private getIdentityConflictColumns(mapping: IdentityColumnMapping): string[] {
    if (!mapping.targetHasProviderId) {
      return mapping.importColumns.includes('provider') && mapping.importColumns.includes('id')
        ? ['provider', 'id']
        : [];
    }
    return mapping.importColumns.includes('id') ? ['id'] : [];
  }

  private async runWithSavepoint(
    client: pg.PoolClient,
    savepoint: string,
    fn: () => Promise<void>
  ): Promise<void> {
    await client.query(`SAVEPOINT ${savepoint}`);
    try {
      await fn();
      await client.query(`RELEASE SAVEPOINT ${savepoint}`);
    } catch (error) {
      await client.query(`ROLLBACK TO SAVEPOINT ${savepoint}`);
      await client.query(`RELEASE SAVEPOINT ${savepoint}`);
      throw error;
    }
  }

  private sanitizedDatabaseError(error: unknown): string {
    const pgError = error as { code?: string; constraint?: string };
    if (!pgError.code) return 'database rejected row';

    return `Postgres error ${pgError.code}${pgError.constraint ? ` (${pgError.constraint})` : ''}`;
  }

  async sync(): Promise<AuthSyncResult> {
    const userColumns = await this.getCommonColumns('users');
    const identityMapping = this.config.options.auth.migrateIdentities
      ? await this.getIdentityColumnMapping()
      : null;

    logger.info(`Auth users sync will copy ${userColumns.length} common columns`);
    if (identityMapping) {
      logger.info(`Auth identities sync will copy ${identityMapping.importColumns.length} target columns`);
    }

    if (this.config.dryRun) {
      logger.info('[DRY RUN] Would export and import auth users');
      const users = await this.exportUsers(userColumns);
      const identities = identityMapping
        ? await this.exportIdentities(identityMapping.exportColumns)
        : [];
      return {
        usersImported: users.length,
        identitiesImported: identities.length,
        errors: [],
      };
    }

    const errors: string[] = [];

    // Export from source (can use separate connections - read-only)
    const users = await this.exportUsers(userColumns);
    const identities = identityMapping
      ? this.mapIdentityRows(
        await this.exportIdentities(identityMapping.exportColumns),
        identityMapping
      )
      : [];
    const identityConflictColumns = identityMapping
      ? this.getIdentityConflictColumns(identityMapping)
      : [];

    // Acquire a SINGLE connection for ALL import operations
    // This ensures session_replication_role = replica is applied consistently
    const client = await this.targetPool.connect();
    let usersImported = 0;
    let identitiesImported = 0;
    let transactionStarted = false;

    try {
      // Disable triggers/constraints on THIS connection
      // This setting is session-specific and will persist for all operations on this connection
      logger.info('Disabling triggers for auth import...');
      await client.query('SET session_replication_role = replica;');
      await client.query('BEGIN');
      transactionStarted = true;

      // Clear target using the same connection
      await this.clearTargetAuth(client);

      // Import users in batches using the same connection
      logger.info(`Importing ${users.length} users in batches of ${BATCH_SIZE}...`);
      for (let i = 0; i < users.length; i += BATCH_SIZE) {
        const batch = users.slice(i, i + BATCH_SIZE);
        const batchNum = Math.floor(i / BATCH_SIZE) + 1;
        try {
          await this.runWithSavepoint(client, `auth_users_batch_${batchNum}`, async () => {
            await this.importUsersBatch(batch, userColumns, client);
          });
          usersImported += batch.length;
          logger.debug(`Imported users batch ${batchNum}/${Math.ceil(users.length / BATCH_SIZE)} (${batch.length} users)`);
        } catch (error) {
          // Batch failed — retry records individually to save valid ones
          logger.warn(`User batch ${batchNum} failed, retrying ${batch.length} users individually...`);
          for (let j = 0; j < batch.length; j++) {
            const user = batch[j];
            try {
              await this.runWithSavepoint(client, `auth_user_${batchNum}_${j + 1}`, async () => {
                await this.importUsersBatch([user], userColumns, client);
              });
              usersImported++;
            } catch (individualError) {
              const msg = `Failed to import user at source row ${i + j + 1}: ${this.sanitizedDatabaseError(individualError)}`;
              logger.warn(msg);
              errors.push(msg);
            }
          }
        }
      }

      // Import identities in batches using the same connection
      if (identityMapping && identities.length > 0) {
        logger.info(`Importing ${identities.length} identities in batches of ${BATCH_SIZE}...`);
        for (let i = 0; i < identities.length; i += BATCH_SIZE) {
          const batch = identities.slice(i, i + BATCH_SIZE);
          const batchNum = Math.floor(i / BATCH_SIZE) + 1;
          try {
            await this.runWithSavepoint(client, `auth_identities_batch_${batchNum}`, async () => {
              await this.importIdentitiesBatch(batch, identityMapping.importColumns, identityConflictColumns, client);
            });
            identitiesImported += batch.length;
            logger.debug(`Imported identities batch ${batchNum}/${Math.ceil(identities.length / BATCH_SIZE)} (${batch.length} identities)`);
          } catch (error) {
            // Batch failed — retry records individually to save valid ones
            logger.warn(`Identity batch ${batchNum} failed, retrying ${batch.length} identities individually...`);
            for (let j = 0; j < batch.length; j++) {
              const identity = batch[j];
              try {
                await this.runWithSavepoint(client, `auth_identity_${batchNum}_${j + 1}`, async () => {
                  await this.importIdentitiesBatch([identity], identityMapping.importColumns, identityConflictColumns, client);
                });
                identitiesImported++;
              } catch (individualError) {
                const msg = `Failed to import identity at source row ${i + j + 1}: ${this.sanitizedDatabaseError(individualError)}`;
                logger.warn(msg);
                errors.push(msg);
              }
            }
          }
        }
      }

      logger.info(`Auth sync complete: ${usersImported}/${users.length} users, ${identitiesImported}/${identities.length} identities`);
      if (errors.length > 0) {
        throw new SyncError(
          `Auth sync failed: ${errors.length} row(s) failed to import: ${errors.slice(0, 3).join('; ')}${errors.length > 3 ? '; ...' : ''}`,
          ErrorCategory.IMPORT,
          'auth-sync',
          false
        );
      }

      await client.query('COMMIT');
      transactionStarted = false;
    } catch (error) {
      if (transactionStarted) {
        try {
          await client.query('ROLLBACK');
        } catch (rollbackError) {
          logger.warn(`Failed to roll back auth import transaction: ${(rollbackError as Error).message}`);
        }
      }
      throw error;
    } finally {
      // Re-enable triggers/constraints before releasing the connection
      let resetSucceeded = false;
      try {
        await client.query('SET session_replication_role = DEFAULT;');
        logger.debug('Triggers re-enabled for auth');
        resetSucceeded = true;
      } catch (error) {
        logger.warn(`Failed to re-enable triggers: ${(error as Error).message}`);
      }
      // Pass true to destroy connection if reset failed (avoids returning dirty connection to pool)
      client.release(!resetSucceeded);
    }

    return {
      usersImported,
      identitiesImported,
      errors,
    };
  }
}
