import type pg from 'pg';
import type { Config } from '../../types/config.js';
import { logger } from '../../utils/logger.js';
import { AuthSyncResult, SyncError, ErrorCategory } from '../../types/sync.js';
import type { PostgresPool } from '../../clients/postgres-client.js';

const BATCH_SIZE = 500;
type AuthRow = Record<string, unknown>;

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
    conflictColumn: string,
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
    const quotedConflictColumn = this.quoteIdentifier(conflictColumn);
    const updateColumns = columns.filter(column => column !== conflictColumn);
    const conflictAction = updateColumns.length > 0
      ? `DO UPDATE SET ${updateColumns
        .map(column => `${this.quoteIdentifier(column)} = EXCLUDED.${this.quoteIdentifier(column)}`)
        .join(', ')}`
      : 'DO NOTHING';

    await client.query(`
      INSERT INTO auth.${this.quoteIdentifier(tableName)} (${quotedColumns})
      VALUES ${valuePlaceholders.join(', ')}
      ON CONFLICT (${quotedConflictColumn}) ${conflictAction}
    `, values);
  }

  private async importUsersBatch(
    users: AuthRow[],
    columns: string[],
    client: pg.PoolClient
  ): Promise<void> {
    await this.importRowsBatch('users', users, columns, 'id', client);
  }

  private async importIdentitiesBatch(
    identities: AuthRow[],
    columns: string[],
    client: pg.PoolClient
  ): Promise<void> {
    await this.importRowsBatch('identities', identities, columns, 'id', client);
  }

  async sync(): Promise<AuthSyncResult> {
    const userColumns = await this.getCommonColumns('users');
    const identityColumns = this.config.options.auth.migrateIdentities
      ? await this.getCommonColumns('identities')
      : [];

    logger.info(`Auth users sync will copy ${userColumns.length} common columns`);
    if (this.config.options.auth.migrateIdentities) {
      logger.info(`Auth identities sync will copy ${identityColumns.length} common columns`);
    }

    if (this.config.dryRun) {
      logger.info('[DRY RUN] Would export and import auth users');
      const users = await this.exportUsers(userColumns);
      const identities = this.config.options.auth.migrateIdentities
        ? await this.exportIdentities(identityColumns)
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
    const identities = this.config.options.auth.migrateIdentities
      ? await this.exportIdentities(identityColumns)
      : [];

    // Acquire a SINGLE connection for ALL import operations
    // This ensures session_replication_role = replica is applied consistently
    const client = await this.targetPool.connect();
    let usersImported = 0;
    let identitiesImported = 0;

    try {
      // Disable triggers/constraints on THIS connection
      // This setting is session-specific and will persist for all operations on this connection
      logger.info('Disabling triggers for auth import...');
      await client.query('SET session_replication_role = replica;');

      // Clear target using the same connection
      await this.clearTargetAuth(client);

      // Import users in batches using the same connection
      logger.info(`Importing ${users.length} users in batches of ${BATCH_SIZE}...`);
      for (let i = 0; i < users.length; i += BATCH_SIZE) {
        const batch = users.slice(i, i + BATCH_SIZE);
        try {
          await this.importUsersBatch(batch, userColumns, client);
          usersImported += batch.length;
          logger.debug(`Imported users batch ${Math.floor(i / BATCH_SIZE) + 1}/${Math.ceil(users.length / BATCH_SIZE)} (${batch.length} users)`);
        } catch (error) {
          // Batch failed — retry records individually to save valid ones
          const batchNum = Math.floor(i / BATCH_SIZE) + 1;
          logger.warn(`User batch ${batchNum} failed, retrying ${batch.length} users individually...`);
          for (const user of batch) {
            try {
              await this.importUsersBatch([user], userColumns, client);
              usersImported++;
            } catch (individualError) {
              const msg = `Failed to import user ${String(user.id ?? 'unknown')}: ${(individualError as Error).message}`;
              logger.warn(msg);
              errors.push(msg);
            }
          }
        }
      }

      // Import identities in batches using the same connection
      if (identities.length > 0) {
        logger.info(`Importing ${identities.length} identities in batches of ${BATCH_SIZE}...`);
        for (let i = 0; i < identities.length; i += BATCH_SIZE) {
          const batch = identities.slice(i, i + BATCH_SIZE);
          try {
            await this.importIdentitiesBatch(batch, identityColumns, client);
            identitiesImported += batch.length;
            logger.debug(`Imported identities batch ${Math.floor(i / BATCH_SIZE) + 1}/${Math.ceil(identities.length / BATCH_SIZE)} (${batch.length} identities)`);
          } catch (error) {
            // Batch failed — retry records individually to save valid ones
            const batchNum = Math.floor(i / BATCH_SIZE) + 1;
            logger.warn(`Identity batch ${batchNum} failed, retrying ${batch.length} identities individually...`);
            for (const identity of batch) {
              try {
                await this.importIdentitiesBatch([identity], identityColumns, client);
                identitiesImported++;
              } catch (individualError) {
                const msg = `Failed to import identity ${String(identity.id ?? 'unknown')}: ${(individualError as Error).message}`;
                logger.warn(msg);
                errors.push(msg);
              }
            }
          }
        }
      }

      logger.info(`Auth sync complete: ${usersImported}/${users.length} users, ${identitiesImported}/${identities.length} identities`);
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

    if (errors.length > 0) {
      throw new SyncError(
        `Auth sync failed: ${errors.length} row(s) failed to import: ${errors.slice(0, 3).join('; ')}${errors.length > 3 ? '; ...' : ''}`,
        ErrorCategory.IMPORT,
        'auth-sync',
        false
      );
    }

    return {
      usersImported,
      identitiesImported,
      errors,
    };
  }
}
