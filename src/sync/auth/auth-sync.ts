import { randomUUID } from 'crypto';
import type pg from 'pg';
import type { Config } from '../../types/config.js';
import { logger } from '../../utils/logger.js';
import { AuthSyncResult, SyncError, ErrorCategory } from '../../types/sync.js';
import type { PostgresPool } from '../../clients/postgres-client.js';

const BATCH_SIZE = 500;
const TARGET_AUTH_TABLES = [
  'mfa_amr_claims',
  'refresh_tokens',
  'mfa_challenges',
  'mfa_factors',
  'one_time_tokens',
  'flow_state',
  'saml_relay_states',
  'sessions',
  'identities',
];
type AuthRow = Record<string, unknown>;

interface IdentityColumnMapping {
  exportColumns: string[];
  importColumns: string[];
  sourceHasProviderId: boolean;
  targetHasProviderId: boolean;
}

export class AuthSync {
  private sourceUserIds: string[] | null = null;

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
    const result = await client.query(`
      SELECT tables.table_name
      FROM information_schema.tables AS tables
      WHERE tables.table_schema = 'auth'
        AND tables.table_type = 'BASE TABLE'
        AND (
          tables.table_name = ANY($1::text[])
          OR EXISTS (
            SELECT 1
            FROM pg_constraint constraint_obj
            JOIN pg_class relation_obj ON relation_obj.oid = constraint_obj.conrelid
            JOIN pg_namespace relation_ns ON relation_ns.oid = relation_obj.relnamespace
            WHERE constraint_obj.contype = 'f'
              AND constraint_obj.confrelid = 'auth.users'::regclass
              AND relation_ns.nspname = tables.table_schema
              AND relation_obj.relname = tables.table_name
          )
        )
      ORDER BY array_position($1::text[], tables.table_name), tables.table_name
    `, [TARGET_AUTH_TABLES]);
    for (const { table_name: table } of result.rows) {
      if (table !== 'users') {
        await client.query(`DELETE FROM auth.${this.quoteIdentifier(table)}`);
      }
    }
    logger.info('Target auth data cleared');
  }

  private async prepareTargetUsers(
    users: AuthRow[],
    commonColumns: string[],
    client: pg.PoolClient
  ): Promise<void> {
    const sourceUserIds = users.map(user => String(user.id));
    if (sourceUserIds.length === 0) return;

    if (!this.config.options.auth.preservePasswordHashes && commonColumns.includes('encrypted_password')) {
      await client.query(
        'UPDATE auth.users SET encrypted_password = NULL WHERE id = ANY($1::uuid[])',
        [sourceUserIds]
      );
    }

    for (const column of ['email', 'phone'].filter(column => commonColumns.includes(column))) {
      const sourceValues = [...new Set(users
        .map(user => user[column])
        .filter((value): value is string => typeof value === 'string'))];
      if (sourceValues.length === 0) continue;

      const identifier = this.quoteIdentifier(column);
      await client.query(`
        UPDATE auth.users
        SET ${identifier} = NULL
        WHERE NOT (id = ANY($1::uuid[]))
          AND ${identifier} = ANY($2::text[])
      `, [sourceUserIds, sourceValues]);
    }
  }

  async cleanupTargetOnlyUsers(client?: pg.PoolClient): Promise<number> {
    if (!this.sourceUserIds) throw new Error('Auth users must be exported before cleanup');

    const ownClient = !client;
    const dbClient = client ?? await this.targetPool.connect();
    let transactionStarted = false;
    try {
      if (ownClient) {
        await dbClient.query('BEGIN');
        transactionStarted = true;
      }

      const references = await dbClient.query(`
        SELECT
          constraint_obj.conname AS constraint_name,
          referencing_ns.nspname AS schema_name,
          referencing_rel.relname AS table_name,
          referencing_attr.attname AS column_name
        FROM pg_constraint constraint_obj
        JOIN pg_class referencing_rel ON referencing_rel.oid = constraint_obj.conrelid
        JOIN pg_namespace referencing_ns ON referencing_ns.oid = referencing_rel.relnamespace
        JOIN LATERAL generate_subscripts(constraint_obj.confkey, 1) key_index(position) ON true
        JOIN pg_attribute referenced_attr
          ON referenced_attr.attrelid = constraint_obj.confrelid
          AND referenced_attr.attnum = constraint_obj.confkey[key_index.position]
        JOIN pg_attribute referencing_attr
          ON referencing_attr.attrelid = constraint_obj.conrelid
          AND referencing_attr.attnum = constraint_obj.conkey[key_index.position]
        WHERE constraint_obj.contype = 'f'
          AND constraint_obj.confrelid = 'auth.users'::regclass
          AND referenced_attr.attname = 'id'
        ORDER BY referencing_ns.nspname, referencing_rel.relname, constraint_obj.conname
      `);

      const tables = new Map<string, { schema: string; table: string }>();
      for (const reference of references.rows) {
        tables.set(`${reference.schema_name}.${reference.table_name}`, {
          schema: reference.schema_name,
          table: reference.table_name,
        });
      }
      for (const table of tables.values()) {
        await dbClient.query(
          `LOCK TABLE ${this.quoteIdentifier(table.schema)}.${this.quoteIdentifier(table.table)} IN SHARE MODE`
        );
      }

      for (const reference of references.rows) {
        const result = await dbClient.query(`
          SELECT EXISTS (
            SELECT 1
            FROM ${this.quoteIdentifier(reference.schema_name)}.${this.quoteIdentifier(reference.table_name)}
            WHERE ${this.quoteIdentifier(reference.column_name)} IS NOT NULL
              AND NOT (${this.quoteIdentifier(reference.column_name)} = ANY($1::uuid[]))
          ) AS has_target_only_reference
        `, [this.sourceUserIds]);
        if (result.rows[0]?.has_target_only_reference) {
          throw new SyncError(
            `Cannot remove target-only auth users because ${reference.schema_name}.${reference.table_name}.${reference.column_name} still references them`,
            ErrorCategory.VALIDATION,
            'auth-cleanup',
            false
          );
        }
      }

      const result = await dbClient.query(
        'DELETE FROM auth.users WHERE NOT (id = ANY($1::uuid[]))',
        [this.sourceUserIds]
      );
      if (ownClient) {
        await dbClient.query('COMMIT');
        transactionStarted = false;
      }
      logger.info(`Removed ${result.rowCount ?? 0} target-only auth user(s)`);
      return result.rowCount ?? 0;
    } catch (error) {
      if (transactionStarted) await dbClient.query('ROLLBACK').catch(() => {});
      throw error;
    } finally {
      if (ownClient) dbClient.release();
    }
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
    const commonUserColumns = await this.getCommonColumns('users');
    const userColumns = this.config.options.auth.preservePasswordHashes
      ? commonUserColumns
      : commonUserColumns.filter(column => column !== 'encrypted_password');
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
    this.sourceUserIds = users.map(user => String(user.id));
    const identities = identityMapping
      ? this.mapIdentityRows(
        await this.exportIdentities(identityMapping.exportColumns),
        identityMapping
      )
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
      await this.prepareTargetUsers(users, commonUserColumns, client);

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
              await this.importIdentitiesBatch(batch, identityMapping.importColumns, client);
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
                  await this.importIdentitiesBatch([identity], identityMapping.importColumns, client);
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

      if (errors.length > 0) {
        throw new SyncError(
          `Auth sync failed: ${errors.length} row(s) failed to import: ${errors.slice(0, 3).join('; ')}${errors.length > 3 ? '; ...' : ''}`,
          ErrorCategory.IMPORT,
          'auth-sync',
          false
        );
      }

      if (!this.config.options.components.data) {
        await client.query('SET session_replication_role = DEFAULT;');
        await this.cleanupTargetOnlyUsers(client);
      }

      await client.query('COMMIT');
      transactionStarted = false;
      logger.info(`Auth sync complete: ${usersImported}/${users.length} users, ${identitiesImported}/${identities.length} identities`);
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
