import type pg from 'pg';
import type { Config } from '../../types/config.js';
import { logger } from '../../utils/logger.js';
import { AuthUser, AuthIdentity, AuthSyncResult } from '../../types/sync.js';
import type { PostgresPool } from '../../clients/postgres-client.js';

const BATCH_SIZE = 500;

export class AuthSync {
  constructor(
    private config: Config,
    private sourcePool: PostgresPool,
    private targetPool: PostgresPool
  ) {}

  async exportUsers(): Promise<AuthUser[]> {
    logger.info('Exporting auth users from source...');

    const client = await this.sourcePool.connect();
    try {
      const result = await client.query(`
        SELECT
          id,
          email,
          phone,
          encrypted_password,
          email_confirmed_at,
          phone_confirmed_at,
          raw_user_meta_data,
          raw_app_meta_data,
          created_at,
          updated_at,
          banned_until,
          confirmation_token,
          recovery_token,
          email_change_token_new,
          email_change
        FROM auth.users
        ORDER BY created_at
      `);

      logger.info(`Exported ${result.rows.length} users`);
      return result.rows;
    } finally {
      client.release();
    }
  }

  async exportIdentities(): Promise<AuthIdentity[]> {
    logger.info('Exporting auth identities from source...');

    const client = await this.sourcePool.connect();
    try {
      const result = await client.query(`
        SELECT
          id,
          user_id,
          identity_data,
          provider,
          provider_id,
          last_sign_in_at,
          created_at,
          updated_at
        FROM auth.identities
        ORDER BY created_at
      `);

      logger.info(`Exported ${result.rows.length} identities`);
      return result.rows;
    } finally {
      client.release();
    }
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

  /**
   * Import a batch of users via SQL using a provided client connection.
   * Uses multi-row INSERT syntax for better performance.
   * This ensures the operation uses the same connection where
   * session_replication_role = replica has been set.
   */
  private async importUsersBatch(users: AuthUser[], client: pg.PoolClient): Promise<void> {
    if (users.length === 0) return;

    // Build multi-row VALUES clause
    const values: unknown[] = [];
    const valuePlaceholders: string[] = [];
    const columnsPerRow = 15; // Number of user columns (excluding the 3 fixed ones)

    for (let i = 0; i < users.length; i++) {
      const user = users[i];
      const offset = i * columnsPerRow;
      valuePlaceholders.push(
        `($${offset + 1}, $${offset + 2}, $${offset + 3}, $${offset + 4}, ` +
        `$${offset + 5}, $${offset + 6}, $${offset + 7}, $${offset + 8}, ` +
        `$${offset + 9}, $${offset + 10}, $${offset + 11}, $${offset + 12}, ` +
        `$${offset + 13}, $${offset + 14}, $${offset + 15}, ` +
        `'00000000-0000-0000-0000-000000000000', 'authenticated', 'authenticated')`
      );
      values.push(
        user.id,
        user.email,
        user.phone,
        user.encrypted_password,
        user.email_confirmed_at,
        user.phone_confirmed_at,
        JSON.stringify(user.raw_user_meta_data),
        JSON.stringify(user.raw_app_meta_data),
        user.created_at,
        user.updated_at,
        user.banned_until,
        user.confirmation_token,
        user.recovery_token,
        user.email_change_token_new,
        user.email_change
      );
    }

    await client.query(`
      INSERT INTO auth.users (
        id, email, phone, encrypted_password,
        email_confirmed_at, phone_confirmed_at,
        raw_user_meta_data, raw_app_meta_data,
        created_at, updated_at, banned_until,
        confirmation_token, recovery_token,
        email_change_token_new, email_change,
        instance_id, aud, role
      ) VALUES ${valuePlaceholders.join(', ')}
      ON CONFLICT (id) DO UPDATE SET
        email = EXCLUDED.email,
        phone = EXCLUDED.phone,
        encrypted_password = EXCLUDED.encrypted_password,
        email_confirmed_at = EXCLUDED.email_confirmed_at,
        phone_confirmed_at = EXCLUDED.phone_confirmed_at,
        raw_user_meta_data = EXCLUDED.raw_user_meta_data,
        raw_app_meta_data = EXCLUDED.raw_app_meta_data,
        updated_at = EXCLUDED.updated_at,
        banned_until = EXCLUDED.banned_until
    `, values);
  }

  /**
   * Import a batch of identities using a provided client connection.
   * Uses multi-row INSERT syntax for better performance.
   * This ensures the operation uses the same connection where
   * session_replication_role = replica has been set.
   */
  private async importIdentitiesBatch(identities: AuthIdentity[], client: pg.PoolClient): Promise<void> {
    if (identities.length === 0) return;

    // Build multi-row VALUES clause
    const values: unknown[] = [];
    const valuePlaceholders: string[] = [];
    const columnsPerRow = 8;

    for (let i = 0; i < identities.length; i++) {
      const identity = identities[i];
      const offset = i * columnsPerRow;
      valuePlaceholders.push(
        `($${offset + 1}, $${offset + 2}, $${offset + 3}, $${offset + 4}, ` +
        `$${offset + 5}, $${offset + 6}, $${offset + 7}, $${offset + 8})`
      );
      values.push(
        identity.id,
        identity.user_id,
        JSON.stringify(identity.identity_data),
        identity.provider,
        identity.provider_id,
        identity.last_sign_in_at,
        identity.created_at,
        identity.updated_at
      );
    }

    await client.query(`
      INSERT INTO auth.identities (
        id, user_id, identity_data, provider, provider_id,
        last_sign_in_at, created_at, updated_at
      ) VALUES ${valuePlaceholders.join(', ')}
      ON CONFLICT (id) DO UPDATE SET
        identity_data = EXCLUDED.identity_data,
        last_sign_in_at = EXCLUDED.last_sign_in_at,
        updated_at = EXCLUDED.updated_at
    `, values);
  }

  async sync(): Promise<AuthSyncResult> {
    if (this.config.dryRun) {
      logger.info('[DRY RUN] Would export and import auth users');
      const users = await this.exportUsers();
      const identities = await this.exportIdentities();
      return {
        usersImported: users.length,
        identitiesImported: identities.length,
        errors: [],
      };
    }

    const errors: string[] = [];

    // Export from source (can use separate connections - read-only)
    const users = await this.exportUsers();
    const identities = this.config.options.auth.migrateIdentities
      ? await this.exportIdentities()
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
          await this.importUsersBatch(batch, client);
          usersImported += batch.length;
          logger.debug(`Imported users batch ${Math.floor(i / BATCH_SIZE) + 1}/${Math.ceil(users.length / BATCH_SIZE)} (${batch.length} users)`);
        } catch (error) {
          // Batch failed — retry records individually to save valid ones
          const batchNum = Math.floor(i / BATCH_SIZE) + 1;
          logger.warn(`User batch ${batchNum} failed, retrying ${batch.length} users individually...`);
          for (const user of batch) {
            try {
              await this.importUsersBatch([user], client);
              usersImported++;
            } catch (individualError) {
              const msg = `Failed to import user ${user.id}: ${(individualError as Error).message}`;
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
            await this.importIdentitiesBatch(batch, client);
            identitiesImported += batch.length;
            logger.debug(`Imported identities batch ${Math.floor(i / BATCH_SIZE) + 1}/${Math.ceil(identities.length / BATCH_SIZE)} (${batch.length} identities)`);
          } catch (error) {
            // Batch failed — retry records individually to save valid ones
            const batchNum = Math.floor(i / BATCH_SIZE) + 1;
            logger.warn(`Identity batch ${batchNum} failed, retrying ${batch.length} identities individually...`);
            for (const identity of batch) {
              try {
                await this.importIdentitiesBatch([identity], client);
                identitiesImported++;
              } catch (individualError) {
                const msg = `Failed to import identity ${identity.id}: ${(individualError as Error).message}`;
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

    return {
      usersImported,
      identitiesImported,
      errors,
    };
  }
}
