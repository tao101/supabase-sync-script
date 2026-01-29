import type { SupabaseClient } from '@supabase/supabase-js';
import type pg from 'pg';
import type { Config } from '../../types/config.js';
import { logger } from '../../utils/logger.js';
import { AuthUser, AuthIdentity, AuthSyncResult } from '../../types/sync.js';
import type { PostgresPool } from '../../clients/postgres-client.js';

export class AuthSync {
  constructor(
    private config: Config,
    private sourcePool: PostgresPool,
    private targetPool: PostgresPool,
    private targetSupabase: SupabaseClient
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
   * Import a user via SQL using a provided client connection.
   * This ensures the operation uses the same connection where
   * session_replication_role = replica has been set.
   */
  private async importUser(user: AuthUser, client: pg.PoolClient): Promise<void> {
    await client.query(`
      INSERT INTO auth.users (
        id, email, phone, encrypted_password,
        email_confirmed_at, phone_confirmed_at,
        raw_user_meta_data, raw_app_meta_data,
        created_at, updated_at, banned_until,
        confirmation_token, recovery_token,
        email_change_token_new, email_change,
        instance_id, aud, role
      ) VALUES (
        $1, $2, $3, $4,
        $5, $6,
        $7, $8,
        $9, $10, $11,
        $12, $13,
        $14, $15,
        '00000000-0000-0000-0000-000000000000', 'authenticated', 'authenticated'
      )
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
    `, [
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
      user.email_change,
    ]);
  }

  /**
   * Import an identity using a provided client connection.
   * This ensures the operation uses the same connection where
   * session_replication_role = replica has been set.
   */
  private async importIdentity(identity: AuthIdentity, client: pg.PoolClient): Promise<void> {
    await client.query(`
      INSERT INTO auth.identities (
        id, user_id, identity_data, provider, provider_id,
        last_sign_in_at, created_at, updated_at
      ) VALUES (
        $1, $2, $3, $4, $5, $6, $7, $8
      )
      ON CONFLICT (id) DO UPDATE SET
        identity_data = EXCLUDED.identity_data,
        last_sign_in_at = EXCLUDED.last_sign_in_at,
        updated_at = EXCLUDED.updated_at
    `, [
      identity.id,
      identity.user_id,
      JSON.stringify(identity.identity_data),
      identity.provider,
      identity.provider_id,
      identity.last_sign_in_at,
      identity.created_at,
      identity.updated_at,
    ]);
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

      // Import users using the same connection
      for (const user of users) {
        try {
          await this.importUser(user, client);
          usersImported++;
        } catch (error) {
          const msg = `Failed to import user ${user.email || user.id}: ${(error as Error).message}`;
          logger.warn(msg);
          errors.push(msg);
        }
      }

      // Import identities using the same connection
      for (const identity of identities) {
        try {
          await this.importIdentity(identity, client);
          identitiesImported++;
        } catch (error) {
          const msg = `Failed to import identity ${identity.id}: ${(error as Error).message}`;
          logger.warn(msg);
          errors.push(msg);
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
