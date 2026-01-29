import type { SupabaseClient } from '@supabase/supabase-js';
import type { Config, SupabaseConnection } from '../types/config.js';
import { SyncStep, SyncResult, StepResult, SyncError, ErrorCategory } from '../types/sync.js';
import { logger, print } from '../utils/logger.js';
import { TempFileManager } from '../utils/temp-files.js';
import { createSupabaseClient, testSupabaseConnection } from '../clients/supabase-client.js';
import { createPostgresPool, testPostgresConnection, PostgresPool, setSslPreference } from '../clients/postgres-client.js';
import { SchemaSync, DataSync, SequenceSync, RolesSync } from '../sync/database/index.js';
import { AuthSync } from '../sync/auth/index.js';
import { StorageSync } from '../sync/storage/index.js';

export class SyncOrchestrator {
  private sourcePool: PostgresPool | null = null;
  private targetPool: PostgresPool | null = null;
  private sourceSupabase: SupabaseClient | null = null;
  private targetSupabase: SupabaseClient | null = null;
  private tempFileManager: TempFileManager;
  private stepResults: StepResult[] = [];
  private startTime: number = 0;

  constructor(private config: Config) {
    this.tempFileManager = new TempFileManager(config.tempDir);
  }

  async execute(): Promise<SyncResult> {
    this.startTime = Date.now();
    this.stepResults = [];

    try {
      // Initialize temp files
      await this.tempFileManager.init();

      // Run sync steps
      await this.runStep('validate-connections', () => this.validateConnections());

      if (this.config.options.components.roles) {
        await this.runStep('sync-roles', () => this.syncRoles());
      }

      if (this.config.options.components.schema) {
        await this.runStep('sync-schema', () => this.syncSchema());
      }

      // Auth sync must run BEFORE data sync because:
      // - Auth creates users via Admin API which triggers database triggers
      // - These triggers may create rows in tables like user_roles
      // - Data sync will then overwrite those trigger-created rows with the correct source data
      if (this.config.options.components.auth) {
        await this.runStep('sync-auth', () => this.syncAuth());
      }

      if (this.config.options.components.data) {
        await this.runStep('sync-data', () => this.syncData());
        await this.runStep('reset-sequences', () => this.resetSequences());
      }

      if (this.config.options.components.storage) {
        await this.runStep('sync-storage', () => this.syncStorage());
      }

      await this.runStep('verify', () => this.verify());
      await this.runStep('cleanup', () => this.cleanup());

      return this.buildResult(true);
    } catch (error) {
      logger.error('Sync failed', { error: (error as Error).message });
      await this.cleanup();
      return this.buildResult(false, error as Error);
    }
  }

  private async runStep(name: string, fn: () => Promise<void>): Promise<void> {
    const stepStart = Date.now();
    logger.info(`Starting step: ${name}`);

    try {
      await fn();
      const duration = Date.now() - stepStart;
      this.stepResults.push({ name, success: true, duration });
      logger.info(`Completed step: ${name} (${(duration / 1000).toFixed(2)}s)`);
    } catch (error) {
      const duration = Date.now() - stepStart;
      this.stepResults.push({
        name,
        success: false,
        duration,
        error: error as Error,
      });
      logger.error(`Failed step: ${name}`, { error: (error as Error).message });
      throw error;
    }
  }

  private async createPoolWithSslFallback(
    connection: SupabaseConnection,
    label: string
  ): Promise<PostgresPool> {
    // First try with SSL (default behavior)
    let pool = createPostgresPool(connection);
    const result = await testPostgresConnection(pool);

    if (result.success) {
      return pool;
    }

    // If SSL error, retry without SSL
    if (result.error && result.error.includes('SSL')) {
      logger.info(`${label}: SSL not supported, retrying without SSL...`);
      await pool.end();

      // Remember this host doesn't support SSL
      setSslPreference(connection.dbUrl, false);

      pool = createPostgresPool(connection, true);
      const retryResult = await testPostgresConnection(pool);

      if (retryResult.success) {
        logger.info(`${label}: Connected successfully without SSL`);
        return pool;
      }

      await pool.end();
      throw new SyncError(
        `Failed to connect to ${label}: ${retryResult.error || 'Unknown error'}`,
        ErrorCategory.CONNECTION,
        'validate-connections',
        false
      );
    }

    await pool.end();
    throw new SyncError(
      `Failed to connect to ${label}: ${result.error || 'Unknown error'}`,
      ErrorCategory.CONNECTION,
      'validate-connections',
      false
    );
  }

  private async validateConnections(): Promise<void> {
    logger.info('Validating connections...');

    // Create and test database pools with SSL fallback
    this.sourcePool = await this.createPoolWithSslFallback(
      this.config.source,
      'source database'
    );
    print.success('Source database connection OK');

    this.targetPool = await this.createPoolWithSslFallback(
      this.config.target,
      'target database'
    );
    print.success('Target database connection OK');

    // Create Supabase clients
    this.sourceSupabase = createSupabaseClient(this.config.source);
    this.targetSupabase = createSupabaseClient(this.config.target);

    // Test source Supabase API
    const sourceApiOk = await testSupabaseConnection(this.sourceSupabase);
    if (!sourceApiOk) {
      throw new SyncError(
        'Failed to connect to source Supabase API',
        ErrorCategory.CONNECTION,
        'validate-connections',
        false
      );
    }
    print.success('Source Supabase API connection OK');

    // Test target Supabase API
    const targetApiOk = await testSupabaseConnection(this.targetSupabase);
    if (!targetApiOk) {
      throw new SyncError(
        'Failed to connect to target Supabase API',
        ErrorCategory.CONNECTION,
        'validate-connections',
        false
      );
    }
    print.success('Target Supabase API connection OK');
  }

  private async syncRoles(): Promise<void> {
    const rolesSync = new RolesSync(this.config, this.tempFileManager);
    await rolesSync.sync();
  }

  private async syncSchema(): Promise<void> {
    const schemaSync = new SchemaSync(this.config, this.tempFileManager);
    await schemaSync.sync();
  }

  private async syncData(): Promise<void> {
    if (!this.sourcePool || !this.targetPool) throw new Error('Pools not initialized');
    const dataSync = new DataSync(this.config, this.tempFileManager, this.targetPool);
    await dataSync.sync(this.sourcePool);
  }

  private async resetSequences(): Promise<void> {
    if (!this.targetPool) throw new Error('Target pool not initialized');
    const sequenceSync = new SequenceSync(this.config, this.targetPool);
    await sequenceSync.sync();
  }

  private async syncAuth(): Promise<void> {
    if (!this.sourcePool || !this.targetPool || !this.targetSupabase) {
      throw new Error('Clients not initialized');
    }
    const authSync = new AuthSync(
      this.config,
      this.sourcePool,
      this.targetPool,
      this.targetSupabase
    );
    await authSync.sync();
  }

  private async syncStorage(): Promise<void> {
    if (!this.sourceSupabase || !this.targetSupabase) {
      throw new Error('Supabase clients not initialized');
    }
    const storageSync = new StorageSync(
      this.config,
      this.sourceSupabase,
      this.targetSupabase,
      this.targetPool || undefined
    );
    await storageSync.sync();
  }

  private async verify(): Promise<void> {
    logger.info('Verifying sync...');

    if (!this.targetPool) return;

    // Verify sequences
    if (this.config.options.components.data) {
      const sequenceSync = new SequenceSync(this.config, this.targetPool);
      await sequenceSync.verifySequences();
    }

    print.success('Verification complete');
  }

  private async cleanup(): Promise<void> {
    logger.info('Cleaning up...');

    // Close database connections
    if (this.sourcePool) {
      await this.sourcePool.end();
    }
    if (this.targetPool) {
      await this.targetPool.end();
    }

    // Clean up temp files
    await this.tempFileManager.cleanup();

    print.success('Cleanup complete');
  }

  private buildResult(success: boolean, error?: Error): SyncResult {
    const duration = Date.now() - this.startTime;

    return {
      success,
      steps: this.stepResults,
      duration,
      errors: error
        ? [
            new SyncError(
              error.message,
              ErrorCategory.UNKNOWN,
              'orchestrator',
              false,
              error
            ),
          ]
        : [],
    };
  }
}
