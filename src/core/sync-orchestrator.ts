import type { SupabaseClient } from '@supabase/supabase-js';
import type { Config } from '../types/config.js';
import { SyncStep, SyncResult, StepResult, SyncError, ErrorCategory } from '../types/sync.js';
import { logger, print } from '../utils/logger.js';
import { TempFileManager } from '../utils/temp-files.js';
import { createSupabaseClient, testSupabaseConnection } from '../clients/supabase-client.js';
import { createPostgresPool, testPostgresConnection, PostgresPool } from '../clients/postgres-client.js';
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

      if (this.config.options.components.data) {
        await this.runStep('sync-data', () => this.syncData());
        await this.runStep('reset-sequences', () => this.resetSequences());
      }

      if (this.config.options.components.auth) {
        await this.runStep('sync-auth', () => this.syncAuth());
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

  private async validateConnections(): Promise<void> {
    logger.info('Validating connections...');

    // Create clients
    this.sourcePool = createPostgresPool(this.config.source, true);
    this.targetPool = createPostgresPool(this.config.target);
    this.sourceSupabase = createSupabaseClient(this.config.source);
    this.targetSupabase = createSupabaseClient(this.config.target);

    // Test source database
    const sourceDbOk = await testPostgresConnection(this.sourcePool);
    if (!sourceDbOk) {
      throw new SyncError(
        'Failed to connect to source database',
        ErrorCategory.CONNECTION,
        'validate-connections',
        false
      );
    }
    print.success('Source database connection OK');

    // Test target database
    const targetDbOk = await testPostgresConnection(this.targetPool);
    if (!targetDbOk) {
      throw new SyncError(
        'Failed to connect to target database',
        ErrorCategory.CONNECTION,
        'validate-connections',
        false
      );
    }
    print.success('Target database connection OK');

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
    if (!this.targetPool) throw new Error('Target pool not initialized');
    const dataSync = new DataSync(this.config, this.tempFileManager, this.targetPool);
    await dataSync.sync();
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
      this.targetSupabase
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
