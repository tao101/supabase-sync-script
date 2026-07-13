import type { SupabaseClient } from '@supabase/supabase-js';
import type { Config, SupabaseConnection } from '../types/config.js';
import { SyncResult, StepResult, SyncError, ErrorCategory } from '../types/sync.js';
import { logger, print } from '../utils/logger.js';
import { TempFileManager } from '../utils/temp-files.js';
import { createSupabaseClient, testSupabaseConnection } from '../clients/supabase-client.js';
import { createPostgresPool, testPostgresConnection, PostgresPool } from '../clients/postgres-client.js';
import { SchemaSync, DataSync, SequenceSync, RolesSync } from '../sync/database/index.js';
import { AuthSync } from '../sync/auth/index.js';
import { StorageSync } from '../sync/storage/index.js';
import { validateConfig } from '../config/index.js';
import { requiresPostgresTools, testPostgresTools } from '../utils/postgres-tools.js';

const DRY_RUN_STEPS = new Set([
  'sync-roles', 'sync-schema', 'sync-auth', 'sync-data', 'reset-sequences', 'sync-storage',
]);
const STORAGE_REQUEST_TIMEOUT_MS = 5 * 60_000;

export class SyncOrchestrator {
  private sourcePool: PostgresPool | null = null;
  private targetPool: PostgresPool | null = null;
  private sourceSupabase: SupabaseClient | null = null;
  private targetSupabase: SupabaseClient | null = null;
  private authSync: AuthSync | null = null;
  private tempFileManager: TempFileManager;
  private stepResults: StepResult[] = [];
  private warnings: string[] = [];
  private startTime: number = 0;

  constructor(private config: Config) {
    this.tempFileManager = new TempFileManager(config.tempDir);
  }

  async execute(): Promise<SyncResult> {
    this.startTime = Date.now();
    this.stepResults = [];
    this.warnings = [];
    this.authSync = null;

    try {
      await this.runStep('validate-config', async () => {
        const errors = validateConfig(this.config);
        if (errors.length > 0) {
          throw new SyncError(
            `Configuration validation failed: ${errors.join('; ')}`,
            ErrorCategory.VALIDATION,
            'validate-config',
            false
          );
        }
      });

      if (requiresPostgresTools(this.config)) {
        await this.runStep('validate-postgres-tools', async () => {
          const result = await testPostgresTools(this.config);
          if (!result.success) {
            throw new SyncError(
              result.error || 'PostgreSQL client-tool validation failed',
              ErrorCategory.CONNECTION,
              'validate-postgres-tools',
              false
            );
          }
        });
      }

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

      // Auth must exist before application rows that reference auth.users are imported.
      if (this.config.options.components.auth) {
        await this.runStep('sync-auth', () => this.syncAuth());
      }

      if (this.config.options.components.data) {
        await this.runStep('sync-data', () => this.syncData());
        await this.runStep('reset-sequences', () => this.resetSequences());
        if (this.config.options.components.auth && !this.config.dryRun) {
          await this.runStep('cleanup-auth-users', () => this.cleanupAuthUsers());
        }
      }

      if (this.config.options.components.storage) {
        await this.runStep('sync-storage', () => this.syncStorage());
      }

      if (this.config.dryRun) {
        logger.info('[DRY RUN] Skipping post-sync target verification');
      } else {
        await this.runStep('verify', () => this.verify());
      }
      await this.runStep('cleanup', () => this.cleanup());

      return this.buildResult(true);
    } catch (error) {
      logger.error('Sync failed', { error: (error as Error).message });
      try {
        await this.cleanup();
      } catch (cleanupError) {
        if (!(error instanceof SyncError && error.step === 'cleanup')) {
          const warning = `Cleanup also failed: ${String(cleanupError)}`;
          this.warnings.push(warning);
          logger.warn(warning);
        }
      }
      return this.buildResult(false, error as Error);
    }
  }

  private async runStep(name: string, fn: () => Promise<void>): Promise<void> {
    const stepStart = Date.now();
    const warningCount = this.warnings.length;
    logger.info(`Starting step: ${name}`);

    try {
      await fn();
      const duration = Date.now() - stepStart;
      const planned = this.config.dryRun && DRY_RUN_STEPS.has(name);
      this.stepResults.push({
        name,
        success: true,
        status: planned
          ? 'planned'
          : this.warnings.length > warningCount ? 'warning' : 'completed',
        duration,
      });
      logger.info(`Completed step: ${name} (${(duration / 1000).toFixed(2)}s)`);
    } catch (error) {
      const duration = Date.now() - stepStart;
      this.stepResults.push({
        name,
        success: false,
        status: 'failed',
        duration,
        error: error as Error,
      });
      logger.error(`Failed step: ${name}`, { error: (error as Error).message });
      throw error;
    }
  }

  private async createTestedPool(
    connection: SupabaseConnection,
    label: string
  ): Promise<PostgresPool> {
    const pool = createPostgresPool(connection);
    const result = await testPostgresConnection(pool);

    if (result.success) {
      return pool;
    }

    await pool.end();
    const tlsHelp = result.error?.includes('SSL')
      ? ' Remote databases require verified TLS; configure a trusted certificate or explicitly add sslmode=disable only for intentional plaintext.'
      : '';
    throw new SyncError(
      `Failed to connect to ${label}: ${result.error || 'Unknown error'}.${tlsHelp}`,
      ErrorCategory.CONNECTION,
      'validate-connections',
      false
    );
  }

  private async validateConnections(): Promise<void> {
    logger.info('Validating connections...');

    // Create and test database pools with SSL fallback
    this.sourcePool = await this.createTestedPool(
      this.config.source,
      'source database'
    );
    print.success('Source database connection OK');

    this.targetPool = await this.createTestedPool(
      this.config.target,
      'target database'
    );
    print.success('Target database connection OK');

    if (this.config.options.components.storage) {
      const sourceApiOk = await testSupabaseConnection(
        createSupabaseClient(this.config.source, 15_000)
      );
      if (!sourceApiOk) {
        throw new SyncError(
          'Failed to connect to source Supabase API',
          ErrorCategory.CONNECTION,
          'validate-connections',
          false
        );
      }
      print.success('Source Supabase API connection OK');

      const targetApiOk = await testSupabaseConnection(
        createSupabaseClient(this.config.target, 15_000)
      );
      if (!targetApiOk) {
        throw new SyncError(
          'Failed to connect to target Supabase API',
          ErrorCategory.CONNECTION,
          'validate-connections',
          false
        );
      }
      print.success('Target Supabase API connection OK');

      this.sourceSupabase = createSupabaseClient(
        this.config.source,
        STORAGE_REQUEST_TIMEOUT_MS
      );
      this.targetSupabase = createSupabaseClient(
        this.config.target,
        STORAGE_REQUEST_TIMEOUT_MS
      );
    }
  }

  private async syncRoles(): Promise<void> {
    const rolesSync = new RolesSync(this.config, this.tempFileManager);
    await rolesSync.sync();
  }

  private async syncSchema(): Promise<void> {
    if (!this.sourcePool || !this.targetPool) throw new Error('Pools not initialized');
    const schemaSync = new SchemaSync(
      this.config,
      this.tempFileManager,
      this.targetPool,
      this.sourcePool
    );
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
    if (!this.sourcePool || !this.targetPool) {
      throw new Error('Clients not initialized');
    }
    this.authSync = new AuthSync(
      this.config,
      this.sourcePool,
      this.targetPool
    );
    await this.authSync.sync();
  }

  private async cleanupAuthUsers(): Promise<void> {
    if (!this.authSync) throw new Error('Auth sync not initialized');
    await this.authSync.cleanupTargetOnlyUsers();
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

    if (this.config.options.components.data) {
      // Row count verification is handled by DataSync.sync() directly after import
      const dataSync = new DataSync(this.config, this.tempFileManager, this.targetPool);

      // Verify foreign key integrity - FAIL if orphaned records found
      const fkValid = await dataSync.verifyForeignKeys();
      if (!fkValid) {
        throw new SyncError(
          'Data verification failed: foreign key violations detected (orphaned records)',
          ErrorCategory.VALIDATION,
          'verify',
          false
        );
      }

      // Verify sequences - FAIL if invalid
      const sequenceSync = new SequenceSync(this.config, this.targetPool);
      const sequencesValid = await sequenceSync.verifySequences();
      if (!sequencesValid) {
        throw new SyncError(
          'Data verification failed: sequences are invalid',
          ErrorCategory.VALIDATION,
          'verify',
          false
        );
      }
    }

    print.success('Verification complete');
  }

  private async cleanup(): Promise<void> {
    logger.info('Cleaning up...');

    const sourcePool = this.sourcePool;
    const targetPool = this.targetPool;
    this.sourcePool = null;
    this.targetPool = null;
    this.sourceSupabase = null;
    this.targetSupabase = null;

    const cleanupTasks = [
      ...(sourcePool ? [{ name: 'source database pool', run: sourcePool.end(), critical: false }] : []),
      ...(targetPool ? [{ name: 'target database pool', run: targetPool.end(), critical: false }] : []),
      { name: 'temporary files', run: this.tempFileManager.cleanup(), critical: true },
    ];
    const results = await Promise.allSettled(cleanupTasks.map(task => task.run));
    let criticalFailure: string | null = null;
    results.forEach((result, index) => {
      if (result.status === 'rejected') {
        const warning = `Failed to clean up ${cleanupTasks[index].name}: ${String(result.reason)}`;
        if (cleanupTasks[index].critical) {
          criticalFailure = warning;
          logger.error(warning);
        } else {
          this.warnings.push(warning);
          logger.warn(warning);
        }
      }
    });

    if (criticalFailure) {
      throw new SyncError(
        criticalFailure,
        ErrorCategory.UNKNOWN,
        'cleanup',
        false
      );
    }

    print.success('Cleanup complete');
  }

  private buildResult(success: boolean, error?: Error): SyncResult {
    const duration = Date.now() - this.startTime;

    return {
      success,
      partialSuccess: success && this.warnings.length > 0,
      steps: this.stepResults,
      duration,
      errors: error
        ? [
            error instanceof SyncError
              ? error
              : new SyncError(
                error.message,
                ErrorCategory.UNKNOWN,
                'orchestrator',
                false,
                error
              ),
          ]
        : [],
      warnings: this.warnings.length > 0 ? this.warnings : undefined,
    };
  }
}
