#!/usr/bin/env node

import { Command } from 'commander';
import chalk from 'chalk';
import { loadConfig, validateConfig, ConnectionBuilder } from './config/index.js';
import { SyncOrchestrator } from './core/sync-orchestrator.js';
import {
  gatherFullConfig,
  confirmDestructiveOperation,
  createSpinner,
} from './modes/interactive-mode.js';
import { loadCIConfig, printCISummary, logCIProgress } from './modes/ci-mode.js';
import { logger, setLogLevel, print } from './utils/logger.js';
import { detectCIEnvironment } from './config/env.js';
import { testPostgresConnection, createPostgresPool } from './clients/postgres-client.js';
import { testSupabaseConnection, createSupabaseClient } from './clients/supabase-client.js';

const program = new Command();

program
  .name('supabase-sync')
  .description('Full migration sync between Supabase instances')
  .version('1.0.0');

program
  .command('sync')
  .description('Perform full sync from source to target')
  .option('-c, --config <path>', 'Path to config file')
  .option('--ci', 'Run in CI mode (non-interactive)')
  .option('--dry-run', 'Perform dry run without making changes')
  .option('--verbose', 'Enable verbose logging')
  .option('--skip-schema', 'Skip schema sync')
  .option('--skip-data', 'Skip data sync')
  .option('--skip-auth', 'Skip auth sync')
  .option('--skip-storage', 'Skip storage sync')
  .option('--skip-roles', 'Skip roles sync')
  .action(async (options) => {
    try {
      if (options.verbose) {
        setLogLevel('debug');
      }

      const isCI = options.ci || detectCIEnvironment();
      let config;

      if (isCI) {
        // CI mode
        config = await loadCIConfig({
          configPath: options.config,
          overrides: {
            dryRun: options.dryRun,
            verbose: options.verbose,
          },
        });

        // Apply skip flags
        if (options.skipSchema) config.options.components.schema = false;
        if (options.skipData) config.options.components.data = false;
        if (options.skipAuth) config.options.components.auth = false;
        if (options.skipStorage) config.options.components.storage = false;
        if (options.skipRoles) config.options.components.roles = false;
      } else {
        // Interactive mode
        print.header('Supabase Sync Script');
        console.log(chalk.gray('This tool will sync data between Supabase instances.\n'));

        if (options.config) {
          // Load from config but allow interactive confirmation
          config = await loadConfig({
            configPath: options.config,
            overrides: {
              mode: 'interactive',
              dryRun: options.dryRun,
              verbose: options.verbose,
            },
          });
        } else {
          // Fully interactive config gathering
          config = await gatherFullConfig();
          config.dryRun = options.dryRun ?? config.dryRun;
          config.verbose = options.verbose ?? config.verbose;
        }

        // Apply skip flags
        if (options.skipSchema) config.options.components.schema = false;
        if (options.skipData) config.options.components.data = false;
        if (options.skipAuth) config.options.components.auth = false;
        if (options.skipStorage) config.options.components.storage = false;
        if (options.skipRoles) config.options.components.roles = false;

        // Confirm destructive operation
        const builder = new ConnectionBuilder();
        const targetDisplay = builder.getSafeDisplay(config.target);
        const confirmed = await confirmDestructiveOperation(
          `${targetDisplay.type} at ${targetDisplay.host || targetDisplay.projectRef || 'localhost'}`
        );

        if (!confirmed) {
          print.warning('Sync cancelled by user');
          process.exit(0);
        }
      }

      // Validate config
      const errors = validateConfig(config);
      if (errors.length > 0) {
        print.error('Configuration validation failed:');
        errors.forEach(e => console.log(chalk.red(`  - ${e}`)));
        process.exit(1);
      }

      // Run sync
      const orchestrator = new SyncOrchestrator(config);
      const result = await orchestrator.execute();

      // Print summary
      if (isCI) {
        printCISummary(result);
      } else {
        print.header('Sync Summary');
        console.log(chalk.bold(`Status: ${result.success ? chalk.green('SUCCESS') : chalk.red('FAILED')}`));
        console.log(chalk.gray(`Duration: ${(result.duration / 1000).toFixed(2)}s`));
        console.log('\nSteps:');
        for (const step of result.steps) {
          const icon = step.success ? chalk.green('✓') : chalk.red('✗');
          const duration = chalk.gray(`(${(step.duration / 1000).toFixed(2)}s)`);
          console.log(`  ${icon} ${step.name} ${duration}`);
        }
      }

      if (!result.success) {
        process.exit(1);
      }
    } catch (error) {
      logger.error('Sync failed:', { error: (error as Error).message });
      if (options.verbose) {
        console.error(error);
      }
      process.exit(1);
    }
  });

program
  .command('validate')
  .description('Validate configuration without syncing')
  .option('-c, --config <path>', 'Path to config file')
  .action(async (options) => {
    try {
      const config = await loadConfig({ configPath: options.config });
      const errors = validateConfig(config);

      if (errors.length > 0) {
        print.error('Configuration validation failed:');
        errors.forEach(e => console.log(chalk.red(`  - ${e}`)));
        process.exit(1);
      }

      print.success('Configuration is valid');

      // Show summary
      const builder = new ConnectionBuilder();
      console.log('\nSource:', builder.getSafeDisplay(config.source));
      console.log('Target:', builder.getSafeDisplay(config.target));
      console.log('Components:', config.options.components);
    } catch (error) {
      print.error(`Validation failed: ${(error as Error).message}`);
      process.exit(1);
    }
  });

program
  .command('test-connection')
  .description('Test connections to source and target')
  .option('-c, --config <path>', 'Path to config file')
  .action(async (options) => {
    try {
      let config;

      if (options.config || detectCIEnvironment()) {
        config = await loadConfig({ configPath: options.config });
      } else {
        config = await gatherFullConfig();
      }

      print.header('Testing Connections');

      // Test source database
      const sourceSpinner = createSpinner('Testing source database...');
      sourceSpinner.start();
      const sourcePool = createPostgresPool(config.source, true);
      const sourceDbOk = await testPostgresConnection(sourcePool);
      await sourcePool.end();

      if (sourceDbOk) {
        sourceSpinner.succeed('Source database: OK');
      } else {
        sourceSpinner.fail('Source database: FAILED');
      }

      // Test target database
      const targetSpinner = createSpinner('Testing target database...');
      targetSpinner.start();
      const targetPool = createPostgresPool(config.target);
      const targetDbOk = await testPostgresConnection(targetPool);
      await targetPool.end();

      if (targetDbOk) {
        targetSpinner.succeed('Target database: OK');
      } else {
        targetSpinner.fail('Target database: FAILED');
      }

      // Test source Supabase API
      const sourceApiSpinner = createSpinner('Testing source Supabase API...');
      sourceApiSpinner.start();
      const sourceSupabase = createSupabaseClient(config.source);
      const sourceApiOk = await testSupabaseConnection(sourceSupabase);

      if (sourceApiOk) {
        sourceApiSpinner.succeed('Source Supabase API: OK');
      } else {
        sourceApiSpinner.fail('Source Supabase API: FAILED');
      }

      // Test target Supabase API
      const targetApiSpinner = createSpinner('Testing target Supabase API...');
      targetApiSpinner.start();
      const targetSupabase = createSupabaseClient(config.target);
      const targetApiOk = await testSupabaseConnection(targetSupabase);

      if (targetApiOk) {
        targetApiSpinner.succeed('Target Supabase API: OK');
      } else {
        targetApiSpinner.fail('Target Supabase API: FAILED');
      }

      // Summary
      const allOk = sourceDbOk && targetDbOk && sourceApiOk && targetApiOk;
      console.log();
      if (allOk) {
        print.success('All connections successful!');
      } else {
        print.error('Some connections failed');
        process.exit(1);
      }
    } catch (error) {
      print.error(`Connection test failed: ${(error as Error).message}`);
      process.exit(1);
    }
  });

program.parse();
