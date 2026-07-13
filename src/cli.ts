#!/usr/bin/env node

import { Command } from 'commander';
import { createRequire } from 'module';
import chalk from 'chalk';
import { loadConfig, validateConfig, ConnectionBuilder } from './config/index.js';
import { SyncOrchestrator } from './core/sync-orchestrator.js';
import {
  gatherFullConfig,
  confirmDestructiveOperation,
  createSpinner,
} from './modes/interactive-mode.js';
import { printCISummary, logCIConnectionTest, printCIValidationResult } from './modes/ci-mode.js';
import { logger, sanitizeErrorMessage, setLogLevel, print } from './utils/logger.js';
import { resolveCIMode } from './config/env.js';
import { testPostgresConnection, createPostgresPool } from './clients/postgres-client.js';
import { testSupabaseConnection, createSupabaseClient } from './clients/supabase-client.js';
import { requiresPostgresTools, testPostgresTools } from './utils/postgres-tools.js';

const program = new Command();
const { version } = createRequire(import.meta.url)('../package.json') as { version: string };

program
  .name('supabase-sync')
  .description('Full migration sync between Supabase instances')
  .version(version);

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

      let config = options.config
        ? await loadConfig({
          configPath: options.config,
          overrides: {
            mode: options.ci ? 'ci' : undefined,
            dryRun: options.dryRun,
            verbose: options.verbose,
          },
        })
        : undefined;
      const isCI = resolveCIMode(options.ci, config?.mode);

      if (isCI) {
        config ??= await loadConfig({
          configPath: options.config,
          overrides: {
            mode: 'ci',
            dryRun: options.dryRun,
            verbose: options.verbose,
          },
        });
        config.mode = 'ci';
      } else {
        // Interactive mode
        print.header('Supabase Sync Script');
        console.log(chalk.gray('This tool will sync data between Supabase instances.\n'));

        if (!config) {
          // Fully interactive config gathering
          config = await gatherFullConfig({
            schema: options.skipSchema,
            data: options.skipData,
            auth: options.skipAuth,
            storage: options.skipStorage,
            roles: options.skipRoles,
          });
          config.dryRun = options.dryRun ?? config.dryRun;
          config.verbose = options.verbose ?? config.verbose;
        }
      }

      if (config.verbose) setLogLevel('debug');

      // Apply skip flags before validation so skipped components need no setup.
      if (options.skipSchema) config.options.components.schema = false;
      if (options.skipData) config.options.components.data = false;
      if (options.skipAuth) config.options.components.auth = false;
      if (options.skipStorage) config.options.components.storage = false;
      if (options.skipRoles) config.options.components.roles = false;

      // Validate config
      const errors = validateConfig(config);
      if (errors.length > 0) {
        print.error('Configuration validation failed:');
        errors.forEach(e => console.log(chalk.red(`  - ${e}`)));
        process.exit(1);
      }

      if (isCI) {
        const target = new ConnectionBuilder().getSafeDisplay(config.target);
        const components = Object.entries(config.options.components)
          .filter(([, enabled]) => enabled)
          .map(([name]) => name)
          .join(', ');
        console.log(`Plan: ${config.dryRun ? 'DRY RUN' : 'APPLY'}`);
        console.log(`Target database: ${target.dbUrl}`);
        if (config.options.components.storage) console.log(`Target Storage API: ${target.apiUrl}`);
        console.log(`Components: ${components}`);
      }

      if (!isCI) {
        if (config.dryRun) {
          print.info('Dry run: no target data will be changed');
        } else {
          const builder = new ConnectionBuilder();
          const target = builder.getSafeDisplay(config.target);
          const confirmed = await confirmDestructiveOperation(
            [
              `Database: ${target.dbUrl}`,
              config.options.components.storage ? `Storage API: ${target.apiUrl}` : '',
            ].filter(Boolean).join('\n  '),
            config
          );
          if (!confirmed) {
            print.warning('Sync cancelled by user');
            process.exit(0);
          }
        }
      }

      // Run sync
      const orchestrator = new SyncOrchestrator(config);
      const result = await orchestrator.execute();

      // Print summary
      if (isCI) {
        printCISummary(result);
      } else {
        print.header('Sync Summary');

        let statusText: string;
        if (result.success && !result.partialSuccess) {
          statusText = chalk.green('SUCCESS');
        } else if (result.partialSuccess) {
          statusText = chalk.yellow('PARTIAL SUCCESS');
        } else {
          statusText = chalk.red('FAILED');
        }
        console.log(chalk.bold(`Status: ${statusText}`));
        console.log(chalk.gray(`Duration: ${(result.duration / 1000).toFixed(2)}s`));
        console.log('\nSteps:');
        for (const step of result.steps) {
          const icon = step.status === 'planned'
            ? chalk.cyan('○')
            : step.status === 'warning' ? chalk.yellow('!') : step.success ? chalk.green('✓') : chalk.red('✗');
          const label = step.status === 'planned'
            ? chalk.cyan(' [PLANNED]')
            : step.status === 'warning' ? chalk.yellow(' [WARNING]') : '';
          const duration = chalk.gray(`(${(step.duration / 1000).toFixed(2)}s)`);
          console.log(`  ${icon} ${step.name}${label} ${duration}`);
        }

        if (result.warnings && result.warnings.length > 0) {
          console.log(chalk.yellow(`\nWarnings (${result.warnings.length}):`));
          for (const w of result.warnings.slice(0, 20)) {
            console.log(chalk.yellow(`  ! ${w}`));
          }
          if (result.warnings.length > 20) {
            console.log(chalk.yellow(`  ... and ${result.warnings.length - 20} more`));
          }
        }
      }

      if (!result.success) {
        process.exit(1);
      }
    } catch (error) {
      logger.error('Sync failed:', { error: (error as Error).message });
      if (options.verbose) {
        console.error(sanitizeErrorMessage((error as Error).stack || (error as Error).message));
      }
      process.exit(1);
    }
  });

program
  .command('validate')
  .description('Validate configuration without syncing')
  .option('-c, --config <path>', 'Path to config file')
  .option('--ci', 'Run in CI mode (non-interactive)')
  .action(async (options) => {
    try {
      const config = await loadConfig({
        configPath: options.config,
        overrides: { mode: options.ci ? 'ci' : undefined },
      });
      const isCI = resolveCIMode(options.ci, config.mode);
      const errors = validateConfig(config);

      if (errors.length > 0) {
        if (isCI) {
          printCIValidationResult(false, errors);
        } else {
          print.error('Configuration validation failed:');
          errors.forEach(e => console.log(chalk.red(`  - ${e}`)));
        }
        process.exit(1);
      }

      const builder = new ConnectionBuilder();

      if (isCI) {
        printCIValidationResult(true, []);
        console.log(`Source: ${JSON.stringify(builder.getSafeDisplay(config.source))}`);
        console.log(`Target: ${JSON.stringify(builder.getSafeDisplay(config.target))}`);
        console.log(`Components: ${JSON.stringify(config.options.components)}`);
      } else {
        print.success('Configuration is valid');
        console.log('\nSource:', builder.getSafeDisplay(config.source));
        console.log('Target:', builder.getSafeDisplay(config.target));
        console.log('Components:', config.options.components);
      }
    } catch (error) {
      print.error(`Validation failed: ${(error as Error).message}`);
      process.exit(1);
    }
  });

program
  .command('test-connection')
  .description('Test connections to source and target')
  .option('-c, --config <path>', 'Path to config file')
  .option('--ci', 'Run in CI mode (non-interactive)')
  .action(async (options) => {
    try {
      let isCI = resolveCIMode(options.ci);
      let config;

      if (options.config || isCI) {
        config = await loadConfig({
          configPath: options.config,
          overrides: { mode: options.ci ? 'ci' : undefined },
        });
        isCI = resolveCIMode(options.ci, config.mode);
      } else {
        config = await gatherFullConfig();
      }

      // Validate config (especially important now that serviceRoleKey is optional)
      const errors = validateConfig(config);
      if (errors.length > 0) {
        print.error('Configuration validation failed:');
        errors.forEach(e => print.error(`  - ${e}`));
        process.exit(1);
      }

      if (isCI) {
        // CI mode - use plain text output without spinners
        console.log('Testing Connections');
        console.log('='.repeat(40));

        // Test source database
        logCIConnectionTest('Source database', 'testing');
        const sourcePool = createPostgresPool(config.source);
        const sourceDbResult = await testPostgresConnection(sourcePool);
        await sourcePool.end();
        logCIConnectionTest('Source database', sourceDbResult.success ? 'success' : 'failed',
          sourceDbResult.success ? undefined : sourceDbResult.error || 'Unknown error');

        // Test target database
        logCIConnectionTest('Target database', 'testing');
        const targetPool = createPostgresPool(config.target);
        const targetDbResult = await testPostgresConnection(targetPool);
        await targetPool.end();
        logCIConnectionTest('Target database', targetDbResult.success ? 'success' : 'failed',
          targetDbResult.success ? undefined : targetDbResult.error || 'Unknown error');

        let postgresToolsOk = true;
        if (requiresPostgresTools(config)) {
          logCIConnectionTest('PostgreSQL client tools', 'testing');
          const toolsResult = await testPostgresTools(config);
          postgresToolsOk = toolsResult.success;
          logCIConnectionTest(
            'PostgreSQL client tools',
            toolsResult.success ? 'success' : 'failed',
            toolsResult.error
          );
        }

        let sourceApiOk = true;
        let targetApiOk = true;
        if (config.options.components.storage) {
          logCIConnectionTest('Source Supabase API', 'testing');
          sourceApiOk = await testSupabaseConnection(createSupabaseClient(config.source, 15_000));
          logCIConnectionTest('Source Supabase API', sourceApiOk ? 'success' : 'failed');

          logCIConnectionTest('Target Supabase API', 'testing');
          targetApiOk = await testSupabaseConnection(createSupabaseClient(config.target, 15_000));
          logCIConnectionTest('Target Supabase API', targetApiOk ? 'success' : 'failed');
        } else {
          logCIConnectionTest('Supabase APIs', 'skipped', 'storage sync is disabled');
        }

        // Summary
        const allOk = sourceDbResult.success && targetDbResult.success && postgresToolsOk && sourceApiOk && targetApiOk;
        console.log('='.repeat(40));
        if (allOk) {
          console.log('[OK] All connections successful');
        } else {
          console.log('[FAILED] Some connections failed');
          process.exit(1);
        }
      } else {
        // Interactive mode - use spinners
        print.header('Testing Connections');

        // Test source database
        const sourceSpinner = createSpinner('Testing source database...');
        sourceSpinner.start();
        const sourcePool = createPostgresPool(config.source);
        const sourceDbResult = await testPostgresConnection(sourcePool);
        await sourcePool.end();

        if (sourceDbResult.success) {
          sourceSpinner.succeed('Source database: OK');
        } else {
          sourceSpinner.fail(`Source database: FAILED - ${sourceDbResult.error || 'Unknown error'}`);
        }

        // Test target database
        const targetSpinner = createSpinner('Testing target database...');
        targetSpinner.start();
        const targetPool = createPostgresPool(config.target);
        const targetDbResult = await testPostgresConnection(targetPool);
        await targetPool.end();

        if (targetDbResult.success) {
          targetSpinner.succeed('Target database: OK');
        } else {
          targetSpinner.fail(`Target database: FAILED - ${targetDbResult.error || 'Unknown error'}`);
        }

        let postgresToolsOk = true;
        if (requiresPostgresTools(config)) {
          const toolsSpinner = createSpinner('Testing PostgreSQL client tools...');
          toolsSpinner.start();
          const toolsResult = await testPostgresTools(config);
          postgresToolsOk = toolsResult.success;
          toolsResult.success
            ? toolsSpinner.succeed('PostgreSQL client tools: OK')
            : toolsSpinner.fail(`PostgreSQL client tools: FAILED - ${toolsResult.error || 'Unknown error'}`);
        }

        let sourceApiOk = true;
        let targetApiOk = true;
        if (config.options.components.storage) {
          const sourceApiSpinner = createSpinner('Testing source Supabase API...');
          sourceApiSpinner.start();
          sourceApiOk = await testSupabaseConnection(createSupabaseClient(config.source, 15_000));
          sourceApiOk
            ? sourceApiSpinner.succeed('Source Supabase API: OK')
            : sourceApiSpinner.fail('Source Supabase API: FAILED');

          const targetApiSpinner = createSpinner('Testing target Supabase API...');
          targetApiSpinner.start();
          targetApiOk = await testSupabaseConnection(createSupabaseClient(config.target, 15_000));
          targetApiOk
            ? targetApiSpinner.succeed('Target Supabase API: OK')
            : targetApiSpinner.fail('Target Supabase API: FAILED');
        } else {
          print.info('Supabase API tests skipped because storage sync is disabled');
        }

        // Summary
        const allOk = sourceDbResult.success && targetDbResult.success && postgresToolsOk && sourceApiOk && targetApiOk;
        console.log();
        if (allOk) {
          print.success('All connections successful!');
        } else {
          print.error('Some connections failed');
          process.exit(1);
        }
      }
    } catch (error) {
      print.error(`Connection test failed: ${(error as Error).message}`);
      process.exit(1);
    }
  });

program.parse();
