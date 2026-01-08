import inquirer from 'inquirer';
import chalk from 'chalk';
import ora from 'ora';
import type { Config, SupabaseConnection } from '../types/config.js';
import { print } from '../utils/logger.js';
import { createPostgresPool, testPostgresConnection, setSslPreference } from '../clients/postgres-client.js';
import { createSupabaseClient, testSupabaseConnection } from '../clients/supabase-client.js';

async function testDatabaseUrl(dbUrl: string): Promise<boolean> {
  const spinner = ora('Testing database connection...').start();

  try {
    // First try with SSL (for non-localhost)
    const pool = createPostgresPool({ dbUrl } as SupabaseConnection);
    const result = await testPostgresConnection(pool);
    await pool.end();

    if (result.success) {
      spinner.succeed('Database connection successful');
      return true;
    }

    // If SSL error, retry without SSL
    if (result.error && result.error.includes('SSL')) {
      spinner.text = 'SSL not supported, retrying without SSL...';

      // Remember this host doesn't support SSL
      setSslPreference(dbUrl, false);

      const poolNoSsl = createPostgresPool({ dbUrl } as SupabaseConnection, true);
      const resultNoSsl = await testPostgresConnection(poolNoSsl);
      await poolNoSsl.end();

      if (resultNoSsl.success) {
        spinner.succeed('Database connection successful (without SSL)');
        return true;
      } else {
        spinner.fail(`Database connection failed: ${resultNoSsl.error || 'Unknown error'}`);
        return false;
      }
    }

    spinner.fail(`Database connection failed: ${result.error || 'Unknown error'}`);
    return false;
  } catch (error) {
    spinner.fail(`Database connection failed: ${(error as Error).message}`);
    return false;
  }
}

async function testSupabaseApi(apiUrl: string, serviceRoleKey: string): Promise<boolean> {
  const spinner = ora('Testing Supabase API connection...').start();

  try {
    const client = createSupabaseClient({
      dbUrl: '',
      apiUrl,
      serviceRoleKey
    } as SupabaseConnection);
    const success = await testSupabaseConnection(client);

    if (success) {
      spinner.succeed('Supabase API connection successful');
      return true;
    } else {
      spinner.fail('Supabase API connection failed - check your service role key');
      return false;
    }
  } catch (error) {
    spinner.fail(`Supabase API connection failed: ${(error as Error).message}`);
    return false;
  }
}

async function promptWithRetry<T>(
  promptFn: () => Promise<T>,
  testFn: (value: T) => Promise<boolean>,
  maxRetries: number = 3
): Promise<T> {
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    const value = await promptFn();
    const success = await testFn(value);

    if (success) {
      return value;
    }

    if (attempt < maxRetries) {
      const { retry } = await inquirer.prompt([{
        type: 'confirm',
        name: 'retry',
        message: 'Would you like to try again?',
        default: true,
      }]);

      if (!retry) {
        throw new Error('Connection test failed and user chose not to retry');
      }
    } else {
      throw new Error(`Connection test failed after ${maxRetries} attempts`);
    }
  }

  throw new Error('Unexpected error in promptWithRetry');
}

export async function gatherSourceConfig(): Promise<SupabaseConnection> {
  print.header('Source Supabase Configuration');

  console.log(chalk.gray('Enter your source database connection details.\n'));
  console.log(chalk.gray('Database URL format: postgresql://user:password@host:port/database\n'));

  // Get and test database URL
  const dbUrl = await promptWithRetry(
    async () => {
      const { dbUrl } = await inquirer.prompt([{
        type: 'password',
        name: 'dbUrl',
        message: 'Enter source database URL:',
        mask: '*',
        validate: (input: string) => {
          if (!input) return 'Database URL is required';
          if (!input.startsWith('postgresql://') && !input.startsWith('postgres://')) {
            return 'URL must start with postgresql:// or postgres://';
          }
          return true;
        },
      }]);
      return dbUrl;
    },
    testDatabaseUrl
  );

  // Get and test Supabase API
  const { apiUrl } = await inquirer.prompt([{
    type: 'input',
    name: 'apiUrl',
    message: 'Enter source Supabase API URL:',
    validate: (input: string) => {
      if (!input) return 'API URL is required';
      if (!input.startsWith('http://') && !input.startsWith('https://')) {
        return 'URL must start with http:// or https://';
      }
      return true;
    },
  }]);

  const serviceRoleKey = await promptWithRetry(
    async () => {
      const { key } = await inquirer.prompt([{
        type: 'password',
        name: 'key',
        message: 'Enter source service role key:',
        mask: '*',
        validate: (input: string) => {
          if (!input) return 'Service role key is required';
          if (!input.includes('.')) return 'Invalid service role key format';
          return true;
        },
      }]);
      return key;
    },
    async (key) => testSupabaseApi(apiUrl, key)
  );

  return {
    dbUrl,
    apiUrl,
    serviceRoleKey,
    port: 5432,
  };
}

export async function gatherTargetConfig(): Promise<SupabaseConnection> {
  print.header('Target Supabase Configuration');

  console.log(chalk.gray('Enter your target database connection details.\n'));
  console.log(chalk.yellow('WARNING: All data on the target will be replaced!\n'));

  // Get and test database URL
  const dbUrl = await promptWithRetry(
    async () => {
      const { dbUrl } = await inquirer.prompt([{
        type: 'password',
        name: 'dbUrl',
        message: 'Enter target database URL:',
        mask: '*',
        validate: (input: string) => {
          if (!input) return 'Database URL is required';
          if (!input.startsWith('postgresql://') && !input.startsWith('postgres://')) {
            return 'URL must start with postgresql:// or postgres://';
          }
          return true;
        },
      }]);
      return dbUrl;
    },
    testDatabaseUrl
  );

  // Get and test Supabase API
  const { apiUrl } = await inquirer.prompt([{
    type: 'input',
    name: 'apiUrl',
    message: 'Enter target Supabase API URL:',
    validate: (input: string) => {
      if (!input) return 'API URL is required';
      if (!input.startsWith('http://') && !input.startsWith('https://')) {
        return 'URL must start with http:// or https://';
      }
      return true;
    },
  }]);

  const serviceRoleKey = await promptWithRetry(
    async () => {
      const { key } = await inquirer.prompt([{
        type: 'password',
        name: 'key',
        message: 'Enter target service role key:',
        mask: '*',
        validate: (input: string) => {
          if (!input) return 'Service role key is required';
          if (!input.includes('.')) return 'Invalid service role key format';
          return true;
        },
      }]);
      return key;
    },
    async (key) => testSupabaseApi(apiUrl, key)
  );

  return {
    dbUrl,
    apiUrl,
    serviceRoleKey,
    port: 5432,
  };
}

export async function gatherSyncOptions(): Promise<Partial<Config['options']>> {
  print.header('Sync Options');

  const { components } = await inquirer.prompt([
    {
      type: 'checkbox',
      name: 'components',
      message: 'Select components to sync:',
      choices: [
        { name: 'Database Schema', value: 'schema', checked: true },
        { name: 'Database Data', value: 'data', checked: true },
        { name: 'Database Roles', value: 'roles', checked: true },
        { name: 'Auth Users', value: 'auth', checked: true },
        { name: 'Storage Buckets & Files', value: 'storage', checked: true },
      ],
    },
  ]);

  return {
    components: {
      schema: components.includes('schema'),
      data: components.includes('data'),
      roles: components.includes('roles'),
      auth: components.includes('auth'),
      storage: components.includes('storage'),
    },
  };
}

export async function confirmDestructiveOperation(targetDescription: string): Promise<boolean> {
  console.log('\n');
  console.log(chalk.red.bold('⚠️  WARNING: DESTRUCTIVE OPERATION'));
  console.log(chalk.red('─'.repeat(50)));
  console.log(chalk.yellow(`This will REPLACE ALL DATA on the target:`));
  console.log(chalk.white.bold(`  ${targetDescription}`));
  console.log(chalk.red('─'.repeat(50)));
  console.log('\n');

  const { confirmed } = await inquirer.prompt([
    {
      type: 'confirm',
      name: 'confirmed',
      message: chalk.red('Are you absolutely sure you want to proceed?'),
      default: false,
    },
  ]);

  if (confirmed) {
    const { doubleConfirm } = await inquirer.prompt([
      {
        type: 'input',
        name: 'doubleConfirm',
        message: chalk.red('Type "SYNC" to confirm:'),
      },
    ]);
    return doubleConfirm === 'SYNC';
  }

  return false;
}

export async function gatherFullConfig(): Promise<Config> {
  const source = await gatherSourceConfig();
  const target = await gatherTargetConfig();
  const options = await gatherSyncOptions();

  return {
    source,
    target,
    options: {
      components: options.components || {
        schema: true,
        data: true,
        auth: true,
        storage: true,
        roles: true,
      },
      database: {
        includeSchemas: ['public', 'auth', 'storage'],
        excludeSchemas: ['pg_catalog', 'information_schema', 'pg_toast'],
        excludeTables: [],
      },
      storage: {
        excludeBuckets: [],
        maxFileSizeMB: 50,
        concurrency: 5,
      },
      auth: {
        preservePasswordHashes: true,
        migrateIdentities: true,
        skipSessions: true,
      },
    },
    mode: 'interactive',
    dryRun: false,
    verbose: false,
    tempDir: '/tmp/supabase-sync',
  };
}

export function createSpinner(text: string): ReturnType<typeof ora> {
  return ora({
    text,
    spinner: 'dots',
  });
}
