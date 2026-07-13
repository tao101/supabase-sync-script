import inquirer from 'inquirer';
import chalk from 'chalk';
import ora from 'ora';
import { ConfigSchema, type Config, type SupabaseConnection } from '../types/config.js';
import { print } from '../utils/logger.js';
import { createPostgresPool, testPostgresConnection } from '../clients/postgres-client.js';
import { createSupabaseClient, testSupabaseConnection } from '../clients/supabase-client.js';
import { isLegacyJwtKey, isNewSecretKey, isValidApiKey } from '../config/index.js';

// UI-specific helper
function getKeyTypeLabel(key: string): string {
  if (isNewSecretKey(key)) return 'secret key';
  if (isLegacyJwtKey(key)) return 'service role key';
  return 'unknown key type';
}

async function testDatabaseUrl(dbUrl: string): Promise<boolean> {
  const spinner = ora('Testing database connection...').start();

  try {
    const pool = createPostgresPool({ dbUrl } as SupabaseConnection);
    const result = await testPostgresConnection(pool);
    await pool.end();

    if (result.success) {
      spinner.succeed('Database connection successful');
      return true;
    }

    const tlsHelp = result.error?.includes('SSL')
      ? ' Remote databases require verified TLS; use sslmode=disable only for intentional plaintext.'
      : '';
    spinner.fail(`Database connection failed: ${result.error || 'Unknown error'}.${tlsHelp}`);
    return false;
  } catch (error) {
    spinner.fail(`Database connection failed: ${(error as Error).message}`);
    return false;
  }
}

async function testSupabaseApi(apiUrl: string, apiKey: string): Promise<boolean> {
  const keyType = getKeyTypeLabel(apiKey);
  const spinner = ora(`Testing Supabase API connection with ${keyType}...`).start();

  try {
    // Build connection based on key type
    const connection: SupabaseConnection = {
      dbUrl: '',
      apiUrl,
      port: 5432,
    } as SupabaseConnection;

    if (isNewSecretKey(apiKey)) {
      connection.secretKey = apiKey;
    } else {
      connection.serviceRoleKey = apiKey;
    }

    const client = createSupabaseClient(connection, 15_000);
    const success = await testSupabaseConnection(client);

    if (success) {
      spinner.succeed(`Supabase API connection successful (using ${keyType})`);
      return true;
    } else {
      spinner.fail(`Supabase API connection failed - check your ${keyType}`);
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

export async function gatherSourceConfig(includeApi: boolean = true): Promise<SupabaseConnection> {
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

  const connection = { dbUrl, port: 5432 } as SupabaseConnection;
  if (!includeApi) return connection;

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

  const apiKey = await promptWithRetry(
    async () => {
      const { key } = await inquirer.prompt([{
        type: 'password',
        name: 'key',
        message: 'Enter source API key (service_role key or sb_secret_... key):',
        mask: '*',
        validate: (input: string) => {
          if (!input) return 'API key is required';
          if (!isValidApiKey(input)) {
            return 'Invalid key format. Use JWT (legacy service_role) or sb_secret_... (new format)';
          }
          return true;
        },
      }]);
      return key;
    },
    async (key) => testSupabaseApi(apiUrl, key)
  );

  // Return connection with appropriate key field based on format
  connection.apiUrl = apiUrl;

  if (isNewSecretKey(apiKey)) {
    connection.secretKey = apiKey;
  } else {
    connection.serviceRoleKey = apiKey;
  }

  return connection;
}

export async function gatherTargetConfig(includeApi: boolean = true): Promise<SupabaseConnection> {
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

  const connection = { dbUrl, port: 5432 } as SupabaseConnection;
  if (!includeApi) return connection;

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

  const apiKey = await promptWithRetry(
    async () => {
      const { key } = await inquirer.prompt([{
        type: 'password',
        name: 'key',
        message: 'Enter target API key (service_role key or sb_secret_... key):',
        mask: '*',
        validate: (input: string) => {
          if (!input) return 'API key is required';
          if (!isValidApiKey(input)) {
            return 'Invalid key format. Use JWT (legacy service_role) or sb_secret_... (new format)';
          }
          return true;
        },
      }]);
      return key;
    },
    async (key) => testSupabaseApi(apiUrl, key)
  );

  // Return connection with appropriate key field based on format
  connection.apiUrl = apiUrl;

  if (isNewSecretKey(apiKey)) {
    connection.secretKey = apiKey;
  } else {
    connection.serviceRoleKey = apiKey;
  }

  return connection;
}

type SkippedComponents = Partial<Record<keyof Config['options']['components'], boolean>>;

export async function gatherSyncOptions(skipped: SkippedComponents = {}): Promise<Partial<Config['options']>> {
  print.header('Sync Options');
  if (Object.values(skipped).filter(Boolean).length === 5) {
    throw new Error('At least one sync component must remain enabled');
  }

  const { components } = await inquirer.prompt([
    {
      type: 'checkbox',
      name: 'components',
      message: 'Select components to sync:',
      validate: (selected: string[]) => selected.length > 0 || 'Select at least one component',
      choices: [
        { name: 'Database Schema', value: 'schema', checked: !skipped.schema, disabled: skipped.schema ? 'Skipped by command-line flag' : false },
        { name: 'Database Data', value: 'data', checked: !skipped.data, disabled: skipped.data ? 'Skipped by command-line flag' : false },
        { name: 'Database Roles', value: 'roles', checked: !skipped.roles, disabled: skipped.roles ? 'Skipped by command-line flag' : false },
        { name: 'Auth Users', value: 'auth', checked: !skipped.auth, disabled: skipped.auth ? 'Skipped by command-line flag' : false },
        { name: 'Storage Buckets & Files', value: 'storage', checked: !skipped.storage, disabled: skipped.storage ? 'Skipped by command-line flag' : false },
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

export async function confirmDestructiveOperation(
  targetDescription: string,
  config?: Config
): Promise<boolean> {
  const changes = config ? [
    config.options.components.schema && `Replace application schemas: ${config.options.database.includeSchemas.join(', ')}`,
    config.options.components.data && 'Replace rows in included application tables',
    config.options.components.auth && 'Replace auth users/identities and clear existing login sessions',
    config.options.components.storage && 'Upsert Storage buckets/files; target-only objects remain',
    config.options.components.roles && 'Import custom database roles; existing name conflicts fail',
  ].filter(Boolean) : ['Replace data in the selected target scopes'];

  console.log('\n');
  console.log(chalk.red.bold('⚠️  CONFIRM TARGET CHANGES'));
  console.log(chalk.red('─'.repeat(50)));
  console.log(chalk.yellow('This run will change the target:'));
  console.log(chalk.white.bold(`  ${targetDescription}`));
  for (const change of changes) console.log(chalk.yellow(`  • ${change}`));
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

export async function gatherFullConfig(skipped: SkippedComponents = {}): Promise<Config> {
  const options = await gatherSyncOptions(skipped);
  const includeApi = options.components?.storage ?? true;
  const source = await gatherSourceConfig(includeApi);
  const target = await gatherTargetConfig(includeApi);

  return ConfigSchema.parse({
    source,
    target,
    options,
  });
}

export function createSpinner(text: string): ReturnType<typeof ora> {
  return ora({
    text,
    spinner: 'dots',
  });
}
