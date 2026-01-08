import inquirer from 'inquirer';
import chalk from 'chalk';
import ora from 'ora';
import type { Config, SupabaseConnection, ConnectionType } from '../types/config.js';
import { print } from '../utils/logger.js';

export async function gatherSourceConfig(): Promise<SupabaseConnection> {
  print.header('Source Supabase Configuration');

  const { sourceType } = await inquirer.prompt([
    {
      type: 'list',
      name: 'sourceType',
      message: 'Select source Supabase type:',
      choices: [
        { name: 'SaaS (Supabase Cloud)', value: 'saas' },
        { name: 'Self-hosted', value: 'self-hosted' },
        { name: 'Local (supabase start)', value: 'local' },
      ],
    },
  ]);

  const baseConfig: Partial<SupabaseConnection> = { type: sourceType as ConnectionType };

  if (sourceType === 'saas') {
    const { projectRef, dbPassword, serviceRoleKey } = await inquirer.prompt([
      {
        type: 'input',
        name: 'projectRef',
        message: 'Enter project reference (from Supabase dashboard URL):',
        validate: (input: string) => input.length > 0 || 'Project reference is required',
      },
      {
        type: 'password',
        name: 'dbPassword',
        message: 'Enter database password:',
        mask: '*',
        validate: (input: string) => input.length > 0 || 'Database password is required',
      },
      {
        type: 'password',
        name: 'serviceRoleKey',
        message: 'Enter service role key (from Project Settings > API):',
        mask: '*',
        validate: (input: string) => input.length > 0 || 'Service role key is required',
      },
    ]);
    return { ...baseConfig, projectRef, dbPassword, serviceRoleKey } as SupabaseConnection;
  }

  if (sourceType === 'self-hosted') {
    const { host, port, dbPassword, serviceRoleKey, apiUrl } = await inquirer.prompt([
      {
        type: 'input',
        name: 'host',
        message: 'Enter host (e.g., supabase.example.com):',
        validate: (input: string) => input.length > 0 || 'Host is required',
      },
      {
        type: 'number',
        name: 'port',
        message: 'Enter database port:',
        default: 5432,
      },
      {
        type: 'password',
        name: 'dbPassword',
        message: 'Enter database password:',
        mask: '*',
        validate: (input: string) => input.length > 0 || 'Database password is required',
      },
      {
        type: 'password',
        name: 'serviceRoleKey',
        message: 'Enter service role key:',
        mask: '*',
        validate: (input: string) => input.length > 0 || 'Service role key is required',
      },
      {
        type: 'input',
        name: 'apiUrl',
        message: 'Enter API URL (e.g., https://supabase.example.com):',
        validate: (input: string) => input.startsWith('http') || 'Valid URL is required',
      },
    ]);
    return { ...baseConfig, host, port, dbPassword, serviceRoleKey, apiUrl } as SupabaseConnection;
  }

  // Local
  const { dbPassword, serviceRoleKey, port } = await inquirer.prompt([
    {
      type: 'password',
      name: 'dbPassword',
      message: 'Enter database password:',
      mask: '*',
      default: 'postgres',
    },
    {
      type: 'password',
      name: 'serviceRoleKey',
      message: 'Enter service role key (from supabase status):',
      mask: '*',
      validate: (input: string) => input.length > 0 || 'Service role key is required',
    },
    {
      type: 'number',
      name: 'port',
      message: 'Enter database port:',
      default: 54322,
    },
  ]);
  return {
    ...baseConfig,
    dbPassword,
    serviceRoleKey,
    port,
    host: 'localhost',
    apiUrl: 'http://localhost:54321',
  } as SupabaseConnection;
}

export async function gatherTargetConfig(): Promise<SupabaseConnection> {
  print.header('Target Supabase Configuration');

  const { targetType } = await inquirer.prompt([
    {
      type: 'list',
      name: 'targetType',
      message: 'Select target Supabase type:',
      choices: [
        { name: 'Self-hosted', value: 'self-hosted' },
        { name: 'Local (supabase start)', value: 'local' },
      ],
    },
  ]);

  const baseConfig: Partial<SupabaseConnection> = { type: targetType as ConnectionType };

  if (targetType === 'self-hosted') {
    const { host, port, dbPassword, serviceRoleKey, apiUrl } = await inquirer.prompt([
      {
        type: 'input',
        name: 'host',
        message: 'Enter host (e.g., supabase.example.com):',
        validate: (input: string) => input.length > 0 || 'Host is required',
      },
      {
        type: 'number',
        name: 'port',
        message: 'Enter database port:',
        default: 5432,
      },
      {
        type: 'password',
        name: 'dbPassword',
        message: 'Enter database password:',
        mask: '*',
        validate: (input: string) => input.length > 0 || 'Database password is required',
      },
      {
        type: 'password',
        name: 'serviceRoleKey',
        message: 'Enter service role key:',
        mask: '*',
        validate: (input: string) => input.length > 0 || 'Service role key is required',
      },
      {
        type: 'input',
        name: 'apiUrl',
        message: 'Enter API URL (e.g., https://supabase.example.com):',
        validate: (input: string) => input.startsWith('http') || 'Valid URL is required',
      },
    ]);
    return { ...baseConfig, host, port, dbPassword, serviceRoleKey, apiUrl } as SupabaseConnection;
  }

  // Local
  const { dbPassword, serviceRoleKey, port } = await inquirer.prompt([
    {
      type: 'password',
      name: 'dbPassword',
      message: 'Enter database password:',
      mask: '*',
      default: 'postgres',
    },
    {
      type: 'password',
      name: 'serviceRoleKey',
      message: 'Enter service role key (from supabase status):',
      mask: '*',
      validate: (input: string) => input.length > 0 || 'Service role key is required',
    },
    {
      type: 'number',
      name: 'port',
      message: 'Enter database port:',
      default: 54322,
    },
  ]);
  return {
    ...baseConfig,
    dbPassword,
    serviceRoleKey,
    port,
    host: 'localhost',
    apiUrl: 'http://localhost:54321',
  } as SupabaseConnection;
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
