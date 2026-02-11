import type { Config } from '../types/config.js';
import { loadConfig, validateConfig } from '../config/index.js';
import { logger, print } from '../utils/logger.js';

export async function loadCIConfig(options: {
  configPath?: string;
  overrides?: Partial<Config>;
}): Promise<Config> {
  const config = await loadConfig({
    configPath: options.configPath,
    overrides: {
      mode: 'ci',
      ...options.overrides,
    },
  });

  // Validate in CI mode
  const errors = validateConfig(config);
  if (errors.length > 0) {
    logger.error('Configuration validation failed:');
    errors.forEach(e => logger.error(`  - ${e}`));
    throw new Error('Configuration validation failed');
  }

  return config;
}

export function logCIProgress(step: string, status: 'start' | 'success' | 'error', message?: string): void {
  const timestamp = new Date().toISOString();

  switch (status) {
    case 'start':
      console.log(`[${timestamp}] [START] ${step}`);
      break;
    case 'success':
      console.log(`[${timestamp}] [SUCCESS] ${step}${message ? `: ${message}` : ''}`);
      break;
    case 'error':
      console.log(`[${timestamp}] [ERROR] ${step}${message ? `: ${message}` : ''}`);
      break;
  }
}

export function logCIConnectionTest(
  name: string,
  status: 'testing' | 'success' | 'failed',
  message?: string
): void {
  const timestamp = new Date().toISOString();
  switch (status) {
    case 'testing':
      console.log(`[${timestamp}] [TESTING] ${name}`);
      break;
    case 'success':
      console.log(`[${timestamp}] [OK] ${name}${message ? `: ${message}` : ''}`);
      break;
    case 'failed':
      console.log(`[${timestamp}] [FAILED] ${name}${message ? `: ${message}` : ''}`);
      break;
  }
}

export function printCIValidationResult(isValid: boolean, errors: string[]): void {
  if (isValid) {
    console.log('[OK] Configuration is valid');
  } else {
    console.log('[ERROR] Configuration validation failed:');
    errors.forEach(e => console.log(`  - ${e}`));
  }
}

export function printCISummary(results: {
  success: boolean;
  partialSuccess?: boolean;
  duration: number;
  steps: { name: string; success: boolean; duration: number }[];
  warnings?: string[];
}): void {
  console.log('\n');
  console.log('='.repeat(60));
  console.log('SYNC SUMMARY');
  console.log('='.repeat(60));

  let status = results.success ? 'SUCCESS' : 'FAILED';
  if (results.partialSuccess) status = 'PARTIAL_SUCCESS';
  console.log(`Status: ${status}`);
  console.log(`Duration: ${(results.duration / 1000).toFixed(2)}s`);
  console.log('-'.repeat(60));
  console.log('Steps:');

  for (const step of results.steps) {
    const stepStatus = step.success ? '✓' : '✗';
    const duration = (step.duration / 1000).toFixed(2);
    console.log(`  ${stepStatus} ${step.name} (${duration}s)`);
  }

  if (results.warnings && results.warnings.length > 0) {
    console.log('-'.repeat(60));
    console.log(`Warnings (${results.warnings.length}):`);
    for (const w of results.warnings.slice(0, 20)) {
      console.log(`  ! ${w}`);
    }
    if (results.warnings.length > 20) {
      console.log(`  ... and ${results.warnings.length - 20} more`);
    }
  }

  console.log('='.repeat(60));

  if (!results.success) {
    process.exitCode = 1;
  }
}
