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

export function printCISummary(results: {
  success: boolean;
  duration: number;
  steps: { name: string; success: boolean; duration: number }[];
}): void {
  console.log('\n');
  console.log('='.repeat(60));
  console.log('SYNC SUMMARY');
  console.log('='.repeat(60));
  console.log(`Status: ${results.success ? 'SUCCESS' : 'FAILED'}`);
  console.log(`Duration: ${(results.duration / 1000).toFixed(2)}s`);
  console.log('-'.repeat(60));
  console.log('Steps:');

  for (const step of results.steps) {
    const status = step.success ? '✓' : '✗';
    const duration = (step.duration / 1000).toFixed(2);
    console.log(`  ${status} ${step.name} (${duration}s)`);
  }

  console.log('='.repeat(60));

  if (!results.success) {
    process.exitCode = 1;
  }
}
