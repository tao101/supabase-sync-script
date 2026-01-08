import { ErrorCategory, SyncError } from '../types/sync.js';
import { logger } from './logger.js';

export interface RetryOptions {
  maxAttempts?: number;
  baseDelayMs?: number;
  maxDelayMs?: number;
  exponentialBase?: number;
  retryableCategories?: ErrorCategory[];
}

const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

export async function withRetry<T>(
  fn: () => Promise<T>,
  options: RetryOptions = {}
): Promise<T> {
  const {
    maxAttempts = 3,
    baseDelayMs = 1000,
    maxDelayMs = 30000,
    exponentialBase = 2,
    retryableCategories = [ErrorCategory.CONNECTION, ErrorCategory.TIMEOUT],
  } = options;

  let lastError: Error = new Error('Unknown error');

  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      return await fn();
    } catch (error) {
      lastError = error instanceof Error ? error : new Error(String(error));

      // Check if error is retryable
      if (error instanceof SyncError && !retryableCategories.includes(error.category)) {
        throw error;
      }

      if (attempt === maxAttempts) {
        logger.error(`All ${maxAttempts} retry attempts failed`, { error: lastError.message });
        throw lastError;
      }

      const delay = Math.min(
        baseDelayMs * Math.pow(exponentialBase, attempt - 1),
        maxDelayMs
      );

      logger.warn(`Attempt ${attempt}/${maxAttempts} failed, retrying in ${delay}ms`, {
        error: lastError.message,
      });

      await sleep(delay);
    }
  }

  throw lastError;
}
