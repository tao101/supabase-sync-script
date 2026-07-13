import type { SupabaseClient } from '@supabase/supabase-js';
import type { Config } from '../../types/config.js';
import type { PostgresPool } from '../../clients/postgres-client.js';
import { logger } from '../../utils/logger.js';
import { StorageSyncResult, BucketSyncResult, StorageBucket, StorageFile, SyncError, ErrorCategory } from '../../types/sync.js';
import { withRetry } from '../../utils/retry.js';
import { getApplicationSchemas, quoteIdentifier } from '../database/schemas.js';

export class StorageSync {
  constructor(
    private config: Config,
    private sourceSupabase: SupabaseClient,
    private targetSupabase: SupabaseClient,
    private targetPool?: PostgresPool
  ) {}

  async listBuckets(): Promise<StorageBucket[]> {
    logger.info('Listing storage buckets from source...');

    const data = await withRetry(
      async () => {
        const { data, error } = await this.sourceSupabase.storage.listBuckets();

        if (error) {
          throw new Error(`Failed to list buckets: ${error.message}`);
        }

        return data;
      },
      { maxAttempts: 3, baseDelayMs: 2000 }
    );

    // Filter out excluded buckets
    const filtered = (data || []).filter(
      bucket => !this.config.options.storage.excludeBuckets.includes(bucket.name)
    );

    logger.info(`Found ${filtered.length} buckets to sync`);
    return filtered as StorageBucket[];
  }

  async createBucket(bucket: StorageBucket): Promise<void> {
    logger.debug(`Creating bucket: ${bucket.name}`);

    const { error } = await this.targetSupabase.storage.createBucket(bucket.name, {
      public: bucket.public,
      fileSizeLimit: bucket.file_size_limit || undefined,
      allowedMimeTypes: bucket.allowed_mime_types || undefined,
    });

    // Ignore "already exists" errors for idempotency
    if (error && !error.message.includes('already exists')) {
      throw new Error(`Failed to create bucket ${bucket.name}: ${error.message}`);
    }
  }

  async listAllFiles(bucketName: string, prefix: string = ''): Promise<StorageFile[]> {
    const allFiles: StorageFile[] = [];
    let offset = 0;
    const limit = 1000;

    logger.debug(`Listing files in bucket "${bucketName}" with prefix "${prefix || '(root)'}"`);

    while (true) {
      const { data, error } = await this.sourceSupabase.storage
        .from(bucketName)
        .list(prefix || undefined, { limit, offset });

      if (error) {
        logger.error(`Storage list error for ${bucketName}/${prefix}: ${JSON.stringify(error)}`);
        throw new Error(`Failed to list files in ${bucketName}/${prefix}: ${error.message}`);
      }

      logger.debug(`  ${bucketName}/${prefix || '(root)'}: got ${data?.length ?? 0} items (offset=${offset})`);

      if (!data || data.length === 0) break;

      for (const item of data) {
        const fullPath = prefix ? `${prefix}/${item.name}` : item.name;

        if (item.id) {
          // It's a file
          // Extract size from metadata (Supabase storage returns size in metadata)
          const metadata = item.metadata as { size?: unknown; contentLength?: unknown } | null;
          const rawSize = metadata?.size ?? metadata?.contentLength;
          const size = rawSize === undefined || rawSize === null ? Number.NaN : Number(rawSize);
          allFiles.push({
            name: fullPath,
            id: item.id,
            bucket_id: bucketName,
            metadata: item.metadata || {},
            size,
          });
        } else {
          // It's a folder, recurse
          logger.debug(`    Recursing into folder: ${fullPath}`);
          const subFiles = await this.listAllFiles(bucketName, fullPath);
          allFiles.push(...subFiles);
        }
      }

      offset += limit;
      if (data.length < limit) break;
    }

    return allFiles;
  }

  async syncFile(bucketName: string, filePath: string): Promise<void> {
    await withRetry(
      async () => {
        // Download from source
        const { data: fileData, error: downloadError } = await this.sourceSupabase.storage
          .from(bucketName)
          .download(filePath);

        if (downloadError) {
          throw new Error(`Failed to download ${bucketName}/${filePath}: ${downloadError.message}`);
        }

        if (!fileData) {
          throw new Error(`No data returned for ${bucketName}/${filePath}`);
        }

        // Upload to target
        const { error: uploadError } = await this.targetSupabase.storage
          .from(bucketName)
          .upload(filePath, fileData, {
            upsert: true,
            contentType: fileData.type || 'application/octet-stream',
          });

        if (uploadError) {
          throw new Error(`Failed to upload ${bucketName}/${filePath}: ${uploadError.message}`);
        }
      },
      { maxAttempts: 3, baseDelayMs: 1000 }
    );
  }

  private validateFileSizes(bucketName: string, files: StorageFile[]): void {
    const maxBytes = this.config.options.storage.maxFileSizeMB * 1024 * 1024;
    let invalidCount = 0;
    let oversizedCount = 0;
    const oversizedExamples: string[] = [];
    for (const file of files) {
      if (!Number.isFinite(file.size) || file.size < 0) {
        invalidCount++;
      } else if (file.size > maxBytes) {
        oversizedCount++;
        if (oversizedExamples.length < 3) oversizedExamples.push(file.name);
      }
    }

    if (invalidCount > 0) {
      throw new SyncError(
        `${invalidCount} file(s) in bucket ${bucketName} have missing or invalid size metadata`,
        ErrorCategory.STORAGE,
        'storage-preflight',
        false
      );
    }

    if (oversizedCount > 0) {
      throw new SyncError(
        `${oversizedCount} file(s) in bucket ${bucketName} exceed storage.maxFileSizeMB (${this.config.options.storage.maxFileSizeMB} MB): ${oversizedExamples.join(', ')}`,
        ErrorCategory.STORAGE,
        'storage-preflight',
        false
      );
    }
  }

  async syncBucket(bucket: StorageBucket, listedFiles?: StorageFile[]): Promise<BucketSyncResult> {
    logger.info(`Syncing bucket: ${bucket.name}`);

    // List all files
    const files = listedFiles ?? await this.listAllFiles(bucket.name);
    logger.info(`Found ${files.length} files in bucket ${bucket.name}`);
    this.validateFileSizes(bucket.name, files);

    // Create the target bucket only after the source preflight succeeds.
    await this.createBucket(bucket);

    if (files.length === 0) {
      logger.warn(
        `Bucket "${bucket.name}" returned 0 files from the Storage API. ` +
        `This may indicate: (1) the bucket is truly empty, (2) the Storage API URL is incorrect ` +
        `for self-hosted instances, or (3) the service role key lacks storage permissions. ` +
        `Check that the source API URL is correct and that the key has storage.objects read access.`
      );
    }

    let uploaded = 0;
    let failed = 0;

    const LARGE_FILE_THRESHOLD = 10 * 1024 * 1024; // 10MB in bytes
    const configuredConcurrency = this.config.options.storage.concurrency;

    // Separate files into large and small based on threshold
    const largeFiles = files.filter(f => f.size >= LARGE_FILE_THRESHOLD);
    const smallFiles = files.filter(f => f.size < LARGE_FILE_THRESHOLD);

    if (largeFiles.length > 0) {
      logger.info(`Bucket ${bucket.name}: ${largeFiles.length} large files (>=10MB), ${smallFiles.length} small files`);
    }

    const allFiles = [...smallFiles, ...largeFiles];
    let nextFile = 0;
    const workers = Array.from(
      { length: Math.min(configuredConcurrency, allFiles.length) },
      async () => {
        while (nextFile < allFiles.length) {
          const file = allFiles[nextFile++];
          try {
            await this.syncFile(bucket.name, file.name);
            uploaded++;
            logger.debug(`Synced file: ${bucket.name}/${file.name} (${(file.size / 1024 / 1024).toFixed(2)}MB)`);
          } catch (error) {
            failed++;
            logger.warn(`Failed to sync file ${bucket.name}/${file.name}: ${(error as Error).message}`);
          }
        }
      }
    );
    await Promise.all(workers);

    logger.info(`Bucket ${bucket.name}: ${uploaded} uploaded, ${failed} failed`);

    return {
      bucket: bucket.name,
      total: files.length,
      uploaded,
      failed,
    };
  }

  async sync(): Promise<StorageSyncResult> {
    const buckets = await this.listBuckets();
    const plans: { bucket: StorageBucket; total: number }[] = [];
    const dryRunBuckets: BucketSyncResult[] = [];
    for (const bucket of buckets) {
      const files = await this.listAllFiles(bucket.name);
      this.validateFileSizes(bucket.name, files);
      if (this.config.dryRun) {
        dryRunBuckets.push({
          bucket: bucket.name,
          total: files.length,
          uploaded: 0,
          failed: 0,
        });
      } else {
        plans.push({ bucket, total: files.length });
      }
    }

    if (this.config.dryRun) {
      logger.info('[DRY RUN] Would sync storage buckets and files');
      return { buckets: dryRunBuckets };
    }

    const results: BucketSyncResult[] = [];

    for (const { bucket, total } of plans) {
      try {
        const files = await this.listAllFiles(bucket.name);
        if (files.length !== total) {
          logger.warn(`Bucket ${bucket.name} changed after preflight (${total} -> ${files.length} files); validating the new inventory`);
        }
        const result = await this.syncBucket(bucket, files);
        results.push(result);
      } catch (error) {
        logger.error(`Failed to sync bucket ${bucket.name}: ${(error as Error).message}`);
        results.push({
          bucket: bucket.name,
          total: 0,
          uploaded: 0,
          failed: 1,
        });
      }
    }

    const totalFiles = results.reduce((sum, r) => sum + r.total, 0);
    const totalUploaded = results.reduce((sum, r) => sum + r.uploaded, 0);
    const totalFailed = results.reduce((sum, r) => sum + r.failed, 0);

    logger.info(`Storage sync complete: ${results.length} buckets, ${totalUploaded}/${totalFiles} files, ${totalFailed} failed`);

    if (totalFailed > 0) {
      throw new SyncError(
        `Storage sync failed: ${totalFailed}/${totalFiles} files failed to upload`,
        ErrorCategory.STORAGE,
        'storage-sync',
        false
      );
    }

    // Rewrite storage URLs in database to point to target
    if (this.targetPool) {
      await this.rewriteStorageUrls();
    }

    return { buckets: results };
  }

  /**
   * Rewrite storage URLs in database to point to target Supabase instance
   * This updates URLs that reference the source storage to use the target storage
   */
  async rewriteStorageUrls(): Promise<void> {
    logger.info('Rewriting storage URLs in database...');

    const sourceApiUrl = this.config.source.apiUrl;
    const targetApiUrl = this.config.target.apiUrl;
    if (!sourceApiUrl || !targetApiUrl) {
      throw new SyncError(
        'Source and target API URLs are required to rewrite storage URLs',
        ErrorCategory.VALIDATION,
        'storage-url-rewrite',
        false
      );
    }

    // Normalize URLs (remove trailing slashes)
    const sourceUrl = `${sourceApiUrl.replace(/\/$/, '')}/storage/v1/object/public/`;
    const targetUrl = `${targetApiUrl.replace(/\/$/, '')}/storage/v1/object/public/`;

    if (sourceUrl === targetUrl) {
      logger.info('Source and target URLs are the same, skipping URL rewrite');
      return;
    }

    const client = await this.targetPool!.connect();
    let transactionStarted = false;
    try {
      await client.query('BEGIN');
      transactionStarted = true;
      let totalUpdated = 0;

      // Update auth.users raw_user_meta_data avatar_url
      const authResult = await client.query(`
        UPDATE auth.users
        SET raw_user_meta_data = jsonb_set(
          raw_user_meta_data,
          '{avatar_url}',
          to_jsonb($2 || substring(raw_user_meta_data->>'avatar_url' FROM char_length($1) + 1))
        )
        WHERE left(raw_user_meta_data->>'avatar_url', char_length($1)) = $1
      `, [sourceUrl, targetUrl]);

      if (authResult.rowCount && authResult.rowCount > 0) {
        logger.info(`Updated ${authResult.rowCount} avatar URLs in auth.users`);
        totalUpdated += authResult.rowCount;
      }

      // Find and update text columns containing storage URLs in public schema
      const columnsResult = await client.query(`
        SELECT table_schema, table_name, column_name
        FROM information_schema.columns
        WHERE table_schema = ANY($1)
        AND data_type IN ('text', 'character varying')
        AND column_name LIKE '%url%'
      `, [getApplicationSchemas(this.config)]);

      for (const row of columnsResult.rows) {
        try {
          const updateResult = await client.query(`
            UPDATE ${quoteIdentifier(row.table_schema)}.${quoteIdentifier(row.table_name)}
            SET ${quoteIdentifier(row.column_name)} = $2 || substring(${quoteIdentifier(row.column_name)} FROM char_length($1) + 1)
            WHERE left(${quoteIdentifier(row.column_name)}, char_length($1)) = $1
          `, [sourceUrl, targetUrl]);

          if (updateResult.rowCount && updateResult.rowCount > 0) {
            logger.info(`Updated ${updateResult.rowCount} URLs in ${row.table_schema}.${row.table_name}.${row.column_name}`);
            totalUpdated += updateResult.rowCount;
          }
        } catch (error) {
          throw new SyncError(
            `Could not rewrite storage URLs in ${row.table_schema}.${row.table_name}.${row.column_name}: ${(error as Error).message}`,
            ErrorCategory.STORAGE,
            'storage-url-rewrite',
            false,
            error as Error
          );
        }
      }

      await client.query('COMMIT');
      transactionStarted = false;
      logger.info(`Total storage URLs rewritten: ${totalUpdated}`);
    } catch (error) {
      if (transactionStarted) await client.query('ROLLBACK').catch(() => {});
      throw error;
    } finally {
      client.release();
    }
  }
}
