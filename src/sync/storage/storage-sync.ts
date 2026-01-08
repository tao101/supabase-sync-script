import type { SupabaseClient } from '@supabase/supabase-js';
import pLimit from 'p-limit';
import type { Config } from '../../types/config.js';
import { logger } from '../../utils/logger.js';
import { StorageSyncResult, BucketSyncResult, StorageBucket, StorageFile } from '../../types/sync.js';

export class StorageSync {
  constructor(
    private config: Config,
    private sourceSupabase: SupabaseClient,
    private targetSupabase: SupabaseClient
  ) {}

  async listBuckets(): Promise<StorageBucket[]> {
    logger.info('Listing storage buckets from source...');

    const { data, error } = await this.sourceSupabase.storage.listBuckets();

    if (error) {
      throw new Error(`Failed to list buckets: ${error.message}`);
    }

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

    while (true) {
      const { data, error } = await this.sourceSupabase.storage
        .from(bucketName)
        .list(prefix, { limit, offset });

      if (error) {
        throw new Error(`Failed to list files in ${bucketName}/${prefix}: ${error.message}`);
      }

      if (!data || data.length === 0) break;

      for (const item of data) {
        const fullPath = prefix ? `${prefix}/${item.name}` : item.name;

        if (item.id) {
          // It's a file
          allFiles.push({
            name: fullPath,
            id: item.id,
            bucket_id: bucketName,
            metadata: item.metadata || {},
          });
        } else {
          // It's a folder, recurse
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
  }

  async syncBucket(bucket: StorageBucket): Promise<BucketSyncResult> {
    logger.info(`Syncing bucket: ${bucket.name}`);

    // Create bucket on target
    await this.createBucket(bucket);

    // List all files
    const files = await this.listAllFiles(bucket.name);
    logger.info(`Found ${files.length} files in bucket ${bucket.name}`);

    let uploaded = 0;
    let failed = 0;

    // Use concurrency limit for parallel uploads
    const limit = pLimit(this.config.options.storage.concurrency);

    const results = await Promise.allSettled(
      files.map(file =>
        limit(async () => {
          try {
            await this.syncFile(bucket.name, file.name);
            uploaded++;
            logger.debug(`Synced file: ${bucket.name}/${file.name}`);
          } catch (error) {
            failed++;
            logger.warn(`Failed to sync file ${bucket.name}/${file.name}: ${(error as Error).message}`);
            throw error;
          }
        })
      )
    );

    logger.info(`Bucket ${bucket.name}: ${uploaded} uploaded, ${failed} failed`);

    return {
      bucket: bucket.name,
      total: files.length,
      uploaded,
      failed,
    };
  }

  async sync(): Promise<StorageSyncResult> {
    if (this.config.dryRun) {
      logger.info('[DRY RUN] Would sync storage buckets and files');
      const buckets = await this.listBuckets();

      const results: BucketSyncResult[] = [];
      for (const bucket of buckets) {
        const files = await this.listAllFiles(bucket.name);
        results.push({
          bucket: bucket.name,
          total: files.length,
          uploaded: 0,
          failed: 0,
        });
      }

      return { buckets: results };
    }

    const buckets = await this.listBuckets();
    const results: BucketSyncResult[] = [];

    for (const bucket of buckets) {
      try {
        const result = await this.syncBucket(bucket);
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

    return { buckets: results };
  }
}
