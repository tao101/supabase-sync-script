import { execa } from 'execa';
import type { Config } from '../../types/config.js';
import { ConnectionBuilder } from '../../config/connection-builder.js';
import { TempFileManager } from '../../utils/temp-files.js';
import { logger } from '../../utils/logger.js';
import { SyncError, ErrorCategory } from '../../types/sync.js';

export class SchemaSync {
  private connectionBuilder: ConnectionBuilder;

  constructor(
    private config: Config,
    private tempFileManager: TempFileManager
  ) {
    this.connectionBuilder = new ConnectionBuilder();
  }

  async exportSchema(): Promise<string> {
    logger.info('Exporting database schema from source...');

    const sourceDbUrl = this.connectionBuilder.buildDirectDbUrl(this.config.source);
    const dumpFile = await this.tempFileManager.createFile('schema_dump', '.sql');

    const args = [
      sourceDbUrl,
      '--schema-only',
      '--clean',
      '--if-exists',
      '--quote-all-identifiers',
      '--no-owner',
      '--no-privileges',
      '--no-subscriptions',
      '--no-publications',
      '-f', dumpFile,
    ];

    // Include specific schemas
    for (const schema of this.config.options.database.includeSchemas) {
      args.push(`--schema=${schema}`);
    }

    try {
      await execa('pg_dump', args, {
        env: { ...process.env, PGPASSWORD: this.config.source.dbPassword },
      });

      logger.info(`Schema exported to ${dumpFile}`);
      return dumpFile;
    } catch (error) {
      throw new SyncError(
        `Failed to export schema: ${(error as Error).message}`,
        ErrorCategory.EXPORT,
        'schema-export',
        false,
        error as Error
      );
    }
  }

  async importSchema(dumpFile: string): Promise<void> {
    logger.info('Importing database schema to target...');

    const targetDbUrl = this.connectionBuilder.buildDbUrl(this.config.target);

    // Pre-process the dump file to remove problematic statements
    const processedFile = await this.preprocessDumpFile(dumpFile);

    try {
      const result = await execa('psql', [
        targetDbUrl,
        '-f', processedFile,
        '--single-transaction',
        '-v', 'ON_ERROR_STOP=0', // Continue on errors (some objects may already exist)
      ], {
        env: { ...process.env, PGPASSWORD: this.config.target.dbPassword },
        reject: false, // Don't throw on non-zero exit
      });

      // Check for actual errors in stderr
      if (result.stderr && result.stderr.trim()) {
        const errorLines = result.stderr.split('\n').filter(line =>
          line.includes('ERROR') && !line.includes('already exists')
        );
        if (errorLines.length > 0) {
          logger.warn(`Schema import had ${errorLines.length} errors (some may be expected):`);
          errorLines.slice(0, 5).forEach(line => logger.warn(`  ${line.trim()}`));
          if (errorLines.length > 5) {
            logger.warn(`  ... and ${errorLines.length - 5} more errors`);
          }
        }
      }

      if (result.exitCode !== 0) {
        logger.warn(`Schema import completed with exit code ${result.exitCode} (some errors may be expected)`);
      } else {
        logger.info('Schema imported successfully');
      }
    } catch (error) {
      // Log but don't fail - some errors are expected (existing objects)
      logger.warn(`Schema import completed with warnings: ${(error as Error).message}`);
    }
  }

  private async preprocessDumpFile(dumpFile: string): Promise<string> {
    const content = await this.tempFileManager.readFile(dumpFile);

    // Remove problematic statements
    let processed = content
      // Remove extension creation (usually already exists)
      .replace(/CREATE EXTENSION IF NOT EXISTS [^;]+;/gi, '')
      // Remove comments on extensions
      .replace(/COMMENT ON EXTENSION [^;]+;/gi, '')
      // Remove role-related statements (handled separately)
      .replace(/ALTER [^;]+ OWNER TO [^;]+;/gi, '')
      // Remove grant statements
      .replace(/GRANT [^;]+;/gi, '')
      .replace(/REVOKE [^;]+;/gi, '')
      // Remove problematic Supabase-specific objects
      .replace(/CREATE POLICY [^;]+ ON "auth"\."[^"]+" [^;]+;/gi, '')
      .replace(/CREATE POLICY [^;]+ ON "storage"\."[^"]+" [^;]+;/gi, '');

    const processedFile = await this.tempFileManager.createFile('schema_processed', '.sql');
    await this.tempFileManager.writeFile(processedFile, processed);

    return processedFile;
  }

  async sync(): Promise<void> {
    if (this.config.dryRun) {
      logger.info('[DRY RUN] Would export and import database schema');
      return;
    }

    const dumpFile = await this.exportSchema();
    await this.importSchema(dumpFile);
  }
}
