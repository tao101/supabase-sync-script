import type { Config } from '../../types/config.js';
import { logger } from '../../utils/logger.js';
import { SyncError, ErrorCategory, SequenceInfo, SequenceResetResult } from '../../types/sync.js';
import type { PostgresPool } from '../../clients/postgres-client.js';

// Query to find all sequences and their owning tables/columns
const FIND_SEQUENCES_QUERY = `
  SELECT
    seq.relname AS sequence_name,
    ns.nspname AS schema_name,
    tab.relname AS table_name,
    attr.attname AS column_name
  FROM pg_class seq
  JOIN pg_namespace ns ON seq.relnamespace = ns.oid
  JOIN pg_depend dep ON seq.oid = dep.objid
  JOIN pg_class tab ON dep.refobjid = tab.oid
  JOIN pg_attribute attr ON attr.attrelid = tab.oid AND attr.attnum = dep.refobjsubid
  WHERE seq.relkind = 'S'
    AND dep.deptype = 'a'
    AND ns.nspname = ANY($1)
  ORDER BY ns.nspname, seq.relname
`;

export class SequenceSync {
  constructor(
    private config: Config,
    private targetPool: PostgresPool
  ) {}

  async findSequences(): Promise<SequenceInfo[]> {
    logger.info('Finding sequences in target database...');

    const client = await this.targetPool.connect();
    try {
      const result = await client.query(FIND_SEQUENCES_QUERY, [
        this.config.options.database.includeSchemas,
      ]);

      logger.info(`Found ${result.rows.length} sequences`);
      return result.rows;
    } finally {
      client.release();
    }
  }

  async resetSequence(
    schemaName: string,
    tableName: string,
    columnName: string,
    sequenceName: string
  ): Promise<SequenceResetResult> {
    const client = await this.targetPool.connect();
    try {
      // Get max value from the table
      const maxResult = await client.query(
        `SELECT COALESCE(MAX("${columnName}"), 0) as max_val FROM "${schemaName}"."${tableName}"`
      );
      const maxVal = parseInt(maxResult.rows[0]?.max_val || '0', 10);

      // Set sequence value
      // setval(sequence, value, is_called)
      // is_called = true means next call to nextval will return value + 1
      // is_called = false means next call will return value
      const newValue = maxVal > 0 ? maxVal : 1;
      const isCalled = maxVal > 0;

      await client.query(
        `SELECT setval('"${schemaName}"."${sequenceName}"', $1, $2)`,
        [newValue, isCalled]
      );

      logger.debug(
        `Reset sequence ${schemaName}.${sequenceName} to ${newValue} (is_called=${isCalled})`
      );

      return {
        sequence: `${schemaName}.${sequenceName}`,
        table: `${schemaName}.${tableName}`,
        column: columnName,
        newValue,
      };
    } finally {
      client.release();
    }
  }

  async resetAllSequences(): Promise<SequenceResetResult[]> {
    logger.info('Resetting all sequences to match imported data...');

    const sequences = await this.findSequences();
    const results: SequenceResetResult[] = [];

    for (const seq of sequences) {
      try {
        const result = await this.resetSequence(
          seq.schema_name,
          seq.table_name,
          seq.column_name,
          seq.sequence_name
        );
        results.push(result);
      } catch (error) {
        logger.warn(
          `Failed to reset sequence ${seq.schema_name}.${seq.sequence_name}: ${(error as Error).message}`
        );
      }
    }

    logger.info(`Reset ${results.length} sequences`);
    return results;
  }

  async verifySequences(): Promise<boolean> {
    logger.info('Verifying sequence integrity...');

    const sequences = await this.findSequences();
    let allValid = true;

    const client = await this.targetPool.connect();
    try {
      for (const seq of sequences) {
        try {
          // Get current sequence value
          const seqResult = await client.query(
            `SELECT last_value, is_called FROM "${seq.schema_name}"."${seq.sequence_name}"`
          );
          const lastValue = parseInt(seqResult.rows[0]?.last_value || '0', 10);
          const isCalled = seqResult.rows[0]?.is_called;

          // Get max value from table
          const maxResult = await client.query(
            `SELECT COALESCE(MAX("${seq.column_name}"), 0) as max_val FROM "${seq.schema_name}"."${seq.table_name}"`
          );
          const maxVal = parseInt(maxResult.rows[0]?.max_val || '0', 10);

          // Verify: sequence should be >= max value in table
          const effectiveSeqValue = isCalled ? lastValue : lastValue - 1;
          if (effectiveSeqValue < maxVal) {
            logger.warn(
              `Sequence ${seq.schema_name}.${seq.sequence_name} (${effectiveSeqValue}) < max value in ${seq.schema_name}.${seq.table_name} (${maxVal})`
            );
            allValid = false;
          }
        } catch (error) {
          logger.warn(
            `Failed to verify sequence ${seq.schema_name}.${seq.sequence_name}: ${(error as Error).message}`
          );
        }
      }
    } finally {
      client.release();
    }

    if (allValid) {
      logger.info('All sequences are valid');
    } else {
      logger.warn('Some sequences may need attention');
    }

    return allValid;
  }

  async sync(): Promise<SequenceResetResult[]> {
    if (this.config.dryRun) {
      logger.info('[DRY RUN] Would reset all sequences');
      const sequences = await this.findSequences();
      return sequences.map(seq => ({
        sequence: `${seq.schema_name}.${seq.sequence_name}`,
        table: `${seq.schema_name}.${seq.table_name}`,
        column: seq.column_name,
        newValue: 0,
      }));
    }

    return this.resetAllSequences();
  }
}
