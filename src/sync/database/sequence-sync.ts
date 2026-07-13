import pg from 'pg';
import type { Config } from '../../types/config.js';
import { logger } from '../../utils/logger.js';
import { SequenceInfo, SequenceResetResult } from '../../types/sync.js';
import type { PostgresPool } from '../../clients/postgres-client.js';
import { getApplicationSchemas, quoteIdentifier } from './schemas.js';

// Query to find all sequences and their owning tables/columns
const FIND_SEQUENCES_QUERY = `
  SELECT
    seq.relname AS sequence_name,
    ns.nspname AS schema_name,
    tab.relname AS table_name,
    attr.attname AS column_name,
    seq_meta.seqincrement::text AS increment_by,
    seq_meta.seqstart::text AS start_value
  FROM pg_class seq
  JOIN pg_namespace ns ON seq.relnamespace = ns.oid
  JOIN pg_sequence seq_meta ON seq_meta.seqrelid = seq.oid
  JOIN pg_depend dep ON seq.oid = dep.objid
  JOIN pg_class tab ON dep.refobjid = tab.oid
  JOIN pg_attribute attr ON attr.attrelid = tab.oid AND attr.attnum = dep.refobjsubid
  WHERE seq.relkind = 'S'
    AND dep.deptype IN ('a', 'i')
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
        getApplicationSchemas(this.config),
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
    sequenceName: string,
    client?: pg.PoolClient,
    incrementBy: string = '1',
    startValue: string = '1'
  ): Promise<SequenceResetResult> {
    const ownClient = !client;
    const dbClient = client ?? await this.targetPool.connect();
    try {
      const descending = BigInt(incrementBy) < 0n;
      const aggregate = descending ? 'MIN' : 'MAX';
      const boundaryResult = await dbClient.query(
        `SELECT ${aggregate}(${quoteIdentifier(columnName)})::text as boundary_value FROM ${quoteIdentifier(schemaName)}.${quoteIdentifier(tableName)}`
      );
      const boundaryValue = boundaryResult.rows[0]?.boundary_value as string | null | undefined;

      const newValueExact = boundaryValue ?? startValue;
      const isCalled = boundaryValue !== null && boundaryValue !== undefined;

      await dbClient.query(
        'SELECT setval($1::regclass, $2, $3)',
        [`${quoteIdentifier(schemaName)}.${quoteIdentifier(sequenceName)}`, newValueExact, isCalled]
      );

      logger.debug(
        `Reset sequence ${schemaName}.${sequenceName} to ${newValueExact} (is_called=${isCalled})`
      );

      return {
        sequence: `${schemaName}.${sequenceName}`,
        table: `${schemaName}.${tableName}`,
        column: columnName,
        newValue: Number(newValueExact),
        newValueExact,
      };
    } finally {
      // Only release if we acquired the client ourselves
      if (ownClient) {
        dbClient.release();
      }
    }
  }

  async resetAllSequences(): Promise<SequenceResetResult[]> {
    logger.info('Resetting all sequences to match imported data...');

    const sequences = await this.findSequences();
    const results: SequenceResetResult[] = [];

    // Acquire a single connection for all sequence resets to avoid pool churn
    const client = await this.targetPool.connect();
    try {
      for (const seq of sequences) {
        try {
          const result = await this.resetSequence(
            seq.schema_name,
            seq.table_name,
            seq.column_name,
            seq.sequence_name,
            client,
            seq.increment_by,
            seq.start_value
          );
          results.push(result);
        } catch (error) {
          logger.warn(
            `Failed to reset sequence ${seq.schema_name}.${seq.sequence_name}: ${(error as Error).message}`
          );
        }
      }
    } finally {
      client.release();
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
            `SELECT last_value::text AS last_value, is_called FROM ${quoteIdentifier(seq.schema_name)}.${quoteIdentifier(seq.sequence_name)}`
          );
          const lastValue = BigInt(seqResult.rows[0]?.last_value || '0');
          const isCalled = seqResult.rows[0]?.is_called;
          const increment = BigInt(seq.increment_by ?? '1');
          const aggregate = increment < 0n ? 'MIN' : 'MAX';

          const boundaryResult = await client.query(
            `SELECT ${aggregate}(${quoteIdentifier(seq.column_name)})::text AS boundary_value FROM ${quoteIdentifier(seq.schema_name)}.${quoteIdentifier(seq.table_name)}`
          );
          const rawBoundary = boundaryResult.rows[0]?.boundary_value as string | null | undefined;
          if (rawBoundary === null || rawBoundary === undefined) continue;
          const boundaryValue = BigInt(rawBoundary);

          const nextValue = isCalled ? lastValue + increment : lastValue;
          const invalid = increment > 0n
            ? nextValue <= boundaryValue
            : nextValue >= boundaryValue;
          if (invalid) {
            logger.warn(
              `Sequence ${seq.schema_name}.${seq.sequence_name} next value (${nextValue}) conflicts with ${aggregate} value in ${seq.schema_name}.${seq.table_name} (${boundaryValue})`
            );
            allValid = false;
          }
        } catch (error) {
          logger.warn(
            `Failed to verify sequence ${seq.schema_name}.${seq.sequence_name}: ${(error as Error).message}`
          );
          allValid = false;
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
      return sequences.map(seq => {
        const newValueExact = seq.start_value ?? '1';
        return {
          sequence: `${seq.schema_name}.${seq.sequence_name}`,
          table: `${seq.schema_name}.${seq.table_name}`,
          column: seq.column_name,
          newValue: Number(newValueExact),
          newValueExact,
        };
      });
    }

    return this.resetAllSequences();
  }
}
