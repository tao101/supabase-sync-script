import { execa } from 'execa';
import type pg from 'pg';
import type { Config } from '../../types/config.js';
import { ConnectionBuilder } from '../../config/connection-builder.js';
import { TempFileManager } from '../../utils/temp-files.js';
import { logger } from '../../utils/logger.js';
import { SyncError, ErrorCategory } from '../../types/sync.js';
import type { PostgresPool } from '../../clients/postgres-client.js';
import { getApplicationSchemas, getManagedSchemas, quoteIdentifier, stripUnsupportedDumpSettings } from './schemas.js';

interface SchemaGrant {
  grantee: string;
  privilegeType: string;
  isGrantable: boolean;
}

interface DefaultPrivilegeGrant extends SchemaGrant {
  owner: string;
  objectType: string;
}

interface SchemaPrivilegeState {
  owner: string;
  schemaGrants: SchemaGrant[];
  defaultPrivilegeGrants: DefaultPrivilegeGrant[];
}

interface PreservedTrigger {
  schemaName: string;
  tableName: string;
  triggerName: string;
  definition: string;
  enabledMode: string;
}

interface PreparedSchemaReset {
  resetSql: string;
  finalizeSql: string;
}

export class SchemaSync {
  private connectionBuilder: ConnectionBuilder;

  constructor(
    private config: Config,
    private tempFileManager: TempFileManager,
    private targetPool: PostgresPool,
    private sourcePool?: PostgresPool
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
      '--quote-all-identifiers',
      '--no-owner',
      '--no-subscriptions',
      '--no-publications',
      '-f', dumpFile,
    ];

    const schemas = getApplicationSchemas(this.config);
    if (schemas.length === 0) {
      throw new SyncError(
        'No application schemas configured for schema sync',
        ErrorCategory.VALIDATION,
        'schema-export',
        false
      );
    }

    for (const schema of schemas) {
      args.push(`--schema=${schema}`);
    }

    try {
      await execa('pg_dump', args, {
        env: this.connectionBuilder.buildPgEnv(this.config.source),
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

  async importSchema(
    dumpFile: string,
    resetSql: string = '',
    finalizeSql: string = ''
  ): Promise<void> {
    logger.info('Importing database schema to target...');

    const targetDbUrl = this.connectionBuilder.buildDbUrl(this.config.target);

    // Pre-process the dump file to remove problematic statements
    const processedFile = await this.preprocessDumpFile(dumpFile);
    const resetFile = await this.tempFileManager.createFile('schema_reset', '.sql');
    const finalizeFile = await this.tempFileManager.createFile('schema_finalize', '.sql');
    await this.tempFileManager.writeFile(resetFile, resetSql);
    await this.tempFileManager.writeFile(finalizeFile, finalizeSql);

    try {
      await execa('psql', [
        targetDbUrl,
        '-X',
        '--single-transaction',
        '-v', 'ON_ERROR_STOP=1',
        '-f', resetFile,
        '-f', processedFile,
        '-f', finalizeFile,
      ], {
        env: this.connectionBuilder.buildPgEnv(this.config.target),
      });
      logger.info('Schema imported successfully');
    } catch (error) {
      if (error instanceof SyncError) {
        throw error;
      }
      throw new SyncError(
        `Schema import failed: ${(error as Error).message}`,
        ErrorCategory.IMPORT,
        'schema-import',
        false,
        error as Error
      );
    }
  }

  async prepareTargetSchemas(): Promise<PreparedSchemaReset> {
    const schemas = getApplicationSchemas(this.config);
    if (schemas.length === 0) return { resetSql: '', finalizeSql: '' };
    const managedSchemas = getManagedSchemas();

    logger.info(`Preparing target application schema reset: ${schemas.join(', ')}`);

    const client = await this.targetPool.connect();
    try {
      const extensionSchemasResult = await client.query(`
        SELECT n.nspname AS schema_name, e.extname AS extension_name
        FROM pg_extension e
        JOIN pg_namespace n ON n.oid = e.extnamespace
        WHERE n.nspname = ANY($1::text[])
        ORDER BY n.nspname, e.extname
        LIMIT 10
      `, [schemas]);

      if (extensionSchemasResult.rows.length > 0) {
        const examples = extensionSchemasResult.rows
          .map(row => `${row.schema_name} contains extension ${row.extension_name}`)
          .join('; ');

        throw new SyncError(
          `Refusing to reset target schemas because they contain installed extensions: ${examples}`,
          ErrorCategory.VALIDATION,
          'schema-reset',
          false
        );
      }

      const dependenciesResult = await client.query(`
        WITH schema_objects AS (
          SELECT
            dependency.classid,
            dependency.objid,
            namespace_obj.nspname AS schema_name
          FROM pg_depend dependency
          JOIN pg_namespace namespace_obj
            ON dependency.refclassid = 'pg_namespace'::regclass
            AND dependency.refobjid = namespace_obj.oid
          WHERE dependency.deptype = 'n'
        ),
        app_objects AS (
          SELECT classid, objid
          FROM schema_objects
          WHERE schema_name = ANY($1::text[])
        ),
        dependencies AS (
          SELECT
            d.classid,
            d.objid,
            d.objsubid,
            pg_describe_object(d.classid, d.objid, d.objsubid) AS dependent_object,
            pg_describe_object(d.refclassid, d.refobjid, d.refobjsubid) AS referenced_object
          FROM pg_depend d
          JOIN app_objects ao ON ao.classid = d.refclassid AND ao.objid = d.refobjid
          WHERE d.deptype IN ('n', 'a')
            AND NOT (
              d.classid = 'pg_trigger'::regclass
              AND EXISTS (
                SELECT 1
                FROM pg_trigger trigger_obj
                JOIN pg_class trigger_rel ON trigger_rel.oid = trigger_obj.tgrelid
                JOIN pg_namespace trigger_ns ON trigger_ns.oid = trigger_rel.relnamespace
                WHERE trigger_obj.oid = d.objid
                  AND trigger_ns.nspname = ANY($2::text[])
                  AND NOT trigger_obj.tgisinternal
              )
            )
        ),
        dependency_schemas AS (
          SELECT
            COALESCE(
              direct_ns.schema_name,
              class_ns.nspname,
              proc_ns.nspname,
              type_ns.nspname,
              trigger_ns.nspname,
              rewrite_ns.nspname,
              constraint_ns.nspname,
              policy_ns.nspname,
              attrdef_ns.nspname
            ) AS dependent_schema,
            dependent_object,
            referenced_object
          FROM dependencies d
          LEFT JOIN schema_objects direct_ns ON direct_ns.classid = d.classid AND direct_ns.objid = d.objid
          LEFT JOIN pg_class class_obj ON d.classid = 'pg_class'::regclass AND d.objid = class_obj.oid
          LEFT JOIN pg_namespace class_ns ON class_ns.oid = class_obj.relnamespace
          LEFT JOIN pg_proc proc_obj ON d.classid = 'pg_proc'::regclass AND d.objid = proc_obj.oid
          LEFT JOIN pg_namespace proc_ns ON proc_ns.oid = proc_obj.pronamespace
          LEFT JOIN pg_type type_obj ON d.classid = 'pg_type'::regclass AND d.objid = type_obj.oid
          LEFT JOIN pg_namespace type_ns ON type_ns.oid = type_obj.typnamespace
          LEFT JOIN pg_trigger trigger_obj ON d.classid = 'pg_trigger'::regclass AND d.objid = trigger_obj.oid
          LEFT JOIN pg_class trigger_rel ON trigger_rel.oid = trigger_obj.tgrelid
          LEFT JOIN pg_namespace trigger_ns ON trigger_ns.oid = trigger_rel.relnamespace
          LEFT JOIN pg_rewrite rewrite_obj ON d.classid = 'pg_rewrite'::regclass AND d.objid = rewrite_obj.oid
          LEFT JOIN pg_class rewrite_rel ON rewrite_rel.oid = rewrite_obj.ev_class
          LEFT JOIN pg_namespace rewrite_ns ON rewrite_ns.oid = rewrite_rel.relnamespace
          LEFT JOIN pg_constraint constraint_obj ON d.classid = 'pg_constraint'::regclass AND d.objid = constraint_obj.oid
          LEFT JOIN pg_class constraint_rel ON constraint_rel.oid = constraint_obj.conrelid
          LEFT JOIN pg_type constraint_type ON constraint_type.oid = constraint_obj.contypid
          LEFT JOIN pg_namespace constraint_ns ON constraint_ns.oid = COALESCE(constraint_rel.relnamespace, constraint_type.typnamespace)
          LEFT JOIN pg_policy policy_obj ON d.classid = 'pg_policy'::regclass AND d.objid = policy_obj.oid
          LEFT JOIN pg_class policy_rel ON policy_rel.oid = policy_obj.polrelid
          LEFT JOIN pg_namespace policy_ns ON policy_ns.oid = policy_rel.relnamespace
          LEFT JOIN pg_attrdef attrdef_obj ON d.classid = 'pg_attrdef'::regclass AND d.objid = attrdef_obj.oid
          LEFT JOIN pg_class attrdef_rel ON attrdef_rel.oid = attrdef_obj.adrelid
          LEFT JOIN pg_namespace attrdef_ns ON attrdef_ns.oid = attrdef_rel.relnamespace
        )
        SELECT dependent_object, referenced_object
        FROM dependency_schemas
        WHERE dependent_schema IS NULL
          OR dependent_schema <> ALL($1::text[])
        ORDER BY dependent_object, referenced_object
        LIMIT 10
      `, [schemas, managedSchemas]);

      if (dependenciesResult.rows.length > 0) {
        const examples = dependenciesResult.rows
          .map(row => `${row.dependent_object} depends on ${row.referenced_object}`)
          .join('; ');

        throw new SyncError(
          `Refusing to reset target schemas because objects outside the application schemas depend on them: ${examples}`,
          ErrorCategory.VALIDATION,
          'schema-reset',
          false
        );
      }

      const resetStatements: string[] = [];
      const finalizeStatements: string[] = [];
      for (const schema of schemas) {
        const privileges = await this.captureSchemaPrivileges(client, schema);
        const quotedSchema = quoteIdentifier(schema);
        resetStatements.push(
          `DROP SCHEMA IF EXISTS ${quotedSchema} CASCADE;`,
          `CREATE SCHEMA ${quotedSchema};`
        );
        if (privileges) {
          finalizeStatements.push(this.buildSchemaPrivilegeSql(schema, privileges));
        }
      }

      return {
        resetSql: resetStatements.join('\n'),
        finalizeSql: finalizeStatements.join('\n'),
      };
    } catch (error) {
      if (error instanceof SyncError) throw error;
      throw new SyncError(
        `Failed to prepare target schemas: ${(error as Error).message}`,
        ErrorCategory.IMPORT,
        'schema-reset',
        false,
        error as Error
      );
    } finally {
      client.release();
    }
  }

  /** @deprecated Use sync() so reset and import remain in one transaction. */
  async resetTargetSchemas(): Promise<PreservedTrigger[]> {
    const schemas = getApplicationSchemas(this.config);
    if (schemas.length === 0) return [];

    const captureClient = await this.targetPool.connect();
    let preservedTriggers: PreservedTrigger[];
    try {
      preservedTriggers = await this.captureExternalDependentTriggers(captureClient, schemas);
    } finally {
      captureClient.release();
    }

    const prepared = await this.prepareTargetSchemas();
    const client = await this.targetPool.connect();
    try {
      await client.query('BEGIN');
      if (prepared.resetSql) await client.query(prepared.resetSql);
      if (prepared.finalizeSql) await client.query(prepared.finalizeSql);
      await client.query('COMMIT');
      return preservedTriggers;
    } catch (error) {
      await client.query('ROLLBACK').catch(() => {});
      throw error;
    } finally {
      client.release();
    }
  }

  private async captureExternalDependentTriggers(
    client: pg.PoolClient,
    schemas: string[]
  ): Promise<PreservedTrigger[]> {
    const managedSchemas = getManagedSchemas();
    const result = await client.query(`
      WITH app_objects AS (
        SELECT 'pg_class'::regclass::oid AS classid, c.oid AS objid
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = ANY($1::text[])
        UNION ALL
        SELECT 'pg_proc'::regclass::oid, p.oid
        FROM pg_proc p
        JOIN pg_namespace n ON n.oid = p.pronamespace
        WHERE n.nspname = ANY($1::text[])
        UNION ALL
        SELECT 'pg_type'::regclass::oid, t.oid
        FROM pg_type t
        JOIN pg_namespace n ON n.oid = t.typnamespace
        WHERE n.nspname = ANY($1::text[])
      )
      SELECT DISTINCT
        trigger_ns.nspname AS schema_name,
        trigger_rel.relname AS table_name,
        trigger_obj.tgname AS trigger_name,
        pg_get_triggerdef(trigger_obj.oid) AS definition,
        trigger_obj.tgenabled AS enabled_mode
      FROM pg_depend d
      JOIN app_objects ao ON ao.classid = d.refclassid AND ao.objid = d.refobjid
      JOIN pg_trigger trigger_obj ON d.classid = 'pg_trigger'::regclass AND d.objid = trigger_obj.oid
      JOIN pg_class trigger_rel ON trigger_rel.oid = trigger_obj.tgrelid
      JOIN pg_namespace trigger_ns ON trigger_ns.oid = trigger_rel.relnamespace
      WHERE d.deptype IN ('n', 'a')
        AND trigger_ns.nspname = ANY($2::text[])
        AND NOT trigger_obj.tgisinternal
      ORDER BY trigger_ns.nspname, trigger_rel.relname, trigger_obj.tgname
    `, [schemas, managedSchemas]);

    return result.rows.map(row => ({
      schemaName: row.schema_name,
      tableName: row.table_name,
      triggerName: row.trigger_name,
      definition: row.definition,
      enabledMode: row.enabled_mode,
    }));
  }

  private buildTriggerRestoreSql(triggers: PreservedTrigger[]): string {
    return triggers.flatMap(trigger => [
      `DROP TRIGGER IF EXISTS ${quoteIdentifier(trigger.triggerName)} ON ${quoteIdentifier(trigger.schemaName)}.${quoteIdentifier(trigger.tableName)};`,
      `${trigger.definition.replace(/;?\s*$/, '')};`,
      `ALTER TABLE ${quoteIdentifier(trigger.schemaName)}.${quoteIdentifier(trigger.tableName)} ${this.triggerEnabledAction(trigger.enabledMode)} TRIGGER ${quoteIdentifier(trigger.triggerName)};`,
    ]).join('\n');
  }

  private quoteRole(role: string): string {
    return role === 'PUBLIC' ? 'PUBLIC' : quoteIdentifier(role);
  }

  private triggerEnabledAction(enabledMode: string): string {
    switch (enabledMode) {
      case 'D':
        return 'DISABLE';
      case 'R':
        return 'ENABLE REPLICA';
      case 'A':
        return 'ENABLE ALWAYS';
      default:
        return 'ENABLE';
    }
  }

  private async captureSourceExternalDependentTriggers(): Promise<PreservedTrigger[]> {
    if (!this.sourcePool) return [];

    const schemas = getApplicationSchemas(this.config);
    if (schemas.length === 0) return [];

    const client = await this.sourcePool.connect();
    try {
      const triggers = await this.captureExternalDependentTriggers(client, schemas);
      if (triggers.length > 0) {
        logger.info(`Captured ${triggers.length} source external trigger(s) that depend on application schemas`);
      }
      return triggers;
    } finally {
      client.release();
    }
  }

  private async captureSchemaPrivileges(
    client: pg.PoolClient,
    schema: string
  ): Promise<SchemaPrivilegeState | null> {
    const ownerResult = await client.query(`
      SELECT r.rolname AS owner
      FROM pg_namespace n
      JOIN pg_roles r ON r.oid = n.nspowner
      WHERE n.nspname = $1
    `, [schema]);

    if (ownerResult.rows.length === 0) return null;

    const schemaGrantsResult = await client.query(`
      SELECT
        COALESCE(grantee.rolname, 'PUBLIC') AS grantee,
        acl.privilege_type,
        acl.is_grantable
      FROM pg_namespace n
      CROSS JOIN LATERAL aclexplode(COALESCE(n.nspacl, acldefault('n', n.nspowner))) acl
      LEFT JOIN pg_roles grantee ON grantee.oid = acl.grantee
      WHERE n.nspname = $1
    `, [schema]);

    const defaultPrivilegeGrantsResult = await client.query(`
      SELECT
        owner.rolname AS owner,
        CASE da.defaclobjtype
          WHEN 'r' THEN 'TABLES'
          WHEN 'S' THEN 'SEQUENCES'
          WHEN 'f' THEN 'FUNCTIONS'
          WHEN 'T' THEN 'TYPES'
        END AS object_type,
        COALESCE(grantee.rolname, 'PUBLIC') AS grantee,
        acl.privilege_type,
        acl.is_grantable
      FROM pg_default_acl da
      JOIN pg_namespace n ON n.oid = da.defaclnamespace
      JOIN pg_roles owner ON owner.oid = da.defaclrole
      CROSS JOIN LATERAL aclexplode(da.defaclacl) acl
      LEFT JOIN pg_roles grantee ON grantee.oid = acl.grantee
      WHERE n.nspname = $1
        AND da.defaclobjtype IN ('r', 'S', 'f', 'T')
    `, [schema]);

    return {
      owner: ownerResult.rows[0].owner,
      schemaGrants: schemaGrantsResult.rows.map(row => ({
        grantee: row.grantee,
        privilegeType: row.privilege_type,
        isGrantable: row.is_grantable,
      })),
      defaultPrivilegeGrants: defaultPrivilegeGrantsResult.rows.map(row => ({
        owner: row.owner,
        objectType: row.object_type,
        grantee: row.grantee,
        privilegeType: row.privilege_type,
        isGrantable: row.is_grantable,
      })),
    };
  }

  private buildSchemaPrivilegeSql(schema: string, state: SchemaPrivilegeState): string {
    const quotedSchema = quoteIdentifier(schema);
    return [
      `ALTER SCHEMA ${quotedSchema} OWNER TO ${this.quoteRole(state.owner)};`,
      ...state.schemaGrants.map(grant =>
        `GRANT ${grant.privilegeType} ON SCHEMA ${quotedSchema} TO ${this.quoteRole(grant.grantee)}${grant.isGrantable ? ' WITH GRANT OPTION' : ''};`
      ),
      ...state.defaultPrivilegeGrants.map(grant =>
        `ALTER DEFAULT PRIVILEGES FOR ROLE ${this.quoteRole(grant.owner)} IN SCHEMA ${quotedSchema} GRANT ${grant.privilegeType} ON ${grant.objectType} TO ${this.quoteRole(grant.grantee)}${grant.isGrantable ? ' WITH GRANT OPTION' : ''};`
      ),
    ].join('\n');
  }

  private async preprocessDumpFile(dumpFile: string): Promise<string> {
    const content = await this.tempFileManager.readFile(dumpFile);

    // pg_dump flags already exclude owners and managed schemas. Only strip a
    // version-specific SET statement; broad SQL regexes can corrupt function bodies.
    let processed = stripUnsupportedDumpSettings(content);

    for (const schema of getApplicationSchemas(this.config)) {
      const statement = `CREATE SCHEMA ${quoteIdentifier(schema)};`;
      const statementIndex = processed.indexOf(statement);
      const headerIndex = processed.lastIndexOf('-- Name:', statementIndex);
      if (
        statementIndex >= 0 &&
        headerIndex >= 0 &&
        statementIndex - headerIndex < 1000 &&
        processed.slice(headerIndex, statementIndex).includes('Type: SCHEMA;')
      ) {
        processed = `${processed.slice(0, statementIndex)}${processed.slice(statementIndex + statement.length).replace(/^\r?\n/, '')}`;
      }
    }

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
    const sourceTriggers = await this.captureSourceExternalDependentTriggers();
    const prepared = await this.prepareTargetSchemas();
    const finalizeSql = [
      prepared.finalizeSql,
      this.buildTriggerRestoreSql(sourceTriggers),
    ].filter(Boolean).join('\n');
    await this.importSchema(dumpFile, prepared.resetSql, finalizeSql);
  }
}
