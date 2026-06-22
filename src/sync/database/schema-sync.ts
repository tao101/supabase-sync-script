import { execa } from 'execa';
import type pg from 'pg';
import type { Config } from '../../types/config.js';
import { ConnectionBuilder } from '../../config/connection-builder.js';
import { TempFileManager } from '../../utils/temp-files.js';
import { logger } from '../../utils/logger.js';
import { SyncError, ErrorCategory } from '../../types/sync.js';
import type { PostgresPool } from '../../clients/postgres-client.js';
import { getApplicationSchemas, quoteIdentifier, stripUnsupportedDumpSettings } from './schemas.js';

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

export class SchemaSync {
  private connectionBuilder: ConnectionBuilder;

  constructor(
    private config: Config,
    private tempFileManager: TempFileManager,
    private targetPool: PostgresPool
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
      '--no-privileges',
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
        '-v', 'ON_ERROR_STOP=0', // Continue on errors (some objects may already exist)
      ], {
        env: { ...process.env, PGPASSWORD: this.config.target.dbPassword },
        reject: false, // Don't throw on non-zero exit
      });

      // Check for actual errors in stderr
      if (result.stderr && result.stderr.trim()) {
        const errorLines = result.stderr.split('\n').filter(line => {
          if (!line.includes('ERROR')) return false;
          // Filter out expected errors
          if (line.includes('already exists')) return false;
          if (line.includes('must be owner of')) return false; // System tables owned by supabase_admin
          if (line.includes('current transaction is aborted')) return false; // Cascading from other errors
          if (line.includes('permission denied')) return false; // System table permissions
          return true;
        });
        if (errorLines.length > 0) {
          logger.warn(`Schema import had ${errorLines.length} errors:`);
          errorLines.slice(0, 5).forEach(line => logger.warn(`  ${line.trim()}`));
          if (errorLines.length > 5) {
            logger.warn(`  ... and ${errorLines.length - 5} more errors`);
          }
          throw new SyncError(
            `Schema import failed with ${errorLines.length} error(s): ${errorLines[0]?.trim() || 'unknown error'}`,
            ErrorCategory.IMPORT,
            'schema-import',
            false
          );
        }
      }

      if (result.exitCode !== 0) {
        logger.warn(`Schema import completed with exit code ${result.exitCode} (some errors may be expected)`);
      } else {
        logger.info('Schema imported successfully');
      }
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

  async resetTargetSchemas(): Promise<PreservedTrigger[]> {
    const schemas = getApplicationSchemas(this.config);
    if (schemas.length === 0) return [];

    logger.info(`Resetting target application schemas: ${schemas.join(', ')}`);

    const client = await this.targetPool.connect();
    try {
      const preservedTriggers = await this.captureExternalDependentTriggers(client, schemas);
      if (preservedTriggers.length > 0) {
        logger.info(`Preserving ${preservedTriggers.length} external trigger(s) that depend on application schemas`);
      }

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
                  AND trigger_ns.nspname <> ALL($1::text[])
                  AND NOT trigger_obj.tgisinternal
              )
            )
        ),
        dependency_schemas AS (
          SELECT
            COALESCE(
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
        WHERE dependent_schema IS NOT NULL
          AND dependent_schema <> ALL($1::text[])
        ORDER BY dependent_object, referenced_object
        LIMIT 10
      `, [schemas]);

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

      for (const schema of schemas) {
        const privileges = await this.captureSchemaPrivileges(client, schema);
        const quotedSchema = quoteIdentifier(schema);
        await client.query(`DROP SCHEMA IF EXISTS ${quotedSchema} CASCADE`);
        await client.query(`CREATE SCHEMA ${quotedSchema}`);
        await this.restoreSchemaPrivileges(client, schema, privileges);
      }

      return preservedTriggers;
    } catch (error) {
      if (error instanceof SyncError) throw error;
      throw new SyncError(
        `Failed to reset target schemas: ${(error as Error).message}`,
        ErrorCategory.IMPORT,
        'schema-reset',
        false,
        error as Error
      );
    } finally {
      client.release();
    }
  }

  private async captureExternalDependentTriggers(
    client: pg.PoolClient,
    schemas: string[]
  ): Promise<PreservedTrigger[]> {
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
        AND trigger_ns.nspname <> ALL($1::text[])
        AND NOT trigger_obj.tgisinternal
      ORDER BY trigger_ns.nspname, trigger_rel.relname, trigger_obj.tgname
    `, [schemas]);

    return result.rows.map(row => ({
      schemaName: row.schema_name,
      tableName: row.table_name,
      triggerName: row.trigger_name,
      definition: row.definition,
      enabledMode: row.enabled_mode,
    }));
  }

  private async restorePreservedTriggers(triggers: PreservedTrigger[]): Promise<void> {
    if (triggers.length === 0) return;

    logger.info(`Restoring ${triggers.length} preserved external trigger(s)...`);

    const client = await this.targetPool.connect();
    try {
      for (const trigger of triggers) {
        await client.query(
          `DROP TRIGGER IF EXISTS ${quoteIdentifier(trigger.triggerName)} ON ${quoteIdentifier(trigger.schemaName)}.${quoteIdentifier(trigger.tableName)}`
        );
        await client.query(trigger.definition);
        await client.query(
          `ALTER TABLE ${quoteIdentifier(trigger.schemaName)}.${quoteIdentifier(trigger.tableName)} ${this.triggerEnabledAction(trigger.enabledMode)} TRIGGER ${quoteIdentifier(trigger.triggerName)}`
        );
      }
    } catch (error) {
      throw new SyncError(
        `Failed to restore preserved triggers: ${(error as Error).message}`,
        ErrorCategory.IMPORT,
        'schema-reset',
        false,
        error as Error
      );
    } finally {
      client.release();
    }
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

  private async captureSchemaPrivileges(
    client: pg.PoolClient,
    schema: string
  ): Promise<SchemaPrivilegeState> {
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

  private async restoreSchemaPrivileges(
    client: pg.PoolClient,
    schema: string,
    state: SchemaPrivilegeState
  ): Promise<void> {
    const quotedSchema = quoteIdentifier(schema);

    for (const grant of state.schemaGrants) {
      await client.query(
        `GRANT ${grant.privilegeType} ON SCHEMA ${quotedSchema} TO ${this.quoteRole(grant.grantee)}${grant.isGrantable ? ' WITH GRANT OPTION' : ''}`
      );
    }

    for (const grant of state.defaultPrivilegeGrants) {
      await client.query(
        `ALTER DEFAULT PRIVILEGES FOR ROLE ${this.quoteRole(grant.owner)} IN SCHEMA ${quotedSchema} GRANT ${grant.privilegeType} ON ${grant.objectType} TO ${this.quoteRole(grant.grantee)}${grant.isGrantable ? ' WITH GRANT OPTION' : ''}`
      );
    }
  }

  private async preprocessDumpFile(dumpFile: string): Promise<string> {
    const content = await this.tempFileManager.readFile(dumpFile);

    // Remove problematic statements
    let processed = stripUnsupportedDumpSettings(content)
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
    const preservedTriggers = await this.resetTargetSchemas();
    try {
      await this.importSchema(dumpFile);
    } catch (error) {
      try {
        await this.restorePreservedTriggers(preservedTriggers);
      } catch (restoreError) {
        logger.warn(`Failed to restore preserved triggers after schema import failure: ${(restoreError as Error).message}`);
      }
      throw error;
    }
    await this.restorePreservedTriggers(preservedTriggers);
  }
}
