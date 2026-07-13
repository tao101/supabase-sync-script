# supabase-sync

A CLI tool to perform full one-time migrations between Supabase instances. Sync database schema, data, auth users, and storage files.

[![npm version](https://badge.fury.io/js/supabase-sync.svg)](https://www.npmjs.com/package/supabase-sync)

## Quick Start

```bash
# Run directly with npx (no install required)
npx supabase-sync sync

# Or install globally
npm install -g supabase-sync
supabase-sync sync
```

## Supported Sync Combinations

| Source | Target |
|--------|--------|
| SaaS (Supabase Cloud) | Self-hosted |
| SaaS (Supabase Cloud) | Local |
| Self-hosted | Self-hosted |
| Self-hosted | Local |

## Features

- **Database Schema Sync**: Tables, functions, triggers, RLS policies
- **Database Data Sync**: Full data migration with COPY format for performance
- **Sequence Reset**: Automatically resets sequences after import to prevent primary key conflicts
- **Auth Users Sync**: Preserves password hashes by default so users can log in with the same credentials
- **Storage Sync**: Buckets and files with concurrent uploads
- **Roles Sync**: Database roles (filters out built-in Supabase roles)
- **Two Modes**: Interactive (guided prompts) and CI (automated)
- **Dry Run**: Run connection checks and non-mutating preflight without target changes
- **Connection Testing**: Validates database URLs immediately after input

## Prerequisites

- **Node.js** >= 18
- **PostgreSQL client tools** >= 16 (`pg_dump`, `pg_dumpall`, `psql`) installed on your system

### Installing PostgreSQL Client Tools

```bash
# Ubuntu/Debian (enable the official PGDG repository first if needed)
sudo apt-get install postgresql-client-16

# macOS
brew install libpq
export PATH="$(brew --prefix libpq)/bin:$PATH"

# Windows (via chocolatey)
choco install postgresql

# Verify every required tool is version 16 or newer
psql --version
pg_dump --version
pg_dumpall --version
```

See PostgreSQL's official [Linux download instructions](https://www.postgresql.org/download/linux/)
if your distribution does not provide `postgresql-client-16` or a newer version.

## Usage

### Interactive Mode (Recommended for first-time use)

Simply run the command and follow the prompts:

```bash
npx supabase-sync sync
```

You'll be guided through:
1. Choosing which components to sync
2. Entering source database URL (connection tested immediately)
3. Entering source Supabase API credentials when storage sync is enabled
4. Entering target database URL (connection tested immediately)
5. Entering target Supabase API credentials when storage sync is enabled
6. Confirming the operation

### CI Mode

For automated pipelines, use environment variables:

```bash
# Set environment variables
export SOURCE_DB_URL="postgresql://postgres:password@db.your-project.supabase.co:5432/postgres"
export SOURCE_API_URL="https://your-project.supabase.co"
export SOURCE_SERVICE_ROLE_KEY="header.payload.signature"

export TARGET_DB_URL="postgresql://postgres:password@your-server.com:5432/postgres"
export TARGET_API_URL="https://supabase.your-server.com"
export TARGET_SERVICE_ROLE_KEY="header.payload.signature"

# Run in CI mode
npx supabase-sync sync --ci
```

Or use a config file:

```bash
npx supabase-sync sync --ci --config ./config.json
```

## CLI Commands

### sync

Perform full sync from source to target.

```bash
npx supabase-sync sync [options]
```

| Option | Description |
|--------|-------------|
| `-c, --config <path>` | Path to config file |
| `--ci` | Run in CI mode (non-interactive) |
| `--dry-run` | Run connection checks and read-only preflight without target changes |
| `--verbose` | Enable debug logging |
| `--skip-schema` | Skip database schema sync |
| `--skip-data` | Skip database data sync |
| `--skip-auth` | Skip auth users sync |
| `--skip-storage` | Skip storage sync |
| `--skip-roles` | Skip roles sync |

**Examples:**

```bash
# Interactive sync
npx supabase-sync sync

# Run non-mutating connection checks and preflight
npx supabase-sync sync --dry-run

# Sync only database (skip auth and storage)
npx supabase-sync sync --skip-auth --skip-storage

# CI mode with config file
npx supabase-sync sync --ci --config ./config.json --verbose
```

### validate

Validate configuration without syncing.

```bash
npx supabase-sync validate --config ./config.json
```

### test-connection

Test connections to source and target.

When roles, schema, or data sync is enabled, this also verifies the required
PostgreSQL 16+ tools and their libpq connections.

```bash
npx supabase-sync test-connection --config ./config.json
```

## Configuration

### Config File (config.json)

```json
{
  "source": {
    "dbUrl": "postgresql://postgres:your-password@db.your-project.supabase.co:5432/postgres",
    "apiUrl": "https://your-project.supabase.co",
    "serviceRoleKey": "header.payload.signature"
  },
  "target": {
    "dbUrl": "postgresql://postgres:your-password@your-server.com:5432/postgres",
    "apiUrl": "https://supabase.your-server.com",
    "serviceRoleKey": "header.payload.signature"
  },
  "options": {
    "components": {
      "schema": true,
      "data": true,
      "auth": true,
      "storage": true,
      "roles": true
    },
    "database": {
      "includeSchemas": ["public"],
      "excludeTables": []
    },
    "storage": {
      "concurrency": 5,
      "maxFileSizeMB": 50,
      "excludeBuckets": ["temp-uploads"]
    },
    "auth": {
      "preservePasswordHashes": true,
      "migrateIdentities": true,
      "skipSessions": true
    }
  },
  "dryRun": false,
  "verbose": false
}
```

All credentials in the example are placeholders and must be replaced. The example
uses legacy JWT service-role keys. For Supabase's newer key format,
replace each `serviceRoleKey` with `secretKey: "sb_secret_..."`. Do not configure
both key formats on the same connection. API URLs and keys are optional when
storage sync is disabled; auth migration uses the direct database connection.

`database.excludeTables` preserves matching target table data and therefore
requires schema sync to be disabled (for example, `--skip-schema`). A schema
sync replaces the entire selected application schema.

### Environment Variables

```bash
# Mode
SYNC_MODE=ci                    # ci or interactive
SYNC_DRY_RUN=false
SYNC_VERBOSE=false
SYNC_TEMP_DIR=/tmp/supabase-sync

# Source Configuration
SOURCE_DB_URL=postgresql://postgres:password@db.your-project.supabase.co:5432/postgres
SOURCE_API_URL=https://your-project.supabase.co
SOURCE_SERVICE_ROLE_KEY=header.payload.signature
# Or: SOURCE_SECRET_KEY=sb_secret_...

# Target Configuration
TARGET_DB_URL=postgresql://postgres:password@your-server.com:5432/postgres
TARGET_API_URL=https://supabase.your-server.com
TARGET_SERVICE_ROLE_KEY=header.payload.signature
# Or: TARGET_SECRET_KEY=sb_secret_...

# Component toggles
SYNC_SCHEMA=true
SYNC_DATA=true
SYNC_AUTH=true
SYNC_STORAGE=true
SYNC_ROLES=true

# Storage options
STORAGE_CONCURRENCY=5
STORAGE_MAX_FILE_SIZE_MB=50
# STORAGE_EXCLUDE_BUCKETS=temp,cache

# Database selection
DB_INCLUDE_SCHEMAS=public
# DB_EXCLUDE_SCHEMAS=audit
# DB_EXCLUDE_TABLES=public.large_archive
```

Environment variables override config-file values only when they are set. Unknown
config keys and invalid boolean or numeric values are rejected instead of silently
falling back to destructive defaults.

## Database URL Format

The database URL follows the PostgreSQL connection string format:

```
postgresql://user[:password]@host[:port]/database
```

`postgres://` is also accepted. User, host, and database must be explicit so the
Node client and PostgreSQL tools cannot resolve different implicit endpoints.

Remote connections default to certificate- and hostname-verified TLS
(`sslmode=verify-full`) using the system trust store. This requires PostgreSQL 16+
client tools; pass `sslrootcert=/path/to/ca.pem` for a private CA. Exact loopback
hosts default to `sslmode=disable`. Only `verify-full` and `disable` are accepted.
For a remote server that intentionally uses plaintext on a trusted network, opt in
explicitly with `?sslmode=disable`; the CLI never downgrades TLS automatically.

### Examples

**Supabase Cloud (SaaS):**
```
postgresql://postgres:your-password@db.abcdefghijk.supabase.co:5432/postgres
```

**Self-hosted Supabase:**
```
postgresql://postgres:your-password@supabase.example.com:5432/postgres
```

**Local Supabase (supabase start):**
```
postgresql://postgres:postgres@localhost:54322/postgres
```

### Finding Your Database URL

**Supabase Cloud:**
1. Go to your project dashboard
2. Click "Project Settings" > "Database"
3. Copy the connection string (URI format)

**Self-hosted:**
Use your database host, port, and credentials.

**Local:**
Run `supabase status` to see connection details.

## Sync Workflow

```
1. Validate Connections
   ↓
2. Sync Roles (export → filter → import)
   ↓
3. Sync Schema (pg_dump → process → psql)
   ↓
4. Sync Auth Users (before application rows that reference `auth.users`)
   ↓
5. Sync Data (truncate + import in one transaction)
   ↓
6. Reset Sequences (prevents primary key conflicts)
   ↓
7. Sync Storage (create buckets → upload files)
   ↓
8. Verify & Cleanup
```

## Important Notes

### Destructive Operation

Database schema/data and auth sync are destructive for the selected scopes. Existing
data in synced application schemas is deleted and replaced; Storage has the
non-deleting upsert behavior described below.

Raw database schema/data sync is intended for application schemas such as `public`.
Supabase-managed schemas are rejected in `includeSchemas`. `auth` and `storage` are
handled by dedicated steps. `realtime`, `extensions`, `vault`, `graphql`,
`graphql_public`, `net`, `pgsodium`, `supabase_functions`, and
`supabase_migrations` are not migrated.

Schema reset/import and data truncate/import are transactional: a SQL import
failure rolls back the destructive target changes. Source and target endpoints
with the same normalized host, port, and database—or the same Storage API—are
rejected before execution. DNS aliases cannot be identified automatically; do not
configure two different hostnames that reach the same database.

Components commit independently. A failure in a later component does not roll back
an earlier completed role, schema, auth, data, or Storage step.

### Auth Users

- Password hashes are preserved by default; set `preservePasswordHashes` to `false` to omit them
- Sessions, refresh tokens, and other login state are cleared and not synced; `skipSessions` must remain `true`
- OAuth identities are synced by default; set `migrateIdentities` to `false` to omit them

Target-only users are removed only after application data sync succeeds. Auth-only
sync refuses to remove a user while an application table still references it.

### Roles

Role import is strict. If a same-named custom role already exists on the target, the
step fails; use `--skip-roles` for targets where roles are pre-provisioned.

### Foreign Key Constraints

The sync handles foreign key constraints automatically:
- Disables ordinary triggers, including foreign-key enforcement, only in the import transaction
- Truncates all included tables together without `CASCADE`
- Refuses to erase excluded dependent tables
- Re-enables constraints on commit and verifies foreign-key integrity afterward

### Sequences

After importing data, owned PostgreSQL sequences are moved beyond the current
`MAX(id)` (or below `MIN(id)` for descending sequences) to prevent primary-key conflicts.

Ascending and descending serial/identity sequences are reset using exact PostgreSQL
`bigint` strings rather than relying on JavaScript number precision.

### Storage Behavior

Storage sync creates missing buckets and upserts source objects. It does not delete
target-only objects or overwrite settings on an existing bucket. Before the first
target Storage write, every selected object is preflighted; missing size metadata or
an object larger than `storage.maxFileSizeMB` fails the run rather than being skipped.
Storage writes are not transactional: uploads completed before a later failure remain.
Only public Storage object URLs (`/storage/v1/object/public/`) are rewritten, in auth
avatar fields and URL-named text columns in the selected application schemas.

## GitHub Actions Example

```yaml
name: Sync Supabase

on:
  workflow_dispatch:

jobs:
  sync:
    runs-on: ubuntu-latest
    steps:
      - name: Install PostgreSQL client
        run: |
          sudo apt-get install -y postgresql-common
          sudo /usr/share/postgresql-common/pgdg/apt.postgresql.org.sh
          sudo apt-get install -y postgresql-client-16
          psql --version

      - name: Run sync
        env:
          SOURCE_DB_URL: ${{ secrets.SOURCE_DB_URL }}
          SOURCE_API_URL: ${{ secrets.SOURCE_API_URL }}
          SOURCE_SERVICE_ROLE_KEY: ${{ secrets.SOURCE_SERVICE_ROLE_KEY }}
          TARGET_DB_URL: ${{ secrets.TARGET_DB_URL }}
          TARGET_API_URL: ${{ secrets.TARGET_API_URL }}
          TARGET_SERVICE_ROLE_KEY: ${{ secrets.TARGET_SERVICE_ROLE_KEY }}
        run: npx supabase-sync sync --ci
```

## Troubleshooting

### Connection Errors

```bash
# Test connections first
npx supabase-sync test-connection --config ./config.json
```

### Invalid Database URL

Ensure your database URL:
- Starts with `postgresql://` or `postgres://`
- Includes an explicit user, host, and database name
- Contains the correct password (URL-encoded if special characters)
- Uses the correct port (5432 for cloud, 54322 for local)

**URL-encoding special characters in password:**
```
@ → %40
# → %23
? → %3F
/ → %2F
```

### pg_dump/psql Not Found

Ensure PostgreSQL client tools are installed and in your PATH:

```bash
which pg_dump
which pg_dumpall
which psql
```

### Permission Errors

- Ensure database user has sufficient privileges (postgres user recommended)
- Database credentials need access to the `auth` schema for auth migration
- The admin API key needs Storage read/write access when storage sync is enabled

### Timeout on Large Databases

For large databases, consider:
- Syncing components separately (`--skip-storage` first)
- Running on a machine closer to your database

## Programmatic Usage

You can also use this as a library:

```typescript
import { SyncOrchestrator, loadConfig } from 'supabase-sync';

const config = await loadConfig({ configPath: './config.json' });
const orchestrator = new SyncOrchestrator(config);
const result = await orchestrator.execute();

console.log(result.success ? 'Sync complete!' : 'Sync failed');
```

## License

MIT
