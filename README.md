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
- **Auth Users Sync**: Preserves password hashes so users can login with same credentials
- **Storage Sync**: Buckets and files with concurrent uploads
- **Roles Sync**: Database roles (filters out built-in Supabase roles)
- **Two Modes**: Interactive (guided prompts) and CI (automated)
- **Dry Run**: Preview changes without applying them

## Prerequisites

- **Node.js** >= 18
- **PostgreSQL client tools** (`pg_dump`, `psql`) installed on your system

### Installing PostgreSQL Client Tools

```bash
# Ubuntu/Debian
sudo apt-get install postgresql-client

# macOS
brew install postgresql

# Windows (via chocolatey)
choco install postgresql
```

## Usage

### Interactive Mode (Recommended for first-time use)

Simply run the command and follow the prompts:

```bash
npx supabase-sync sync
```

You'll be guided through:
1. Selecting source type and entering credentials
2. Selecting target type and entering credentials
3. Choosing which components to sync
4. Confirming the operation

### CI Mode

For automated pipelines, use environment variables:

```bash
# Set environment variables
export SOURCE_TYPE=saas
export SOURCE_PROJECT_REF=your-project-ref
export SOURCE_DB_PASSWORD=your-password
export SOURCE_SERVICE_ROLE_KEY=your-service-role-key
export TARGET_TYPE=self-hosted
export TARGET_HOST=supabase.example.com
export TARGET_DB_PASSWORD=your-target-password
export TARGET_SERVICE_ROLE_KEY=your-target-key
export TARGET_API_URL=https://supabase.example.com

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
| `--dry-run` | Preview changes without applying |
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

# Dry run to preview changes
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

```bash
npx supabase-sync test-connection --config ./config.json
```

## Configuration

### Config File (config.json)

```json
{
  "source": {
    "type": "saas",
    "projectRef": "abcdefghijklmnop",
    "dbPassword": "your-database-password",
    "serviceRoleKey": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9..."
  },
  "target": {
    "type": "self-hosted",
    "host": "supabase.example.com",
    "port": 5432,
    "dbPassword": "your-target-password",
    "serviceRoleKey": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
    "apiUrl": "https://supabase.example.com"
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
      "includeSchemas": ["public", "auth", "storage"],
      "excludeTables": ["auth.sessions", "auth.refresh_tokens"]
    },
    "storage": {
      "concurrency": 5,
      "maxFileSizeMB": 50,
      "excludeBuckets": ["temp-uploads"]
    }
  },
  "dryRun": false,
  "verbose": false
}
```

### Environment Variables

```bash
# Mode
SYNC_MODE=ci                    # ci or interactive
SYNC_DRY_RUN=false
SYNC_VERBOSE=false

# Source - SaaS (Supabase Cloud)
SOURCE_TYPE=saas
SOURCE_PROJECT_REF=abcdefghij   # From your Supabase dashboard URL
SOURCE_DB_PASSWORD=xxx
SOURCE_SERVICE_ROLE_KEY=xxx

# Source - Self-hosted
SOURCE_TYPE=self-hosted
SOURCE_HOST=supabase.example.com
SOURCE_PORT=5432
SOURCE_DB_PASSWORD=xxx
SOURCE_SERVICE_ROLE_KEY=xxx
SOURCE_API_URL=https://supabase.example.com

# Source - Local (supabase start)
SOURCE_TYPE=local
SOURCE_PORT=54322
SOURCE_DB_PASSWORD=postgres
SOURCE_SERVICE_ROLE_KEY=xxx     # From `supabase status`

# Target (same options as source)
TARGET_TYPE=self-hosted
TARGET_HOST=supabase.example.com
TARGET_PORT=5432
TARGET_DB_PASSWORD=xxx
TARGET_SERVICE_ROLE_KEY=xxx
TARGET_API_URL=https://supabase.example.com

# Component toggles
SYNC_SCHEMA=true
SYNC_DATA=true
SYNC_AUTH=true
SYNC_STORAGE=true
SYNC_ROLES=true

# Storage options
STORAGE_CONCURRENCY=5
STORAGE_MAX_FILE_SIZE_MB=50
STORAGE_EXCLUDE_BUCKETS=temp,cache
```

## Connection Types

### SaaS (Supabase Cloud)

For projects hosted on supabase.com:

- **projectRef**: Found in your dashboard URL (`https://supabase.com/dashboard/project/[projectRef]`)
- **dbPassword**: Project Settings > Database > Connection string
- **serviceRoleKey**: Project Settings > API > service_role key

### Self-hosted

For self-hosted Supabase instances, you'll need:
- Host address
- Database port (default: 5432)
- Database password
- Service role key
- API URL

### Local

For local development with `supabase start`:
- Default port: 54322
- Default password: postgres
- Get service role key from `supabase status`

## Sync Workflow

```
1. Validate Connections
   ↓
2. Sync Roles (export → filter → import)
   ↓
3. Sync Schema (pg_dump → process → psql)
   ↓
4. Sync Data (disable constraints → truncate → import → enable)
   ↓
5. Reset Sequences (prevents primary key conflicts)
   ↓
6. Sync Auth Users (preserves password hashes)
   ↓
7. Sync Storage (create buckets → upload files)
   ↓
8. Verify & Cleanup
```

## Important Notes

### ⚠️ Destructive Operation

This tool performs a **full replacement** of data on the target. All existing data in the synced schemas will be **deleted and replaced**.

### Auth Users

- Password hashes are preserved - users can login with the same credentials
- Sessions and refresh tokens are NOT synced (users need to login again)
- OAuth identities are synced

### Foreign Key Constraints

The sync handles foreign key constraints automatically:
- Disables triggers during import
- Defers constraint checking
- Data is imported in dependency order
- Constraints are re-enabled after import

### Sequences

After importing data, all PostgreSQL sequences are reset to `MAX(id) + 1` to prevent primary key conflicts on new inserts.

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
        run: sudo apt-get install -y postgresql-client

      - name: Run sync
        env:
          SOURCE_TYPE: saas
          SOURCE_PROJECT_REF: ${{ secrets.SOURCE_PROJECT_REF }}
          SOURCE_DB_PASSWORD: ${{ secrets.SOURCE_DB_PASSWORD }}
          SOURCE_SERVICE_ROLE_KEY: ${{ secrets.SOURCE_SERVICE_ROLE_KEY }}
          TARGET_TYPE: self-hosted
          TARGET_HOST: ${{ secrets.TARGET_HOST }}
          TARGET_DB_PASSWORD: ${{ secrets.TARGET_DB_PASSWORD }}
          TARGET_SERVICE_ROLE_KEY: ${{ secrets.TARGET_SERVICE_ROLE_KEY }}
          TARGET_API_URL: ${{ secrets.TARGET_API_URL }}
        run: npx supabase-sync sync --ci
```

## Troubleshooting

### Connection Errors

```bash
# Test connections first
npx supabase-sync test-connection --config ./config.json
```

### pg_dump/psql Not Found

Ensure PostgreSQL client tools are installed and in your PATH:

```bash
which pg_dump
which psql
```

### Permission Errors

- Ensure database password has sufficient privileges
- Service role key must have admin access for auth operations

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
