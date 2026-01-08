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
- **Connection Testing**: Validates database URLs immediately after input

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
1. Entering source database URL (connection tested immediately)
2. Entering source Supabase API URL and service role key
3. Entering target database URL (connection tested immediately)
4. Entering target Supabase API URL and service role key
5. Choosing which components to sync
6. Confirming the operation

### CI Mode

For automated pipelines, use environment variables:

```bash
# Set environment variables
export SOURCE_DB_URL="postgresql://postgres:password@db.your-project.supabase.co:5432/postgres"
export SOURCE_API_URL="https://your-project.supabase.co"
export SOURCE_SERVICE_ROLE_KEY="eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9..."

export TARGET_DB_URL="postgresql://postgres:password@your-server.com:5432/postgres"
export TARGET_API_URL="https://supabase.your-server.com"
export TARGET_SERVICE_ROLE_KEY="eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9..."

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
    "dbUrl": "postgresql://postgres:your-password@db.your-project.supabase.co:5432/postgres",
    "apiUrl": "https://your-project.supabase.co",
    "serviceRoleKey": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9..."
  },
  "target": {
    "dbUrl": "postgresql://postgres:your-password@your-server.com:5432/postgres",
    "apiUrl": "https://supabase.your-server.com",
    "serviceRoleKey": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9..."
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
SYNC_TEMP_DIR=/tmp/supabase-sync

# Source Configuration
SOURCE_DB_URL=postgresql://postgres:password@db.your-project.supabase.co:5432/postgres
SOURCE_API_URL=https://your-project.supabase.co
SOURCE_SERVICE_ROLE_KEY=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...

# Target Configuration
TARGET_DB_URL=postgresql://postgres:password@your-server.com:5432/postgres
TARGET_API_URL=https://supabase.your-server.com
TARGET_SERVICE_ROLE_KEY=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...

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
```

## Database URL Format

The database URL follows the PostgreSQL connection string format:

```
postgresql://[user]:[password]@[host]:[port]/[database]
```

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

### Destructive Operation

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
- Starts with `postgresql://`
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
which psql
```

### Permission Errors

- Ensure database user has sufficient privileges (postgres user recommended)
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
