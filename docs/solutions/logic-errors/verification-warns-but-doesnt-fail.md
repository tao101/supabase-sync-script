---
title: "Data verification silently passes on row count mismatches"
category: logic-errors
tags: [data-integrity, verification, error-handling, sync, supabase]
module: sync/database
symptom: "Sync completes successfully even when row counts don't match between source and target databases"
root_cause: "verifyDataCounts() only logged warnings instead of returning failure status and throwing an error"
date: 2026-02-03
---

# Data Verification Silently Passes on Row Count Mismatches

## Problem Statement

The Supabase sync script's verification step only logged warnings when row count mismatches were detected between source and destination databases, but didn't actually fail the sync operation. This meant users couldn't trust that a "successful" sync completion was actually valid - data could be missing without any error being raised.

**Symptom:** Sync completes with exit code 0 even when tables have missing rows.

**Observable behavior:**
```
✓ Data imported successfully
⚠ Found 3 tables with row count mismatches:
    public.users: source=1000, target=998
    public.orders: source=5000, target=4995
✓ Verification complete  <-- This should have FAILED
```

## Root Cause

The `verifyDataCounts()` method had return type `Promise<void>` - it could only log warnings, not communicate failure to callers. The orchestrator's `verify()` method called it but had no way to know if verification passed or failed.

This is a classic "silent failure" anti-pattern where validation methods detect problems but don't propagate failure status.

## Solution

### Step 1: Modify `verifyDataCounts()` to Return a Boolean

**File:** `src/sync/database/data-sync.ts`

```typescript
// BEFORE
async verifyDataCounts(sourcePool: PostgresPool): Promise<void> {
  // ... verification logic ...
  if (mismatches.length > 0) {
    logger.warn(`Found ${mismatches.length} tables with row count mismatches:`);
    // Logs warning but doesn't indicate failure
  }
}

// AFTER
async verifyDataCounts(sourcePool: PostgresPool): Promise<boolean> {
  // ... verification logic ...
  if (mismatches.length > 0) {
    logger.warn(`Found ${mismatches.length} tables with row count mismatches:`);
    return false;  // Indicate verification failed
  }
  return true;  // Indicate verification passed
}
```

### Step 2: Update Orchestrator to Check Results and Throw on Failure

**File:** `src/core/sync-orchestrator.ts`

```typescript
// BEFORE
private async verify(): Promise<void> {
  if (this.config.options.components.data) {
    const sequenceSync = new SequenceSync(this.config, this.targetPool);
    await sequenceSync.verifySequences();  // Return value ignored!
  }
  print.success('Verification complete');
}

// AFTER
private async verify(): Promise<void> {
  if (this.config.options.components.data && this.sourcePool) {
    // Verify row counts - FAIL if mismatch
    const dataSync = new DataSync(this.config, this.tempFileManager, this.targetPool);
    const countsMatch = await dataSync.verifyDataCounts(this.sourcePool);
    if (!countsMatch) {
      throw new SyncError(
        'Data verification failed: row counts do not match between source and target',
        ErrorCategory.VALIDATION,
        'verify',
        false  // not recoverable
      );
    }

    // Verify sequences - FAIL if invalid
    const sequenceSync = new SequenceSync(this.config, this.targetPool);
    const sequencesValid = await sequenceSync.verifySequences();
    if (!sequencesValid) {
      throw new SyncError(
        'Data verification failed: sequences are invalid',
        ErrorCategory.VALIDATION,
        'verify',
        false
      );
    }
  }
  print.success('Verification complete');
}
```

### Why This Works

1. **Minimal changes:** ~25 lines across 2 files
2. **Proper error propagation:** `SyncError` with `ErrorCategory.VALIDATION` is thrown
3. **Backward compatible:** Warning logs still generated for debugging
4. **Clear exit code:** Sync now exits with code 1 on verification failure

## Prevention

### Code Review Checklist

- [ ] Does every validation/verification method have its return value checked by the caller?
- [ ] Are `logger.warn()` calls accompanied by appropriate error handling?
- [ ] For boolean-returning functions: what happens if the caller ignores the return value?
- [ ] Is there a test that verifies the system FAILS when validation returns false?

### Architectural Principles

**Principle 1: Verification Should Be Gating**
Verification is meaningless if it doesn't prevent the process from succeeding.

**Principle 2: Warnings are for Recoverable Issues Only**
Data integrity issues in sync tools are NOT recoverable - they should be errors.

**Principle 3: Return Types Should Reflect Consequences**
- `void` = side effects only, caller need not check anything
- `boolean` = caller MUST check and act on the result
- Throwing = explicitly forces error handling

### Recommended Test Cases

```typescript
describe('SyncOrchestrator', () => {
  it('should fail sync when row counts do not match', async () => {
    mockDataSync.verifyDataCounts.mockResolvedValue(false);

    const result = await orchestrator.execute();
    expect(result.success).toBe(false);
    expect(result.errors[0].category).toBe(ErrorCategory.VALIDATION);
  });
});
```

### Linting Rules

Enable `@typescript-eslint/no-floating-promises` to catch ignored return values:
```javascript
// .eslintrc.js
{
  rules: {
    '@typescript-eslint/no-floating-promises': 'error',
  }
}
```

## Related Documentation

- [Data Integrity Check Plan](../../plans/2026-02-03-feat-data-integrity-check-plan.md) - Full implementation plan (Phase 2 hash verification pending)
- [Data Integrity Brainstorm](../../brainstorms/2026-02-03-data-integrity-check-brainstorm.md) - Original requirements

## Code References

- `src/sync/database/data-sync.ts:187` - `verifyDataCounts()` implementation
- `src/sync/database/sequence-sync.ts:113` - `verifySequences()` implementation
- `src/core/sync-orchestrator.ts:232` - `verify()` orchestration
- `src/types/sync.ts` - `SyncError` and `ErrorCategory.VALIDATION`
