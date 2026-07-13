import assert from 'node:assert/strict';
import test from 'node:test';
import { getApplicationSchemas } from '../src/sync/database/schemas.js';
import { baseConfig } from './fixture.js';

test('excludes configured and Supabase-managed schemas', () => {
  const config = structuredClone(baseConfig);
  config.options.database.includeSchemas = [' public ', 'audit', 'auth'];
  config.options.database.excludeSchemas = ['audit'];

  assert.deepEqual(getApplicationSchemas(config), ['public']);
});
