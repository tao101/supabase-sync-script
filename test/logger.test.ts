import assert from 'node:assert/strict';
import test from 'node:test';
import { sanitizeConfig, sanitizeErrorMessage } from '../src/utils/logger.js';

test('redacts credentials embedded in database URLs', () => {
  const sanitized = sanitizeConfig({
    source: {
      dbUrl: 'postgresql://postgres:top-secret@db.example.com/postgres',
    },
  });

  assert.equal(
    (sanitized.source as Record<string, unknown>).dbUrl,
    '***REDACTED***'
  );
});

test('redacts database credentials from subprocess errors', () => {
  const sanitized = sanitizeErrorMessage(
    "Command failed: psql 'postgresql://user:p%40ss@db.example.com/postgres?password=query-secret&sslpassword=tls-secret&oauth_client_secret=oauth-secret' PGPASSWORD=env-secret"
  );

  assert.doesNotMatch(sanitized, /p%40ss|query-secret|tls-secret|oauth-secret|env-secret/);
  assert.match(sanitized, /REDACTED/);
});
