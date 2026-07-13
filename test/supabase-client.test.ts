import assert from 'node:assert/strict';
import test from 'node:test';
import { createSupabaseClient, testSupabaseConnection } from '../src/clients/supabase-client.js';
import { baseConfig } from './fixture.js';

test('bounds Supabase connection checks with an aborting fetch', async () => {
  const originalFetch = globalThis.fetch;
  globalThis.fetch = ((_input: RequestInfo | URL, init?: RequestInit) =>
    new Promise((_resolve, reject) => {
      init?.signal?.addEventListener('abort', () => reject(init.signal?.reason), { once: true });
    })) as typeof fetch;

  try {
    const client = createSupabaseClient(baseConfig.source, 10);
    assert.equal(await testSupabaseConnection(client), false);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('keeps the timeout active while the response body is streaming', async () => {
  const originalFetch = globalThis.fetch;
  globalThis.fetch = (async (_input: RequestInfo | URL, init?: RequestInit) =>
    new Response(new ReadableStream({
      start(controller) {
        controller.enqueue(new TextEncoder().encode('[]'));
        init?.signal?.addEventListener(
          'abort',
          () => controller.error(init.signal?.reason),
          { once: true }
        );
      },
    }), { headers: { 'content-type': 'application/json' } })) as typeof fetch;

  try {
    const client = createSupabaseClient(baseConfig.source, 10);
    assert.equal(await testSupabaseConnection(client), false);
  } finally {
    globalThis.fetch = originalFetch;
  }
});
