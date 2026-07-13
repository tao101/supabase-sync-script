import assert from 'node:assert/strict';
import test from 'node:test';
import { SequenceSync } from '../src/sync/database/sequence-sync.js';
import { baseConfig } from './fixture.js';

test('discovers sequences owned by serial and identity columns', async () => {
  let query = '';
  const client = {
    async query(text: string) {
      query = text;
      return { rows: [] };
    },
    release() {},
  };
  const sync = new SequenceSync(baseConfig, { async connect() { return client; } } as never);

  await sync.findSequences();

  assert.match(query, /dep\.deptype IN \('a', 'i'\)/);
});

test('quotes table identifiers and passes sequence names as parameters', async () => {
  const calls: { text: string; values?: unknown[] }[] = [];
  const client = {
    async query(text: string, values?: unknown[]) {
      calls.push({ text, values });
      return text.includes('MAX') ? { rows: [{ boundary_value: '2' }] } : { rows: [] };
    },
    release() {},
  };
  const sync = new SequenceSync(baseConfig, {} as never);

  await sync.resetSequence('odd"schema', 'odd"table', 'odd"column', "odd'seq", client as never);

  assert.equal(
    calls[0].text,
    'SELECT MAX("odd""column")::text as boundary_value FROM "odd""schema"."odd""table"'
  );
  assert.equal(calls[1].text, 'SELECT setval($1::regclass, $2, $3)');
  assert.deepEqual(calls[1].values, ['"odd""schema"."odd\'seq"', '2', true]);
});

test('resets descending sequences from the minimum existing value', async () => {
  const calls: { text: string; values?: unknown[] }[] = [];
  const client = {
    async query(text: string, values?: unknown[]) {
      calls.push({ text, values });
      return text.includes('MIN') ? { rows: [{ boundary_value: '90' }] } : { rows: [] };
    },
    release() {},
  };
  const sync = new SequenceSync(baseConfig, {} as never);

  await sync.resetSequence('public', 'items', 'id', 'items_id_seq', client as never, '-10', '-1');

  assert.match(calls[0].text, /MIN\("id"\)/);
  assert.deepEqual(calls[1].values, ['"public"."items_id_seq"', '90', true]);
});

test('preserves bigint sequence values without JavaScript number rounding', async () => {
  const calls: { text: string; values?: unknown[] }[] = [];
  const client = {
    async query(text: string, values?: unknown[]) {
      calls.push({ text, values });
      return text.includes('MAX')
        ? { rows: [{ boundary_value: '9007199254740993' }] }
        : { rows: [] };
    },
    release() {},
  };
  const sync = new SequenceSync(baseConfig, {} as never);

  const result = await sync.resetSequence('public', 'items', 'id', 'items_id_seq', client as never);

  assert.equal(calls[1].values?.[1], '9007199254740993');
  assert.equal(typeof result.newValue, 'number');
  assert.equal(result.newValueExact, '9007199254740993');
});
