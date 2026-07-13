import {
  type SequenceInfo,
  type SequenceResetResult,
  type SupabaseConnection,
  SchemaSync,
  createPostgresClient,
  createPostgresPool,
} from '../src/index.js';

const connection: SupabaseConnection = {
  dbUrl: 'postgresql://user@localhost/database',
  apiUrl: 'http://localhost:54321',
  port: 5432,
};
const apiUrl: string = connection.apiUrl;
const sequence: SequenceInfo = {
  sequence_name: 'items_id_seq',
  schema_name: 'public',
  table_name: 'items',
  column_name: 'id',
};
const result: SequenceResetResult = {
  sequence: 'public.items_id_seq',
  table: 'public.items',
  column: 'id',
  newValue: 1,
};

declare const schemaSync: SchemaSync;
void apiUrl;
void sequence;
void result;
void schemaSync.resetTargetSchemas();
void createPostgresPool(connection, true);
void createPostgresClient(connection, true);
