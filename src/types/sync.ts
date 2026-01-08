export interface SyncStep {
  name: string;
  fn: () => Promise<void>;
  enabled?: boolean;
}

export interface SyncResult {
  success: boolean;
  steps: StepResult[];
  duration: number;
  errors: SyncError[];
}

export interface StepResult {
  name: string;
  success: boolean;
  duration: number;
  error?: Error;
  details?: Record<string, unknown>;
}

export enum ErrorCategory {
  CONNECTION = 'CONNECTION',
  AUTHENTICATION = 'AUTHENTICATION',
  PERMISSION = 'PERMISSION',
  VALIDATION = 'VALIDATION',
  EXPORT = 'EXPORT',
  IMPORT = 'IMPORT',
  STORAGE = 'STORAGE',
  TIMEOUT = 'TIMEOUT',
  UNKNOWN = 'UNKNOWN',
}

export class SyncError extends Error {
  constructor(
    message: string,
    public category: ErrorCategory,
    public step: string,
    public recoverable: boolean,
    public originalError?: Error
  ) {
    super(message);
    this.name = 'SyncError';
  }
}

export interface AuthUser {
  id: string;
  email: string | null;
  phone: string | null;
  encrypted_password: string | null;
  email_confirmed_at: string | null;
  phone_confirmed_at: string | null;
  raw_user_meta_data: Record<string, unknown>;
  raw_app_meta_data: Record<string, unknown>;
  created_at: string;
  updated_at: string;
  banned_until: string | null;
  confirmation_token: string | null;
  recovery_token: string | null;
  email_change_token_new: string | null;
  email_change: string | null;
}

export interface AuthIdentity {
  id: string;
  user_id: string;
  identity_data: Record<string, unknown>;
  provider: string;
  provider_id: string;
  last_sign_in_at: string | null;
  created_at: string;
  updated_at: string;
}

export interface StorageBucket {
  id: string;
  name: string;
  public: boolean;
  file_size_limit: number | null;
  allowed_mime_types: string[] | null;
  created_at: string;
  updated_at: string;
}

export interface StorageFile {
  name: string;
  id: string | null;
  bucket_id: string;
  metadata: Record<string, unknown>;
}

export interface BucketSyncResult {
  bucket: string;
  total: number;
  uploaded: number;
  failed: number;
}

export interface StorageSyncResult {
  buckets: BucketSyncResult[];
}

export interface AuthSyncResult {
  usersImported: number;
  identitiesImported: number;
  errors: string[];
}

export interface SequenceInfo {
  sequence_name: string;
  schema_name: string;
  table_name: string;
  column_name: string;
}

export interface SequenceResetResult {
  sequence: string;
  table: string;
  column: string;
  newValue: number;
}
