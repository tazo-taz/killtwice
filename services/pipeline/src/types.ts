export type Op = 'upsert' | 'delete';
export type Mode = 'backfill' | 'stream' | 'replay';

export interface RecordDoc {
  id: number;
  external_id: string;
  email: string | null;
  full_name: string | null;
  country: string | null;
  status: string | null;
  attributes: Record<string, unknown>;
  deleted: boolean;
  created_at: string;
  updated_at: string;
  row_version: number;
}

/**
 * The canonical unit that moves through the pipeline. Both readers produce
 * these; both writers consume them. `version` is the guard both sinks check.
 */
export interface Envelope {
  id: number;
  version: number;
  op: Op;
  doc: RecordDoc | null; // null only for a hard delete
  mode: Mode;
  offset: string; // chunk key for backfill, LSN for stream
}

export type ErrorClass =
  | 'mapping'
  | 'too_large'
  | 'rejected'
  | 'unroutable'
  | 'unknown';

export interface BatchResult {
  written: number;
  stale: number;
  deadLettered: number;
  /**
   * Every envelope the sinks refused for good. Always populated, even when
   * the caller asked not to dead-letter them - a replay needs to know its
   * item failed again without the DLQ growing a second copy of it.
   */
  failures: { env: Envelope; sink: string; errorClass: ErrorClass; detail: unknown }[];
}
