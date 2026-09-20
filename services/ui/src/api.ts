export interface Status {
  healthy: boolean;
  backfill: {
    status: string;
    rows_done: number;
    rows_total: number;
    percent: number;
    last_key: number;
    checkpoint: number;
  };
  stream: {
    running: boolean;
    paused: boolean;
    lag_seconds: number;
    slot_lag_bytes: number;
    confirmed_lsn: string | null;
    events_done: number;
    buffered: number;
  };
  throughput: { backfill: number; stream: number; total: number };
  dlq: { total: number; by_sink: Record<string, number> };
  sinks: { elasticsearch: boolean; rabbitmq: boolean };
  counts: { source: number; elasticsearch: number; consumer: number };
  guarantee: string;
  checked_at: string;
}

export interface DlqItem {
  id: string;
  sink: string;
  record_id: string;
  row_version: string;
  op: string;
  error_class: string;
  error_detail: unknown;
  source_mode: string | null;
  attempts: number;
  status: string;
  first_seen_at: string;
}

export interface RecordHit {
  id: number;
  email: string | null;
  full_name: string | null;
  country: string | null;
  status: string | null;
  deleted: boolean;
  row_version: number;
  updated_at: string;
}

async function json<T>(res: globalThis.Response): Promise<T> {
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
  return (await res.json()) as T;
}

export const api = {
  status: () => fetch('/api/status').then(json<Status>),

  dlq: (status = 'pending') => fetch(`/api/dlq?status=${status}&limit=50`).then(json<DlqItem[]>),

  records: (q: string) =>
    fetch(`/api/records?q=${encodeURIComponent(q)}&size=25`).then(
      json<{ total: number; hits: RecordHit[] }>,
    ),

  consumerStats: () => fetch('/consumer/stats').then(json<Record<string, number>>),

  post: <T>(path: string, body?: unknown): Promise<T> =>
    fetch(path, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body ?? {}),
    }).then(json<T>),
};
