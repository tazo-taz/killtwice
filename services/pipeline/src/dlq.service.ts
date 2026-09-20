import { Injectable } from '@nestjs/common';
import { randomUUID } from 'node:crypto';
import { DbService } from './db.service';
import { MetricsService } from './metrics.service';
import { Envelope, ErrorClass } from './types';

export interface DlqRow {
  id: string;
  sink: string;
  record_id: string;
  row_version: string;
  op: string;
  payload: unknown;
  error_class: string;
  error_detail: unknown;
  source_mode: string | null;
  source_offset: string | null;
  attempts: number;
  status: string;
  first_seen_at: string;
  last_seen_at: string;
}

/**
 * A table, not a queue. ADR-004: a DLX alone cannot answer "how many and
 * why", and cannot be replayed once the source row has moved on - so the
 * full payload is stored here and replay never re-reads the source.
 */
@Injectable()
export class DlqService {
  constructor(
    private readonly db: DbService,
    private readonly metrics: MetricsService,
  ) {}

  async add(
    sink: 'elasticsearch' | 'rabbitmq',
    env: Envelope,
    errorClass: ErrorClass,
    detail: unknown,
  ): Promise<void> {
    await this.db.pipeline.query(
      `INSERT INTO pipeline.dlq_items
         (id, sink, record_id, row_version, op, payload,
          error_class, error_detail, source_mode, source_offset)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
      [
        randomUUID(),
        sink,
        env.id,
        env.version,
        env.op,
        JSON.stringify(env.doc ?? {}),
        errorClass,
        JSON.stringify({ detail }),
        env.mode,
        env.offset,
      ],
    );
    this.metrics.rowsFailed.inc({ sink, class: errorClass });
  }

  async list(status = 'pending', limit = 100): Promise<DlqRow[]> {
    const res = await this.db.pipeline.query<DlqRow>(
      `SELECT * FROM pipeline.dlq_items
        WHERE ($1 = 'all' OR status = $1)
        ORDER BY first_seen_at DESC
        LIMIT $2`,
      [status, limit],
    );
    return res.rows;
  }

  async pending(limit: number, ids?: string[]): Promise<DlqRow[]> {
    const res = ids?.length
      ? await this.db.pipeline.query<DlqRow>(
          `SELECT * FROM pipeline.dlq_items
            WHERE status = 'pending' AND id = ANY($1::text[])
            ORDER BY first_seen_at LIMIT $2`,
          [ids, limit],
        )
      : await this.db.pipeline.query<DlqRow>(
          `SELECT * FROM pipeline.dlq_items
            WHERE status = 'pending'
            ORDER BY first_seen_at LIMIT $1`,
          [limit],
        );
    return res.rows;
  }

  async resolve(id: string): Promise<void> {
    await this.db.pipeline.query(
      `UPDATE pipeline.dlq_items
          SET status = 'resolved', last_seen_at = now()
        WHERE id = $1`,
      [id],
    );
  }

  async bumpAttempt(id: string, detail: unknown): Promise<void> {
    await this.db.pipeline.query(
      `UPDATE pipeline.dlq_items
          SET attempts = attempts + 1,
              last_seen_at = now(),
              error_detail = $2
        WHERE id = $1`,
      [id, JSON.stringify({ detail })],
    );
  }

  async counts(): Promise<Record<string, number>> {
    const res = await this.db.pipeline.query<{ sink: string; n: string }>(
      `SELECT sink, count(*) AS n
         FROM pipeline.dlq_items
        WHERE status = 'pending'
        GROUP BY sink`,
    );
    const out: Record<string, number> = { elasticsearch: 0, rabbitmq: 0 };
    for (const row of res.rows) out[row.sink] = Number(row.n);
    return out;
  }

  async refreshGauge(): Promise<number> {
    const counts = await this.counts();
    let total = 0;
    for (const [sink, n] of Object.entries(counts)) {
      this.metrics.dlqSize.set({ sink }, n);
      total += n;
    }
    return total;
  }
}
