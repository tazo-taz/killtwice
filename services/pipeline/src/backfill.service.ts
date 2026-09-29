import { Injectable } from '@nestjs/common';
import { config } from './config';
import { logger } from './logger';
import { DbService } from './db.service';
import { MetricsService } from './metrics.service';
import { WriterService } from './writer.service';
import { Envelope, RecordDoc } from './types';

const JOB_ID = 'default';

export interface BackfillState {
  status: string;
  last_key: string;
  max_key: string;
  rows_done: string;
  rows_total: string;
  chunk_size: number;
  started_at: string;
  updated_at: string;
  error: string | null;
}

/**
 * Keyset pagination, never OFFSET: at 5M rows OFFSET turns every chunk into
 * a scan of everything before it, and the job gets quadratically slower as it
 * progresses. `WHERE id > last_key ORDER BY id LIMIT n` stays flat.
 */
@Injectable()
export class BackfillService {
  private running = false;
  private loopHandle: Promise<void> | null = null;

  constructor(
    private readonly db: DbService,
    private readonly writer: WriterService,
    private readonly metrics: MetricsService,
  ) {}

  async state(): Promise<BackfillState | null> {
    const res = await this.db.pipeline.query<BackfillState>(
      `SELECT status, last_key, max_key, rows_done, rows_total, chunk_size,
              started_at, updated_at, error
         FROM pipeline.backfill_jobs WHERE id = $1`,
      [JOB_ID],
    );
    return res.rows[0] ?? null;
  }

  /**
   * Resume, not restart. Called on boot: if the job was running when the
   * process was killed, it must come back running from its checkpoint
   * without anyone pressing anything. That is G1.
   */
  async resumeIfRunning(): Promise<void> {
    const state = await this.state();
    if (!state || state.status !== 'running') return;
    logger.info(
      { mode: 'backfill', chunk_key: state.last_key, rows_done: state.rows_done },
      `resuming backfill from checkpoint ${state.last_key}`,
    );
    this.spawn();
  }

  async start(chunkSize?: number): Promise<BackfillState> {
    const existing = await this.state();
    const chunk = chunkSize ?? existing?.chunk_size ?? config.backfillChunkSize;

    if (!existing || existing.status === 'done' || existing.status === 'failed') {
      // Snapshot max(id) now. Without it the backfill chases rows the mutator
      // is still inserting and never terminates; anything newer than the
      // snapshot arrives through the stream instead.
      const maxRes = await this.db.source.query<{ max: string | null }>(
        'SELECT max(id) AS max FROM records',
      );
      const maxKey = Number(maxRes.rows[0]?.max ?? 0);
      const totalRes = await this.db.source.query<{ n: string }>(
        'SELECT count(*) AS n FROM records WHERE id <= $1',
        [maxKey],
      );

      await this.db.pipeline.query(
        `INSERT INTO pipeline.backfill_jobs
           (id, status, last_key, max_key, rows_done, rows_total, chunk_size, started_at, updated_at)
         VALUES ($1,'running',0,$2,0,$3,$4, now(), now())
         ON CONFLICT (id) DO UPDATE SET
           status='running', last_key=0, max_key=EXCLUDED.max_key, rows_done=0,
           rows_total=EXCLUDED.rows_total, chunk_size=EXCLUDED.chunk_size,
           error=NULL, started_at=now(), updated_at=now()`,
        [JOB_ID, maxKey, Number(totalRes.rows[0].n), chunk],
      );
      logger.info({ mode: 'backfill', max_key: maxKey }, 'backfill job created');
    } else {
      await this.db.pipeline.query(
        `UPDATE pipeline.backfill_jobs
            SET status='running', chunk_size=$2, error=NULL, updated_at=now()
          WHERE id=$1`,
        [JOB_ID, chunk],
      );
    }

    this.spawn();
    return (await this.state()) as BackfillState;
  }

  async stop(): Promise<void> {
    this.running = false;
    await this.db.pipeline.query(
      `UPDATE pipeline.backfill_jobs SET status='paused', updated_at=now()
        WHERE id=$1 AND status='running'`,
      [JOB_ID],
    );
  }

  private spawn(): void {
    if (this.running) return;
    this.running = true;
    this.loopHandle = this.loop().catch(async (err: unknown) => {
      logger.error({ mode: 'backfill', err: String(err) }, 'backfill loop crashed');
      this.running = false;
      await this.db.pipeline.query(
        `UPDATE pipeline.backfill_jobs SET status='failed', error=$2, updated_at=now()
          WHERE id=$1`,
        [JOB_ID, String(err)],
      );
    });
  }

  private async loop(): Promise<void> {
    while (this.running) {
      const state = await this.state();
      if (!state || state.status !== 'running') {
        this.running = false;
        break;
      }

      const lastKey = Number(state.last_key);
      const maxKey = Number(state.max_key);

      const chunk = await this.db.source.query<RecordDoc>(
        `SELECT id, external_id, email, full_name, country, status, attributes,
                deleted, created_at, updated_at, row_version
           FROM records
          WHERE id > $1 AND id <= $2
          ORDER BY id
          LIMIT $3`,
        [lastKey, maxKey, state.chunk_size],
      );

      if (chunk.rows.length === 0) {
        await this.db.pipeline.query(
          `UPDATE pipeline.backfill_jobs SET status='done', updated_at=now() WHERE id=$1`,
          [JOB_ID],
        );
        logger.info({ mode: 'backfill', rows_done: state.rows_done }, 'backfill complete');
        this.running = false;
        this.metrics.queueDepth.set({ mode: 'backfill' }, 0);
        break;
      }

      this.metrics.rowsRead.inc({ mode: 'backfill' }, chunk.rows.length);
      this.metrics.queueDepth.set({ mode: 'backfill' }, chunk.rows.length);

      const newKey = Number(chunk.rows[chunk.rows.length - 1].id);
      const envelopes: Envelope[] = chunk.rows.map((doc) => ({
        id: Number(doc.id),
        version: Number(doc.row_version),
        op: 'upsert',
        doc: { ...doc, id: Number(doc.id), row_version: Number(doc.row_version) },
        mode: 'backfill',
        offset: String(newKey),
      }));

      // Blocks until both sinks have accounted for every envelope. Nothing is
      // read ahead while this is outstanding, so a sink outage stops the
      // reader instead of filling memory.
      await this.writer.writeBatch(envelopes, 'backfill');

      // Only now. Crashing one line earlier replays this chunk, which the
      // version guard absorbs. Crashing one line later would lose it.
      //
      // Compare-and-set on last_key: the checkpoint only moves if it is still
      // where this loop read it. A stop followed quickly by a start leaves the
      // old loop finishing its in-flight chunk while a new one begins, and a
      // reset starts a fresh job underneath it. An unconditional write from
      // the old loop would drag the new job's checkpoint forward past rows it
      // has not read - silent loss. Whoever loses the race stops; the other
      // loop owns the job.
      const moved = await this.db.pipeline.query(
        `UPDATE pipeline.backfill_jobs
            SET last_key = $2, rows_done = rows_done + $3, updated_at = now()
          WHERE id = $1 AND last_key = $4`,
        [JOB_ID, newKey, chunk.rows.length, lastKey],
      );
      if (moved.rowCount === 0) {
        logger.warn(
          { mode: 'backfill', chunk_key: lastKey },
          'checkpoint moved underneath this loop, yielding the job',
        );
        break;
      }

      this.metrics.backfillLastKey.set(newKey);
      this.metrics.queueDepth.set({ mode: 'backfill' }, 0);
    }
  }
}
