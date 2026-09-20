import { Injectable } from '@nestjs/common';
import { LogicalReplicationService, PgoutputPlugin } from 'pg-logical-replication';
import { config } from './config';
import { logger } from './logger';
import { DbService } from './db.service';
import { MetricsService } from './metrics.service';
import { WriterService } from './writer.service';
import { sleep, nextDelay } from './util/backoff';
import { Envelope, RecordDoc } from './types';

interface PgoutputMessage {
  tag: string;
  relation?: { name: string };
  new?: Record<string, unknown>;
  old?: Record<string, unknown>;
  key?: Record<string, unknown>;
  commitTime?: unknown;
  commitEndLsn?: string;
}

interface Buffered {
  env: Envelope;
  lsn: string;
  commitTs: Date | null;
}

export interface CdcState {
  running: boolean;
  paused: boolean;
  confirmed_lsn: string | null;
  last_commit_ts: string | null;
  events_done: number;
  buffered: number;
  lag_seconds: number;
  slot_lag_bytes: number;
}

/**
 * Incremental mode. ADR-003: a logical replication slot rather than an
 * updated_at poll, because the slot is a durable server-side cursor - resume
 * is exact, DELETEs are visible, and the slot's own lag is a free metric.
 *
 * The price is that the slot retains WAL while we are behind, which is why
 * slot_lag_bytes is surfaced and not buried.
 */
@Injectable()
export class CdcService {
  private service: LogicalReplicationService | null = null;
  private enabled = false;
  private paused = false;
  private buffer: Buffered[] = [];
  private lastCommitTs: Date | null = null;
  private eventsDone = 0;
  private confirmedLsn: string | null = null;
  private flusher: Promise<void> | null = null;

  constructor(
    private readonly db: DbService,
    private readonly writer: WriterService,
    private readonly metrics: MetricsService,
  ) {}

  async ensureSlot(): Promise<void> {
    const exists = await this.db.source.query(
      'SELECT 1 FROM pg_replication_slots WHERE slot_name = $1',
      [config.slotName],
    );
    if (exists.rowCount && exists.rowCount > 0) return;
    await this.db.source.query('SELECT pg_create_logical_replication_slot($1, $2)', [
      config.slotName,
      'pgoutput',
    ]);
    logger.info({ slot: config.slotName }, 'created replication slot');
  }

  async dropSlot(): Promise<void> {
    await this.stop();
    await this.db.source
      .query('SELECT pg_drop_replication_slot($1)', [config.slotName])
      .catch(() => undefined);
    await this.db.pipeline.query('DELETE FROM pipeline.stream_checkpoint WHERE slot_name = $1', [
      config.slotName,
    ]);
    this.confirmedLsn = null;
    this.eventsDone = 0;
    this.lastCommitTs = null;
  }

  async start(): Promise<void> {
    if (this.enabled) return;
    await this.ensureSlot();

    const cp = await this.db.pipeline.query<{ confirmed_lsn: string; events_done: string }>(
      'SELECT confirmed_lsn, events_done FROM pipeline.stream_checkpoint WHERE slot_name = $1',
      [config.slotName],
    );
    if (cp.rows[0]) {
      this.confirmedLsn = cp.rows[0].confirmed_lsn;
      this.eventsDone = Number(cp.rows[0].events_done);
      logger.info(
        { mode: 'stream', lsn: this.confirmedLsn },
        `resuming stream from LSN ${this.confirmedLsn}`,
      );
    }

    this.enabled = true;
    void this.runSubscription();
    this.flusher = this.runFlusher();
    await this.db.pipeline.query(
      `INSERT INTO pipeline.runtime_flags (key, value) VALUES ('stream','on')
       ON CONFLICT (key) DO UPDATE SET value='on'`,
    );
  }

  async stop(): Promise<void> {
    this.enabled = false;
    try {
      await this.service?.stop();
    } catch {
      /* already down */
    }
    this.service = null;
    await this.db.pipeline.query(
      `INSERT INTO pipeline.runtime_flags (key, value) VALUES ('stream','off')
       ON CONFLICT (key) DO UPDATE SET value='off'`,
    );
  }

  async resumeIfRunning(): Promise<void> {
    const flag = await this.db.pipeline.query<{ value: string }>(
      `SELECT value FROM pipeline.runtime_flags WHERE key='stream'`,
    );
    // Default on: both modes are supposed to run simultaneously, so the
    // stream only stays down if someone explicitly turned it off.
    if (!flag.rows[0] || flag.rows[0].value === 'on') await this.start();
  }

  private toDate(commitTime: unknown): Date | null {
    if (commitTime instanceof Date) return commitTime;
    if (typeof commitTime === 'bigint') {
      // pgoutput sends microseconds since 2000-01-01.
      const epoch2000 = Date.UTC(2000, 0, 1);
      return new Date(epoch2000 + Number(commitTime / 1000n));
    }
    if (typeof commitTime === 'number') return new Date(commitTime);
    return null;
  }

  private toEnvelope(msg: PgoutputMessage, lsn: string): Envelope | null {
    if (msg.relation?.name !== 'records') return null;

    if (msg.tag === 'insert' || msg.tag === 'update') {
      const row = msg.new as unknown as RecordDoc;
      if (!row) return null;
      return {
        id: Number(row.id),
        version: Number(row.row_version),
        op: 'upsert',
        doc: { ...row, id: Number(row.id), row_version: Number(row.row_version) },
        mode: 'stream',
        offset: lsn,
      };
    }

    if (msg.tag === 'delete') {
      // REPLICA IDENTITY FULL means the old row is here, and with it the
      // row_version the delete must be ordered by.
      const row = (msg.old ?? msg.key) as unknown as RecordDoc | undefined;
      if (!row) return null;
      return {
        id: Number(row.id),
        version: Number(row.row_version),
        op: 'delete',
        doc: null,
        mode: 'stream',
        offset: lsn,
      };
    }

    return null;
  }

  private async runSubscription(): Promise<void> {
    let attempt = 0;

    while (this.enabled) {
      // Backpressure: while paused we are not consuming from the slot at all.
      // Postgres holds the changes in WAL, which is a durable buffer we do
      // not have to pay for in RAM. This is the sleep that keeps G3's CPU
      // assertion honest.
      if (this.paused) {
        await sleep(100);
        continue;
      }

      const service = new LogicalReplicationService(
        { connectionString: config.sourceDbUrl },
        { acknowledge: { auto: false, timeoutSeconds: 0 } },
      );
      this.service = service;

      const plugin = new PgoutputPlugin({
        protoVersion: 1,
        publicationNames: [config.publicationName],
      });

      service.on('data', (lsn: string, log: PgoutputMessage) => {
        if (log.tag === 'commit') {
          this.lastCommitTs = this.toDate(log.commitTime) ?? this.lastCommitTs;
          return;
        }
        const env = this.toEnvelope(log, lsn);
        if (!env) return;

        this.buffer.push({ env, lsn, commitTs: this.lastCommitTs });
        this.metrics.rowsRead.inc({ mode: 'stream' });
        this.metrics.queueDepth.set({ mode: 'stream' }, this.buffer.length);

        if (this.buffer.length >= config.queueCapacity && !this.paused) {
          this.paused = true;
          this.metrics.readerPaused.set({ mode: 'stream' }, 1);
          logger.warn(
            { mode: 'stream', buffered: this.buffer.length },
            'stream buffer full, pausing the reader',
          );
          void service.stop().catch(() => undefined);
        }
      });

      service.on('error', (err: unknown) => {
        logger.warn({ mode: 'stream', err: String(err) }, 'replication stream error');
      });

      try {
        // Two calls rather than passing undefined: the library types the
        // start LSN as required, and "start where the slot left off" is a
        // different request from "start at this LSN".
        if (this.confirmedLsn) {
          await service.subscribe(plugin, config.slotName, this.confirmedLsn);
        } else {
          await service.subscribe(plugin, config.slotName);
        }
        attempt = 0;
      } catch (err) {
        attempt += 1;
        const delay = nextDelay(attempt);
        logger.warn(
          { mode: 'stream', attempt, delay_ms: delay, err: String(err) },
          'replication subscribe failed, backing off',
        );
        await sleep(delay);
      }
    }
  }

  private async runFlusher(): Promise<void> {
    while (this.enabled || this.buffer.length > 0) {
      if (this.buffer.length === 0) {
        await sleep(200);
        continue;
      }

      const take = this.buffer.splice(0, config.cdcBatchSize);
      const envelopes = take.map((b) => b.env);
      const lastLsn = take[take.length - 1].lsn;
      const lastCommit = take[take.length - 1].commitTs;

      await this.writer.writeBatch(envelopes, 'stream');

      // Same ordering rule as the backfill: sinks first, cursor second.
      this.eventsDone += take.length;
      this.confirmedLsn = lastLsn;
      await this.db.pipeline.query(
        `INSERT INTO pipeline.stream_checkpoint
           (slot_name, confirmed_lsn, last_commit_ts, events_done, updated_at)
         VALUES ($1,$2,$3,$4, now())
         ON CONFLICT (slot_name) DO UPDATE SET
           confirmed_lsn = EXCLUDED.confirmed_lsn,
           last_commit_ts = COALESCE(EXCLUDED.last_commit_ts, pipeline.stream_checkpoint.last_commit_ts),
           events_done = EXCLUDED.events_done,
           updated_at = now()`,
        [config.slotName, lastLsn, lastCommit, this.eventsDone],
      );

      // Telling Postgres it may release the WAL up to here. Only now - the
      // slot is our durable buffer and it must not be trimmed ahead of the
      // sinks.
      try {
        await this.service?.acknowledge(lastLsn);
      } catch {
        /* stream is between subscriptions; the persisted LSN covers us */
      }

      this.metrics.queueDepth.set({ mode: 'stream' }, this.buffer.length);

      if (this.paused && this.buffer.length < config.queueCapacity / 2) {
        this.paused = false;
        this.metrics.readerPaused.set({ mode: 'stream' }, 0);
        logger.info({ mode: 'stream' }, 'stream buffer drained, resuming the reader');
      }
    }
  }

  async slotLagBytes(): Promise<number> {
    const res = await this.db.source.query<{ lag: string | null }>(
      `SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn) AS lag
         FROM pg_replication_slots WHERE slot_name = $1`,
      [config.slotName],
    );
    return Number(res.rows[0]?.lag ?? 0);
  }

  lagSeconds(): number {
    if (!this.lastCommitTs) return 0;
    return Math.max(0, (Date.now() - this.lastCommitTs.getTime()) / 1000);
  }

  async state(): Promise<CdcState> {
    return {
      running: this.enabled,
      paused: this.paused,
      confirmed_lsn: this.confirmedLsn,
      last_commit_ts: this.lastCommitTs?.toISOString() ?? null,
      events_done: this.eventsDone,
      buffered: this.buffer.length,
      lag_seconds: this.lagSeconds(),
      slot_lag_bytes: await this.slotLagBytes().catch(() => 0),
    };
  }
}
