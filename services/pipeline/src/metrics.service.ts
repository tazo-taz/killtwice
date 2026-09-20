import { Injectable } from '@nestjs/common';
import {
  Counter,
  Gauge,
  Histogram,
  Registry,
  collectDefaultMetrics,
} from 'prom-client';

/**
 * The metric set is named after the five questions G5 requires answering:
 * where is the backfill, what is the throughput, what is the lag, how big is
 * the DLQ, is it healthy. Anything that can fail has a counter, because a
 * failure mode nobody can count is not finished.
 */
@Injectable()
export class MetricsService {
  readonly registry = new Registry();

  readonly backfillRowsDone = new Gauge({
    name: 'pipeline_backfill_rows_done',
    help: 'Rows the backfill has fully landed in both sinks',
    registers: [this.registry],
  });

  readonly backfillRowsTotal = new Gauge({
    name: 'pipeline_backfill_rows_total',
    help: 'Rows the backfill intends to move (snapshot at job start)',
    registers: [this.registry],
  });

  readonly backfillLastKey = new Gauge({
    name: 'pipeline_backfill_last_key',
    help: 'The G1 checkpoint: highest source id confirmed in both sinks',
    registers: [this.registry],
  });

  readonly rowsRead = new Counter({
    name: 'pipeline_rows_read_total',
    help: 'Rows read from the source',
    labelNames: ['mode'] as const,
    registers: [this.registry],
  });

  readonly rowsWritten = new Counter({
    name: 'pipeline_rows_written_total',
    help: 'Rows acknowledged by a sink',
    labelNames: ['sink'] as const,
    registers: [this.registry],
  });

  readonly rowsStale = new Counter({
    name: 'pipeline_rows_stale_total',
    help: 'Writes rejected as not-newer. Expected and healthy: this is the version guard doing its job',
    labelNames: ['sink'] as const,
    registers: [this.registry],
  });

  readonly rowsFailed = new Counter({
    name: 'pipeline_rows_failed_total',
    help: 'Rows that could not be written and went to the DLQ',
    labelNames: ['sink', 'class'] as const,
    registers: [this.registry],
  });

  readonly retries = new Counter({
    name: 'pipeline_retries_total',
    help: 'Sink write attempts that failed and were retried after a backoff',
    labelNames: ['sink'] as const,
    registers: [this.registry],
  });

  readonly throughput = new Gauge({
    name: 'pipeline_throughput_rows_per_second',
    help: 'Rows per second over a trailing 10s window',
    labelNames: ['mode'] as const,
    registers: [this.registry],
  });

  readonly cdcLagSeconds = new Gauge({
    name: 'pipeline_cdc_lag_seconds',
    help: 'now() minus the commit timestamp of the last processed change',
    registers: [this.registry],
  });

  readonly cdcSlotLagBytes = new Gauge({
    name: 'pipeline_cdc_slot_lag_bytes',
    help: 'WAL retained by the replication slot. Unbounded growth here fills the source disk',
    registers: [this.registry],
  });

  readonly dlqSize = new Gauge({
    name: 'pipeline_dlq_size',
    help: 'Pending items in the dead letter queue',
    labelNames: ['sink'] as const,
    registers: [this.registry],
  });

  readonly sinkUp = new Gauge({
    name: 'pipeline_sink_up',
    help: '1 when the last write to this sink succeeded, 0 while it is failing',
    labelNames: ['sink'] as const,
    registers: [this.registry],
  });

  readonly queueDepth = new Gauge({
    name: 'pipeline_queue_depth',
    help: 'Envelopes buffered in memory, per reader. Bounded by QUEUE_CAPACITY',
    labelNames: ['mode'] as const,
    registers: [this.registry],
  });

  readonly readerPaused = new Gauge({
    name: 'pipeline_reader_paused',
    help: '1 while a reader has stopped reading because of backpressure',
    labelNames: ['mode'] as const,
    registers: [this.registry],
  });

  readonly writeDuration = new Histogram({
    name: 'pipeline_write_duration_seconds',
    help: 'Time for one batch write to a sink',
    labelNames: ['sink'] as const,
    buckets: [0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30],
    registers: [this.registry],
  });

  private readonly windows = new Map<string, { at: number; count: number }[]>();

  constructor() {
    collectDefaultMetrics({ register: this.registry, prefix: 'pipeline_proc_' });
    this.sinkUp.set({ sink: 'elasticsearch' }, 0);
    this.sinkUp.set({ sink: 'rabbitmq' }, 0);
    for (const mode of ['backfill', 'stream']) {
      this.throughput.set({ mode }, 0);
      this.queueDepth.set({ mode }, 0);
      this.readerPaused.set({ mode }, 0);
    }
  }

  /** Trailing 10s rate. Cheap enough to call on every batch. */
  recordThroughput(mode: string, rows: number): void {
    const now = Date.now();
    const w = this.windows.get(mode) ?? [];
    w.push({ at: now, count: rows });
    const cutoff = now - 10_000;
    while (w.length > 0 && w[0].at < cutoff) w.shift();
    this.windows.set(mode, w);
    const total = w.reduce((sum, e) => sum + e.count, 0);
    this.throughput.set({ mode }, Math.round((total / 10) * 10) / 10);
  }

  /** Called on a timer so throughput decays to 0 when nothing is moving. */
  decay(): void {
    for (const mode of this.windows.keys()) this.recordThroughput(mode, 0);
  }

  metrics(): Promise<string> {
    return this.registry.metrics();
  }
}
