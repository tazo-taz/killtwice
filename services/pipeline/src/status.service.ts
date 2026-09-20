import { Injectable, OnApplicationBootstrap, OnModuleDestroy } from '@nestjs/common';
import { config } from './config';
import { logger } from './logger';
import { BackfillService } from './backfill.service';
import { CdcService } from './cdc.service';
import { ChaosService } from './chaos.service';
import { DbService } from './db.service';
import { DlqService } from './dlq.service';
import { MetricsService } from './metrics.service';
import { EsService } from './sinks/es.service';
import { MqService } from './sinks/mq.service';

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

/**
 * The five G5 answers in one response, and the timer that keeps the gauges
 * truthful. Anything a person needs in order to say "the system is fine"
 * without opening the code has to be reachable from here.
 */
@Injectable()
export class StatusService implements OnApplicationBootstrap, OnModuleDestroy {
  private timer: ReturnType<typeof setInterval> | null = null;

  constructor(
    private readonly db: DbService,
    private readonly backfill: BackfillService,
    private readonly cdc: CdcService,
    private readonly dlq: DlqService,
    private readonly es: EsService,
    private readonly mq: MqService,
    private readonly chaos: ChaosService,
    private readonly metrics: MetricsService,
  ) {}

  onApplicationBootstrap(): void {
    this.timer = setInterval(() => {
      void this.refresh().catch((err: unknown) =>
        logger.debug({ err: String(err) }, 'gauge refresh failed'),
      );
    }, 2000);
  }

  onModuleDestroy(): void {
    if (this.timer) clearInterval(this.timer);
  }

  private async refresh(): Promise<void> {
    this.metrics.decay();

    const state = await this.backfill.state();
    if (state) {
      this.metrics.backfillRowsDone.set(Number(state.rows_done));
      this.metrics.backfillRowsTotal.set(Number(state.rows_total));
      this.metrics.backfillLastKey.set(Number(state.last_key));
    }

    this.metrics.cdcLagSeconds.set(this.cdc.lagSeconds());
    this.metrics.cdcSlotLagBytes.set(await this.cdc.slotLagBytes().catch(() => 0));
    await this.dlq.refreshGauge();

    // Liveness of each sink, so `sink_up` is a real probe and not just a
    // record of whether we happened to write recently.
    this.metrics.sinkUp.set({ sink: 'elasticsearch' }, (await this.es.ping()) ? 1 : 0);
    this.metrics.sinkUp.set({ sink: 'rabbitmq' }, (await this.mq.ping()) ? 1 : 0);
  }

  async consumerCount(): Promise<number> {
    const res = await this.db.pipeline
      .query<{ n: string }>('SELECT count(*) AS n FROM consumer.projection')
      .catch(() => null);
    return res ? Number(res.rows[0].n) : 0;
  }

  async status(): Promise<Status> {
    const [state, stream, dlqCounts] = await Promise.all([
      this.backfill.state(),
      this.cdc.state(),
      this.dlq.counts(),
    ]);

    const [sourceCount, esCount, consumerCount, esUp, mqUp] = await Promise.all([
      this.chaos.sourceCount().catch(() => -1),
      this.es.count().catch(() => -1),
      this.consumerCount(),
      this.es.ping(),
      this.mq.ping(),
    ]);

    const rowsDone = Number(state?.rows_done ?? 0);
    const rowsTotal = Number(state?.rows_total ?? 0);
    const dlqTotal = Object.values(dlqCounts).reduce((a, b) => a + b, 0);

    const tputBackfill = await this.throughputFor('backfill');
    const tputStream = await this.throughputFor('stream');

    return {
      // "Healthy" is deliberately narrow: both sinks reachable and the
      // backfill not in a failed state. A full DLQ is not unhealthy - it is
      // visible, which is the point.
      healthy: esUp && mqUp && state?.status !== 'failed',
      backfill: {
        status: state?.status ?? 'none',
        rows_done: rowsDone,
        rows_total: rowsTotal,
        percent: rowsTotal > 0 ? Math.round((rowsDone / rowsTotal) * 1000) / 10 : 0,
        last_key: Number(state?.last_key ?? 0),
        checkpoint: Number(state?.last_key ?? 0),
      },
      stream: {
        running: stream.running,
        paused: stream.paused,
        lag_seconds: Math.round(stream.lag_seconds * 100) / 100,
        slot_lag_bytes: stream.slot_lag_bytes,
        confirmed_lsn: stream.confirmed_lsn,
        events_done: stream.events_done,
        buffered: stream.buffered,
      },
      throughput: {
        backfill: tputBackfill,
        stream: tputStream,
        total: Math.round((tputBackfill + tputStream) * 10) / 10,
      },
      dlq: { total: dlqTotal, by_sink: dlqCounts },
      sinks: { elasticsearch: esUp, rabbitmq: mqUp },
      counts: { source: sourceCount, elasticsearch: esCount, consumer: consumerCount },
      guarantee: 'at-least-once delivery, effectively-once application',
      checked_at: new Date().toISOString(),
    };
  }

  private async throughputFor(mode: string): Promise<number> {
    const metric = await this.metrics.throughput.get();
    const entry = metric.values.find((v) => v.labels.mode === mode);
    return entry ? Number(entry.value) : 0;
  }

  /**
   * Full reset. `make seed` truncates the source, so every downstream copy of
   * it has to go too - otherwise the counts G2 compares are comparing a fresh
   * source against a stale index.
   */
  async reset(): Promise<void> {
    logger.warn('resetting pipeline state, sinks and checkpoints');
    await this.backfill.stop();
    await this.cdc.dropSlot();

    await this.db.pipeline.query('TRUNCATE pipeline.backfill_jobs');
    await this.db.pipeline.query('TRUNCATE pipeline.dlq_items');
    await this.db.pipeline.query('TRUNCATE consumer.projection');
    await this.db.pipeline.query('UPDATE consumer.stats SET value = 0');

    await this.es.client.indices
      .delete({ index: config.esIndex }, { ignore: [404] })
      .catch(() => undefined);
    await this.es.ensureIndex();
  }
}
