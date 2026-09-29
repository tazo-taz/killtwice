import { Injectable } from '@nestjs/common';
import { randomUUID } from 'node:crypto';
import { config } from './config';
import { logger } from './logger';
import { DlqService } from './dlq.service';
import { MetricsService } from './metrics.service';
import { BulkOutcome, EsService } from './sinks/es.service';
import { MqService } from './sinks/mq.service';
import { sleep, nextDelay } from './util/backoff';
import { BatchResult, Envelope, ErrorClass, Mode } from './types';

type Sink = 'elasticsearch' | 'rabbitmq';

/**
 * Writes one batch to both sinks and does not return until every envelope is
 * accounted for: written, refused as stale, or dead-lettered.
 *
 * The caller advances its checkpoint only on return. That ordering is the
 * whole of G1 and G2 - crash before the return and the batch is replayed,
 * which the version guard absorbs; advance before the return and the batch
 * is silently lost, which nothing absorbs.
 */
@Injectable()
export class WriterService {
  constructor(
    private readonly es: EsService,
    private readonly mq: MqService,
    private readonly dlq: DlqService,
    private readonly metrics: MetricsService,
  ) {}

  /**
   * Both readers can carry the same record in one batch, and the stream can
   * carry several versions of it. Keeping only the newest is cheap and stops
   * the sinks doing work they would immediately refuse.
   */
  private dedupe(envelopes: Envelope[]): Envelope[] {
    const newest = new Map<number, Envelope>();
    for (const env of envelopes) {
      const prev = newest.get(env.id);
      if (!prev || env.version > prev.version) newest.set(env.id, env);
    }
    return [...newest.values()];
  }

  /**
   * `dlq: false` still records failures in the result, it just does not write
   * them to the table. Replay uses that: the item is already in the DLQ, and
   * a failed replay must bump its attempt count rather than insert a twin.
   */
  async writeBatch(
    envelopes: Envelope[],
    mode: Mode,
    opts: { dlq?: boolean } = {},
  ): Promise<BatchResult> {
    const batch = this.dedupe(envelopes);
    const result: BatchResult = { written: 0, stale: 0, deadLettered: 0, failures: [] };
    if (batch.length === 0) return result;

    const toDlq = opts.dlq !== false;
    const batchId = randomUUID();
    await this.toElasticsearch(batch, batchId, result, toDlq);
    await this.toRabbitmq(batch, batchId, result, toDlq);

    this.metrics.recordThroughput(mode, batch.length);
    return result;
  }

  private async fail(
    sink: Sink,
    env: Envelope,
    errorClass: ErrorClass,
    detail: unknown,
    result: BatchResult,
    toDlq: boolean,
  ): Promise<void> {
    result.failures.push({ env, sink, errorClass, detail });
    if (toDlq) {
      await this.dlq.add(sink, env, errorClass, detail);
      result.deadLettered += 1;
    }
  }

  /** `written` and `stale` in the result describe the index, the sink that holds state. */
  private toElasticsearch(
    batch: Envelope[],
    batchId: string,
    result: BatchResult,
    toDlq: boolean,
  ): Promise<void> {
    return this.deliver('elasticsearch', batch, batchId, result, toDlq, {
      send: async (pending) => {
        const outcome = await this.es.bulk(pending, batchId);
        result.written += outcome.ok.length;
        result.stale += outcome.stale.length;
        return outcome;
      },
      exhausted: () => 'rejected',
    });
  }

  /**
   * The broker has no per-item verdicts beyond confirmed or not, so every
   * unconfirmed message is retryable until the item budget runs out.
   */
  private toRabbitmq(
    batch: Envelope[],
    batchId: string,
    result: BatchResult,
    toDlq: boolean,
  ): Promise<void> {
    return this.deliver('rabbitmq', batch, batchId, result, toDlq, {
      send: async (pending) => {
        const outcome = await this.mq.publishBatch(pending, batchId);
        return { ok: outcome.ok, stale: [], retryable: outcome.failed, permanent: [] };
      },
      exhausted: (detail) => (detail === 'unroutable' ? 'unroutable' : 'rejected'),
    });
  }

  /**
   * The retry loop both sinks share. `send` throws only when the whole round
   * trip failed (sink down, timeout); per-item failures come back in the
   * outcome. Items that succeeded are never re-sent, which is the clause G4
   * calls unacceptable to violate.
   */
  private async deliver(
    sink: Sink,
    batch: Envelope[],
    batchId: string,
    result: BatchResult,
    toDlq: boolean,
    how: {
      send: (pending: Envelope[]) => Promise<BulkOutcome>;
      /** Error class for an item still retryable when its budget runs out. */
      exhausted: (detail: unknown) => ErrorClass;
    },
  ): Promise<void> {
    let pending = batch;

    // Two separate counters on purpose. A sink being down must never consume
    // an item's attempt budget - otherwise a long outage would dead-letter
    // healthy rows, which is data loss wearing a DLQ costume.
    let transportAttempt = 0;
    let itemAttempt = 0;

    while (pending.length > 0) {
      const stopTimer = this.metrics.writeDuration.startTimer({ sink });
      let outcome: BulkOutcome;
      try {
        outcome = await how.send(pending);
        stopTimer();
        this.metrics.sinkUp.set({ sink }, 1);
        transportAttempt = 0;
      } catch (err) {
        stopTimer();
        this.metrics.sinkUp.set({ sink }, 0);
        this.metrics.retries.inc({ sink });
        transportAttempt += 1;
        const delay = nextDelay(transportAttempt);
        logger.warn(
          { batch_id: batchId, attempt: transportAttempt, delay_ms: delay, err: String(err) },
          `${sink} unreachable, backing off`,
        );
        await sleep(delay);
        continue;
      }

      this.metrics.rowsWritten.inc({ sink }, outcome.ok.length);
      this.metrics.rowsStale.inc({ sink }, outcome.stale.length);

      for (const p of outcome.permanent) {
        await this.fail(sink, p.env, p.errorClass, p.detail, result, toDlq);
      }

      if (outcome.retryable.length === 0) return;

      itemAttempt += 1;
      if (itemAttempt >= config.maxItemAttempts) {
        for (const r of outcome.retryable) {
          await this.fail(sink, r.env, how.exhausted(r.detail), r.detail, result, toDlq);
        }
        return;
      }

      pending = outcome.retryable.map((r) => r.env);
      this.metrics.retries.inc({ sink }, pending.length);
      await sleep(nextDelay(itemAttempt));
    }
  }
}
