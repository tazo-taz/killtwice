import { Injectable } from '@nestjs/common';
import { randomUUID } from 'node:crypto';
import { config } from './config';
import { logger } from './logger';
import { DlqService } from './dlq.service';
import { MetricsService } from './metrics.service';
import { EsService } from './sinks/es.service';
import { MqService } from './sinks/mq.service';
import { sleep, nextDelay } from './util/backoff';
import { BatchResult, Envelope, ErrorClass, Mode } from './types';

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
    sink: 'elasticsearch' | 'rabbitmq',
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

  private async toElasticsearch(
    batch: Envelope[],
    batchId: string,
    result: BatchResult,
    toDlq: boolean,
  ): Promise<void> {
    let pending = batch;

    // Two separate counters on purpose. A sink being down must never consume
    // an item's attempt budget - otherwise a long outage would dead-letter
    // healthy rows, which is data loss wearing a DLQ costume.
    let transportAttempt = 0;
    let itemAttempt = 0;

    while (pending.length > 0) {
      const stopTimer = this.metrics.writeDuration.startTimer({ sink: 'elasticsearch' });
      let outcome;
      try {
        outcome = await this.es.bulk(pending, batchId);
        stopTimer();
        this.metrics.sinkUp.set({ sink: 'elasticsearch' }, 1);
        transportAttempt = 0;
      } catch (err) {
        stopTimer();
        this.metrics.sinkUp.set({ sink: 'elasticsearch' }, 0);
        this.metrics.retries.inc({ sink: 'elasticsearch' });
        transportAttempt += 1;
        const delay = nextDelay(transportAttempt);
        logger.warn(
          { batch_id: batchId, attempt: transportAttempt, delay_ms: delay, err: String(err) },
          'elasticsearch unreachable, backing off',
        );
        await sleep(delay);
        continue;
      }

      result.written += outcome.ok.length;
      result.stale += outcome.stale.length;
      this.metrics.rowsWritten.inc({ sink: 'elasticsearch' }, outcome.ok.length);
      this.metrics.rowsStale.inc({ sink: 'elasticsearch' }, outcome.stale.length);

      // The ones that succeeded are committed. They are never re-sent, which
      // is the clause G4 calls unacceptable to violate.
      for (const p of outcome.permanent) {
        await this.fail('elasticsearch', p.env, p.errorClass, p.detail, result, toDlq);
      }

      if (outcome.retryable.length === 0) return;

      itemAttempt += 1;
      if (itemAttempt >= config.maxItemAttempts) {
        for (const r of outcome.retryable) {
          await this.fail('elasticsearch', r.env, 'rejected', r.detail, result, toDlq);
        }
        return;
      }

      pending = outcome.retryable.map((r) => r.env);
      this.metrics.retries.inc({ sink: 'elasticsearch' }, pending.length);
      await sleep(nextDelay(itemAttempt));
    }
  }

  private async toRabbitmq(
    batch: Envelope[],
    batchId: string,
    result: BatchResult,
    toDlq: boolean,
  ): Promise<void> {
    let pending = batch;
    let transportAttempt = 0;
    let itemAttempt = 0;

    while (pending.length > 0) {
      const stopTimer = this.metrics.writeDuration.startTimer({ sink: 'rabbitmq' });
      let outcome;
      try {
        outcome = await this.mq.publishBatch(pending, batchId);
        stopTimer();
        this.metrics.sinkUp.set({ sink: 'rabbitmq' }, 1);
        transportAttempt = 0;
      } catch (err) {
        stopTimer();
        this.metrics.sinkUp.set({ sink: 'rabbitmq' }, 0);
        this.metrics.retries.inc({ sink: 'rabbitmq' });
        transportAttempt += 1;
        const delay = nextDelay(transportAttempt);
        logger.warn(
          { batch_id: batchId, attempt: transportAttempt, delay_ms: delay, err: String(err) },
          'rabbitmq unreachable, backing off',
        );
        await sleep(delay);
        continue;
      }

      this.metrics.rowsWritten.inc({ sink: 'rabbitmq' }, outcome.ok.length);
      if (outcome.failed.length === 0) return;

      itemAttempt += 1;
      if (itemAttempt >= config.maxItemAttempts) {
        for (const f of outcome.failed) {
          const cls: ErrorClass = f.detail === 'unroutable' ? 'unroutable' : 'rejected';
          await this.fail('rabbitmq', f.env, cls, f.detail, result, toDlq);
        }
        return;
      }

      pending = outcome.failed.map((f) => f.env);
      this.metrics.retries.inc({ sink: 'rabbitmq' }, pending.length);
      await sleep(nextDelay(itemAttempt));
    }
  }
}
