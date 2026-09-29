import { Injectable, OnModuleDestroy } from '@nestjs/common';
import * as amqp from 'amqplib';
import { config } from '../config';
import { logger } from '../logger';
import { Envelope } from '../types';

export interface PublishOutcome {
  ok: Envelope[];
  failed: { env: Envelope; detail: unknown }[];
}

// Derived rather than named: amqplib moved from @types/amqplib to bundled
// types mid-0.10.x and renamed Connection to ChannelModel on the way. Naming
// the types directly breaks on one version or the other; deriving them from
// the function signatures works on both.
type Conn = Awaited<ReturnType<typeof amqp.connect>>;
type ConfirmCh = Awaited<ReturnType<Conn['createConfirmChannel']>>;

@Injectable()
export class MqService implements OnModuleDestroy {
  private conn: Conn | null = null;
  private channel: ConfirmCh | null = null;
  private connecting: Promise<ConfirmCh> | null = null;
  private returned = new Set<string>();

  /**
   * Topology is asserted by both this service and the consumer, so whichever
   * starts first wins and the other is a no-op. The DLX is what gives the
   * queue side of the DLQ for free.
   */
  private async connect(): Promise<ConfirmCh> {
    const conn = await amqp.connect(config.amqpUrl);
    conn.on('error', (err: unknown) =>
      logger.warn({ err: String(err) }, 'amqp connection error'),
    );
    conn.on('close', () => {
      this.conn = null;
      this.channel = null;
    });

    const ch = await conn.createConfirmChannel();

    // A channel can close on its own - a broker-side channel error, say - with
    // the connection still up. Without this the dead channel stays cached and
    // every publish fails against it forever, which the writer can only
    // answer by backing off forever.
    ch.on('close', () => {
      if (this.channel === ch) this.channel = null;
    });

    await ch.assertExchange(config.mqExchange, 'topic', { durable: true });
    await ch.assertExchange(config.mqDlx, 'topic', { durable: true });
    await ch.assertQueue(config.mqQueue, {
      durable: true,
      deadLetterExchange: config.mqDlx,
    });
    await ch.bindQueue(config.mqQueue, config.mqExchange, '#');
    await ch.assertQueue(config.mqDlq, { durable: true });
    await ch.bindQueue(config.mqDlq, config.mqDlx, '#');

    // mandatory publishes that find no queue come back here rather than
    // vanishing. An unroutable message is a DLQ item, not a shrug.
    ch.on('return', (msg: { properties?: { messageId?: string } }) => {
      const id = msg?.properties?.messageId;
      if (id) this.returned.add(id);
    });

    this.conn = conn;
    this.channel = ch;
    logger.info('connected to rabbitmq');
    return ch;
  }

  private async ensureChannel(): Promise<ConfirmCh> {
    if (this.channel) return this.channel;
    if (!this.connecting) {
      this.connecting = this.connect().finally(() => {
        this.connecting = null;
      });
    }
    return this.connecting;
  }

  /**
   * Publishes the batch and waits for the broker to confirm every message.
   * Throws if the channel itself is gone, which the caller turns into a
   * backoff - the checkpoint does not move in the meantime.
   */
  async publishBatch(envelopes: Envelope[], batchId: string): Promise<PublishOutcome> {
    const out: PublishOutcome = { ok: [], failed: [] };
    if (envelopes.length === 0) return out;

    const ch = await this.ensureChannel();
    const pending: Promise<void>[] = [];

    for (const env of envelopes) {
      // Stable id: the consumer dedupes on (id, version) anyway, but this
      // makes a redelivery visible in the broker rather than invisible.
      const messageId = `${env.id}:${env.version}`;
      const routingKey = env.op === 'delete' ? 'record.delete' : 'record.upsert';
      const body = Buffer.from(
        JSON.stringify({
          id: env.id,
          row_version: env.version,
          op: env.op,
          mode: env.mode,
          doc: env.doc,
          emitted_at: new Date().toISOString(),
        }),
      );

      let written = true;
      pending.push(
        new Promise<void>((resolve) => {
          written = ch.publish(
            config.mqExchange,
            routingKey,
            body,
            {
              persistent: true,
              messageId,
              contentType: 'application/json',
              mandatory: true,
            },
            (err) => {
              if (err) out.failed.push({ env, detail: String(err) });
              else if (this.returned.delete(messageId))
                out.failed.push({ env, detail: 'unroutable' });
              else out.ok.push(env);
              resolve();
            },
          );
        }),
      );

      // The socket buffer is full. Waiting for drain is the difference
      // between backpressure and an unbounded memory climb.
      if (!written) await this.drained(ch);
    }

    await Promise.all(pending);
    if (out.failed.length > 0) {
      logger.warn(
        { batch_id: batchId, failed: out.failed.length },
        'rabbitmq did not confirm every message in the batch',
      );
    }
    return out;
  }

  /**
   * Resolves on 'drain', rejects if the channel closes first. Waiting on
   * 'drain' alone would hang forever on a connection lost mid-batch, with the
   * batch and its checkpoint stuck behind it; rejecting turns that into an
   * ordinary transport failure the writer backs off from.
   */
  private drained(ch: ConfirmCh): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      const onDrain = (): void => {
        ch.off('close', onClose);
        resolve();
      };
      const onClose = (): void => {
        ch.off('drain', onDrain);
        reject(new Error('channel closed while waiting for drain'));
      };
      ch.once('drain', onDrain);
      ch.once('close', onClose);
    });
  }

  async queueDepth(queue: string): Promise<number> {
    const ch = await this.ensureChannel();
    const q = await ch.checkQueue(queue);
    return q.messageCount;
  }

  async ping(): Promise<boolean> {
    try {
      await this.ensureChannel();
      return this.channel !== null;
    } catch {
      return false;
    }
  }

  async onModuleDestroy(): Promise<void> {
    try {
      await this.channel?.close();
      await this.conn?.close();
    } catch {
      /* shutting down anyway */
    }
  }
}
