import * as amqp from 'amqplib';
import { createServer } from 'node:http';
import { Pool } from 'pg';
import pino from 'pino';

/**
 * The independent consumer. A separate service, a separate container, its own
 * Postgres schema and its own lifecycle - a RabbitMQ queue with nothing
 * reading it proves nothing, and a "consumer" living inside the pipeline
 * process proves even less, because it would die with it.
 *
 * It applies the same invariant the Elasticsearch sink applies, by hand:
 * a message is only applied when its row_version is newer than the one
 * already stored. That is what makes at-least-once delivery safe.
 */

const logger = pino({
  level: process.env.LOG_LEVEL ?? 'info',
  base: { service: 'consumer' },
  timestamp: pino.stdTimeFunctions.isoTime,
});

const cfg = {
  port: Number(process.env.PORT ?? 3001),
  dbUrl: process.env.PIPELINE_DB_URL ?? 'postgres://app:app@postgres:5432/app',
  amqpUrl: process.env.AMQP_URL ?? 'amqp://app:app@rabbitmq:5672',
  exchange: process.env.MQ_EXCHANGE ?? 'records.changes',
  queue: process.env.MQ_QUEUE ?? 'records.consumer',
  dlx: 'records.dlx',
  dlq: 'records.dlq',
  prefetch: Number(process.env.PREFETCH ?? 200),
};

const db = new Pool({ connectionString: cfg.dbUrl, max: 8 });

interface ChangeEvent {
  id: number;
  row_version: number;
  op: 'upsert' | 'delete';
  mode: string;
  doc: {
    email: string | null;
    country: string | null;
    status: string | null;
    deleted: boolean;
  } | null;
}

const counters = {
  messages_received: 0,
  applied: 0,
  // Duplicate deliveries the version guard refused. This is the number that
  // demonstrates the declared guarantee, so it has to survive a kill.
  dropped_stale: 0,
  redelivered: 0,
  rejected: 0,
};
let connected = false;

/**
 * Make every counter exist at zero before anything reads /stats. Without
 * this a counter that has not fired yet is simply absent from the response,
 * and a gate reading it cannot tell "none happened" from "this build does
 * not have that counter".
 */
async function ensureCounters(): Promise<void> {
  for (const key of Object.keys(counters)) {
    await db.query(
      `INSERT INTO consumer.stats (key, value) VALUES ($1, 0) ON CONFLICT (key) DO NOTHING`,
      [key],
    );
  }
}

async function flushCounters(): Promise<void> {
  const entries = Object.entries(counters).filter(([, v]) => v > 0);
  if (entries.length === 0) return;
  for (const [key, value] of entries) {
    await db.query(
      `INSERT INTO consumer.stats (key, value) VALUES ($1, $2)
       ON CONFLICT (key) DO UPDATE SET value = consumer.stats.value + EXCLUDED.value`,
      [key, value],
    );
    counters[key as keyof typeof counters] = 0;
  }
}

/**
 * Returns true when the write was applied, false when it was dropped for
 * being not-newer. The WHERE clause on the conflict branch is the guard;
 * without it a redelivered old message would overwrite a newer one.
 */
async function apply(event: ChangeEvent): Promise<boolean> {
  const res = await db.query(
    `INSERT INTO consumer.projection
       (record_id, row_version, op, email, country, status, deleted, updated_at)
     VALUES ($1,$2,$3,$4,$5,$6,$7, now())
     ON CONFLICT (record_id) DO UPDATE SET
       row_version = EXCLUDED.row_version,
       op          = EXCLUDED.op,
       email       = EXCLUDED.email,
       country     = EXCLUDED.country,
       status      = EXCLUDED.status,
       deleted     = EXCLUDED.deleted,
       updated_at  = now()
     WHERE projection.row_version < EXCLUDED.row_version`,
    [
      event.id,
      event.row_version,
      event.op,
      event.doc?.email ?? null,
      event.doc?.country ?? null,
      event.doc?.status ?? null,
      // A delete is stored as a tombstone rather than a removal, so a late
      // stale upsert cannot resurrect the record.
      event.op === 'delete' ? true : (event.doc?.deleted ?? false),
    ],
  );
  return (res.rowCount ?? 0) > 0;
}

async function consume(): Promise<void> {
  const conn = await amqp.connect(cfg.amqpUrl);
  conn.on('close', () => {
    connected = false;
    logger.warn('amqp connection closed, reconnecting');
    setTimeout(() => void start(), 2000);
  });
  conn.on('error', (err: unknown) => logger.warn({ err: String(err) }, 'amqp error'));

  const ch = await conn.createChannel();
  await ch.assertExchange(cfg.exchange, 'topic', { durable: true });
  await ch.assertExchange(cfg.dlx, 'topic', { durable: true });
  await ch.assertQueue(cfg.queue, { durable: true, deadLetterExchange: cfg.dlx });
  await ch.bindQueue(cfg.queue, cfg.exchange, '#');
  await ch.assertQueue(cfg.dlq, { durable: true });
  await ch.bindQueue(cfg.dlq, cfg.dlx, '#');

  // Bounded in-flight work. Without a prefetch the broker pushes the whole
  // queue at this process and the 256MB limit does the rest.
  await ch.prefetch(cfg.prefetch);
  connected = true;
  logger.info({ queue: cfg.queue, prefetch: cfg.prefetch }, 'consumer ready');

  await ch.consume(cfg.queue, (msg: amqp.ConsumeMessage | null) => {
    if (!msg) return;
    void (async () => {
      counters.messages_received += 1;
      if (msg.fields.redelivered) counters.redelivered += 1;

      let event: ChangeEvent;
      try {
        event = JSON.parse(msg.content.toString()) as ChangeEvent;
      } catch (err) {
        // Unparseable: no amount of retrying fixes it, so it goes straight
        // to the dead letter exchange instead of cycling forever.
        counters.rejected += 1;
        logger.warn({ err: String(err) }, 'rejecting unparseable message');
        ch.nack(msg, false, false);
        return;
      }

      try {
        const applied = await apply(event);
        if (applied) counters.applied += 1;
        else counters.dropped_stale += 1;

        // Ack after the projection write, never before. Same ordering rule
        // as the pipeline's checkpoint: a crash here redelivers, and the
        // version guard makes that a no-op.
        ch.ack(msg);
      } catch (err) {
        logger.error({ err: String(err), record_id: event.id }, 'projection write failed');
        // Requeue: a database hiccup is not the message's fault.
        ch.nack(msg, false, true);
      }
    })();
  });
}

async function start(): Promise<void> {
  try {
    await ensureCounters();
    await consume();
  } catch (err) {
    logger.warn({ err: String(err) }, 'consumer start failed, retrying in 3s');
    setTimeout(() => void start(), 3000);
  }
}

createServer((req, res) => {
  if (req.url === '/health') {
    res.writeHead(connected ? 200 : 503, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ status: connected ? 'ok' : 'disconnected' }));
    return;
  }
  if (req.url === '/stats') {
    void db
      .query<{ key: string; value: string }>('SELECT key, value FROM consumer.stats')
      .then(async (r) => {
        const stats: Record<string, number> = {};
        for (const row of r.rows) stats[row.key] = Number(row.value);
        const distinct = await db.query<{ n: string }>(
          'SELECT count(*) AS n FROM consumer.projection',
        );
        stats.stored_rows = Number(distinct.rows[0].n);
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify(stats));
      })
      .catch(() => {
        res.writeHead(500);
        res.end('{}');
      });
    return;
  }
  res.writeHead(404);
  res.end();
}).listen(cfg.port, () => logger.info({ port: cfg.port }, 'consumer http listening'));

setInterval(() => void flushCounters().catch(() => undefined), 1000);
void start();
