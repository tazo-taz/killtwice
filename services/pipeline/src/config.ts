const num = (v: string | undefined, d: number): number => {
  const n = Number(v);
  return Number.isFinite(n) && n > 0 ? n : d;
};

export const config = {
  port: num(process.env.PORT, 3000),

  sourceDbUrl: process.env.SOURCE_DB_URL ?? 'postgres://app:app@postgres:5432/app',
  pipelineDbUrl: process.env.PIPELINE_DB_URL ?? 'postgres://app:app@postgres:5432/app',

  esNode: process.env.ES_NODE ?? 'http://elasticsearch:9200',
  esIndex: process.env.ES_INDEX ?? 'records',

  amqpUrl: process.env.AMQP_URL ?? 'amqp://app:app@rabbitmq:5672',
  mqExchange: process.env.MQ_EXCHANGE ?? 'records.changes',
  mqQueue: 'records.consumer',
  mqDlx: 'records.dlx',
  mqDlq: 'records.dlq',

  slotName: process.env.SLOT_NAME ?? 'records_slot',
  publicationName: process.env.PUBLICATION_NAME ?? 'records_pub',

  backfillChunkSize: num(process.env.BACKFILL_CHUNK_SIZE, 2000),
  cdcBatchSize: num(process.env.CDC_BATCH_SIZE, 1000),

  // Bounded on purpose. When the buffer hits this, the reader stops reading -
  // that is the backpressure point, and the reason a sink outage does not
  // turn into an OOM.
  queueCapacity: num(process.env.QUEUE_CAPACITY, 20000),

  // Retry envelope for a sink that is failing. Sleeps, never spins.
  backoffMinMs: num(process.env.BACKOFF_MIN_MS, 100),
  backoffMaxMs: num(process.env.BACKOFF_MAX_MS, 30000),

  // After this many attempts a *retryable* error is treated as permanent and
  // the item goes to the DLQ, so one poisoned-but-retryable row cannot block
  // the pipeline forever.
  maxItemAttempts: num(process.env.MAX_ITEM_ATTEMPTS, 8),

  logLevel: process.env.LOG_LEVEL ?? 'info',
};

export type Config = typeof config;
