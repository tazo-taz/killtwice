import pino from 'pino';
import { config } from './config';

// Structured JSON only. Every line that belongs to a unit of work carries
// mode + chunk_key/lsn + batch_id, so a failure can be located without
// reading code - which is the whole point of G5.
export const logger = pino({
  level: config.logLevel,
  base: { service: 'pipeline' },
  timestamp: pino.stdTimeFunctions.isoTime,
});

export type Logger = typeof logger;
