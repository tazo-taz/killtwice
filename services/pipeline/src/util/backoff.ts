import { config } from '../config';

export const sleep = (ms: number): Promise<void> =>
  new Promise((resolve) => setTimeout(resolve, ms));

/**
 * Exponential backoff with full jitter, capped. Jitter matters because both
 * sink writers wake up at the same moment after an outage; without it they
 * retry in lockstep and hammer a sink that is still coming back.
 */
export function nextDelay(attempt: number): number {
  const exp = Math.min(
    config.backoffMaxMs,
    config.backoffMinMs * 2 ** Math.max(0, attempt - 1),
  );
  return Math.floor(config.backoffMinMs + Math.random() * (exp - config.backoffMinMs + 1));
}
