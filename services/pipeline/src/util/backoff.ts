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

/**
 * Retry until it works. There is no attempt limit on purpose: a sink being
 * down is not an error the pipeline can resolve by giving up, and giving up
 * would mean losing the batch. The checkpoint simply does not advance while
 * this is looping, which is exactly the behaviour G3 checks.
 *
 * Every path through here sleeps. Nothing spins.
 */
export async function retryForever<T>(
  fn: () => Promise<T>,
  onError: (err: unknown, attempt: number, delayMs: number) => void,
  isStopped: () => boolean = () => false,
): Promise<T> {
  let attempt = 0;
  for (;;) {
    try {
      return await fn();
    } catch (err) {
      attempt += 1;
      const delay = nextDelay(attempt);
      onError(err, attempt, delay);
      if (isStopped()) throw err;
      await sleep(delay);
    }
  }
}
