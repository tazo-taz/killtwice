import { Injectable } from '@nestjs/common';
import { logger } from './logger';
import { DbService } from './db.service';
import { DlqService } from './dlq.service';
import { WriterService } from './writer.service';
import { Envelope, RecordDoc } from './types';

export interface ReplayResult {
  attempted: number;
  resolved: number;
  still_failing: number;
  from: 'payload' | 'source';
}

/**
 * The replay path. A DLQ nobody can drain is a log file with extra steps.
 *
 * Two modes, both real:
 *  - payload (default): rebuild the envelope from the stored document. Works
 *    even if the source row has since been deleted. Replaying an unchanged
 *    poisoned payload fails again, which is correct - the attempt counter
 *    goes up and the item stays pending.
 *  - source: re-read the row as it is now. This is the mode to use after
 *    fixing whatever made the row unacceptable.
 */
@Injectable()
export class ReplayService {
  constructor(
    private readonly db: DbService,
    private readonly dlq: DlqService,
    private readonly writer: WriterService,
  ) {}

  async replay(
    ids?: string[],
    from: 'payload' | 'source' = 'payload',
    limit = 500,
  ): Promise<ReplayResult> {
    const items = await this.dlq.pending(limit, ids);
    const result: ReplayResult = {
      attempted: items.length,
      resolved: 0,
      still_failing: 0,
      from,
    };
    if (items.length === 0) return result;

    for (const item of items) {
      let doc: RecordDoc | null = null;

      if (from === 'source') {
        const res = await this.db.source.query<RecordDoc>(
          `SELECT id, external_id, email, full_name, country, status, attributes,
                  deleted, created_at, updated_at, row_version
             FROM records WHERE id = $1`,
          [item.record_id],
        );
        doc = res.rows[0] ?? null;
      } else {
        doc = item.payload as RecordDoc;
      }

      // Source row is gone and we were asked to read from source: the delete
      // already propagated, so there is nothing left to replay.
      if (!doc && from === 'source') {
        await this.dlq.resolve(item.id);
        result.resolved += 1;
        continue;
      }

      const env: Envelope = {
        id: Number(item.record_id),
        version: doc ? Number(doc.row_version) : Number(item.row_version),
        op: item.op === 'delete' ? 'delete' : 'upsert',
        doc: doc ? { ...doc, id: Number(doc.id), row_version: Number(doc.row_version) } : null,
        mode: 'replay',
        offset: item.source_offset ?? '',
      };

      // dlq:false - this item is already in the table. A failed replay bumps
      // its attempt count; it does not insert a second copy of itself.
      const outcome = await this.writer.writeBatch([env], 'replay', { dlq: false });

      // Written or refused as stale both count: either way the sink now holds
      // something at least as new as this item.
      if (outcome.failures.length === 0) {
        await this.dlq.resolve(item.id);
        result.resolved += 1;
      } else {
        await this.dlq.bumpAttempt(item.id, outcome.failures[0].detail);
        result.still_failing += 1;
      }
    }

    await this.dlq.refreshGauge();
    logger.info({ ...result }, 'dlq replay finished');
    return result;
  }
}
