import { Injectable } from '@nestjs/common';
import { config } from './config';
import { DbService } from './db.service';
import { EsService } from './sinks/es.service';

export interface SampleResult {
  checked: number;
  missing: number;
  mismatched: number;
  bad_ids: number[];
}

export interface CountsResult {
  source: number;
  source_live: number;
  elasticsearch: number;
  consumer: number;
  consumer_distinct: number;
  dlq_pending: number;
}

/**
 * Comparison helpers the gates call. They read the source from Postgres and
 * the documents from Elasticsearch and compare them - no pipeline state is
 * consulted, so this cannot report agreement that is not there.
 *
 * It lives in the service because a 1000-document mget is not something to
 * drive from a shell script, not because the pipeline should be trusted to
 * grade itself.
 */
@Injectable()
export class VerifyService {
  constructor(
    private readonly db: DbService,
    private readonly es: EsService,
  ) {}

  async sample(n = 1000): Promise<SampleResult> {
    const rows = await this.db.source.query<{ id: string; row_version: string }>(
      `SELECT id, row_version FROM records
        WHERE NOT deleted
        ORDER BY random() LIMIT $1`,
      [n],
    );
    const result: SampleResult = {
      checked: rows.rows.length,
      missing: 0,
      mismatched: 0,
      bad_ids: [],
    };
    if (rows.rows.length === 0) return result;

    const expected = new Map<string, number>();
    for (const r of rows.rows) expected.set(String(r.id), Number(r.row_version));

    const res = await this.es.client.mget<{ row_version: number }>({
      index: config.esIndex,
      ids: [...expected.keys()],
    });

    for (const doc of res.docs) {
      const id = doc._id;
      const want = expected.get(id) ?? 0;
      if (!('found' in doc) || !doc.found || !doc._source) {
        result.missing += 1;
        if (result.bad_ids.length < 20) result.bad_ids.push(Number(id));
        continue;
      }
      // The index may legitimately be ahead: a newer version landing between
      // the Postgres read and the mget is not a mismatch. Being *behind* is.
      if (Number(doc._source.row_version) < want) {
        result.mismatched += 1;
        if (result.bad_ids.length < 20) result.bad_ids.push(Number(id));
      }
    }

    return result;
  }

  async counts(): Promise<CountsResult> {
    const [src, live, esCount, cons, dlq] = await Promise.all([
      this.db.source.query<{ n: string }>('SELECT count(*) AS n FROM records'),
      this.db.source.query<{ n: string }>('SELECT count(*) AS n FROM records WHERE NOT deleted'),
      this.es.count().catch(() => -1),
      this.db.pipeline.query<{ n: string; d: string }>(
        'SELECT count(*) AS n, count(DISTINCT record_id) AS d FROM consumer.projection',
      ),
      this.db.pipeline.query<{ n: string }>(
        `SELECT count(*) AS n FROM pipeline.dlq_items WHERE status='pending'`,
      ),
    ]);

    return {
      source: Number(src.rows[0].n),
      source_live: Number(live.rows[0].n),
      elasticsearch: esCount,
      consumer: Number(cons.rows[0].n),
      consumer_distinct: Number(cons.rows[0].d),
      dlq_pending: Number(dlq.rows[0].n),
    };
  }
}
