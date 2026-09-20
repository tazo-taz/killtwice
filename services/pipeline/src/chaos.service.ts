import { Injectable } from '@nestjs/common';
import { DbService } from './db.service';
import { logger } from './logger';

export interface PoisonResult {
  inserted: number;
  poisoned: number;
  good: number;
  first_id: number;
  last_id: number;
  poisoned_ids: number[];
}

/**
 * Failure injection against the source, not against the pipeline. The poison
 * has to be something Elasticsearch genuinely refuses - a pipeline that
 * pretends to reject its own rows proves nothing about G4.
 */
@Injectable()
export class ChaosService {
  constructor(private readonly db: DbService) {}

  /**
   * Inserts a batch where `poison` of the rows carry attributes.score as a
   * string. Postgres accepts it (JSONB has no opinion), the index mapping is
   * `dynamic: strict` with score typed integer, so Elasticsearch answers
   * document_parsing_exception for exactly those items and 200 OK for the
   * rest - which is the per-item partial failure G4 is about.
   */
  async poison(total = 500, poison = 3): Promise<PoisonResult> {
    const good = total - poison;

    const inserted = await this.db.source.query<{ id: string }>(
      `INSERT INTO records (external_id, email, full_name, country, status, attributes)
       SELECT gen_random_uuid(),
              'chaos' || g || '@example.com',
              'Chaos ' || g,
              'GE', 'active',
              jsonb_build_object('score', (g % 100), 'tier', 'free',
                                 'signup_source', 'organic', 'blob', repeat('x', 700))
       FROM generate_series(1, $1) AS g
       RETURNING id`,
      [good],
    );

    const poisoned = await this.db.source.query<{ id: string }>(
      `INSERT INTO records (external_id, email, full_name, country, status, attributes)
       SELECT gen_random_uuid(),
              'poison' || g || '@example.com',
              'Poison ' || g,
              'GE', 'active',
              jsonb_build_object('score', 'not-a-number', 'tier', 'free',
                                 'signup_source', 'organic', 'blob', repeat('x', 700))
       FROM generate_series(1, $1) AS g
       RETURNING id`,
      [poison],
    );

    const ids = [...inserted.rows, ...poisoned.rows].map((r) => Number(r.id));
    const poisonedIds = poisoned.rows.map((r) => Number(r.id));

    logger.warn({ total, poison, poisoned_ids: poisonedIds }, 'chaos: poisoned batch inserted');

    return {
      inserted: ids.length,
      poisoned: poisonedIds.length,
      good,
      first_id: Math.min(...ids),
      last_id: Math.max(...ids),
      poisoned_ids: poisonedIds,
    };
  }

  /** Makes the poisoned rows valid again, so a replay has something to do. */
  async unpoison(ids?: number[]): Promise<number> {
    const res = ids?.length
      ? await this.db.source.query(
          `UPDATE records
              SET attributes = jsonb_set(attributes, '{score}', to_jsonb((id % 100)::int))
            WHERE id = ANY($1::bigint[])`,
          [ids],
        )
      : await this.db.source.query(
          `UPDATE records
              SET attributes = jsonb_set(attributes, '{score}', to_jsonb((id % 100)::int))
            WHERE jsonb_typeof(attributes->'score') = 'string'`,
        );
    return res.rowCount ?? 0;
  }

  /**
   * Live source mutations, so backfill and stream are genuinely racing on the
   * same rows rather than politely taking turns.
   */
  async mutate(count = 1000, op: 'update' | 'delete' | 'mixed' = 'mixed'): Promise<number> {
    if (op === 'delete') {
      const res = await this.db.source.query(
        `UPDATE records SET deleted = true
          WHERE id IN (SELECT id FROM records WHERE NOT deleted ORDER BY random() LIMIT $1)`,
        [count],
      );
      return res.rowCount ?? 0;
    }

    if (op === 'update') {
      const res = await this.db.source.query(
        `UPDATE records
            SET status = (ARRAY['active','trial','churned','paused'])[1 + floor(random()*4)::int]
          WHERE id IN (SELECT id FROM records ORDER BY random() LIMIT $1)`,
        [count],
      );
      return res.rowCount ?? 0;
    }

    const updated = await this.mutate(Math.ceil(count * 0.8), 'update');
    const deleted = await this.mutate(Math.floor(count * 0.2), 'delete');
    return updated + deleted;
  }

  /** A real DELETE, to prove the CDC path handles one and not just soft deletes. */
  async hardDelete(count = 10): Promise<number> {
    const res = await this.db.source.query(
      `DELETE FROM records
        WHERE id IN (SELECT id FROM records ORDER BY random() LIMIT $1)`,
      [count],
    );
    return res.rowCount ?? 0;
  }

  async sourceCount(includeDeleted = true): Promise<number> {
    const res = await this.db.source.query<{ n: string }>(
      includeDeleted
        ? 'SELECT count(*) AS n FROM records'
        : 'SELECT count(*) AS n FROM records WHERE NOT deleted',
    );
    return Number(res.rows[0].n);
  }
}
