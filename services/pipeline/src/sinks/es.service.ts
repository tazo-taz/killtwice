import { Injectable } from '@nestjs/common';
import { Client } from '@elastic/elasticsearch';
import { config } from '../config';
import { logger } from '../logger';
import { Envelope, ErrorClass } from '../types';

/** Only for items the bulk response reported an error on - success needs no verdict. */
export type ItemVerdict =
  | { kind: 'stale' }
  | { kind: 'retryable'; detail: unknown }
  | { kind: 'permanent'; errorClass: ErrorClass; detail: unknown };

export interface BulkOutcome {
  ok: Envelope[];
  stale: Envelope[];
  retryable: { env: Envelope; detail: unknown }[];
  permanent: { env: Envelope; errorClass: ErrorClass; detail: unknown }[];
}

/** The cluster is busy or degraded - the same item may well succeed later. */
const RETRYABLE_TYPES = new Set([
  'circuit_breaking_exception',
  'es_rejected_execution_exception',
  'unavailable_shards_exception',
  'cluster_block_exception',
  'no_shard_available_action_exception',
  'process_cluster_event_timeout_exception',
]);

/** The document itself is unacceptable. Retrying it changes nothing. */
const MAPPING_TYPES = new Set([
  'document_parsing_exception',
  'mapper_parsing_exception',
  'strict_dynamic_mapping_exception',
  'illegal_argument_exception',
]);

/**
 * Per-item classification. This is the whole of G4: `_bulk` answers HTTP 200
 * with individual failures inside the body, so a client that only checks the
 * HTTP status drops records silently and never knows.
 */
function classify(status: number, type: string): ItemVerdict {
  if (status === 409 || type === 'version_conflict_engine_exception') {
    // Not an error. The version guard refusing a not-newer write is the
    // system working, and it is what makes a replayed duplicate a no-op.
    return { kind: 'stale' };
  }
  if (status === 429 || status >= 502 || RETRYABLE_TYPES.has(type)) {
    return { kind: 'retryable', detail: type };
  }
  if (MAPPING_TYPES.has(type)) {
    return { kind: 'permanent', errorClass: 'mapping', detail: type };
  }
  if (status === 413 || type === 'entity_content_too_long_exception') {
    return { kind: 'permanent', errorClass: 'too_large', detail: type };
  }
  return { kind: 'permanent', errorClass: 'rejected', detail: type };
}

@Injectable()
export class EsService {
  readonly client = new Client({
    node: config.esNode,
    // Bounded so a paused Elasticsearch produces a timeout we can back off
    // from, rather than a socket that hangs forever holding the batch.
    requestTimeout: 15_000,
    pingTimeout: 3_000,
    // Retries are ours, not the client's - we need to classify per item and
    // count the attempts, which the built-in retry hides.
    maxRetries: 0,
  });

  async ensureIndex(): Promise<void> {
    const exists = await this.client.indices.exists({ index: config.esIndex });
    if (exists) return;

    await this.client.indices.create({
      index: config.esIndex,
      settings: {
        number_of_shards: 1,
        number_of_replicas: 0,
        refresh_interval: '1s',
      },
      mappings: {
        // strict on purpose. It is what makes G4's poison a genuine
        // Elasticsearch rejection instead of something the pipeline
        // pretends to reject.
        dynamic: 'strict',
        properties: {
          id: { type: 'long' },
          external_id: { type: 'keyword' },
          email: { type: 'keyword' },
          full_name: { type: 'text' },
          country: { type: 'keyword' },
          status: { type: 'keyword' },
          deleted: { type: 'boolean' },
          created_at: { type: 'date' },
          updated_at: { type: 'date' },
          row_version: { type: 'long' },
          attributes: {
            dynamic: 'strict',
            properties: {
              score: { type: 'integer' },
              tier: { type: 'keyword' },
              signup_source: { type: 'keyword' },
              blob: { type: 'text', index: false },
            },
          },
        },
      },
    });
    logger.info({ index: config.esIndex }, 'created elasticsearch index');
  }

  /**
   * One `_bulk` round trip. Throws only when the whole request failed (sink
   * down, timeout) - which the caller turns into a backoff. Per-item failures
   * come back in the outcome and are never thrown, because 497 of 500 rows
   * succeeded and re-sending them is explicitly unacceptable.
   */
  async bulk(envelopes: Envelope[], batchId: string): Promise<BulkOutcome> {
    const out: BulkOutcome = { ok: [], stale: [], retryable: [], permanent: [] };
    if (envelopes.length === 0) return out;

    const operations: unknown[] = [];
    for (const env of envelopes) {
      if (env.op === 'delete') {
        operations.push({
          delete: {
            _index: config.esIndex,
            _id: String(env.id),
            version: env.version,
            version_type: 'external',
          },
        });
      } else {
        operations.push({
          index: {
            _index: config.esIndex,
            _id: String(env.id),
            version: env.version,
            version_type: 'external',
          },
        });
        operations.push(env.doc);
      }
    }

    const res = await this.client.bulk({ operations: operations as object[] });

    if (!res.errors) {
      out.ok = envelopes;
      return out;
    }

    res.items.forEach((item, i) => {
      const env = envelopes[i];
      const r = item.index ?? item.delete ?? item.create ?? item.update;
      if (!r || !r.error) {
        out.ok.push(env);
        return;
      }
      const verdict = classify(r.status ?? 0, r.error.type ?? 'unknown');
      switch (verdict.kind) {
        case 'stale':
          out.stale.push(env);
          break;
        case 'retryable':
          out.retryable.push({ env, detail: r.error });
          break;
        case 'permanent':
          out.permanent.push({ env, errorClass: verdict.errorClass, detail: r.error });
          logger.warn(
            { batch_id: batchId, record_id: env.id, error: r.error.type },
            'elasticsearch rejected item permanently',
          );
      }
    });

    return out;
  }

  async count(): Promise<number> {
    const res = await this.client.count({ index: config.esIndex });
    return res.count;
  }

  async ping(): Promise<boolean> {
    try {
      await this.client.ping();
      return true;
    } catch {
      return false;
    }
  }
}
