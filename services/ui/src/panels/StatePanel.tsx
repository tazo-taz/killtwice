import type { Status } from '../api';

function Stat({ label, value, sub }: { label: string; value: string; sub?: string }) {
  return (
    <div className="stat">
      <div className="label">{label}</div>
      <div className="value">{value}</div>
      {sub ? <div className="sub">{sub}</div> : null}
    </div>
  );
}

const fmt = (n: number): string => n.toLocaleString('en-US');

const bytes = (n: number): string => {
  if (n < 1024) return `${n} B`;
  if (n < 1024 ** 2) return `${(n / 1024).toFixed(1)} KB`;
  if (n < 1024 ** 3) return `${(n / 1024 ** 2).toFixed(1)} MB`;
  return `${(n / 1024 ** 3).toFixed(2)} GB`;
};

/**
 * Panel 1. Every number G5 requires is on this screen, so the five questions
 * can be answered without opening a terminal.
 */
export function StatePanel({ status, consumer }: { status: Status; consumer: Record<string, number> }) {
  const b = status.backfill;
  const s = status.stream;

  return (
    <section className="panel">
      <h2>1 · State</h2>

      <div className="row">
        <span className={`pill ${status.healthy ? 'ok' : 'bad'}`}>
          {status.healthy ? 'healthy' : 'unhealthy'}
        </span>
        <span className={`pill ${status.sinks.elasticsearch ? 'ok' : 'bad'}`}>
          elasticsearch {status.sinks.elasticsearch ? 'up' : 'down'}
        </span>
        <span className={`pill ${status.sinks.rabbitmq ? 'ok' : 'bad'}`}>
          rabbitmq {status.sinks.rabbitmq ? 'up' : 'down'}
        </span>
        {s.paused ? <span className="pill warn">reader paused (backpressure)</span> : null}
      </div>

      <div className="bar">
        <div style={{ width: `${Math.min(100, b.percent)}%` }} />
      </div>
      <div className="muted" style={{ fontSize: 12, marginBottom: 12 }}>
        backfill {b.status} — {fmt(b.rows_done)} / {fmt(b.rows_total)} ({b.percent}%), checkpoint at
        id {fmt(b.checkpoint)}
      </div>

      <div className="grid">
        <Stat label="throughput" value={`${fmt(status.throughput.total)}/s`}
              sub={`backfill ${fmt(status.throughput.backfill)} · stream ${fmt(status.throughput.stream)}`} />
        <Stat label="incremental lag" value={`${s.lag_seconds}s`}
              sub={s.running ? `slot ${bytes(s.slot_lag_bytes)}` : 'stream stopped'} />
        <Stat label="dlq" value={fmt(status.dlq.total)}
              sub={`es ${status.dlq.by_sink.elasticsearch ?? 0} · mq ${status.dlq.by_sink.rabbitmq ?? 0}`} />
        <Stat label="source rows" value={fmt(status.counts.source)} />
        <Stat label="in elasticsearch" value={fmt(status.counts.elasticsearch)}
              sub={`${fmt(status.counts.source - status.counts.elasticsearch)} behind`} />
        <Stat label="in consumer" value={fmt(status.counts.consumer)}
              sub={`${fmt(consumer.messages_received ?? 0)} delivered`} />
      </div>

      <div className="log">
        {`guarantee   ${status.guarantee}
lsn         ${s.confirmed_lsn ?? '-'}
events      ${fmt(s.events_done)} processed, ${fmt(s.buffered)} buffered
redelivered ${fmt(consumer.redelivered ?? 0)} messages the broker re-sent
stale drops ${fmt(consumer.dropped_stale ?? 0)} (version guard refusing not-newer writes)`}
      </div>
    </section>
  );
}
