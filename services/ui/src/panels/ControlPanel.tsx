import { useEffect, useState } from 'react';
import { api, type DlqItem, type Status } from '../api';

/**
 * Panel 3. Start, stop, replay, reset. Every button here is a POST that
 * `make verify` also calls - nothing in this panel is reachable only by hand.
 */
export function ControlPanel({ status, log }: { status: Status; log: (line: string) => void }) {
  const [busy, setBusy] = useState<string | null>(null);
  const [dlq, setDlq] = useState<DlqItem[]>([]);
  const [chunk, setChunk] = useState(2000);

  const refreshDlq = () => {
    api.dlq().then(setDlq).catch(() => undefined);
  };

  useEffect(() => {
    refreshDlq();
    const timer = setInterval(refreshDlq, 3000);
    return () => clearInterval(timer);
  }, []);

  const run = async (name: string, fn: () => Promise<unknown>) => {
    setBusy(name);
    try {
      const res = await fn();
      log(`${name}: ${JSON.stringify(res)}`);
    } catch (err) {
      log(`${name}: FAILED ${String(err)}`);
    } finally {
      setBusy(null);
      refreshDlq();
    }
  };

  return (
    <section className="panel">
      <h2>3 · Control</h2>

      <div className="row">
        <input type="number" value={chunk} min={100} step={100}
               onChange={(e) => setChunk(Number(e.target.value))} />
        <button disabled={!!busy}
                onClick={() => run('backfill start', () => api.post('/api/backfill/start', { chunkSize: chunk }))}>
          start backfill
        </button>
        <button disabled={!!busy}
                onClick={() => run('backfill stop', () => api.post('/api/backfill/stop'))}>
          stop backfill
        </button>
      </div>

      <div className="row">
        <button disabled={!!busy || status.stream.running}
                onClick={() => run('stream start', () => api.post('/api/stream/start'))}>
          start stream
        </button>
        <button disabled={!!busy || !status.stream.running}
                onClick={() => run('stream stop', () => api.post('/api/stream/stop'))}>
          stop stream
        </button>
        <button className="danger" disabled={!!busy}
                onClick={() => run('reset', () => api.post('/api/reset'))}>
          reset everything
        </button>
      </div>

      <div className="row">
        <button disabled={!!busy || dlq.length === 0}
                onClick={() => run('dlq replay (payload)', () => api.post('/api/dlq/replay', { from: 'payload' }))}>
          replay dlq
        </button>
        <button disabled={!!busy || dlq.length === 0}
                onClick={() => run('dlq replay (source)', () => api.post('/api/dlq/replay', { from: 'source' }))}>
          replay from source
        </button>
        <span className="muted" style={{ fontSize: 12 }}>
          {dlq.length} pending
        </span>
      </div>

      <div className="scroll">
        <table>
          <thead>
            <tr>
              <th>record</th>
              <th>sink</th>
              <th>class</th>
              <th className="num">tries</th>
            </tr>
          </thead>
          <tbody>
            {dlq.map((d) => (
              <tr key={d.id}>
                <td className="num">{d.record_id}</td>
                <td>{d.sink}</td>
                <td>{d.error_class}</td>
                <td className="num">{d.attempts}</td>
              </tr>
            ))}
            {dlq.length === 0 ? (
              <tr><td colSpan={4} className="muted">dead letter queue is empty</td></tr>
            ) : null}
          </tbody>
        </table>
      </div>
    </section>
  );
}
