import { useState } from 'react';
import { api } from '../api';

/**
 * Panel 4. Break it on purpose.
 *
 * The one thing this panel cannot do is kill a container - the browser has no
 * access to the Docker socket, and handing it one would be a worse idea than
 * the convenience is worth. `make verify` does the killing; this panel does
 * everything reachable over the API.
 */
export function ChaosPanel({ log }: { log: (line: string) => void }) {
  const [busy, setBusy] = useState(false);
  const [mutations, setMutations] = useState(1000);
  const [batch, setBatch] = useState(500);
  const [poison, setPoison] = useState(3);

  const run = async (name: string, fn: () => Promise<unknown>) => {
    setBusy(true);
    try {
      log(`${name}: ${JSON.stringify(await fn())}`);
    } catch (err) {
      log(`${name}: FAILED ${String(err)}`);
    } finally {
      setBusy(false);
    }
  };

  return (
    <section className="panel">
      <h2>4 · Chaos</h2>

      <div className="row">
        <input type="number" value={mutations} min={1} step={100}
               onChange={(e) => setMutations(Number(e.target.value))} />
        <button disabled={busy}
                onClick={() => run('mutate', () => api.post('/api/chaos/mutate', { count: mutations, op: 'mixed' }))}>
          generate source changes
        </button>
        <button disabled={busy}
                onClick={() => run('hard delete', () => api.post('/api/chaos/hard-delete', { count: 10 }))}>
          hard delete 10
        </button>
      </div>

      <div className="row">
        <input type="number" value={batch} min={1} step={100}
               onChange={(e) => setBatch(Number(e.target.value))} />
        <input type="number" value={poison} min={1}
               onChange={(e) => setPoison(Number(e.target.value))} />
        <button className="danger" disabled={busy}
                onClick={() => run('poison', () => api.post('/api/chaos/poison', { total: batch, poison }))}>
          poison a batch
        </button>
        <button disabled={busy}
                onClick={() => run('unpoison', () => api.post('/api/chaos/unpoison'))}>
          fix poisoned rows
        </button>
      </div>

      <p className="muted" style={{ fontSize: 12, lineHeight: 1.5 }}>
        Poisoning inserts {batch} rows of which {poison} carry <code>attributes.score</code> as a
        string. The index mapping is <code>dynamic: strict</code> with <code>score</code> typed as an
        integer, so Elasticsearch genuinely refuses those items and accepts the rest — the
        partial-failure case, not a simulation of one.
      </p>
      <p className="muted" style={{ fontSize: 12, lineHeight: 1.5 }}>
        To take a sink down, pause its container: <code>docker compose pause elasticsearch</code>.
        To kill the pipeline mid-load: <code>docker compose kill pipeline</code>. Both are what
        <code> make verify</code> does on its own.
      </p>
    </section>
  );
}
