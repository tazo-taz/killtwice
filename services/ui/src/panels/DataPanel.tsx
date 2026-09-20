import { useEffect, useState } from 'react';
import { api, type RecordHit } from '../api';

/** Panel 2. Browse and search what actually landed in the search index. */
export function DataPanel() {
  const [q, setQ] = useState('');
  const [live, setLive] = useState(true);
  const [total, setTotal] = useState(0);
  const [hits, setHits] = useState<RecordHit[]>([]);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    const load = () => {
      api
        .records(q)
        .then((res) => {
          if (cancelled) return;
          setHits(res.hits);
          setTotal(res.total);
          setError(null);
        })
        .catch((err: unknown) => !cancelled && setError(String(err)));
    };
    load();
    // Polling rather than websockets: the sort is row_version descending, so
    // every poll shows the newest changes arriving. Enough to watch the
    // pipeline work without a second transport to keep alive.
    const timer = live ? setInterval(load, 2000) : null;
    return () => {
      cancelled = true;
      if (timer) clearInterval(timer);
    };
  }, [q, live]);

  return (
    <section className="panel">
      <h2>2 · Data</h2>

      <div className="row">
        <input
          type="text"
          placeholder="search email, name, country, status"
          value={q}
          onChange={(e) => setQ(e.target.value)}
        />
        <button onClick={() => setLive((v) => !v)}>{live ? 'pause live' : 'go live'}</button>
      </div>

      <div className="muted" style={{ fontSize: 12, marginBottom: 6 }}>
        {error ? `error: ${error}` : `${total.toLocaleString('en-US')} matching documents, newest first`}
      </div>

      <div className="scroll">
        <table>
          <thead>
            <tr>
              <th>id</th>
              <th>email</th>
              <th>country</th>
              <th>status</th>
              <th className="num">version</th>
            </tr>
          </thead>
          <tbody>
            {hits.map((h) => (
              <tr key={h.id}>
                <td className="num">{h.id}</td>
                <td>
                  {h.email}
                  {h.deleted ? <span className="pill warn" style={{ marginLeft: 6 }}>deleted</span> : null}
                </td>
                <td>{h.country}</td>
                <td>{h.status}</td>
                <td className="num">{h.row_version}</td>
              </tr>
            ))}
            {hits.length === 0 ? (
              <tr>
                <td colSpan={5} className="muted">nothing indexed yet</td>
              </tr>
            ) : null}
          </tbody>
        </table>
      </div>
    </section>
  );
}
