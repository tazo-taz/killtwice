import { useCallback, useEffect, useState } from 'react';
import { api, type Status } from './api';
import { StatePanel } from './panels/StatePanel';
import { DataPanel } from './panels/DataPanel';
import { ControlPanel } from './panels/ControlPanel';
import { ChaosPanel } from './panels/ChaosPanel';

export default function App() {
  const [status, setStatus] = useState<Status | null>(null);
  const [consumer, setConsumer] = useState<Record<string, number>>({});
  const [error, setError] = useState<string | null>(null);
  const [lines, setLines] = useState<string[]>([]);

  const log = useCallback((line: string) => {
    const stamp = new Date().toLocaleTimeString();
    setLines((prev) => [`${stamp}  ${line}`, ...prev].slice(0, 40));
  }, []);

  useEffect(() => {
    const load = () => {
      api
        .status()
        .then((s) => {
          setStatus(s);
          setError(null);
        })
        .catch((err: unknown) => setError(String(err)));
      api.consumerStats().then(setConsumer).catch(() => undefined);
    };
    load();
    const timer = setInterval(load, 1000);
    return () => clearInterval(timer);
  }, []);

  return (
    <>
      <header>
        <h1>Kill It Twice</h1>
        <span className="guarantee">
          {status ? status.guarantee : 'connecting…'}
          {error ? ` — pipeline unreachable (${error})` : ''}
        </span>
      </header>

      {status ? (
        <main>
          <StatePanel status={status} consumer={consumer} />
          <DataPanel />
          <ControlPanel status={status} log={log} />
          <ChaosPanel log={log} />
          <section className="panel" style={{ gridColumn: '1 / -1' }}>
            <h2>Activity</h2>
            <div className="log" style={{ maxHeight: 160 }}>
              {lines.length ? lines.join('\n') : 'no actions yet'}
            </div>
          </section>
        </main>
      ) : (
        <main>
          <section className="panel">
            <h2>Waiting for the pipeline</h2>
            <p className="muted">
              The pipeline is not answering on <code>/api/status</code> yet. If you just killed it,
              this is what G1 looks like from the outside — it should come back on its own.
            </p>
          </section>
        </main>
      )}
    </>
  );
}
