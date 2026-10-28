
import { useCallback, useEffect, useState } from "react";
import "./App.css";

const API = "http://localhost:8200";

function App() {
  const [apiKey, setApiKey] = useState("");
  const [bucket, setBucket] = useState("gateway-demo");
  const [buckets, setBuckets] = useState([]);
  const [objects, setObjects] = useState([]);
  const [cluster, setCluster] = useState(null);
  const [repair, setRepair] = useState(null);
  const [file, setFile] = useState(null);
  const [mode, setMode] = useState("fixed");
  const [tags, setTags] = useState('{"type":"document"}');
  const [method, setMethod] = useState("replication");
  const [message, setMessage] = useState("");
  const [busy, setBusy] = useState(false);
  const [search, setSearch] = useState({
    prefix: "",
    min_size: "",
    max_size: "",
    date_from: "",
    date_to: "",
    tag_key: "",
    tag_value: ""
  });

  const request = useCallback(async (path, options = {}) => {
    const headers = new Headers(options.headers || {});
    headers.set("X-API-Key", apiKey);

    const response = await fetch(API + path, {
      ...options,
      headers
    });

    if (!response.ok) {
      const content = await response.text();
      throw new Error(`${response.status}: ${content}`);
    }

    return response;
  }, [apiKey]);

  const load = useCallback(async () => {
    if (!apiKey) return;

    try {
      const [b, o, c, r] = await Promise.all([
        request("/buckets").then(r => r.json()),
        request("/objects?limit=1000").then(r => r.json()),
        request("/cluster/status").then(r => r.json()),
        request("/repair/progress").then(r => r.json())
      ]);

      setBuckets(b);
      setObjects(o.objects || []);
      setCluster(c);
      setRepair(r);
    } catch (error) {
      setMessage(error.message);
    }
  }, [apiKey, request]);

  useEffect(() => {
    if (!apiKey) return;

    load();
    const interval = setInterval(load, 10000);

    return () => clearInterval(interval);
  }, [apiKey, load]);

  async function execute(action) {
    setBusy(true);
    setMessage("");

    try {
      await action();
      await load();
    } catch (error) {
      setMessage(error.message);
    } finally {
      setBusy(false);
    }
  }

  function createBucket() {
    execute(async () => {
      await request("/buckets", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          name: bucket,
          method,
          n: 3,
          w: 2,
          r: 2,
          k: 4,
          m: 2
        })
      });
      setMessage("Bucket created successfully.");
    });
  }

  function upload() {
    if (!file) {
      setMessage("Select a file first.");
      return;
    }

    execute(async () => {
      const form = new FormData();
      form.append("file", file);
      form.append("mode", mode);
      form.append("tags", tags);

      const path = `/objects/${encodeURIComponent(bucket)}/${encodeURIComponent(file.name)}`;

      const response = await request(path, {
        method: "POST",
        body: form
      });

      const result = await response.json();
      setMessage(`Uploaded ${file.name} — ${result.chunk_count} chunks.`);
      setFile(null);
    });
  }

  async function download(object) {
    setBusy(true);

    try {
      const response = await request(
        `/objects/${encodeURIComponent(object.bucket)}/${encodeURIComponent(object.object_key)}?version=${object.version}`
      );

      const blob = await response.blob();
      const url = URL.createObjectURL(blob);

      const link = document.createElement("a");
      link.href = url;
      link.download = object.object_key;
      link.click();

      URL.revokeObjectURL(url);
    } catch (error) {
      setMessage(error.message);
    } finally {
      setBusy(false);
    }
  }

  function deleteObject(object) {
    if (!window.confirm(`Delete ${object.object_key}?`)) return;

    execute(async () => {
      await request(
        `/objects/${encodeURIComponent(object.bucket)}/${encodeURIComponent(object.object_key)}?version=${object.version}`,
        { method: "DELETE" }
      );

      setMessage("Object deleted.");
    });
  }

  function searchObjects() {
    execute(async () => {
      const params = new URLSearchParams();

      for (const [key, value] of Object.entries(search)) {
        if (value !== "") {
          params.set(
            key,
            key.startsWith("date_")
              ? new Date(value).toISOString()
              : value
          );
        }
      }

      params.set("limit", "1000");

      const result = await request(
        "/search?" + params.toString()
      ).then(r => r.json());

      setObjects(result.objects);
      setMessage(`Found ${result.total} matching objects.`);
    });
  }

  function resetSearch() {
    setSearch({
      prefix: "",
      min_size: "",
      max_size: "",
      date_from: "",
      date_to: "",
      tag_key: "",
      tag_value: ""
    });

    execute(async () => {
      const result = await request(
        "/objects?limit=1000"
      ).then(r => r.json());

      setObjects(result.objects);
    });
  }

  function changeSearch(key, value) {
    setSearch(current => ({
      ...current,
      [key]: value
    }));
  }

  const nodes = cluster?.nodes || [];
  const totalChunks = nodes.reduce(
    (total, node) => total + node.chunk_count,
    0
  );

  return (
    <div className="app">
      <aside className="sidebar">
        <div className="brand">◈ Distributed Storage</div>
        <div className="nav-link active">Dashboard</div>
        <div className="nav-link">Objects</div>
        <div className="nav-link">Cluster Monitoring</div>
        <div className="nav-link">Search</div>
        <div className="sidebar-bottom">Distributed Systems Midterm</div>
      </aside>

      <main className="main">
        <header className="header">
          <div>
            <h1>Storage Dashboard</h1>
            <p>Manage your files and monitor the cluster.</p>
          </div>
          <button onClick={load} disabled={busy || !apiKey}>
            Refresh
          </button>
        </header>

        <section className="panel">
          <h2>Gateway Authentication</h2>
          <div className="form-row">
            <input
              type="password"
              placeholder="Enter your Gateway API key"
              value={apiKey}
              onChange={event => setApiKey(event.target.value)}
            />
            <span className={apiKey ? "status good" : "status"}>
              {apiKey ? "Key entered" : "Not connected"}
            </span>
          </div>
        </section>

        {message && <div className="notice">{message}</div>}

        <section className="stats-grid">
          <div className="stat">
            <span>Online Nodes</span>
            <strong>{cluster?.online_nodes ?? "—"}</strong>
            <small>of {cluster?.total_nodes ?? "—"} registered</small>
          </div>
          <div className="stat">
            <span>Stored Objects</span>
            <strong>{objects.length}</strong>
            <small>Displayed objects</small>
          </div>
          <div className="stat">
            <span>Chunks on Nodes</span>
            <strong>{totalChunks}</strong>
            <small>Physical chunks reported</small>
          </div>
          <div className="stat">
            <span>Degraded Objects</span>
            <strong>{cluster?.degraded_objects ?? "—"}</strong>
            <small>Require recovery or redundancy</small>
          </div>
        </section>

        <section className="two-columns">
          <div className="panel">
            <h2>Create Bucket</h2>
            <label>Bucket name</label>
            <input value={bucket} onChange={e => setBucket(e.target.value)} />
            <label>Protection</label>
            <select value={method} onChange={e => setMethod(e.target.value)}>
              <option value="replication">Replication (3 copies)</option>
              <option value="erasure">Erasure Coding (4+2)</option>
            </select>
            <button onClick={createBucket} disabled={busy || !apiKey}>
              Create / Configure Bucket
            </button>
            <div className="helper">
              Existing buckets: {buckets.map(b => b.name).join(", ") || "None"}
            </div>
          </div>

          <div className="panel">
            <h2>Upload Object</h2>
            <label>Target bucket</label>
            <select value={bucket} onChange={e => setBucket(e.target.value)}>
              {!buckets.some(b => b.name === bucket) && (
                <option value={bucket}>{bucket}</option>
              )}
              {buckets.map(b => (
                <option key={b.name} value={b.name}>{b.name}</option>
              ))}
            </select>
            <label>Choose file</label>
            <input type="file" onChange={e => setFile(e.target.files?.[0] || null)} />
            <label>Chunking mode</label>
            <select value={mode} onChange={e => setMode(e.target.value)}>
              <option value="fixed">Fixed 4 MB</option>
              <option value="cdc">Content-defined</option>
            </select>
            <label>Object tags (JSON)</label>
            <input value={tags} onChange={e => setTags(e.target.value)} />
            <button onClick={upload} disabled={busy || !apiKey}>
              Upload File
            </button>
          </div>
        </section>

        <section className="panel">
          <div className="section-heading">
            <h2>Cluster Nodes</h2>
            <span>Live updates every 10 seconds</span>
          </div>

          <div className="node-grid">
            {nodes.map(node => (
              <div className="node-card" key={node.node_id}>
                <div className="node-title">
                  <strong>{node.node_id}</strong>
                  <span className={`status ${node.status === "online" ? "good" : "bad"}`}>
                    {node.status}
                  </span>
                </div>
                <div className="node-number">{node.chunk_count} chunks</div>
                <div className="bar">
                  <div style={{
                    width: `${totalChunks ? node.chunk_count / totalChunks * 100 : 0}%`
                  }} />
                </div>
                <small>
                  {(node.free_space / (1024 ** 3)).toFixed(2)} GB free
                </small>
              </div>
            ))}
          </div>
        </section>

        <section className="panel">
          <h2>Repair Monitoring</h2>
          <div className="repair-grid">
            <div><span>Completed Repairs</span><strong>{repair?.metrics?.repairs_completed ?? 0}</strong></div>
            <div><span>Failed Repairs</span><strong>{repair?.metrics?.repairs_failed ?? 0}</strong></div>
            <div><span>Bytes Read</span><strong>{repair?.metrics?.bytes_read ?? 0}</strong></div>
            <div><span>Bytes Written</span><strong>{repair?.metrics?.bytes_written ?? 0}</strong></div>
            <div><span>Unhealthy Fragments</span><strong>{repair?.degraded_fragments ?? 0}</strong></div>
          </div>
          <p className="helper">
            Repair counters are updated by the metadata service's background worker.
          </p>
        </section>

        <section className="panel">
          <h2>Search Objects — Elasticsearch</h2>
          <div className="search-grid">
            <input placeholder="Key prefix" value={search.prefix}
              onChange={e => changeSearch("prefix", e.target.value)} />
            <input type="number" placeholder="Minimum size (bytes)" value={search.min_size}
              onChange={e => changeSearch("min_size", e.target.value)} />
            <input type="number" placeholder="Maximum size (bytes)" value={search.max_size}
              onChange={e => changeSearch("max_size", e.target.value)} />
            <input type="datetime-local" value={search.date_from}
              onChange={e => changeSearch("date_from", e.target.value)} />
            <input type="datetime-local" value={search.date_to}
              onChange={e => changeSearch("date_to", e.target.value)} />
            <input placeholder="Tag key" value={search.tag_key}
              onChange={e => changeSearch("tag_key", e.target.value)} />
            <input placeholder="Tag value" value={search.tag_value}
              onChange={e => changeSearch("tag_value", e.target.value)} />
          </div>
          <div className="action-row">
            <button onClick={searchObjects} disabled={busy || !apiKey}>Search</button>
            <button className="secondary" onClick={resetSearch} disabled={busy || !apiKey}>Show All</button>
          </div>
        </section>

        <section className="panel">
          <h2>Stored Objects</h2>
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Bucket</th><th>Key</th><th>Size</th>
                  <th>Version</th><th>Tags</th><th>Actions</th>
                </tr>
              </thead>
              <tbody>
                {objects.map((object, index) => (
                  <tr key={`${object.bucket}-${object.object_key}-${object.version}-${index}`}>
                    <td>{object.bucket}</td>
                    <td>{object.object_key}</td>
                    <td>{(object.size / 1024).toFixed(1)} KB</td>
                    <td>{object.version}</td>
                    <td>{JSON.stringify(object.tags || {})}</td>
                    <td>
                      <div className="action-row">
                        <button onClick={() => download(object)} disabled={busy}>Download</button>
                        <button className="danger" onClick={() => deleteObject(object)} disabled={busy}>Delete</button>
                      </div>
                    </td>
                  </tr>
                ))}
                {objects.length === 0 && (
                  <tr><td colSpan="6" className="empty">No objects found</td></tr>
                )}
              </tbody>
            </table>
          </div>
        </section>
      </main>
    </div>
  );
}

export default App;
