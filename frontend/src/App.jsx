import React, { useState } from 'react';
import './App.css';

export default function App() {
  const [topK, setTopK] = useState(5);
  const [rrf, setRrf] = useState(60);
  const [inputValue, setInputValue] = useState('');
  const [loading, setLoading] = useState(false);
  const [circuitBreaker, setCircuitBreaker] = useState('CLOSED (Healthy)');
  const [history, setHistory] = useState([]);

  const toggleInspector = (index) => {
    setHistory(prev => {
      const newHistory = [...prev];
      newHistory[index].inspectorOpen = !newHistory[index].inspectorOpen;
      return newHistory;
    });
  };

  const handleSend = async (e) => {
    if (e.key === 'Enter' && inputValue.trim() && !loading) {
      const query = inputValue.trim();
      setInputValue('');
      setLoading(true);

      const newMessage = { query, answer: '...', telemetry: null, inspectorOpen: true };
      setHistory(prev => [...prev, newMessage]);

      try {
        const res = await fetch('http://localhost:8080/v1/search', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ query, top_k: Number(topK), rrf_k: Number(rrf) })
        });
        
        if (!res.ok) throw new Error('Backend unreachable or returned an error status');
        
        const data = await res.json();

        // Build telemetry ONLY from real backend data — zero hardcoded values
        const realTelemetry = {
          latencyTotalMs: data.telemetry?.latency_total_ms ?? null,
          latencySearchMs: data.telemetry?.latency_search_ms ?? null,
          traceId: data.telemetry?.trace_id || null,
          resultCount: data.telemetry?.result_count ?? 0,
          docs: (data.results || []).map(r => ({
            id: r.document_id + "_" + (r.chunk_index ?? 0),
            src: r.document_id,
            score: r.score,
            ctx: r.chunk_text
          }))
        };
        
        setHistory(prev => {
          const newHistory = [...prev];
          newHistory[newHistory.length - 1].answer = data.answer || "No answer returned by backend.";
          newHistory[newHistory.length - 1].telemetry = realTelemetry;
          return newHistory;
        });

      } catch (err) {
        setCircuitBreaker('OPEN (Connection Failed)');
        
        setHistory(prev => {
          const newHistory = [...prev];
          newHistory[newHistory.length - 1].answer = `Error: ${err.message}. Please ensure the Go backend is running at http://localhost:8080.`;
          return newHistory;
        });
        
        // Auto-recover breaker after 3s
        setTimeout(() => setCircuitBreaker('CLOSED (Healthy)'), 3000);
      } finally {
        setLoading(false);
      }
    }
  };

  return (
    <>
      <header>
        <div className="brand">
          <div className="logo-icon">
            <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="var(--ember)" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
              <polygon points="12 2 22 20 2 20" fill="var(--ember-hot)" opacity="0.2"/>
            </svg>
          </div>
          <h1>Retriever <span className="version">v1.0.0</span> <span className="engine">Go Engine</span></h1>
        </div>
        <div className="status" style={{
          background: circuitBreaker.includes('OPEN') ? 'rgba(239, 68, 68, 0.1)' : undefined,
          borderColor: circuitBreaker.includes('OPEN') ? 'rgba(239, 68, 68, 0.2)' : undefined,
          color: circuitBreaker.includes('OPEN') ? '#ef4444' : undefined
        }}>
          <span className="dot" style={{ background: circuitBreaker.includes('OPEN') ? '#ef4444' : undefined }}></span> 
          CIRCUIT BREAKER: {circuitBreaker}
        </div>
      </header>

      <div className="layout">
        <aside className="panel-card config">
          <h2>Configuration</h2>
          <div className="field">
            <div className="top-row">Top-K Chunks</div>
            <div className="track-wrap">
              <span className="val-bubble" style={{ left: `${((topK - 1) / (20 - 1)) * 100}%` }}>{topK}</span>
              <input 
                type="range" 
                min="1" 
                max="20" 
                value={topK} 
                onChange={e => setTopK(e.target.value)} 
              />
            </div>
            <div className="meta"><span>Default 5</span><span>Range 1–20</span></div>
          </div>
          <div className="field">
            <div className="top-row">RRF Constant (k)</div>
            <div className="track-wrap">
              <span className="val-bubble" style={{ left: `${((rrf - 1) / (120 - 1)) * 100}%` }}>{rrf}</span>
              <input 
                type="range" 
                min="1" 
                max="120" 
                value={rrf} 
                onChange={e => setRrf(e.target.value)} 
              />
            </div>
            <div className="meta"><span>Default 60</span></div>
          </div>
          <div className="link-btn" onClick={() => window.open('http://localhost:3000', '_blank')}>
            <div><div className="t">View Metrics in Grafana</div><div className="u">http://localhost:3000</div></div>
            <span className="chev">›</span>
          </div>
        </aside>

        <main>
          <div className="chat-history">
            {history.length === 0 && (
              <div className="empty-state">
                <div className="empty-icon-wrap">
                  <svg width="32" height="32" viewBox="0 0 24 24" fill="none" stroke="var(--sub)" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
                    <rect x="3" y="3" width="18" height="18" rx="2" ry="2"/>
                    <line x1="3" y1="9" x2="21" y2="9"/>
                    <line x1="9" y1="21" x2="9" y2="9"/>
                  </svg>
                </div>
                <h2>Retriever is ready</h2>
                <p>Query the RAG pipeline to search your vector database.</p>
              </div>
            )}
            {history.map((msg, idx) => (
              <React.Fragment key={idx}>
                <div className="chat-row">
                  <div className="icon-col"><span className="swirl">👤</span></div>
                  <p>{msg.query}</p>
                </div>
                <div className="chat-row answer">
                  <div className="icon-col"><span className="lbl">AI</span></div>
                  <p>{msg.answer}</p>
                </div>

                {msg.telemetry && (
                  <section className="panel-card inspector">
                    <div className="inspector-head" onClick={() => toggleInspector(idx)}>
                      <h3>🛠 System Engineering Telemetry Inspector</h3>
                      <span className="caret" style={{ transform: msg.inspectorOpen ? 'rotate(180deg)' : 'rotate(0deg)' }}>▲</span>
                    </div>
                    
                    {msg.inspectorOpen && (
                      <div className="inspector-body">
                        <p className="section-label">Performance Vitals <span className="subtle">(Real Measurements)</span></p>
                        <div className="stat-grid">
                          <div className="panel-card stat"><div className="label">Total Request Latency</div><div className="value">{msg.telemetry.latencyTotalMs != null ? `${msg.telemetry.latencyTotalMs}ms` : '—'}</div></div>
                          <div className="panel-card stat"><div className="label">Search Latency (Embed + DB)</div><div className="value">{msg.telemetry.latencySearchMs != null ? `${msg.telemetry.latencySearchMs}ms` : '—'}</div></div>
                          <div className="panel-card stat"><div className="label">Results Retrieved</div><div className="value">{msg.telemetry.resultCount}</div></div>
                          <div className="panel-card stat"><div className="label">Search Mode</div><div className="value">Hybrid RRF</div></div>
                        </div>

                        <p className="section-label">OpenTelemetry Correlation</p>
                        <div className="otel-grid">
                          <div><label>W3C Trace ID</label><div className="code-box">{msg.telemetry.traceId || 'N/A (OTel collector offline)'}</div></div>
                          <div><label>View Tempo Trace Waterfall</label><div className="code-box">{msg.telemetry.traceId ? <a href={`http://localhost:3000/explore?schemaVersion=1&panes=${encodeURIComponent(JSON.stringify({"pane1":{"datasource":"tempo","queries":[{"refId":"A","datasource":{"type":"tempo","uid":"tempo"},"queryType":"traceql","query":msg.telemetry.traceId}],"range":{"from":"now-1h","to":"now"}}}))}`} target="_blank" rel="noreferrer" style={{color: 'inherit', textDecoration: 'underline'}}>{`http://localhost:3000/explore?schemaVersion=1&panes=... (Trace: ${msg.telemetry.traceId})`}</a> : 'N/A'}</div></div>
                        </div>

                        <p className="section-label">Documents Retrieved via Reciprocal Rank Fusion</p>
                        <div className="doc-grid">
                          {(msg.telemetry.docs || []).map(d => (
                            <div key={d.id} className="panel-card doc-card">
                              <h4>Metadata</h4>
                              <div className="row"><b>Chunk ID:</b> {d.id}</div>
                              <div className="row"><b>Source Document:</b> {d.src}</div>
                              <div className="metrics">
                                <h4>Metrics</h4>
                                <div className="row"><b>RRF Fusion Score:</b> {d.score.toFixed(4)}</div>
                              </div>
                              <details className="expander">
                                <summary>Expand Context</summary>
                                <div className="content">{d.ctx}</div>
                              </details>
                            </div>
                          ))}
                        </div>
                      </div>
                    )}
                  </section>
                )}
              </React.Fragment>
            ))}
          </div>

          <div className="chat-input-wrapper">
            <input 
              type="text" 
              className="chat-input" 
              placeholder="Query the RAG pipeline..."
              value={inputValue}
              onChange={e => setInputValue(e.target.value)}
              onKeyDown={handleSend}
              disabled={loading}
            />
          </div>
        </main>
      </div>
    </>
  );
}
