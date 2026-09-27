import React, { useState } from 'react';
import './App.css';

export default function App() {
  const [topK, setTopK] = useState(5);
  const [rrf, setRrf] = useState(60);
  const [inspectorOpen, setInspectorOpen] = useState(true);

  const docs = [
    {id:'20003398', src:'Document Past.pdf', cos:0.82, bm25:0.85, rrf:0.36, ctx:'Context: the latest performance optimization techniques for HNSW vector search.'},
    {id:'20003393', src:'Document Past.pdf', cos:0.323, bm25:0.23, rrf:0.38, ctx:'Context: the latest performance optimization techniques for HNSW vector search.'},
    {id:'20003338', src:'Document Past.pdf', cos:0.37, bm25:0.25, rrf:0.13, ctx:'Context: the latest performance optimization techniques for HNSW vector search.'}
  ];

  return (
    <>
      <header>
        <div className="brand">
          <span className="mark">🔺</span>
          <h1>Retriever <span>v1.0.0 [Go Engine]</span></h1>
        </div>
        <div className="status"><span className="dot"></span> CIRCUIT BREAKER: CLOSED (Healthy)</div>
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
          <div className="chat-row">
            <div className="icon-col"><span className="swirl">👤</span></div>
            <p>Explain the latest performance optimization techniques for HNSW vector search.</p>
          </div>
          <div className="chat-row answer">
            <div className="icon-col"><span className="lbl">AI</span></div>
            <p>The retriever fused dense vector similarity from pgvector with sparse keyword matches
            from tsvector, re-ranked the merged candidates with Reciprocal Rank Fusion, and passed the
            top-scoring chunks to the generation model to ground its answer in retrieved context.</p>
          </div>

          <section className="panel-card inspector" id="inspector">
            <div className="inspector-head" onClick={() => setInspectorOpen(!inspectorOpen)}>
              <h3>🛠 System Engineering Telemetry Inspector</h3>
              <span className="caret" style={{ transform: inspectorOpen ? 'rotate(180deg)' : 'rotate(0deg)' }}>▲</span>
            </div>
            
            {inspectorOpen && (
              <div className="inspector-body">
                <p className="section-label">Performance Vitals <span className="subtle">(Stat Tiles)</span></p>
                <div className="stat-grid">
                  <div className="panel-card stat"><div className="label">Total Request Latency</div><div className="value">124.5ms</div></div>
                  <div className="panel-card stat"><div className="label">Vector Search (pgvector)</div><div className="value">22.1ms</div></div>
                  <div className="panel-card stat"><div className="label">Keyword Search (tsvector)</div><div className="value">10.4ms</div></div>
                  <div className="panel-card stat"><div className="label">LLM Generation</div><div className="value">84.7ms</div></div>
                </div>

                <p className="section-label">OpenTelemetry Correlation</p>
                <div className="otel-grid">
                  <div><label>W3C Trace ID</label><div className="code-box">e58f96c21a0040e698d24508499291fb</div></div>
                  <div><label>View Tempo Trace Waterfall</label><div className="code-box">http://localhost:3000/explore?left=%5B%22now-1h%22,%22now%22,%22Tempo%22,%7B%22query%22:%22e58f96c21a0040e698d24508499291fb%22%7D%5D</div></div>
                </div>

                <p className="section-label">Documents Retrieved via Reciprocal Rank Fusion</p>
                <div className="doc-grid">
                  {docs.map(d => (
                    <div key={d.id} className="panel-card doc-card">
                      <h4>Metadata</h4>
                      <div className="row"><b>Chunk ID:</b> {d.id}</div>
                      <div className="row"><b>Source Document:</b> {d.src}</div>
                      <div className="metrics">
                        <h4>Metrics</h4>
                        <div className="row"><b>Vector Cosine Score:</b> {d.cos}</div>
                        <div className="row"><b>Keyword BM25 Score:</b> {d.bm25}</div>
                        <div className="row"><b>Final RRF Score:</b> {d.rrf}</div>
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

          <div className="chat-input-wrapper">
            <input 
              type="text" 
              className="chat-input" 
              placeholder="Query the RAG pipeline..."
              disabled
            />
          </div>
        </main>
      </div>
    </>
  );
}
