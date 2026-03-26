import { useState, useEffect, useCallback, useRef } from 'react'
import './styles.css'

const API_BASE = import.meta.env.VITE_API_URL || 'http://localhost:8000'
const USE_MOCK = import.meta.env.VITE_USE_MOCK !== 'false'

// ─── Mock Data (mirrors V_DATA_MARKETPLACE + DATA_PRODUCT_CATALOG) ─────────
const MOCK_PRODUCTS = [
  {
    product_id: 'DP-001',
    product_name: 'Sales Performance',
    domain: 'Sales',
    description: 'Regional and global sales KPIs, order volumes, revenue trends, and performance metrics across SAP Europe, JDE Americas, and QAD EMEA business units.',
    owner_team: 'Sales Analytics',
    trust_score: 98.9,
    freshness_label: 'Dynamic · 15 min lag',
    access_tier: 'Restricted',
    sla_availability: 99.5,
    row_count: 15000,
    source_systems: ['SAP Europe', 'JDE Americas', 'QAD EMEA'],
    tags: ['revenue', 'orders', 'regional', 'star-schema'],
    share_available: true,
    schema: [
      { col: 'order_key', type: 'VARCHAR', desc: 'Surrogate key (SHA256)' },
      { col: 'region', type: 'VARCHAR', desc: 'Geographic region' },
      { col: 'revenue_usd', type: 'NUMBER', desc: 'Net revenue, masked for non-finance roles' },
      { col: 'order_date', type: 'DATE', desc: 'Order date' },
      { col: 'product_line', type: 'VARCHAR', desc: 'Vertiv product category' },
    ],
  },
  {
    product_id: 'DP-002',
    product_name: 'Customer 360',
    domain: 'CRM',
    description: 'Unified customer profile combining SAP, JDE, QAD ERP master data with Salesforce CRM signals and ML-derived CLV / churn probability features.',
    owner_team: 'Platform Engineering',
    trust_score: 98.7,
    freshness_label: 'Dynamic · 5 min lag',
    access_tier: 'Restricted',
    sla_availability: 99.5,
    row_count: 1500,
    source_systems: ['SAP Europe', 'JDE Americas', 'QAD EMEA', 'Salesforce Global'],
    tags: ['customers', 'crm', 'unified', 'ml-ready', 'scd2'],
    share_available: true,
    schema: [
      { col: 'customer_key', type: 'VARCHAR', desc: 'Cross-system surrogate key' },
      { col: 'customer_name', type: 'VARCHAR', desc: 'PII masked — ANALYST+ role required' },
      { col: 'email', type: 'VARCHAR', desc: 'Masked — last 4 chars visible' },
      { col: 'clv_score', type: 'FLOAT', desc: 'ML-predicted customer lifetime value' },
      { col: 'churn_probability', type: 'FLOAT', desc: 'XGBoost churn score [0–1]' },
    ],
  },
  {
    product_id: 'DP-003',
    product_name: 'Revenue Analytics',
    domain: 'Finance',
    description: 'Point-in-time correct ML feature store for revenue forecasting. Strict AS_OF_DATE boundary prevents data leakage. Includes ARPU, LTV, and segment-level predictions.',
    owner_team: 'Data Science',
    trust_score: 99.6,
    freshness_label: 'Daily · 02:00 UTC',
    access_tier: 'Public',
    sla_availability: 99.9,
    row_count: 1500,
    source_systems: ['Silver ML Feature Store'],
    tags: ['ml', 'forecasting', 'clv', 'pit-correct', 'features'],
    share_available: false,
    schema: [
      { col: 'customer_key', type: 'VARCHAR', desc: 'Feature entity key' },
      { col: 'as_of_date', type: 'DATE', desc: 'Point-in-time snapshot date' },
      { col: 'avg_order_value', type: 'FLOAT', desc: '90-day rolling average' },
      { col: 'predicted_revenue_90d', type: 'FLOAT', desc: 'XGBoost forecast' },
      { col: 'segment', type: 'VARCHAR', desc: 'ML-derived customer segment' },
    ],
  },
  {
    product_id: 'DP-004',
    product_name: 'Opportunity Signals',
    domain: 'Sales',
    description: 'Salesforce pipeline enriched with Snowflake Cortex AI vector embeddings for opportunity scoring, next-best-action recommendations, and competitive intelligence.',
    owner_team: 'Revenue Operations',
    trust_score: 98.7,
    freshness_label: 'Dynamic · 15 min lag',
    access_tier: 'Restricted',
    sla_availability: 99.5,
    row_count: 2500,
    source_systems: ['Salesforce Global'],
    tags: ['pipeline', 'ai', 'cortex', 'opportunities', 'embeddings'],
    share_available: false,
    schema: [
      { col: 'opportunity_id', type: 'VARCHAR', desc: 'Salesforce opportunity ID' },
      { col: 'amount_usd', type: 'NUMBER', desc: 'Deal value, role-masked' },
      { col: 'win_probability', type: 'FLOAT', desc: 'ML win score [0–1]' },
      { col: 'embedding_vector', type: 'ARRAY', desc: '1536-dim Cortex embedding' },
      { col: 'next_best_action', type: 'VARCHAR', desc: 'Cortex LLM recommendation' },
    ],
  },
]

// ─── API Layer ──────────────────────────────────────────────────────────────
async function fetchProducts() {
  if (USE_MOCK) return MOCK_PRODUCTS
  const res = await fetch(`${API_BASE}/products`)
  if (!res.ok) throw new Error('Failed to fetch products')
  return res.json()
}

async function searchProducts(query) {
  if (USE_MOCK) {
    const q = query.toLowerCase()
    return MOCK_PRODUCTS.filter(p =>
      p.product_name.toLowerCase().includes(q) ||
      p.description.toLowerCase().includes(q) ||
      p.tags.some(t => t.includes(q)) ||
      p.domain.toLowerCase().includes(q)
    ).map(p => ({ ...p, relevance: 0.92 }))
  }
  const res = await fetch(`${API_BASE}/search?q=${encodeURIComponent(query)}`)
  if (!res.ok) throw new Error('Search failed')
  return res.json()
}

async function requestAccess(productId, userInfo) {
  if (USE_MOCK) return { success: true, ticket: `REQ-${Date.now()}` }
  const res = await fetch(`${API_BASE}/access-request`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ product_id: productId, ...userInfo }),
  })
  return res.json()
}

// ─── Icons ──────────────────────────────────────────────────────────────────
const SearchIcon = () => (
  <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
    <circle cx="11" cy="11" r="8"/><path d="m21 21-4.35-4.35"/>
  </svg>
)
const ShieldIcon = () => (
  <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
    <path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z"/>
  </svg>
)
const BoltIcon = () => (
  <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
    <polygon points="13 2 3 14 12 14 11 22 21 10 12 10 13 2"/>
  </svg>
)
const SparkIcon = () => (
  <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
    <path d="m12 3-1.912 5.813a2 2 0 0 1-1.275 1.275L3 12l5.813 1.912a2 2 0 0 1 1.275 1.275L12 21l1.912-5.813a2 2 0 0 1 1.275-1.275L21 12l-5.813-1.912a2 2 0 0 1-1.275-1.275L12 3Z"/>
  </svg>
)
const CloseIcon = () => (
  <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
    <path d="M18 6 6 18M6 6l12 12"/>
  </svg>
)
const ShareIcon = () => (
  <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
    <path d="M4 12v8a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2v-8"/><polyline points="16 6 12 2 8 6"/><line x1="12" y1="2" x2="12" y2="15"/>
  </svg>
)

// ─── Trust Score Ring ────────────────────────────────────────────────────────
function TrustRing({ score, size = 52 }) {
  const r = (size - 8) / 2
  const circ = 2 * Math.PI * r
  const dash = (score / 100) * circ
  const color = score >= 99 ? '#22c55e' : score >= 97 ? '#fe5b1b' : '#f59e0b'
  return (
    <svg width={size} height={size} viewBox={`0 0 ${size} ${size}`}>
      <circle cx={size/2} cy={size/2} r={r} fill="none" stroke="#2a3040" strokeWidth="4"/>
      <circle
        cx={size/2} cy={size/2} r={r}
        fill="none" stroke={color} strokeWidth="4"
        strokeDasharray={`${dash} ${circ}`}
        strokeLinecap="round"
        transform={`rotate(-90 ${size/2} ${size/2})`}
        style={{ transition: 'stroke-dasharray 1s ease' }}
      />
      <text x={size/2} y={size/2+1} textAnchor="middle" dominantBaseline="central"
        style={{ fontSize: '10px', fontWeight: 600, fill: color, fontFamily: 'IBM Plex Mono, monospace' }}>
        {score.toFixed(1)}
      </text>
    </svg>
  )
}

// ─── Domain Badge ────────────────────────────────────────────────────────────
const DOMAIN_COLORS = {
  Sales: { bg: '#1a2035', border: '#fe5b1b', text: '#fe5b1b' },
  CRM: { bg: '#1a2030', border: '#818cf8', text: '#818cf8' },
  Finance: { bg: '#1a2520', border: '#22c55e', text: '#22c55e' },
}
function DomainBadge({ domain }) {
  const c = DOMAIN_COLORS[domain] || { bg: '#1a2535', border: '#64748b', text: '#94a3b8' }
  return (
    <span style={{ background: c.bg, border: `1px solid ${c.border}`, color: c.text,
      padding: '2px 8px', borderRadius: 4, fontSize: 11, fontWeight: 600, letterSpacing: '0.05em' }}>
      {domain.toUpperCase()}
    </span>
  )
}

// ─── Product Card ────────────────────────────────────────────────────────────
function ProductCard({ product, onClick }) {
  return (
    <div className="product-card" onClick={() => onClick(product)}>
      <div className="card-header">
        <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
          <TrustRing score={product.trust_score} />
          <div>
            <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 4 }}>
              <DomainBadge domain={product.domain} />
              {product.share_available && (
                <span className="badge-share"><ShareIcon /> Share</span>
              )}
            </div>
            <h3 className="card-title">{product.product_name}</h3>
          </div>
        </div>
        <span className={`access-badge ${product.access_tier === 'Public' ? 'access-public' : 'access-restricted'}`}>
          {product.access_tier === 'Restricted' ? <><ShieldIcon /> Restricted</> : <><BoltIcon /> Public</>}
        </span>
      </div>

      <p className="card-desc">{product.description}</p>

      <div className="card-meta">
        <div className="meta-item">
          <span className="meta-label">Freshness</span>
          <span className="meta-value">{product.freshness_label}</span>
        </div>
        <div className="meta-item">
          <span className="meta-label">Rows</span>
          <span className="meta-value mono">{product.row_count.toLocaleString()}</span>
        </div>
        <div className="meta-item">
          <span className="meta-label">SLA</span>
          <span className="meta-value mono">{product.sla_availability}%</span>
        </div>
      </div>

      <div className="card-tags">
        {product.tags.slice(0, 4).map(t => (
          <span key={t} className="tag">#{t}</span>
        ))}
      </div>

      <div className="card-footer">
        <span className="card-id">{product.product_id}</span>
        <button className="btn-secondary" onClick={e => { e.stopPropagation(); onClick(product) }}>
          View Details →
        </button>
      </div>
    </div>
  )
}

// ─── Product Modal ────────────────────────────────────────────────────────────
function ProductModal({ product, onClose }) {
  const [tab, setTab] = useState('overview')
  const [reqName, setReqName] = useState('')
  const [reqEmail, setReqEmail] = useState('')
  const [reqReason, setReqReason] = useState('')
  const [submitted, setSubmitted] = useState(null)
  const [submitting, setSubmitting] = useState(false)

  useEffect(() => {
    const handler = e => { if (e.key === 'Escape') onClose() }
    window.addEventListener('keydown', handler)
    return () => window.removeEventListener('keydown', handler)
  }, [onClose])

  const handleRequest = async () => {
    setSubmitting(true)
    try {
      const result = await requestAccess(product.product_id, { name: reqName, email: reqEmail, reason: reqReason })
      setSubmitted(result)
    } catch {
      setSubmitted({ success: false, ticket: null })
    }
    setSubmitting(false)
  }

  return (
    <div className="modal-overlay" onClick={e => e.target === e.currentTarget && onClose()}>
      <div className="modal">
        <div className="modal-header">
          <div style={{ display: 'flex', alignItems: 'center', gap: 14 }}>
            <TrustRing score={product.trust_score} size={60} />
            <div>
              <div style={{ display: 'flex', gap: 8, marginBottom: 6 }}>
                <DomainBadge domain={product.domain} />
                <span className="modal-id">{product.product_id}</span>
              </div>
              <h2 className="modal-title">{product.product_name}</h2>
              <p className="modal-owner">Owned by {product.owner_team}</p>
            </div>
          </div>
          <button className="modal-close" onClick={onClose}><CloseIcon /></button>
        </div>

        <div className="modal-tabs">
          {['overview', 'schema', 'access'].map(t => (
            <button key={t} className={`tab-btn ${tab === t ? 'active' : ''}`} onClick={() => setTab(t)}>
              {t.charAt(0).toUpperCase() + t.slice(1)}
            </button>
          ))}
        </div>

        <div className="modal-body">
          {tab === 'overview' && (
            <div>
              <p className="modal-desc">{product.description}</p>
              <div className="detail-grid">
                <div className="detail-card">
                  <span className="detail-label">Trust Score</span>
                  <span className="detail-value accent mono">{product.trust_score}%</span>
                </div>
                <div className="detail-card">
                  <span className="detail-label">SLA Availability</span>
                  <span className="detail-value mono">{product.sla_availability}%</span>
                </div>
                <div className="detail-card">
                  <span className="detail-label">Data Freshness</span>
                  <span className="detail-value">{product.freshness_label}</span>
                </div>
                <div className="detail-card">
                  <span className="detail-label">Row Count</span>
                  <span className="detail-value mono">{product.row_count.toLocaleString()}</span>
                </div>
                <div className="detail-card">
                  <span className="detail-label">Snowflake Share</span>
                  <span className={`detail-value ${product.share_available ? 'green' : 'muted'}`}>
                    {product.share_available ? 'Available' : 'Not available'}
                  </span>
                </div>
                <div className="detail-card">
                  <span className="detail-label">Access Tier</span>
                  <span className="detail-value">{product.access_tier}</span>
                </div>
              </div>
              <div style={{ marginTop: 20 }}>
                <p className="detail-label" style={{ marginBottom: 8 }}>Source Systems</p>
                <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap' }}>
                  {product.source_systems.map(s => (
                    <span key={s} className="source-badge">{s}</span>
                  ))}
                </div>
              </div>
            </div>
          )}

          {tab === 'schema' && (
            <div>
              <p className="detail-label" style={{ marginBottom: 12 }}>Output contract — {product.schema.length} columns</p>
              <table className="schema-table">
                <thead>
                  <tr>
                    <th>Column</th>
                    <th>Type</th>
                    <th>Description</th>
                  </tr>
                </thead>
                <tbody>
                  {product.schema.map(col => (
                    <tr key={col.col}>
                      <td className="mono col-name">{col.col}</td>
                      <td className="type-badge-cell"><span className="type-badge">{col.type}</span></td>
                      <td className="col-desc">{col.desc}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
              <p className="schema-note">
                Schema governed by OUTPUT_CONTRACT v1.0 · Row Access Policy: REGIONAL_DATA_POLICY · Masking: MASK_PII_STRING, MASK_EMAIL, MASK_FINANCIAL_NUMBER
              </p>
            </div>
          )}

          {tab === 'access' && (
            <div>
              {submitted ? (
                <div className="success-box">
                  {submitted.success ? (
                    <>
                      <div className="success-icon">✓</div>
                      <h3>Access request submitted</h3>
                      <p className="mono" style={{ marginTop: 8 }}>Ticket ID: {submitted.ticket}</p>
                      <p style={{ marginTop: 8, color: 'var(--text-secondary)' }}>
                        You will receive confirmation within 24 hours.
                      </p>
                    </>
                  ) : (
                    <>
                      <h3>Submission failed</h3>
                      <p style={{ color: 'var(--text-secondary)' }}>Please try again or contact the data platform team.</p>
                    </>
                  )}
                </div>
              ) : (
                <>
                  <p style={{ color: 'var(--text-secondary)', marginBottom: 20 }}>
                    Request access to <strong style={{ color: 'var(--text-primary)' }}>{product.product_name}</strong>. 
                    The {product.owner_team} team will review and provision access within 24 hours.
                  </p>
                  <div className="form-field">
                    <label className="form-label">Full Name</label>
                    <input className="form-input" value={reqName} onChange={e => setReqName(e.target.value)} placeholder="Your name" />
                  </div>
                  <div className="form-field">
                    <label className="form-label">Work Email</label>
                    <input className="form-input" type="email" value={reqEmail} onChange={e => setReqEmail(e.target.value)} placeholder="you@vertiv.com" />
                  </div>
                  <div className="form-field">
                    <label className="form-label">Business Justification</label>
                    <textarea className="form-input form-textarea" value={reqReason} onChange={e => setReqReason(e.target.value)} placeholder="Describe your use case..." rows={3} />
                  </div>
                  <button
                    className="btn-primary"
                    onClick={handleRequest}
                    disabled={!reqName || !reqEmail || !reqReason || submitting}
                  >
                    {submitting ? 'Submitting…' : 'Submit Access Request'}
                  </button>
                </>
              )}
            </div>
          )}
        </div>
      </div>
    </div>
  )
}

// ─── Main App ────────────────────────────────────────────────────────────────
export default function App() {
  const [products, setProducts] = useState([])
  const [filtered, setFiltered] = useState([])
  const [query, setQuery] = useState('')
  const [isSearching, setIsSearching] = useState(false)
  const [aiMode, setAiMode] = useState(false)
  const [selectedProduct, setSelectedProduct] = useState(null)
  const [loading, setLoading] = useState(true)
  const [domainFilter, setDomainFilter] = useState('All')
  const [tierFilter, setTierFilter] = useState('All')
  const debounceRef = useRef(null)

  useEffect(() => {
    fetchProducts().then(data => {
      setProducts(data)
      setFiltered(data)
      setLoading(false)
    }).catch(() => setLoading(false))
  }, [])

  const applyFilters = useCallback((list, domain, tier) => {
    return list.filter(p =>
      (domain === 'All' || p.domain === domain) &&
      (tier === 'All' || p.access_tier === tier)
    )
  }, [])

  const handleSearch = useCallback((q) => {
    setQuery(q)
    clearTimeout(debounceRef.current)
    if (!q.trim()) {
      setAiMode(false)
      setFiltered(applyFilters(products, domainFilter, tierFilter))
      return
    }
    debounceRef.current = setTimeout(async () => {
      setIsSearching(true)
      try {
        const results = await searchProducts(q)
        setAiMode(true)
        setFiltered(applyFilters(results, domainFilter, tierFilter))
      } catch {
        setAiMode(false)
      }
      setIsSearching(false)
    }, 350)
  }, [products, domainFilter, tierFilter, applyFilters])

  useEffect(() => {
    if (!query.trim()) setFiltered(applyFilters(products, domainFilter, tierFilter))
  }, [domainFilter, tierFilter, products, query, applyFilters])

  const domains = ['All', ...new Set(products.map(p => p.domain))]

  return (
    <div className="app">
      {/* Header */}
      <header className="header">
        <div className="header-inner">
          <div className="logo">
            <div className="logo-mark">V</div>
            <div>
              <span className="logo-name">Vertiv</span>
              <span className="logo-sub">Data Marketplace</span>
            </div>
          </div>
          <nav className="nav">
            <a className="nav-link active" href="#">Catalog</a>
            <a className="nav-link" href="#">Lineage</a>
            <a className="nav-link" href="#">Governance</a>
            <a className="nav-link" href="#">DQ Dashboard ↗</a>
          </nav>
          <div className="header-right">
            <span className="live-badge"><span className="live-dot" />Live · Snowflake</span>
          </div>
        </div>
      </header>

      {/* Hero */}
      <section className="hero">
        <div className="hero-inner">
          <div className="hero-eyebrow"><SparkIcon /> Powered by Snowflake Cortex AI</div>
          <h1 className="hero-title">Discover your data products</h1>
          <p className="hero-sub">4 certified data products · 19,500 rows ingested · DAMA 6-dimension quality validated</p>
          <div className="search-wrap">
            <div className="search-box">
              <span className="search-icon">{isSearching ? <span className="spinner" /> : <SearchIcon />}</span>
              <input
                className="search-input"
                placeholder="Search by domain, tag, or describe what you need…"
                value={query}
                onChange={e => handleSearch(e.target.value)}
              />
              {aiMode && <span className="ai-badge"><SparkIcon /> AI</span>}
              {query && <button className="search-clear" onClick={() => handleSearch('')}>×</button>}
            </div>
            <div className="search-hint">Try: "ML features", "customer data", "Salesforce pipeline", "PIT-correct"</div>
          </div>
        </div>
      </section>

      {/* Catalog */}
      <main className="catalog">
        <aside className="sidebar">
          <div className="filter-group">
            <p className="filter-label">Domain</p>
            {domains.map(d => (
              <button key={d} className={`filter-btn ${domainFilter === d ? 'active' : ''}`}
                onClick={() => setDomainFilter(d)}>
                {d}
                <span className="filter-count">
                  {d === 'All' ? products.length : products.filter(p => p.domain === d).length}
                </span>
              </button>
            ))}
          </div>
          <div className="filter-group">
            <p className="filter-label">Access Tier</p>
            {['All', 'Public', 'Restricted'].map(t => (
              <button key={t} className={`filter-btn ${tierFilter === t ? 'active' : ''}`}
                onClick={() => setTierFilter(t)}>
                {t}
                <span className="filter-count">
                  {t === 'All' ? products.length : products.filter(p => p.access_tier === t).length}
                </span>
              </button>
            ))}
          </div>
          <div className="sidebar-stats">
            <p className="filter-label">Platform Stats</p>
            <div className="stat-row"><span>DQ Score</span><span className="stat-val mono accent">91.6%</span></div>
            <div className="stat-row"><span>Avg Trust</span><span className="stat-val mono accent">98.97%</span></div>
            <div className="stat-row"><span>Sources</span><span className="stat-val mono">4 ERP/CRM</span></div>
            <div className="stat-row"><span>RBAC Roles</span><span className="stat-val mono">9</span></div>
          </div>
        </aside>

        <div className="content">
          <div className="content-header">
            <div>
              <span className="result-count">{filtered.length} product{filtered.length !== 1 ? 's' : ''}</span>
              {aiMode && <span className="ai-result-label"> · Cortex AI results</span>}
            </div>
          </div>

          {loading ? (
            <div className="loading">Loading data products…</div>
          ) : filtered.length === 0 ? (
            <div className="empty">
              <p>No products match your search.</p>
              <button className="btn-secondary" onClick={() => handleSearch('')}>Clear search</button>
            </div>
          ) : (
            <div className="grid">
              {filtered.map(p => <ProductCard key={p.product_id} product={p} onClick={setSelectedProduct} />)}
            </div>
          )}
        </div>
      </main>

      {/* Footer */}
      <footer className="footer">
        <span>Vertiv Data Platform · Built on Snowflake · DAMA 6-Dimension DQ · Dehghani Data Mesh</span>
        <span className="footer-right">Medallion Architecture: Bronze → Silver → Gold</span>
      </footer>

      {/* Modal */}
      {selectedProduct && (
        <ProductModal product={selectedProduct} onClose={() => setSelectedProduct(null)} />
      )}
    </div>
  )
}
