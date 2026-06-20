import React, { useState, useEffect, useCallback } from 'react'
import GlobalNav from './GlobalNav.jsx'
import './styles/app.css'
import { beepSound } from './hooks/useAudio.js'
import AppSidebar from './components/AppSidebar.jsx'
import { WatchlistTab, GroupTab } from './components/WatchlistTab.jsx'
import AlertsTab from './components/AlertsTab.jsx'
import SettingsModal from './components/SettingsModal.jsx'

import { useWatchlist } from './hooks/useWatchlist.js'
import { useAlerts } from './hooks/useAlerts.js'
import { useSignalStream } from './hooks/useSignalStream.js'
import { useSignalAlerts, SignalAlertContainer } from './components/SignalAlert.jsx'

const EXCHANGE_NAMES = {
  binance: 'Binance', okx: 'OKX', bybit: 'Bybit',
  bitget: 'Bitget', gate: 'Gate.io', huobi: 'HTX', kucoin: 'KuCoin'
}

const MONITOR_TAB_KEY = 'ikun_monitor_tabs'
const PUSH_SCOPE_KEY = 'ikun_push_scope'
const DEFAULT_MONITOR_TABS = { io: true }
const DEFAULT_PUSH_SCOPE = { groups: {} }

function loadJson(key, fallback) {
  try { return { ...fallback, ...JSON.parse(localStorage.getItem(key) || '{}') } }
  catch { return fallback }
}

function loadMonitorTabs() {
  const saved = loadJson(MONITOR_TAB_KEY, DEFAULT_MONITOR_TABS)
  const next = { io: saved.io !== false }
  localStorage.setItem(MONITOR_TAB_KEY, JSON.stringify(next))
  return next
}

function loadPushScope() {
  const saved = loadJson(PUSH_SCOPE_KEY, DEFAULT_PUSH_SCOPE)
  const next = { groups: saved.groups || {} }
  localStorage.setItem(PUSH_SCOPE_KEY, JSON.stringify(next))
  return next
}

function normalizeSymbol(value) {
  const raw = typeof value === 'string' ? value : value?.symbol
  let symbol = String(raw || '').toUpperCase().trim()
  if (!symbol) return ''
  if (symbol.includes('@')) symbol = symbol.split('@')[0]
  if (symbol.includes(':')) symbol = symbol.split(':')[0]
  if (!symbol.endsWith('/USDT')) symbol = symbol.replace(/USDT.*/, '') + '/USDT'
  return symbol
}

function uniqueSymbols(...lists) {
  const seen = new Set()
  const result = []
  lists.flat().forEach(item => {
    const symbol = normalizeSymbol(item)
    if (!symbol || seen.has(symbol)) return
    seen.add(symbol)
    result.push(symbol)
  })
  return result
}

function signalToScanItem(signal) {
  const symbol = normalizeSymbol(signal)
  if (!symbol) return null
  const type = signal.signal || signal.type || '-'
  return {
    symbol,
    price: signal.price,
    trend: signal.trend || '-',
    signal: type,
    detail: signal.detail || '策略信号触发',
    action: signal.action || '-',
    candle_time: signal.candle_time,
  }
}

function mergeScanItem(list, item) {
  if (!item?.symbol) return list
  const next = Array.isArray(list) ? [...list] : []
  const index = next.findIndex(row => normalizeSymbol(row) === item.symbol)
  if (index >= 0) next[index] = { ...next[index], ...item }
  else next.push(item)
  return next
}

function safeStorageScope(scope) {
  return String(scope || 'anon').replace(/[^a-zA-Z0-9_.-]/g, '_').slice(0, 80) || 'anon'
}

function watchlistStorageKey(exchangeId, scope) {
  return `ikun_wl_${safeStorageScope(scope)}_${exchangeId || 'binance'}`
}

function readWatchlistStorage(exchangeId, scope) {
  try {
    const data = JSON.parse(localStorage.getItem(watchlistStorageKey(exchangeId, scope)) || '[]')
    return Array.isArray(data) ? data : []
  } catch {
    return []
  }
}

function mergeWatchlistItems(...lists) {
  const merged = []
  const seen = new Set()
  lists.flat().forEach(item => {
    if (!item?.symbol) return
    const exchangeId = String(item.exchangeId || item.exchange || 'binance').toLowerCase()
    const key = item.key || `${normalizeSymbol(item.symbol)}@${exchangeId}`
    if (seen.has(key)) return
    seen.add(key)
    merged.push({ ...item, key, exchangeId })
  })
  return merged.slice(0, 20)
}

function syncServerWatchlistToLocal(serverWatchlist, fallbackExchange, scope) {
  const byExchange = {}
  Object.keys(EXCHANGE_NAMES).forEach(exchangeId => { byExchange[exchangeId] = [] })

  ;(Array.isArray(serverWatchlist) ? serverWatchlist : []).forEach(item => {
    const symbol = normalizeSymbol(typeof item === 'string' ? item : item?.symbol)
    if (!symbol) return
    const exchangeId = String(
      typeof item === 'string' ? fallbackExchange : (item?.exchange || item?.exchangeId || fallbackExchange)
    || 'binance').toLowerCase()
    if (!byExchange[exchangeId]) byExchange[exchangeId] = []
    const base = symbol.replace('/USDT', '')
    const label = EXCHANGE_NAMES[exchangeId] || exchangeId.toUpperCase()
    byExchange[exchangeId].push({
      key: `${symbol}@${exchangeId}`,
      symbol,
      exchangeId,
      display: `${base}USDT.P`,
      exchangeLabel: label,
    })
  })

  Object.entries(byExchange).forEach(([exchangeId, serverList]) => {
    const existing = readWatchlistStorage(exchangeId, scope)
    const merged = mergeWatchlistItems(existing, serverList)
    localStorage.setItem(watchlistStorageKey(exchangeId, scope), JSON.stringify(merged))
  })
}

function ShellPage({ title, subtitle, children }) {
  return (
    <div className="main-content">
      <div className="list-container" style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-end', gap: 16, borderBottom: '1px solid var(--border-color)', paddingBottom: 14 }}>
          <div>
            <div style={{ color: 'var(--text-primary)', fontSize: 22, fontWeight: 800, fontFamily: "'Rajdhani', sans-serif" }}>{title}</div>
            {subtitle && <div style={{ color: 'var(--text-secondary)', fontSize: 13, marginTop: 4 }}>{subtitle}</div>}
          </div>
        </div>
        {children}
      </div>
    </div>
  )
}

function CommunityPage() {
  return (
    <div className="main-content">
      <div style={{ flex: 1, minHeight: 0 }}>
        <iframe
          src="https://www.binance.com/zh-CN/square"
          title="Binance Square"
          style={{ width: '100%', height: '100%', border: 'none', background: '#000' }}
          sandbox="allow-scripts allow-same-origin allow-forms allow-popups allow-popups-to-escape-sandbox allow-storage-access-by-user-activation"
        />
      </div>
    </div>
  )
}

// ── 站内持仓数据浏览器 ──────────────────────────────────────
function IndicatorsPage({ apiHeaders }) {
  const [indicators, setIndicators] = React.useState([])
  const [name, setName] = React.useState('')
  const [code, setCode] = React.useState("//@version=5\nindicator('My Signal', overlay=true)")
  const [prompt, setPrompt] = React.useState('')
  const [status, setStatus] = React.useState('')
  const [aiLoading, setAiLoading] = React.useState(false)
  const load = React.useCallback(() => {
    fetch('/api/indicators', { headers: apiHeaders() })
      .then(r => r.json())
      .then(res => setIndicators(Array.isArray(res.indicators) ? res.indicators : []))
      .catch(() => setIndicators([]))
  }, [apiHeaders])

  React.useEffect(() => { load() }, [load])

  function saveIndicator() {
    fetch('/api/indicators/save', {
      method: 'POST',
      headers: apiHeaders({ 'Content-Type': 'application/json' }),
      body: JSON.stringify({ name, code }),
    }).then(r => r.json()).then(res => {
      setStatus(res.status === 'success' ? '指标已保存' : (res.msg || '保存失败'))
      if (res.status === 'success') {
        setIndicators(res.indicators || [])
        setName('')
      }
    }).catch(() => setStatus('网络错误'))
  }

  function deleteIndicator(id) {
    fetch(`/api/indicators/${id}`, { method: 'DELETE', headers: apiHeaders() })
      .then(r => r.json())
      .then(res => {
        setStatus(res.status === 'success' ? '已删除' : (res.msg || '删除失败'))
        if (res.status === 'success') setIndicators(res.indicators || [])
      }).catch(() => setStatus('删除失败'))
  }

  function generateCode() {
    if (!prompt.trim()) { setStatus('请输入生成要求'); return }
    setAiLoading(true)
    fetch('/api/ai/chat', {
      method: 'POST',
      headers: apiHeaders({ 'Content-Type': 'application/json' }),
      body: JSON.stringify({ stream: false, messages: [{ role: 'user', content: prompt }] }),
    }).then(r => r.json()).then(res => {
      if (res.status === 'success') {
        setCode(res.content || '')
        setStatus('AI 已生成代码')
      } else {
        setStatus(res.msg || 'AI 生成失败，请先在设置里配置豆包 API Key')
      }
    }).catch(() => setStatus('AI 请求失败')).finally(() => setAiLoading(false))
  }

  return (
    <ShellPage title="指标开发" subtitle="保存 TradingView PineScript 指标，也可以用 AI 辅助生成。">
      <div style={{ display: 'grid', gridTemplateColumns: 'minmax(0, 1fr) 320px', gap: 16 }}>
        <div style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
          <input className="bn-input" value={name} onChange={e => setName(e.target.value)} placeholder="指标名称" />
          <textarea className="bn-input" value={code} onChange={e => setCode(e.target.value)} style={{ minHeight: 260, fontFamily: 'monospace', resize: 'vertical' }} />
          <div style={{ display: 'flex', gap: 10, justifyContent: 'flex-end' }}>
            <button className="icon-btn login-btn" disabled={!name.trim() || !code.trim()} onClick={saveIndicator}>保存指标</button>
          </div>
          {status && <div style={{ color: 'var(--text-secondary)', fontSize: 12 }}>{status}</div>}
        </div>
        <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
          <textarea className="bn-input" value={prompt} onChange={e => setPrompt(e.target.value)} placeholder="描述你要生成的指标逻辑..." style={{ minHeight: 100, resize: 'vertical' }} />
          <button className="icon-btn" onClick={generateCode} disabled={aiLoading}>{aiLoading ? '生成中...' : 'AI 生成 PineScript'}</button>
          <div style={{ border: '1px solid var(--border-color)', borderRadius: 8, overflow: 'hidden' }}>
            <div style={{ padding: 10, color: 'var(--text-primary)', fontWeight: 700, background: 'var(--input-bg)' }}>已保存指标</div>
            {indicators.length === 0 ? <div style={{ padding: 14, color: 'var(--text-secondary)', fontSize: 13 }}>暂无指标</div> : indicators.map(item => (
              <div key={item.id} style={{ display: 'flex', justifyContent: 'space-between', gap: 8, padding: 10, borderTop: '1px solid var(--border-color)' }}>
                <button className="icon-btn" onClick={() => { setName(item.name); setCode(item.code) }}>{item.name}</button>
                <button className="icon-btn" onClick={() => deleteIndicator(item.id)}>删除</button>
              </div>
            ))}
          </div>
        </div>
      </div>
    </ShellPage>
  )
}

const DEFAULT_IO_TOOLS = [
  { id: 'orion',   name: 'Orion Terminal',  tag: 'OI',     url: 'https://screener.orionterminal.com/',  color: '#4db8ff' },
]

function IoBrowser() {
  const [customTools, setCustomTools] = React.useState(() => JSON.parse(localStorage.getItem('ik_oi_custom') || '[]'))
  const [active, setActive] = React.useState('orion')
  const [blocked, setBlocked] = React.useState({})
  
  const allTools = [...DEFAULT_IO_TOOLS, ...customTools]
  const tool = allTools.find(t => t.id === active) || allTools[0]

  function handleLoad(e) {
    try {
      const doc = e.target.contentDocument
      if (!doc || doc.URL === 'about:blank') return
    } catch {
      setBlocked(b => ({ ...b, [active]: true }))
    }
  }

  function handleAddCustom() {
    const name = window.prompt('请输入工具名称 (例如：CoinGlass)')
    if (!name) return
    const url = window.prompt('请输入网址 URL (例如：https://www.coinglass.com/)')
    if (!url) return
    
    // 简单的 URL 校验补全
    const finalUrl = url.startsWith('http') ? url : 'https://' + url
    
    const newTool = {
      id: 'custom_' + Date.now(),
      name,
      tag: '自定义',
      url: finalUrl,
      color: '#8b5cf6'
    }
    const next = [...customTools, newTool]
    setCustomTools(next)
    localStorage.setItem('ik_oi_custom', JSON.stringify(next))
    setActive(newTool.id)
  }

  function handleDeleteCustom(e, id) {
    e.stopPropagation()
    if (!window.confirm('确定移除这个自定义工具吗？')) return
    const next = customTools.filter(t => t.id !== id)
    setCustomTools(next)
    localStorage.setItem('ik_oi_custom', JSON.stringify(next))
    if (active === id) setActive('orion')
  }

  return (
    <div style={{ display: 'flex', flexDirection: 'column', height: '100%', overflow: 'hidden' }}>
      <div style={{
        display: 'flex', gap: 6, flexShrink: 0, flexWrap: 'wrap',
        padding: '10px 0 12px', borderBottom: '1px solid var(--border-color)',
      }}>
        {allTools.map(t => (
          <button key={t.id} onClick={() => setActive(t.id)}
            style={{
              display: 'flex', alignItems: 'center', gap: 6,
              padding: '5px 14px', borderRadius: 20, border: 'none', cursor: 'pointer',
              fontFamily: "'Rajdhani', sans-serif", fontWeight: 700, fontSize: 13,
              background: active === t.id ? t.color : 'var(--input-bg)',
              color: active === t.id ? '#000' : 'var(--text-secondary)',
              transition: 'all 0.15s', opacity: active === t.id ? 1 : 0.7,
            }}
          >
            {t.name}
            <span style={{
              fontSize: 10, fontWeight: 700, letterSpacing: '0.04em',
              background: active === t.id ? 'rgba(0,0,0,0.18)' : 'var(--border-color)',
              color: active === t.id ? '#000' : 'var(--text-secondary)',
              padding: '1px 6px', borderRadius: 8,
            }}>{t.tag}</span>
            {t.id.startsWith('custom_') && (
              <div 
                onClick={(e) => handleDeleteCustom(e, t.id)}
                style={{
                  display: 'flex', alignItems: 'center', justifyContent: 'center',
                  width: 14, height: 14, borderRadius: '50%', background: 'rgba(0,0,0,0.2)',
                  color: '#fff', fontSize: 10, marginLeft: 2,
                }}
              >✕</div>
            )}
          </button>
        ))}
        
        {/* 新增按钮 */}
        <button onClick={handleAddCustom}
          style={{
            display: 'flex', alignItems: 'center', gap: 6,
            padding: '5px 14px', borderRadius: 20, border: '1px dashed var(--border-color)', cursor: 'pointer',
            fontFamily: "'Rajdhani', sans-serif", fontWeight: 700, fontSize: 13,
            background: 'transparent', color: 'var(--text-secondary)',
            transition: 'all 0.15s',
          }}
          onMouseEnter={e => { e.currentTarget.style.color='var(--accent)'; e.currentTarget.style.borderColor='var(--accent)' }}
          onMouseLeave={e => { e.currentTarget.style.color='var(--text-secondary)'; e.currentTarget.style.borderColor='var(--border-color)' }}
        >
          + 添加工具
        </button>
        <a href={tool.url} target="_blank" rel="noopener noreferrer"
          style={{
            marginLeft: 'auto', display: 'flex', alignItems: 'center', gap: 5,
            fontSize: 12, color: 'var(--text-secondary)', textDecoration: 'none',
            padding: '5px 12px', borderRadius: 20, border: '1px solid var(--border-color)',
            transition: 'color 0.15s, border-color 0.15s',
          }}
          onMouseEnter={e => { e.currentTarget.style.color='var(--accent)'; e.currentTarget.style.borderColor='var(--accent)' }}
          onMouseLeave={e => { e.currentTarget.style.color='var(--text-secondary)'; e.currentTarget.style.borderColor='var(--border-color)' }}
        >
          <svg width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5">
            <path d="M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6"/>
            <polyline points="15 3 21 3 21 9"/><line x1="10" y1="14" x2="21" y2="3"/>
          </svg>
          独立窗口
        </a>
      </div>

      <div style={{ flex: 1, position: 'relative', minHeight: 0 }}>
        {blocked[active] ? (
          <div style={{
            height: '100%', display: 'flex', flexDirection: 'column',
            alignItems: 'center', justifyContent: 'center', gap: 16,
            color: 'var(--text-secondary)',
          }}>
            <svg width="48" height="48" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5" opacity="0.4">
              <circle cx="12" cy="12" r="10"/><line x1="4.93" y1="4.93" x2="19.07" y2="19.07"/>
            </svg>
            <div style={{ textAlign: 'center' }}>
              <div style={{ fontSize: 15, fontWeight: 700, color: 'var(--text-primary)', marginBottom: 6 }}>
                {tool.name} 不允许内嵌
              </div>
              <div style={{ fontSize: 12, marginBottom: 18 }}>该网站设置了安全策略，阻止了 iframe 加载</div>
              <a href={tool.url} target="_blank" rel="noopener noreferrer"
                style={{
                  display: 'inline-flex', alignItems: 'center', gap: 6,
                  background: tool.color, color: '#000',
                  padding: '9px 22px', borderRadius: 20,
                  fontFamily: "'Rajdhani', sans-serif", fontWeight: 700, fontSize: 14,
                  textDecoration: 'none',
                }}
              >
                在新窗口打开 {tool.name} →
              </a>
            </div>
          </div>
        ) : (
          <iframe
            key={active}
            src={tool.url}
            title={tool.name}
            onLoad={handleLoad}
            style={{ width: '100%', height: '100%', border: 'none', borderRadius: 8, background: '#000' }}
            sandbox="allow-scripts allow-same-origin allow-forms allow-popups allow-popups-to-escape-sandbox allow-storage-access-by-user-activation"
          />
        )}
      </div>
    </div>
  )
}
// ────────────────────────────────────────────────────────────

export default function App({ currentUser, onOpenLogin, doLogout: propDoLogout, onNavigate, activePage, activeIndicator, onClearIndicator, externalExchange }) {
  // ── UI 状态 ──
  const [activeTab, setActiveTab] = useState('watchlist')
  const [sidebarOpen, setSidebarOpen] = useState(false)
  const [modalOpen, setModalOpen] = useState(false)
  const [modalTab, setModalTab] = useState('api')
  const [toast, setToast] = useState('')
  const [cookieOk, setCookieOk] = useState(!!localStorage.getItem('ikun_cookie_ok'))
  const [localPage, setLocalPage] = useState(activePage || 'monitor')
  const [localUser, setLocalUser] = useState(null)
  const [showAuthModal, setShowAuthModal] = useState(false)
  const [authMode, setAuthMode] = useState('login')
  const [authEmail, setAuthEmail] = useState('')
  const [authPassword, setAuthPassword] = useState('')
  const [authNickname, setAuthNickname] = useState('')
  const [authStatus, setAuthStatus] = useState('')

  // ── 自选列表分组管理 ──
  const [listGroups, setListGroups] = useState(() => {
    try { return JSON.parse(localStorage.getItem('ikun_listgroups') || '[]') }
    catch { return [] }
  })
  const [showListModal, setShowListModal] = useState(false)
  const [showStratModal, setShowStratModal] = useState(false)
  const [listModalTab, setListModalTab] = useState('group')
  const [newGroupName, setNewGroupName] = useState('')
  const [newUrl, setNewUrl] = useState('')
  const [newUrlLabel, setNewUrlLabel] = useState('')
  const [monitorTabs, setMonitorTabs] = useState(loadMonitorTabs)
  const [pushScope, setPushScope] = useState(loadPushScope)

  // ── 设置配置 ──
  const [authToken, setAuthToken] = useState(localStorage.getItem('ikun_token') || '')
  const [cfg, setCfg] = useState({ apiKey: '', secretKey: '', email: '', emailPass: '', proxy: '', doubaoApiKey: '' })
  const [alertSettings, setAlertSettings] = useState({ app_push: false, toast: true, email: false, sound: false, sound_type: 'beep' })
  const [currentExchange, setCurrentExchange] = useState(
    () => externalExchange || localStorage.getItem('ikun_exchange') || 'binance'
  )
  // 外部交易所切换后同步到监控台
  useEffect(() => {
    if (externalExchange && externalExchange !== currentExchange) {
      setCurrentExchange(externalExchange)
    }
  }, [externalExchange]) // eslint-disable-line
  const [showEmailBindTip, setShowEmailBindTip] = useState(false)
  const [emailTemplate, setEmailTemplate] = useState({ include_price: true, include_trend: true, include_signal: true, include_detail: true, include_action: true })
  const [timeframe, setTimeframe] = useState('15m')
  const [triggerMode, setTriggerMode] = useState('close')
  const [watchlistMode, setWatchlistMode] = useState('favorites')
  const [testEmailResult, setTestEmailResult] = useState('')
  const [testEmailLoading, setTestEmailLoading] = useState(false)

  // ── 自定义音效 ──
  const [customSounds, setCustomSounds] = useState([])
  const [soundName, setSoundName] = useState('')
  const soundFileRef = React.useRef(null)

  // ── Toast 工具 ──
  const showToast = useCallback((msg) => {
    setToast(msg)
    setTimeout(() => setToast(''), 2500)
  }, [])

  useEffect(() => {
    const closeDesktopSidebar = () => {
      if (window.innerWidth > 768) setSidebarOpen(false)
    }
    closeDesktopSidebar()
    window.addEventListener('resize', closeDesktopSidebar)
    return () => window.removeEventListener('resize', closeDesktopSidebar)
  }, [])

  const persistMonitorTabs = useCallback((next) => {
    setMonitorTabs(next)
    localStorage.setItem(MONITOR_TAB_KEY, JSON.stringify(next))
  }, [])

  const closeMonitorTab = useCallback((tab) => {
    const next = { ...monitorTabs, [tab]: false }
    persistMonitorTabs(next)
    if (activeTab === tab) setActiveTab('watchlist')
  }, [activeTab, monitorTabs, persistMonitorTabs])

  const openMonitorTab = useCallback((tab) => {
    if (tab !== 'io') return
    const next = { ...monitorTabs, [tab]: true }
    persistMonitorTabs(next)
    setActiveTab(tab)
  }, [monitorTabs, persistMonitorTabs])

  const persistPushScope = useCallback((next) => {
    setPushScope(next)
    localStorage.setItem(PUSH_SCOPE_KEY, JSON.stringify(next))
  }, [])

  const setPushEnabled = useCallback((scope, id, enabled) => {
    const groups = { ...(pushScope.groups || {}), [id]: enabled }
    persistPushScope({ ...pushScope, groups })
  }, [pushScope, persistPushScope])

  const apiHeaders = useCallback((extra = {}) => {
    const token = authToken || localStorage.getItem('ikun_token') || ''
    return token ? { ...extra, 'X-Token': token } : extra
  }, [authToken])

  const requestedPage = activePage || localPage
  const effectivePage = requestedPage === 'market' ? 'monitor' : requestedPage
  const effectiveUser = currentUser || localUser
  const storageScope = effectiveUser?.email || 'anon'
  const handleNavigate = useCallback((page) => {
    if (onNavigate) onNavigate(page)
    else setLocalPage(page)
  }, [onNavigate])

  useEffect(() => {
    const token = localStorage.getItem('ikun_token')
    if (!token || currentUser !== undefined) return
    fetch('/api/auth/check', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'X-Token': token },
      body: JSON.stringify({ token }),
    }).then(r => r.json()).then(res => {
      if (res.status === 'success' || res.status === 'ok' || res.ok) setLocalUser(res.user)
      else localStorage.removeItem('ikun_token')
    }).catch(() => {})
  }, [currentUser])

  function submitAuth() {
    if (!authEmail.trim() || !authPassword) { setAuthStatus('请输入邮箱和密码'); return }
    const path = authMode === 'register' ? '/api/auth/register' : '/api/auth/login'
    setAuthStatus(authMode === 'register' ? '注册中...' : '登录中...')
    fetch(path, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ email: authEmail.trim(), password: authPassword, nickname: authNickname.trim() }),
    }).then(r => r.json()).then(res => {
      if (res.status !== 'success') { setAuthStatus(res.msg || '认证失败'); return }
      localStorage.setItem('ikun_token', res.token)
      setAuthToken(res.token)
      setLocalUser(res.user)
      setShowAuthModal(false)
      setAuthStatus('')
      setAuthPassword('')
      showToast(authMode === 'register' ? '注册成功' : '登录成功')
    }).catch(() => setAuthStatus('网络错误'))
  }

  function localLogout() {
    const token = authToken || localStorage.getItem('ikun_token') || ''
    fetch('/api/auth/logout', {
      method: 'POST',
      headers: apiHeaders({ 'Content-Type': 'application/json' }),
      body: JSON.stringify({ token }),
    }).catch(() => {}).finally(() => {
      localStorage.removeItem('ikun_token')
      setAuthToken('')
      setLocalUser(null)
      showToast('已退出')
    })
  }

  // ── 数据 Hook ──
  const watchlistHook = useWatchlist(timeframe, triggerMode, showToast, currentExchange, storageScope)
  const {
    watchlist, setWatchlist,
    marketMovers, moversScanData,
    watchlistMode: wlMode, setWatchlistMode: setWlMode,
    scanData, setScanData,
    connStatus,
    searchQ, setSearchQ,
    showDropdown, setShowDropdown,
    searchWrapRef,
    searchStatus,
    groupSearchQ, setGroupSearchQ,
    groupShowDropdown, setGroupShowDropdown,
    groupSearchWrapRef,
    groupSearchStatus,
    filteredSymbols, filteredGroupSymbols,
    fetchData, addSymbol, removeSymbol,
  } = watchlistHook

  // 这里我们将 useWatchlist 内部的 watchlistMode 同步给 App 的 watchlistMode state
  // 实际上更好的做法是让 App 控制这个 state，或者让 hook 返回。
  // 之前的代码里 App 已经有一个 watchlistMode 了，我们统一一下。
  useEffect(() => {
    setWlMode(watchlistMode)
  }, [watchlistMode, setWlMode])

  const {
    alertLog, setAlertLog,
    unreadCount, setUnreadCount,
    notifyItems, setNotifyItems,
    addToAlertLogItems, markAllRead,
  } = useAlerts(timeframe, alertSettings, showToast, storageScope)

  const exchangeLabel = EXCHANGE_NAMES[currentExchange] || currentExchange

  // ── 实时信号通知系统 ─────────────────────────────
  const { alerts: signalAlerts, addAlert, dismiss: dismissAlert } = useSignalAlerts({
    soundEnabled: alertSettings.sound !== false,
  })

  // watchlist symbols are always pushed; optional monitor lists join only when checked.
  const watchlistSymbols = watchlist.map(it => (typeof it === 'string' ? it : it.symbol))
  const pushSymbols = uniqueSymbols(
    watchlistSymbols,
    listGroups
      .filter(g => !g.url && pushScope.groups?.[g.id])
      .flatMap(g => g.symbols || []),
  )

  const { streamStatus } = useSignalStream({
    exchange:  currentExchange,
    symbols:   pushSymbols,
    timeframe,
    enabled:   pushSymbols.length > 0,
    onSignal:  (signal) => {
      addAlert(signal)
      addToAlertLogItems(signal)
      const scanItem = signalToScanItem(signal)
      if (!scanItem) return
      if (watchlistSymbols.some(symbol => normalizeSymbol(symbol) === scanItem.symbol)) {
        setScanData(prev => mergeScanItem(prev, scanItem))
      }
    },
  })

  // ── 初始化加载设置，登录/切换账号后重新加载（同步watchlist） ──
  useEffect(() => {
    if (currentUser !== undefined || localUser) {
      loadSettings()
      loadCustomSounds()
    }
  }, [currentUser, localUser, authToken])

  function loadSettings() {
    fetch('/api/get_settings', { headers: apiHeaders() }).then(r => r.json()).then(d => {
      setCfg({
        apiKey: d.apiKey || '',
        secretKey: d.secretKey || '',
        email: d.email || '',
        emailPass: d.emailPass || '',
        proxy: d.proxy || '',
        doubaoApiKey: d.doubaoApiKey || '',
      })
      if (d.alertSettings) setAlertSettings(v => ({ ...v, ...d.alertSettings }))
      if (d.emailTemplate) setEmailTemplate(v => ({ ...v, ...d.emailTemplate }))
      if (d.timeframe) setTimeframe(d.timeframe)
      if (d.triggerMode) setTriggerMode(d.triggerMode)
      if (d.watchlistMode) setWatchlistMode('favorites')
      if (d.exchangeId) {
        setCurrentExchange(d.exchangeId)
        localStorage.setItem('ikun_exchange', d.exchangeId)
      }
      if (Array.isArray(d.watchlist)) {
        syncServerWatchlistToLocal(d.watchlist, d.exchangeId || 'binance', storageScope)
        // 刷新当前交易所列表（触发 useWatchlist 重读 localStorage）
        window.__ikun_wl_synced = true
        window.dispatchEvent(new Event('ikun_wl_sync'))
      }
    })
  }

  function loadCustomSounds() {
    fetch('/api/list_sounds', { headers: apiHeaders() }).then(r => r.json()).then(setCustomSounds)
  }

  function syncAlertSettings(next, tf, tm, wm) {
    // tf/tm/wm 可以传入盖辆当前 state（用于 onChange 时 state 还未更新的情况）
    const _tf = tf || timeframe
    const _tm = tm || triggerMode
    const _wm = 'favorites'
    // 如果 timeframe 发生变化，同时更新 state
    if (tf && tf !== timeframe) setTimeframe(tf)
    if (wm && watchlistMode !== 'favorites') setWatchlistMode('favorites')
    const settings = { ...next }
    delete settings._timeframe  // 去掉内部传递字段
    setAlertSettings(settings)
    fetch('/api/save_settings', {
      method: 'POST',
      headers: apiHeaders({ 'Content-Type': 'application/json' }),
      body: JSON.stringify({ alertSettings: settings, timeframe: _tf, triggerMode: _tm, watchlistMode: _wm })
    })
  }

  function currentWatchlistPayload() {
    return (watchlist || []).map(item => {
      if (typeof item === 'string') return { symbol: item, exchange: currentExchange }
      return {
        symbol: item.symbol,
        exchange: item.exchange || item.exchangeId || currentExchange,
      }
    }).filter(item => item.symbol)
  }

  function saveSettings() {
    fetch('/api/save_settings', {
      method: 'POST',
      headers: apiHeaders({ 'Content-Type': 'application/json' }),
      body: JSON.stringify({
        apiKey: cfg.apiKey, secretKey: cfg.secretKey,
        email: cfg.email, emailPass: cfg.emailPass, proxy: cfg.proxy,
        doubaoApiKey: cfg.doubaoApiKey,
        timeframe, triggerMode, watchlistMode: 'favorites', emailTemplate, alertSettings,
        exchangeId: currentExchange,
        watchlist: currentWatchlistPayload(),
      })
    }).then(r => r.json()).then(() => {
      showToast('配置已保存')
      setModalOpen(false)
      fetchData()
    })
  }

  function sendTestEmail() {
    setTestEmailLoading(true)
    setTestEmailResult('')
    fetch('/api/save_settings', {
      method: 'POST',
      headers: apiHeaders({ 'Content-Type': 'application/json' }),
      body: JSON.stringify({ email: cfg.email, emailPass: cfg.emailPass })
    }).then(() => {
      return fetch('/api/test_email', { method: 'POST', headers: apiHeaders({ 'Content-Type': 'application/json' }), body: '{}' })
    }).then(r => r.json()).then(d => {
      setTestEmailResult((d.status === 'success' ? '发送成功: ' : '发送失败: ') + d.msg)
      setTestEmailLoading(false)
    }).catch(e => {
      setTestEmailResult('请求失败: ' + (e?.message || '网络错误，请检查后端是否在线'))
      setTestEmailLoading(false)
    })
  }

  function uploadSound() {
    const file = soundFileRef.current?.files[0]
    if (!file) { showToast('请选择MP3文件'); return }
    const fd = new FormData()
    fd.append('file', file)
    fd.append('name', soundName || file.name.replace('.mp3', ''))
    fetch('/api/upload_sound', { method: 'POST', headers: apiHeaders(), body: fd }).then(r => r.json()).then(d => {
      if (d.status === 'error') { showToast(d.msg); return }
      setCustomSounds(d.sounds || [])
      setSoundName('')
      if (soundFileRef.current) soundFileRef.current.value = ''
      showToast('音效已上传')
    })
  }

  function deleteSound(file) {
    fetch('/api/delete_sound', {
      method: 'POST',
      headers: apiHeaders({ 'Content-Type': 'application/json' }),
      body: JSON.stringify({ file })
    }).then(r => r.json()).then(d => {
      setCustomSounds(d.sounds || [])
      showToast('已删除')
    })
  }

  function addSymbolToGroup(groupId, raw) {
    let s = raw.toUpperCase().trim()
    if (s.includes(':')) s = s.split(':')[0]
    if (!s.endsWith('/USDT')) s = s.replace('USDT', '') + '/USDT'
    const next = listGroups.map(g => {
      if (g.id !== groupId) return g
      if ((g.symbols || []).includes(s)) { showToast('已在列表中'); return g }
      return { ...g, symbols: [...(g.symbols || []), s] }
    })
    setListGroups(next)
    localStorage.setItem('ikun_listgroups', JSON.stringify(next))
    setGroupSearchQ('')
    setGroupShowDropdown(false)
    showToast('已添加 ' + s)
  }

  function removeSymbolFromGroup(groupId, sym) {
    const next = listGroups.map(g => {
      if (g.id !== groupId) return g
      return { ...g, symbols: (g.symbols || []).filter(s => s !== sym) }
    })
    setListGroups(next)
    localStorage.setItem('ikun_listgroups', JSON.stringify(next))
    showToast('已移除 ' + sym)
  }


  return (
    <>
      {/* 实时信号通知卡（全局浮层） */}
      <SignalAlertContainer signals={signalAlerts} onDismiss={dismissAlert} />

      <GlobalNav
        activePage={effectivePage}
        onNavigate={handleNavigate}
        currentUser={effectiveUser}
        onOpenLogin={onOpenLogin || (() => setShowAuthModal(true))}
        onLogout={propDoLogout || localLogout}
      />

      {effectivePage !== 'monitor' && (
        <div className="app-container">
          {effectivePage === 'community' && <CommunityPage />}
          {effectivePage === 'indicators' && <IndicatorsPage apiHeaders={apiHeaders} />}
        </div>
      )}

      {effectivePage === 'monitor' && <div className={`sidebar-overlay ${sidebarOpen ? 'show' : ''}`} onClick={() => setSidebarOpen(false)} />}

      {effectivePage === 'monitor' && <div className="app-container">
        <AppSidebar
          sidebarOpen={sidebarOpen}
          timeframe={timeframe} setTimeframe={setTimeframe}
          triggerMode={triggerMode} setTriggerMode={setTriggerMode}
          watchlistMode={watchlistMode}
          setWatchlistMode={(mode) => {
            setWatchlistMode('favorites')
            setActiveTab('watchlist')
          }}
          alertSettings={alertSettings} syncAlertSettings={syncAlertSettings}
          setShowEmailBindTip={setShowEmailBindTip}
          connStatus={connStatus}
          customSounds={customSounds}
          setShowStratModal={setShowStratModal}
        />

        <div className="main-content">
          {/* 当前应用指标横幅 */}
          {activeIndicator && (
            <div className="active-ind-banner">
              <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="#4db8ff" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" style={{ flexShrink: 0 }}>
                <path d="M22 12h-4l-3 9L9 3l-3 9H2"/>
              </svg>
              <span className="aib-label">当前指标</span>
              <span className="aib-name">{activeIndicator.name}</span>
              {activeIndicator.author && (
                <span className="aib-author">by {activeIndicator.author}</span>
              )}
              <button className="aib-close" onClick={onClearIndicator} title="移除指标">
                <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round">
                  <line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/>
                </svg>
              </button>
            </div>
          )}

          {/* Tab 标题栏 */}
          <div className="tabs-header">
            <button
              className={`tab-btn ${activeTab === 'watchlist' ? 'active' : ''}`}
              onClick={() => { 
                setActiveTab('watchlist'); 
                setWatchlistMode('favorites'); 
              }}
            >

              <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"
                strokeLinecap="round" strokeLinejoin="round" style={{ marginRight: 6, verticalAlign: -3 }}>
                <polygon points="12 2 15.09 8.26 22 9.27 17 14.14 18.18 21.02 12 17.77 5.82 21.02 7 14.14 2 9.27 8.91 8.26 12 2"/>
              </svg>
              自选列表 ({watchlist.length}/20)
            </button>

            {listGroups.map(g => (
              <button key={g.id}
                className={`tab-btn ${activeTab === 'group_' + g.id ? 'active' : ''}`}
                onClick={() => g.url ? window.open(g.url, '_blank') : setActiveTab('group_' + g.id)}
                title={g.url ? `打开 ${g.url}` : g.name}
              >{g.url ? '🌐' : '📋'} {g.name}</button>
            ))}

            <button
              className={`tab-btn ${activeTab === 'alerts' ? 'active' : ''}`}
              onClick={() => setActiveTab('alerts')}
            >
              <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"
                strokeLinecap="round" strokeLinejoin="round" style={{ marginRight: 6, verticalAlign: -3 }}>
                <path d="M18 8A6 6 0 0 0 6 8c0 7-3 9-3 9h18s-3-2-3-9"/>
                <path d="M13.73 21a2 2 0 0 1-3.46 0"/>
              </svg>
              快讯日志 {unreadCount > 0 ? <span className="alert-count-badge">{unreadCount}</span> : null}
            </button>

            {monitorTabs.io && (
              <button
                className={`tab-btn ${activeTab === 'io' ? 'active' : ''}`}
                onClick={() => setActiveTab('io')}
              >
                <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"
                  strokeLinecap="round" strokeLinejoin="round" style={{ marginRight: 6, verticalAlign: -3 }}>
                  <line x1="18" y1="20" x2="18" y2="10"/>
                  <line x1="12" y1="20" x2="12" y2="4"/>
                  <line x1="6" y1="20" x2="6" y2="14"/>
                </svg>
                OI 持仓异动
                <span onClick={e => { e.stopPropagation(); closeMonitorTab('io') }} title="关闭标签页" style={{ marginLeft: 8, color: 'var(--text-secondary)', fontSize: 14, padding: '0 3px', borderRadius: 4 }}>x</span>
              </button>
            )}

            {listGroups.length < 8 ? (
              <button
                title="添加列表或监控网站"
                onClick={() => setShowListModal(true)}
                style={{
                  height: 28, padding: '0 10px', borderRadius: 14,
                  border: '1.5px solid var(--accent)',
                  background: 'transparent', color: 'var(--accent)',
                  fontSize: 18, cursor: 'pointer', display: 'flex',
                  alignItems: 'center', justifyContent: 'center',
                  flexShrink: 0, transition: 'all 0.18s', fontWeight: 700,
                  alignSelf: 'center', marginLeft: 2
                }}
                onMouseEnter={e => { e.currentTarget.style.background = 'var(--accent)'; e.currentTarget.style.color = '#000' }}
                onMouseLeave={e => { e.currentTarget.style.background = 'transparent'; e.currentTarget.style.color = 'var(--accent)' }}
              >+</button>
            ) : (
              <span style={{ fontSize: 11, color: 'var(--text-secondary)', alignSelf: 'center', padding: '0 8px', opacity: 0.5 }}>列表已满</span>
            )}
          </div>

          {activeTab === 'watchlist' && (
              <WatchlistTab
                watchlist={watchlist}
                scanData={scanData}
                watchlistMode="favorites"
                exchangeLabel={currentExchange.toUpperCase()}
                searchQ={searchQ} setSearchQ={setSearchQ}
                showDropdown={showDropdown} setShowDropdown={setShowDropdown}
                searchWrapRef={searchWrapRef}
                filteredSymbols={filteredSymbols}
                searchStatus={searchStatus}
                addSymbol={addSymbol}
                removeSymbol={removeSymbol}
                showPushToggle={false}
              />
          )}

          {listGroups.filter(g => !g.url).map(g => activeTab === 'group_' + g.id && (
            <GroupTab
              key={g.id} group={g} scanData={scanData} exchangeLabel={exchangeLabel}
              groupSearchQ={groupSearchQ} setGroupSearchQ={setGroupSearchQ}
              groupShowDropdown={groupShowDropdown} setGroupShowDropdown={setGroupShowDropdown}
              groupSearchWrapRef={groupSearchWrapRef}
              filteredGroupSymbols={filteredGroupSymbols}
              groupSearchStatus={groupSearchStatus}
              addSymbolToGroup={addSymbolToGroup} removeSymbolFromGroup={removeSymbolFromGroup}
              showPushToggle
              pushEnabled={!!pushScope.groups?.[g.id]}
              onTogglePush={(enabled) => setPushEnabled('group', g.id, enabled)}
            />
          ))}

          {activeTab === 'alerts' && (
            <AlertsTab
              alertLog={alertLog} setAlertLog={setAlertLog}
              unreadCount={unreadCount} setUnreadCount={setUnreadCount}
              markAllRead={markAllRead}
            />
          )}

          {activeTab === 'io' && (
            <IoBrowser />
          )}
        </div>
      </div>}

      {showAuthModal && (
        <div className="modal" onClick={e => e.target === e.currentTarget && setShowAuthModal(false)}>
          <div style={{ background: 'var(--sidebar-bg)', border: '1px solid var(--border-color)', borderRadius: 12, width: 420, maxWidth: '92vw', padding: 24 }}>
            <div style={{ display: 'flex', gap: 0, marginBottom: 18, borderBottom: '1px solid var(--border-color)' }}>
              {[['login', '登录'], ['register', '注册']].map(([key, label]) => (
                <button key={key} className={`login-tab-btn ${authMode === key ? 'active' : ''}`} onClick={() => { setAuthMode(key); setAuthStatus('') }}>{label}</button>
              ))}
            </div>
            <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
              {authMode === 'register' && (
                <input className="bn-input" value={authNickname} onChange={e => setAuthNickname(e.target.value)} placeholder="昵称，可选" />
              )}
              <input className="bn-input" value={authEmail} onChange={e => setAuthEmail(e.target.value)} placeholder="邮箱" />
              <input className="bn-input" type="password" value={authPassword} onChange={e => setAuthPassword(e.target.value)} placeholder="密码" onKeyDown={e => { if (e.key === 'Enter') submitAuth() }} />
              {authStatus && <div style={{ color: 'var(--text-secondary)', fontSize: 12 }}>{authStatus}</div>}
              <div style={{ display: 'flex', justifyContent: 'flex-end', gap: 10, marginTop: 8 }}>
                <button className="icon-btn" onClick={() => setShowAuthModal(false)}>取消</button>
                <button className="icon-btn login-btn" onClick={submitAuth}>{authMode === 'register' ? '注册' : '登录'}</button>
              </div>
            </div>
          </div>
        </div>
      )}

      {showListModal && (
        <div className="modal" onClick={e => e.target === e.currentTarget && setShowListModal(false)}>
          <div className="modal-container" style={{ width: 480, height: 'auto', flexDirection: 'column', padding: 0 }}>
            <div style={{ padding: '20px 24px 0', display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
              <div style={{ fontSize: 17, fontWeight: 700, color: 'var(--text-primary)' }}>新增</div>
              <button onClick={() => setShowListModal(false)} style={{ background: 'none', border: 'none', color: 'var(--text-secondary)', fontSize: 20, cursor: 'pointer', lineHeight: 1 }}>×</button>
            </div>
            <div style={{ display: 'flex', gap: 0, padding: '12px 24px 0', borderBottom: '1px solid var(--border-color)' }}>
              {[['group', '📋  新建列表'], ['url', '🌐  添加网站']].map(([t, l]) => (
                <button key={t} onClick={() => setListModalTab(t)} style={{
                  padding: '8px 18px', background: 'none', border: 'none', cursor: 'pointer', fontSize: 14, fontWeight: 600,
                  color: listModalTab === t ? 'var(--accent)' : 'var(--text-secondary)',
                  borderBottom: listModalTab === t ? '2px solid var(--accent)' : '2px solid transparent',
                  transition: 'all 0.15s', marginBottom: -1
                }}>{l}</button>
              ))}
            </div>
            <div style={{ padding: '24px 24px 28px' }}>
              {!monitorTabs.io && (
                <div style={{ marginBottom: 18, padding: '12px 14px', background: 'var(--hover-bg)', border: '1px solid var(--border-color)', borderRadius: 8 }}>
                  <div style={{ color: 'var(--text-secondary)', fontSize: 12, marginBottom: 10 }}>可恢复的监控标签</div>
                  <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap' }}>
                    {!monitorTabs.io && <button className="icon-btn" onClick={() => { openMonitorTab('io'); setShowListModal(false) }}>OI 持仓异动</button>}
                  </div>
                </div>
              )}
              {listModalTab === 'group' && (
                <div>
                  <p style={{ color: 'var(--text-secondary)', fontSize: 13, marginBottom: 18, lineHeight: 1.6 }}>
                    创建一个新的自命名自选列表，可以在不同列表之间快速切换，分组管理你的监控标的。
                  </p>
                  <div style={{ marginBottom: 14 }}>
                    <label style={{ fontSize: 13, color: 'var(--text-secondary)', display: 'block', marginBottom: 6 }}>列表名称</label>
                    <input className="bn-input" value={newGroupName} onChange={e => setNewGroupName(e.target.value)}
                      placeholder="例如：BTC 主力仓、山寨季观察..." autoFocus
                      onKeyDown={e => {
                        if (e.key === 'Enter' && newGroupName.trim()) {
                          const g = { id: Date.now(), name: newGroupName.trim(), symbols: [] }
                          const next = [...listGroups, g]
                          setListGroups(next); localStorage.setItem('ikun_listgroups', JSON.stringify(next))
                          showToast(`列表「${g.name}」已创建`); setNewGroupName(''); setShowListModal(false)
                        }
                      }}
                    />
                  </div>
                  <div style={{ display: 'flex', justifyContent: 'flex-end', gap: 10 }}>
                    <button className="icon-btn" onClick={() => setShowListModal(false)}>取消</button>
                    <button className="icon-btn login-btn" disabled={!newGroupName.trim()} onClick={() => {
                      const g = { id: Date.now(), name: newGroupName.trim(), symbols: [] }
                      const next = [...listGroups, g]
                      setListGroups(next); localStorage.setItem('ikun_listgroups', JSON.stringify(next))
                      showToast(`列表「${g.name}」已创建`); setNewGroupName(''); setShowListModal(false)
                    }}>创建</button>
                  </div>
                  {listGroups.filter(g => !g.url).length > 0 && (
                    <div style={{ marginTop: 20 }}>
                      <div style={{ fontSize: 12, color: 'var(--text-secondary)', marginBottom: 10, letterSpacing: '0.04em', textTransform: 'uppercase' }}>已有自选列表</div>
                      {listGroups.filter(g => !g.url).map(g => (
                        <div key={g.id} style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', padding: '10px 14px', background: 'var(--hover-bg)', border: '1px solid var(--border-color)', borderRadius: 8, marginBottom: 8, transition: 'border-color 0.15s' }}
                          onMouseEnter={e => e.currentTarget.style.borderColor = 'var(--accent)'}
                          onMouseLeave={e => e.currentTarget.style.borderColor = 'var(--border-color)'}
                        >
                          <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
                            <div style={{ width: 34, height: 34, borderRadius: 8, flexShrink: 0, background: 'rgba(77,184,255,0.1)', border: '1px solid rgba(77,184,255,0.18)', display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: 16 }}>📋</div>
                            <div>
                              <div style={{ fontSize: 14, fontWeight: 700, color: 'var(--text-primary)', fontFamily: "'Rajdhani',sans-serif" }}>{g.name}</div>
                              <div style={{ fontSize: 11, color: 'var(--text-secondary)', marginTop: 1 }}>{(g.symbols || []).length} 个标的</div>
                            </div>
                          </div>
                          <button onClick={() => { const next = listGroups.filter(x => x.id !== g.id); setListGroups(next); localStorage.setItem('ikun_listgroups', JSON.stringify(next)) }}
                            style={{ background: 'none', border: 'none', color: 'var(--text-secondary)', cursor: 'pointer', fontSize: 18, lineHeight: 1, padding: '4px 6px', borderRadius: 4, transition: 'color 0.15s' }}
                            onMouseEnter={e => e.currentTarget.style.color = '#f6465d'}
                            onMouseLeave={e => e.currentTarget.style.color = 'var(--text-secondary)'}
                            title="删除列表"
                          >×</button>
                        </div>
                      ))}
                    </div>
                  )}
                </div>
              )}
              {listModalTab === 'url' && (
                <div>
                  <p style={{ color: 'var(--text-secondary)', fontSize: 13, marginBottom: 18, lineHeight: 1.6 }}>
                    添加外部监控网站（如 TradingView、CoinGlass、Coingecko 等），可在监控台侧边快速跳转。
                  </p>
                  <div style={{ marginBottom: 14 }}>
                    <label style={{ fontSize: 13, color: 'var(--text-secondary)', display: 'block', marginBottom: 6 }}>网站 URL</label>
                    <input className="bn-input" value={newUrl} onChange={e => setNewUrl(e.target.value)} placeholder="https://www.tradingview.com/..." />
                  </div>
                  <div style={{ marginBottom: 20 }}>
                    <label style={{ fontSize: 13, color: 'var(--text-secondary)', display: 'block', marginBottom: 6 }}>显示名称（可选）</label>
                    <input className="bn-input" value={newUrlLabel} onChange={e => setNewUrlLabel(e.target.value)} placeholder="TradingView 行情" />
                  </div>
                  <div style={{ display: 'flex', justifyContent: 'flex-end', gap: 10 }}>
                    <button className="icon-btn" onClick={() => setShowListModal(false)}>取消</button>
                    <button className="icon-btn login-btn" disabled={!newUrl.trim()} onClick={() => {
                      const label = newUrlLabel.trim() || newUrl.replace(/^https?:\/\//, '').split('/')[0]
                      const g = { id: Date.now(), name: label, url: newUrl.trim(), symbols: [] }
                      const next = [...listGroups, g]
                      setListGroups(next); localStorage.setItem('ikun_listgroups', JSON.stringify(next))
                      showToast(`已添加：${label}`); setNewUrl(''); setNewUrlLabel(''); setShowListModal(false)
                    }}>添加</button>
                  </div>
                  {listGroups.filter(g => g.url).length > 0 && (
                    <div style={{ marginTop: 20 }}>
                      <div style={{ fontSize: 12, color: 'var(--text-secondary)', marginBottom: 10, letterSpacing: '0.04em', textTransform: 'uppercase' }}>已添加的监控网站</div>
                      {listGroups.filter(g => g.url).map(g => (
                        <div key={g.id} style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', padding: '10px 14px', background: 'var(--hover-bg)', border: '1px solid var(--border-color)', borderRadius: 8, marginBottom: 8, transition: 'border-color 0.15s' }}
                          onMouseEnter={e => e.currentTarget.style.borderColor = 'var(--accent)'}
                          onMouseLeave={e => e.currentTarget.style.borderColor = 'var(--border-color)'}
                        >
                          <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
                            <div style={{ width: 34, height: 34, borderRadius: 8, flexShrink: 0, background: 'rgba(252,213,53,0.08)', border: '1px solid rgba(252,213,53,0.18)', display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: 16 }}>🌐</div>
                            <div>
                              <a href={g.url} target="_blank" rel="noopener noreferrer"
                                style={{ fontSize: 14, fontWeight: 700, color: 'var(--accent)', textDecoration: 'none', fontFamily: "'Rajdhani',sans-serif", display: 'block' }}
                              >{g.name}</a>
                              <div style={{ fontSize: 11, color: 'var(--text-secondary)', marginTop: 1, maxWidth: 200, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{g.url}</div>
                            </div>
                          </div>
                          <button onClick={() => { const next = listGroups.filter(x => x.id !== g.id); setListGroups(next); localStorage.setItem('ikun_listgroups', JSON.stringify(next)) }}
                            style={{ background: 'none', border: 'none', color: 'var(--text-secondary)', cursor: 'pointer', fontSize: 18, lineHeight: 1, padding: '4px 6px', borderRadius: 4, transition: 'color 0.15s' }}
                            onMouseEnter={e => e.currentTarget.style.color = '#f6465d'}
                            onMouseLeave={e => e.currentTarget.style.color = 'var(--text-secondary)'}
                            title="删除"
                          >×</button>
                        </div>
                      ))}
                    </div>
                  )}
                </div>
              )}
            </div>
          </div>
        </div>
      )}

      {showEmailBindTip && (
        <div className="modal" onClick={e => e.target === e.currentTarget && setShowEmailBindTip(false)}>
          <div style={{ background: 'var(--sidebar-bg)', borderRadius: 14, border: '1px solid var(--border-color)', padding: '32px 28px 28px', width: 420, maxWidth: '92vw', boxShadow: '0 24px 64px rgba(0,0,0,0.55)', position: 'relative' }}>
            <button onClick={() => setShowEmailBindTip(false)} style={{ position: 'absolute', top: 14, right: 16, background: 'none', border: 'none', color: 'var(--text-secondary)', fontSize: 22, cursor: 'pointer', lineHeight: 1 }}>×</button>
            <div style={{ width: 48, height: 48, borderRadius: 12, marginBottom: 18, background: 'rgba(255,150,0,0.12)', border: '1px solid rgba(255,150,0,0.25)', display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: 24 }}>📧</div>
            <div style={{ fontFamily: "'Rajdhani', sans-serif", fontSize: 18, fontWeight: 700, color: 'var(--text-primary)', marginBottom: 10 }}>需要绑定发件邮箱</div>
            <div style={{ fontSize: 13, color: 'var(--text-secondary)', lineHeight: 1.7, marginBottom: 24 }}>
              开启邮件推送前，请先在设置中配置你的 SMTP 发件邮箱。<br />目前支持 QQ 邮箱、Gmail、163 等主流邮箱。
            </div>
            <div style={{ display: 'flex', gap: 10 }}>
              <button onClick={() => setShowEmailBindTip(false)} style={{ flex: 1, padding: '10px 0', borderRadius: 8, background: 'none', border: '1px solid var(--border-color)', color: 'var(--text-secondary)', cursor: 'pointer', fontSize: 13, fontFamily: "'Rajdhani', sans-serif", fontWeight: 700 }}>稍后再说</button>
              <button onClick={() => { setShowEmailBindTip(false); setModalOpen(true); setModalTab('email') }}
                style={{ flex: 1, padding: '10px 0', borderRadius: 8, background: 'var(--accent)', border: 'none', color: '#030812', cursor: 'pointer', fontSize: 13, fontFamily: "'Rajdhani', sans-serif", fontWeight: 700 }}>前往绑定</button>
            </div>
          </div>
        </div>
      )}

      {showStratModal && (
        <div className="modal" onClick={e => e.target === e.currentTarget && setShowStratModal(false)}>
          <div style={{ background: 'var(--sidebar-bg)', borderRadius: 16, border: '1px solid var(--border-color)', padding: '36px 32px 32px', width: 480, maxWidth: '92vw', boxShadow: '0 24px 64px rgba(0,0,0,0.55)', position: 'relative' }}>
            <button onClick={() => setShowStratModal(false)} style={{ position: 'absolute', top: 16, right: 16, background: 'none', border: 'none', color: 'var(--text-secondary)', fontSize: 22, cursor: 'pointer', lineHeight: 1 }}>×</button>
            <div style={{ fontFamily: "'Rajdhani', sans-serif", fontSize: 20, fontWeight: 700, color: 'var(--text-primary)', marginBottom: 8 }}>添加更多策略</div>
            <div style={{ fontSize: 13, color: 'var(--text-secondary)', marginBottom: 28, lineHeight: 1.6 }}>你希望从哪里获取策略？</div>
            <div style={{ display: 'flex', flexDirection: 'column', gap: 14 }}>
              <button onClick={() => { setShowStratModal(false); handleNavigate('community') }}
                style={{ background: 'var(--input-bg)', border: '1px solid var(--border-color)', borderRadius: 10, padding: '18px 20px', cursor: 'pointer', textAlign: 'left', transition: 'border-color 0.2s, background 0.2s', display: 'flex', alignItems: 'flex-start', gap: 16 }}
                onMouseEnter={e => { e.currentTarget.style.borderColor = 'var(--accent)'; e.currentTarget.style.background = 'var(--hover-bg)' }}
                onMouseLeave={e => { e.currentTarget.style.borderColor = 'var(--border-color)'; e.currentTarget.style.background = 'var(--input-bg)' }}
              >
                <div style={{ width: 44, height: 44, borderRadius: 10, flexShrink: 0, background: 'rgba(77,184,255,0.12)', border: '1px solid rgba(77,184,255,0.2)', display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: 22 }}>🌐</div>
                <div>
                  <div style={{ fontFamily: "'Rajdhani', sans-serif", fontWeight: 700, fontSize: 15, color: 'var(--text-primary)', marginBottom: 4 }}>前往社区</div>
                  <div style={{ fontSize: 12.5, color: 'var(--text-secondary)', lineHeight: 1.55 }}>在社区中寻找其他开发者和交易员分享的知名策略，一键订阅即可使用</div>
                </div>
              </button>
              <button onClick={() => { setShowStratModal(false); handleNavigate('indicators') }}
                style={{ background: 'var(--input-bg)', border: '1px solid var(--border-color)', borderRadius: 10, padding: '18px 20px', cursor: 'pointer', textAlign: 'left', transition: 'border-color 0.2s, background 0.2s', display: 'flex', alignItems: 'flex-start', gap: 16 }}
                onMouseEnter={e => { e.currentTarget.style.borderColor = '#0ecb81'; e.currentTarget.style.background = 'var(--hover-bg)' }}
                onMouseLeave={e => { e.currentTarget.style.borderColor = 'var(--border-color)'; e.currentTarget.style.background = 'var(--input-bg)' }}
              >
                <div style={{ width: 44, height: 44, borderRadius: 10, flexShrink: 0, background: 'rgba(14,203,129,0.1)', border: '1px solid rgba(14,203,129,0.2)', display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: 22 }}>⚗️</div>
                <div>
                  <div style={{ fontFamily: "'Rajdhani', sans-serif", fontWeight: 700, fontSize: 15, color: 'var(--text-primary)', marginBottom: 4 }}>自建专属策略</div>
                  <div style={{ fontSize: 12.5, color: 'var(--text-secondary)', lineHeight: 1.55 }}>在指标开发页用 AI 生成 PineScript 代码，打造属于你自己的专属量化策略</div>
                </div>
              </button>
            </div>
          </div>
        </div>
      )}

      {modalOpen && (
        <SettingsModal
          modalTab={modalTab} setModalTab={setModalTab}
          onClose={() => setModalOpen(false)} onSave={saveSettings}
          cfg={cfg} setCfg={setCfg}
          emailTemplate={emailTemplate} setEmailTemplate={setEmailTemplate}
          customSounds={customSounds}
          soundName={soundName} setSoundName={setSoundName} soundFileRef={soundFileRef}
          uploadSound={uploadSound} deleteSound={deleteSound}
          sendTestEmail={sendTestEmail} testEmailLoading={testEmailLoading} testEmailResult={testEmailResult}
        />
      )}

      {notifyItems.length > 0 && (
        <div className="notify-stack">
          {notifyItems.map((a, i) => {
            const actionColor = a.action === 'LONG' ? '#0ecb81' : a.action === 'SHORT' ? '#f6465d' : '#888'
            const actionText = a.action === 'LONG' ? 'LONG 做多' : a.action === 'SHORT' ? 'SHORT 做空' : a.action
            const kTime = a.trigger_time_full || a.trigger_time || ''
            return (
              <div key={String(i) + a.symbol + a.time} className="notify-card"
                onClick={() => { setActiveTab('alerts'); setNotifyItems([]) }}>
                {i === 0 && (
                  <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 4 }}>
                    <span style={{ fontSize: 11, color: 'var(--text-secondary)' }}>信号推送 x{notifyItems.length}</span>
                    <button onClick={e => { e.stopPropagation(); setNotifyItems([]) }} style={{ background: 'none', border: 'none', color: 'var(--text-secondary)', cursor: 'pointer', fontSize: 14 }}>✕</button>
                  </div>
                )}
                <div className="nc-sym">{a.symbol} · {a.timeframe || timeframe}</div>
                <div className="nc-info">{a.signal} · {a.detail}</div>
                <div className="nc-action" style={{ color: actionColor }}>{actionText}</div>
                <div className="nc-time">{kTime}</div>
              </div>
            )
          })}
        </div>
      )}

      {toast && <div className="toast-bar">{toast}</div>}

      {!cookieOk && (
        <div style={{ position: 'fixed', bottom: 0, left: 0, right: 0, background: 'var(--card-bg)', borderTop: '1px solid var(--border-color)', padding: '14px 24px', display: 'flex', justifyContent: 'space-between', alignItems: 'center', zIndex: 9999 }}>
          <span style={{ fontSize: 13, color: 'var(--text-secondary)' }}>🍪 本站使用 localStorage 保存自选列表和快讯记录</span>
          <button style={{ background: 'var(--accent)', color: '#000', border: 'none', padding: '8px 20px', borderRadius: 4, fontWeight: 'bold', cursor: 'pointer' }}
            onClick={() => { localStorage.setItem('ikun_cookie_ok', '1'); setCookieOk(true) }}>我知道了</button>
        </div>
      )}

      <div className="mobile-bottom-nav">
        <button className={activeTab === 'watchlist' ? 'active' : ''} onClick={() => { setActiveTab('watchlist'); fetchData() }}>
          <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><polygon points="12 2 15.09 8.26 22 9.27 17 14.14 18.18 21.02 12 17.77 5.82 21.02 7 14.14 2 9.27 8.91 8.26 12 2"/></svg>
          自选
        </button>
        <button className={activeTab === 'alerts' ? 'active' : ''} onClick={() => setActiveTab('alerts')}>
          <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d="M18 8A6 6 0 0 0 6 8c0 7-3 9-3 9h18s-3-2-3-9"/><path d="M13.73 21a2 2 0 0 1-3.46 0"/></svg>
          快讯
        </button>
        <button className={activeTab === 'io' ? 'active' : ''} onClick={() => openMonitorTab('io')}>
          <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><line x1="18" y1="20" x2="18" y2="10"/><line x1="12" y1="20" x2="12" y2="4"/><line x1="6" y1="20" x2="6" y2="14"/></svg>
          持仓
        </button>
        <button onClick={() => setSidebarOpen(v => !v)}>
          <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><circle cx="12" cy="12" r="3"/><path d="M12 1v2M12 21v2M4.22 4.22l1.42 1.42M18.36 18.36l1.42 1.42M1 12h2M21 12h2M4.22 19.78l1.42-1.42M18.36 5.64l1.42-1.42"/></svg>
          配置
        </button>
      </div>
    </>
  )
}


