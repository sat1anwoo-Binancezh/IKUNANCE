import React, { useEffect, useRef, useState } from 'react'
import './styles/global-nav.css'

const NAV_ITEMS = [
  { id: 'monitor', label: '监控' },
  { id: 'signals', label: '信号' },
  { id: 'market', label: '市场' },
  { id: 'community', label: '社区' },
  { id: 'indicators', label: '指标' },
  { id: 'premium', label: '会员' },
]

const THEME_OPTIONS = [
  { id: 'dark', label: 'Dark' },
  { id: 'light', label: 'Light' },
  { id: 'vc', label: 'VC' },
  { id: 'mc', label: 'MC' },
]

const THEMES = {
  dark: {
    '--bg-color': '#030812',
    '--bg-secondary': '#0b0e11',
    '--sidebar-bg': '#0b0e11',
    '--card-bg': 'rgba(5,13,30,0.85)',
    '--text-primary': '#e2eaf4',
    '--text-secondary': '#9ca3af',
    '--border-color': 'rgba(60,140,220,0.14)',
    '--nav-bg': 'rgba(4,10,22,0.92)',
    '--nav-border': 'rgba(60,140,220,0.14)',
    '--accent': '#4db8ff',
    '--ikun-blue': '#4db8ff',
    '--binance-yellow': '#4db8ff',
    '--binance-green': '#0ecb81',
    '--binance-red': '#f6465d',
    '--hover-bg': 'rgba(77,184,255,0.08)',
    '--input-bg': 'rgba(5,13,30,0.6)',
  },
  light: {
    '--bg-color': '#f0f4fa',
    '--bg-secondary': '#e4eaf5',
    '--sidebar-bg': '#ffffff',
    '--card-bg': 'rgba(255,255,255,0.92)',
    '--text-primary': '#0d1a2e',
    '--text-secondary': '#4a5568',
    '--border-color': 'rgba(40,100,180,0.18)',
    '--nav-bg': 'rgba(240,244,250,0.96)',
    '--nav-border': 'rgba(40,100,180,0.18)',
    '--accent': '#1a56db',
    '--ikun-blue': '#1a56db',
    '--binance-yellow': '#1a56db',
    '--binance-green': '#0a9b66',
    '--binance-red': '#d9304f',
    '--hover-bg': 'rgba(26,86,219,0.08)',
    '--input-bg': '#ffffff',
  },
  vc: {
    '--bg-color': '#2a2535',
    '--bg-secondary': '#231f2e',
    '--sidebar-bg': '#201c2b',
    '--card-bg': 'rgba(45,38,58,0.95)',
    '--text-primary': '#ff79c6',
    '--text-secondary': '#bd93f9',
    '--border-color': 'rgba(80,255,180,0.35)',
    '--nav-bg': 'rgba(32,28,43,0.96)',
    '--nav-border': 'rgba(80,255,180,0.3)',
    '--accent': '#ff79c6',
    '--ikun-blue': '#50ffb4',
    '--binance-yellow': '#ff79c6',
    '--binance-green': '#50ffb4',
    '--binance-red': '#ff5555',
    '--hover-bg': 'rgba(255,121,198,0.1)',
    '--input-bg': 'rgba(35,31,46,0.9)',
  },
  mc: {
    '--bg-color': '#1a1209',
    '--bg-secondary': '#221808',
    '--sidebar-bg': '#221808',
    '--card-bg': 'rgba(30,20,8,0.92)',
    '--text-primary': '#f5dfa0',
    '--text-secondary': '#a08858',
    '--border-color': 'rgba(180,120,20,0.22)',
    '--nav-bg': 'rgba(20,14,4,0.96)',
    '--nav-border': 'rgba(180,120,20,0.22)',
    '--accent': '#e0a020',
    '--ikun-blue': '#e0a020',
    '--binance-yellow': '#7CCC19',
    '--binance-green': '#7CCC19',
    '--binance-red': '#e85d5d',
    '--hover-bg': 'rgba(224,160,32,0.1)',
    '--input-bg': 'rgba(30,20,8,0.8)',
  },
}

function applyTheme(themeId) {
  const id = THEMES[themeId] ? themeId : 'dark'
  const root = document.documentElement
  root.setAttribute('data-theme', id)
  Object.entries(THEMES[id]).forEach(([key, value]) => root.style.setProperty(key, value))
}

export default function GlobalNav({ activePage, onNavigate, currentUser, onOpenLogin, onLogout }) {
  const [themeOpen, setThemeOpen] = useState(false)
  const [userOpen, setUserOpen] = useState(false)
  const [mobileOpen, setMobileOpen] = useState(false)
  const [theme, setTheme] = useState(() => localStorage.getItem('ikun_theme') || 'dark')
  const themeRef = useRef(null)
  const userRef = useRef(null)

  useEffect(() => {
    applyTheme(theme)
  }, [theme])

  useEffect(() => {
    function closeMenus(event) {
      if (themeRef.current && !themeRef.current.contains(event.target)) setThemeOpen(false)
      if (userRef.current && !userRef.current.contains(event.target)) setUserOpen(false)
    }
    document.addEventListener('mousedown', closeMenus)
    return () => document.removeEventListener('mousedown', closeMenus)
  }, [])

  function navigate(page) {
    setMobileOpen(false)
    onNavigate?.(page)
  }

  function chooseTheme(nextTheme) {
    setTheme(nextTheme.id)
    setThemeOpen(false)
    localStorage.setItem('ikun_theme', nextTheme.id)
  }

  const themeLabel = THEME_OPTIONS.find(item => item.id === theme)?.label || 'Dark'
  const userName = currentUser?.nickname || currentUser?.username || currentUser?.email?.split('@')[0] || ''
  const avatarText = (userName || currentUser?.email || '?').charAt(0).toUpperCase()

  return (
    <>
      <nav className="gn-nav">
        <button className="gn-logo" onClick={() => navigate('monitor')}>
          <img className="gn-logo-img gn-logo-dark" src="https://sc01.alicdn.com/kf/Ac3903ff596f74d1f804452cc4ffff11e4.png" alt="IK" />
          <img className="gn-logo-img gn-logo-light" src="https://sc01.alicdn.com/kf/A62ded46173cd4a2ba0fe462ba9fdfc1eM.png" alt="IK" />
          <span className="gn-logo-word"><em>I</em>KUNANCE</span>
        </button>

        <div className="gn-items">
          {NAV_ITEMS.map(item => (
            <button
              key={item.id}
              className={`gn-btn ${activePage === item.id ? 'gn-btn--active' : ''} ${item.id === 'premium' ? 'gn-btn--premium' : ''}`}
              onClick={() => navigate(item.id)}
            >
              {item.id === 'premium' ? <span className="gn-premium-label">{item.label}</span> : item.label}
            </button>
          ))}
        </div>

        <div className="gn-right">
          <div ref={themeRef} style={{ position: 'relative' }}>
            <button className="gn-hub-btn" onClick={() => setThemeOpen(open => !open)} title="主题">
              <svg viewBox="0 0 20 20" fill="none" width="16" height="16">
                {theme === 'light' ? (
                  <>
                    <circle cx="10" cy="10" r="4" stroke="currentColor" strokeWidth="1.4" />
                    <path d="M10 2v2M10 16v2M2 10h2M16 10h2M4.5 4.5l1.5 1.5M14 14l1.5 1.5M4.5 15.5L6 14M14 6l1.5-1.5" stroke="currentColor" strokeWidth="1.4" strokeLinecap="round" />
                  </>
                ) : (
                  <path d="M17.5 11.5A7.5 7.5 0 119.5 2.5a5.5 5.5 0 008 9z" stroke="currentColor" strokeWidth="1.4" strokeLinejoin="round" />
                )}
              </svg>
              <span>{themeLabel}</span>
            </button>
            {themeOpen && (
              <div className="gn-dropdown gn-dropdown--right">
                {THEME_OPTIONS.map(item => (
                  <button key={item.id} className={`gn-dd-item ${theme === item.id ? 'gn-dd-item--sel' : ''}`} onClick={() => chooseTheme(item)}>
                    {item.label}
                  </button>
                ))}
              </div>
            )}
          </div>

          <button className="gn-hub-btn" onClick={() => navigate('settings')} title="设置">
            <svg viewBox="0 0 20 20" fill="none" width="16" height="16">
              <circle cx="10" cy="10" r="3" stroke="currentColor" strokeWidth="1.4" />
              <path d="M10 2v1.5M10 16.5V18M2 10h1.5M16.5 10H18M4.1 4.1l1.1 1.1M14.8 14.8l1.1 1.1M4.1 15.9l1.1-1.1M14.8 5.2l1.1-1.1" stroke="currentColor" strokeWidth="1.4" strokeLinecap="round" />
            </svg>
            <span>设置</span>
          </button>

          {currentUser ? (
            <div ref={userRef} style={{ position: 'relative' }}>
              <button className="gn-user-btn" onClick={() => setUserOpen(open => !open)}>
                {currentUser.avatar ? <img src={currentUser.avatar} alt="" className="gn-avatar" /> : <span className="gn-avatar-text">{avatarText}</span>}
                <span className="gn-user-name">{userName}</span>
                <svg className={`gn-caret ${userOpen ? 'gn-caret--open' : ''}`} viewBox="0 0 10 6" fill="none" width="9">
                  <path d="M1 1l4 4 4-4" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" />
                </svg>
              </button>
              {userOpen && (
                <div className="gn-dropdown gn-dropdown--right">
                  <div className="gn-dd-userinfo">
                    <div className="gn-dd-username">{userName}</div>
                    <div className="gn-dd-email">{currentUser.email}</div>
                  </div>
                  <button className="gn-dd-item" onClick={() => { setUserOpen(false); navigate('settings') }}>账户设置</button>
                  <button className="gn-dd-item gn-dd-item--danger" onClick={() => { setUserOpen(false); onLogout?.() }}>退出登录</button>
                </div>
              )}
            </div>
          ) : (
            <button className="gn-login-btn" onClick={onOpenLogin}>
              <svg viewBox="0 0 20 20" fill="none" width="15" height="15">
                <circle cx="10" cy="7" r="3.5" stroke="currentColor" strokeWidth="1.4" />
                <path d="M3 18c0-3.3 3.1-6 7-6s7 2.7 7 6" stroke="currentColor" strokeWidth="1.4" strokeLinecap="round" />
              </svg>
              登录
            </button>
          )}
        </div>

        <button className={`gn-burger ${mobileOpen ? 'gn-burger--open' : ''}`} onClick={() => setMobileOpen(open => !open)} aria-label="菜单">
          <span />
          <span />
          <span />
        </button>
      </nav>

      {mobileOpen && (
        <div className="gn-mobile-overlay" onClick={() => setMobileOpen(false)}>
          <div className="gn-mobile-drawer" onClick={event => event.stopPropagation()}>
            {NAV_ITEMS.map(item => (
              <button
                key={item.id}
                className={`gn-mobile-item ${activePage === item.id ? 'gn-mobile-item--active' : ''} ${item.id === 'premium' ? 'gn-mobile-item--premium' : ''}`}
                onClick={() => navigate(item.id)}
              >
                {item.id === 'premium' ? <span className="gn-premium-label">{item.label}</span> : item.label}
              </button>
            ))}
            <div className="gn-mobile-divider" />
            {currentUser ? (
              <button className="gn-mobile-item gn-mobile-item--danger" onClick={() => { setMobileOpen(false); onLogout?.() }}>退出登录</button>
            ) : (
              <button className="gn-mobile-item" onClick={() => { setMobileOpen(false); onOpenLogin?.() }}>登录 / 注册</button>
            )}
          </div>
        </div>
      )}
    </>
  )
}
