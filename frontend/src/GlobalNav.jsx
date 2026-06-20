import React from 'react'

export default function GlobalNav({ activePage, onNavigate, currentUser, onOpenLogin, onLogout }) {
  return (
    <header className="global-nav">
      <button className="brand-btn" onClick={() => onNavigate?.('monitor')}>IKUNANCE</button>
      <nav className="global-nav-links">
        <button className={activePage === 'monitor' ? 'active' : ''} onClick={() => onNavigate?.('monitor')}>监控台</button>
        <button className={activePage === 'community' ? 'active' : ''} onClick={() => onNavigate?.('community')}>社区</button>
      </nav>
      <div className="global-nav-user">
        {currentUser ? (
          <button onClick={onLogout}>{currentUser.email || currentUser.username || '退出'}</button>
        ) : (
          <button onClick={onOpenLogin}>登录</button>
        )}
      </div>
    </header>
  )
}
