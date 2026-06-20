import React from 'react'

export default function AppSidebar({
  sidebarOpen,
  timeframe,
  setTimeframe,
  triggerMode,
  setTriggerMode,
  watchlistMode,
  setWatchlistMode,
  alertSettings,
  syncAlertSettings,
  setShowEmailBindTip,
  connStatus,
  customSounds,
  setShowStratModal,
}) {
  const updateAlert = (key, value) => syncAlertSettings?.({ ...alertSettings, [key]: value })

  return (
    <aside className={`app-sidebar ${sidebarOpen ? 'open' : ''}`}>
      <section className="sidebar-section">
        <div className="sidebar-title">周期</div>
        <div className="segmented">
          {['15m', '1h', '4h', '1d'].map(item => (
            <button key={item} className={timeframe === item ? 'active' : ''} onClick={() => setTimeframe(item)}>{item}</button>
          ))}
        </div>
      </section>
      <section className="sidebar-section">
        <div className="sidebar-title">触发</div>
        <div className="segmented">
          <button className={triggerMode === 'close' ? 'active' : ''} onClick={() => setTriggerMode('close')}>收线</button>
          <button className={triggerMode === 'realtime' ? 'active' : ''} onClick={() => setTriggerMode('realtime')}>实时</button>
        </div>
      </section>
      <section className="sidebar-section">
        <div className="sidebar-title">列表</div>
        <div className="segmented">
          <button className={watchlistMode === 'favorites' ? 'active' : ''} onClick={() => setWatchlistMode('favorites')}>自选</button>
        </div>
      </section>
      <section className="sidebar-section">
        <div className="sidebar-title">通知</div>
        <label><input type="checkbox" checked={!!alertSettings.toast} onChange={e => updateAlert('toast', e.target.checked)} /> 桌面提示</label>
        <label><input type="checkbox" checked={!!alertSettings.email} onChange={e => updateAlert('email', e.target.checked)} /> 邮件</label>
        <label><input type="checkbox" checked={!!alertSettings.sound} onChange={e => updateAlert('sound', e.target.checked)} /> 声音</label>
        <button onClick={() => setShowEmailBindTip?.(true)}>邮件绑定</button>
      </section>
      <section className="sidebar-section">
        <div className="sidebar-title">状态</div>
        <div className="sidebar-status">{connStatus}</div>
        <div className="sidebar-status">音效 {customSounds?.length || 0}</div>
        <button onClick={() => setShowStratModal?.(true)}>策略</button>
      </section>
    </aside>
  )
}
