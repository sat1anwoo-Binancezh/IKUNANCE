import React from 'react'
import { beepSound } from '../hooks/useAudio.js'

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
  function persistAlertSettings(next, tf = timeframe, tm = triggerMode, wm = watchlistMode) {
    syncAlertSettings?.(next, tf, tm, wm)
  }

  function updateTimeframe(nextTimeframe) {
    setTimeframe?.(nextTimeframe)
    persistAlertSettings({ ...alertSettings, _timeframe: nextTimeframe }, nextTimeframe)
  }

  function updateTrigger(nextTrigger) {
    setTriggerMode?.(nextTrigger)
    persistAlertSettings(alertSettings, timeframe, nextTrigger)
  }

  function updateWatchlistMode() {
    setWatchlistMode?.('favorites')
    persistAlertSettings(alertSettings, timeframe, triggerMode, 'favorites')
  }

  function updateAlert(key, value) {
    const next = { ...alertSettings, [key]: value }
    persistAlertSettings(next)
    if (key === 'email' && value) setShowEmailBindTip?.(true)
  }

  function previewSound(value) {
    if (value && value.startsWith('custom:')) {
      const audio = new Audio(`/api/sound/${value.replace('custom:', '')}`)
      audio.volume = 0.5
      audio.play().catch(() => {})
      return
    }
    beepSound?.(value || 'beep')
  }

  function updateSound(value) {
    const next = { ...alertSettings, sound_type: value }
    persistAlertSettings(next)
    previewSound(value)
  }

  return (
    <aside className={`sidebar ${sidebarOpen ? 'open' : ''}`}>
      <h2 style={{ color: 'var(--text-primary)', marginTop: 0, fontSize: 16, marginBottom: 20, borderLeft: '3px solid var(--accent)', paddingLeft: 10 }}>
        指标配置
      </h2>

      <div className="control-group">
        <span className="label">当前指标</span>
        <select className="bn-select" defaultValue="native">
          <option value="native">MACD 趋势确认/演进</option>
          <option disabled>─────────────</option>
          <option disabled>RSI 超买超卖 (即将上线)</option>
          <option disabled>布林带突破 (即将上线)</option>
        </select>
        <button
          onClick={() => setShowStratModal?.(true)}
          style={{
            marginTop: 8,
            width: '100%',
            padding: 8,
            background: 'var(--input-bg)',
            border: '1px dashed var(--border-color)',
            color: 'var(--text-secondary)',
            borderRadius: 4,
            cursor: 'pointer',
            fontSize: 12,
          }}
        >
          + 添加更多指标
        </button>
      </div>

      <div className="control-group">
        <span className="label">监控周期 (Timeframe)</span>
        <select className="bn-select" value={timeframe} onChange={event => updateTimeframe(event.target.value)}>
          <option value="15m">15m</option>
          <option value="1h">1h</option>
          <option value="4h">4h</option>
          <option value="1d">1d</option>
        </select>
      </div>

      <div className="control-group">
        <span className="label">触发条件 (Trigger)</span>
        <select className="bn-select" value={triggerMode} onChange={event => updateTrigger(event.target.value)}>
          <option value="close">每根K线收盘时触发一次</option>
          <option value="realtime">实时检测</option>
        </select>
      </div>

      <div className="control-group">
        <span className="label">监控列表模式</span>
        <select className="bn-select" value="favorites" onChange={updateWatchlistMode}>
          <option value="favorites">自选列表 (Favorites)</option>
        </select>
      </div>

      <div className="control-group">
        <span className="label">警报渠道 (快速设置)</span>
        <div className="checkbox-group">
          {[
            ['toast', '网页弹窗'],
            ['sound', '声音提醒'],
            ['app_push', 'APP 推送'],
            ['email', '邮件发送'],
          ].map(([key, label]) => (
            <label key={key} className="checkbox-item">
              <input type="checkbox" checked={!!alertSettings[key]} onChange={event => updateAlert(key, event.target.checked)} />
              {label}
            </label>
          ))}
        </div>

        <div style={{ marginTop: 15 }}>
          <span className="label">提示音效 (Ringtone)</span>
          <select className="bn-select" value={alertSettings.sound_type || 'beep'} onChange={event => updateSound(event.target.value)}>
            <option value="beep">标准 Beep (默认)</option>
            <option value="retro">复古游戏 (Retro)</option>
            <option value="alarm">紧急警报 (Alarm)</option>
            <option value="sonar">深海声纳 (Sonar)</option>
            {(customSounds || []).map(sound => (
              <option key={sound.file} value={`custom:${sound.file}`}>{sound.name}</option>
            ))}
          </select>
        </div>
      </div>

      <div className="status-panel-large">
        <div className="status-pulse" />
        <div style={{ color: 'var(--binance-green)', fontWeight: 'bold', fontSize: 15, marginBottom: 5 }}>
          全自动监控运行中
        </div>
        <div style={{ color: 'var(--text-secondary)', fontSize: 12 }}>
          状态: {connStatus}<br />监控中
        </div>
      </div>
    </aside>
  )
}
