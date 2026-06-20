import React, { useCallback, useState } from 'react'
import { beepSound } from '../hooks/useAudio.js'

export function useSignalAlerts({ soundEnabled } = {}) {
  const [alerts, setAlerts] = useState([])

  const addAlert = useCallback((signal) => {
    if (!signal) return
    const item = {
      ...signal,
      id: signal.id || `${signal.symbol || 'signal'}-${Date.now()}-${Math.random().toString(16).slice(2)}`,
      time: signal.time || new Date().toLocaleTimeString(),
    }
    setAlerts(prev => [item, ...prev].slice(0, 5))
    if (soundEnabled) beepSound()
  }, [soundEnabled])

  const dismiss = useCallback((id) => {
    setAlerts(prev => prev.filter(item => item.id !== id))
  }, [])

  return { alerts, addAlert, dismiss }
}

export function SignalAlertContainer({ signals, onDismiss }) {
  if (!signals?.length) return null
  return (
    <div className="signal-alert-stack">
      {signals.map(signal => (
        <div key={signal.id} className="signal-alert-card">
          <div className="signal-alert-main">
            <div className="signal-alert-symbol">{signal.symbol || 'SIGNAL'}</div>
            <div className="signal-alert-detail">{signal.signal || signal.detail || '策略信号触发'}</div>
          </div>
          <button className="signal-alert-close" onClick={() => onDismiss?.(signal.id)}>×</button>
        </div>
      ))}
    </div>
  )
}
