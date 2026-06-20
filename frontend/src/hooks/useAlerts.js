import { useCallback, useEffect, useMemo, useState } from 'react'

const ALERT_LOG_KEY = 'ikun_alert_log'

function safeScope(scope) {
  return String(scope || 'anon').replace(/[^a-zA-Z0-9_.-]/g, '_').slice(0, 80) || 'anon'
}

function readStoredAlerts(storageKey) {
  try {
    const data = JSON.parse(localStorage.getItem(storageKey) || '[]')
    return Array.isArray(data) ? data : []
  } catch {
    return []
  }
}

export function useAlerts(timeframe, alertSettings, showToast, storageScope = 'anon') {
  const storageKey = useMemo(() => `${ALERT_LOG_KEY}_${safeScope(storageScope)}`, [storageScope])
  const [alertLog, setAlertLog] = useState(() => readStoredAlerts(storageKey))
  const [unreadCount, setUnreadCount] = useState(0)
  const [notifyItems, setNotifyItems] = useState([])

  useEffect(() => {
    setAlertLog(readStoredAlerts(storageKey))
    setUnreadCount(0)
  }, [storageKey])

  useEffect(() => {
    localStorage.setItem(storageKey, JSON.stringify(alertLog.slice(0, 300)))
  }, [alertLog, storageKey])

  const addToAlertLogItems = useCallback((items) => {
    const incoming = Array.isArray(items) ? items : [items]
    const normalized = incoming.filter(Boolean).map(item => ({
      ...item,
      timeframe: item.timeframe || timeframe,
      time: item.time || new Date().toLocaleString(),
    }))
    if (normalized.length === 0) return

    setAlertLog(prev => [...normalized, ...prev].slice(0, 300))
    setUnreadCount(prev => prev + normalized.length)

    if (alertSettings?.toast !== false) {
      setNotifyItems(prev => [...normalized, ...prev].slice(0, 5))
      showToast?.(`收到 ${normalized.length} 条策略信号`)
    }
  }, [alertSettings?.toast, showToast, timeframe])

  const markAllRead = useCallback(() => {
    setUnreadCount(0)
  }, [])

  return {
    alertLog,
    setAlertLog,
    unreadCount,
    setUnreadCount,
    notifyItems,
    setNotifyItems,
    addToAlertLogItems,
    markAllRead,
  }
}
