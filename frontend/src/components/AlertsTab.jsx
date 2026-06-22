import React from 'react'

function parseAlertTime(item) {
  const raw = item.trigger_time_full || item.time || item.candle_time || item.timestamp || item.trigger_time || ''
  const value = String(raw || '').trim()
  const match = value.match(/^(\d{4})[-/](\d{1,2})[-/](\d{1,2})[ T](\d{1,2}):(\d{2})/)
  if (match) {
    return {
      group: `${Number(match[2])}月${Number(match[3])}日`,
      time: `${match[4].padStart(2, '0')}:${match[5]}`,
    }
  }
  if (/^\d{1,2}:\d{2}/.test(value)) return { group: '今天', time: value.slice(0, 5) }
  return { group: '今天', time: value || '-' }
}

function normalizeAction(item) {
  const raw = String(item.action || item.signal || item.side || '').toUpperCase()
  if (raw.includes('LONG') || raw.includes('做多')) return 'LONG'
  if (raw.includes('SHORT') || raw.includes('做空')) return 'SHORT'
  return raw || '-'
}

function signalTitle(item) {
  if (item.type) return item.type
  const detail = String(item.detail || '')
  if (detail.includes('首次增强')) return '趋势演进'
  return '趋势确认'
}

function formatCopySymbol(symbol) {
  const raw = String(symbol || '').toUpperCase().trim()
  if (!raw) return ''
  if (raw.endsWith('.P')) return raw.replace(/\//g, '')
  const compact = raw.split(':')[0].split('@')[0].replace(/\//g, '').replace(/[^A-Z0-9]/g, '')
  return compact.endsWith('USDT') ? `${compact}.P` : compact
}

function copyText(text) {
  if (!text) return
  if (navigator.clipboard?.writeText && window.isSecureContext) {
    navigator.clipboard.writeText(text).catch(() => {})
    return
  }
  const node = document.createElement('textarea')
  node.value = text
  node.setAttribute('readonly', '')
  node.style.position = 'fixed'
  node.style.left = '-9999px'
  document.body.appendChild(node)
  node.select()
  try { document.execCommand('copy') } catch {}
  document.body.removeChild(node)
}

export default function AlertsTab({ alertLog, setAlertLog, unreadCount, setUnreadCount, markAllRead }) {
  function clearLog() {
    setAlertLog([])
    setUnreadCount(0)
  }

  const groups = (alertLog || []).reduce((acc, item, index) => {
    const parsed = parseAlertTime(item)
    const group = acc.find(entry => entry.label === parsed.group)
    const row = { item, index, parsed }
    if (group) group.rows.push(row)
    else acc.push({ label: parsed.group, rows: [row] })
    return acc
  }, [])

  return (
    <div className="list-container alerts-tab" style={{ paddingTop: 28 }}>
      <div
        className="alerts-toolbar"
        style={{
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between',
          gap: 16,
          paddingBottom: 18,
          borderBottom: '1px solid var(--border-color)',
        }}
      >
        <div>
          <div style={{ color: 'var(--text-primary)', fontSize: 17, fontWeight: 700 }}>信号快讯记录</div>
          <div style={{ color: 'var(--text-secondary)', fontSize: 12, marginTop: 5 }}>未读 {unreadCount || 0}</div>
        </div>
        <div style={{ display: 'flex', gap: 8 }}>
          <button className="icon-btn" onClick={markAllRead}>全部已读</button>
          <button className="icon-btn" onClick={clearLog}>清空</button>
        </div>
      </div>

      {groups.length === 0 ? (
        <div style={{ padding: '44px 0', color: 'var(--text-secondary)', textAlign: 'center' }}>暂无快讯</div>
      ) : (
        <div style={{ paddingTop: 22 }}>
          {groups.map(group => (
            <section key={group.label} style={{ marginBottom: 22 }}>
              <div style={{ color: 'var(--text-secondary)', fontSize: 13, fontWeight: 700, margin: '0 0 12px 2px' }}>
                {group.label}
              </div>
              <div role="list">
                {group.rows.map(({ item, index, parsed }) => {
                  const action = normalizeAction(item)
                  const actionColor = action === 'LONG' ? 'var(--binance-green)' : action === 'SHORT' ? 'var(--binance-red)' : 'var(--text-secondary)'
                  const unread = index < (unreadCount || 0)
                  return (
                    <div
                      role="listitem"
                      className={`alert-log-item ${unread ? 'unread' : ''}`}
                      key={`${item.symbol || 'signal'}-${item.time || item.trigger_time || index}-${index}`}
                      style={{
                        padding: '18px 24px',
                        borderLeft: unread ? '3px solid var(--accent)' : '3px solid transparent',
                      }}
                    >
                      <div className="al-title" style={{ color: 'var(--accent)', fontSize: 16, fontWeight: 800 }}>
                        {signalTitle(item)}{' '}
                        <button
                          type="button"
                          onClick={() => copyText(formatCopySymbol(item.symbol))}
                          title={`点击复制 ${formatCopySymbol(item.symbol) || item.symbol || ''}`}
                          style={{
                            background: 'none',
                            border: 'none',
                            color: 'inherit',
                            cursor: 'pointer',
                            font: 'inherit',
                            fontWeight: 'inherit',
                            padding: 0,
                          }}
                        >
                          {item.symbol || '-'}
                        </button>
                      </div>
                      <div className="al-sub" style={{ display: 'flex', flexWrap: 'wrap', gap: 6, alignItems: 'center', fontSize: 13 }}>
                        <span>{item.timeframe || '15m'}</span>
                        <span>·</span>
                        <span>{item.detail || item.signal || '策略信号触发'}</span>
                        <span>·</span>
                        <span style={{ color: actionColor, fontWeight: 800 }}>{action}</span>
                        <span>·</span>
                        <span>{parsed.time}</span>
                      </div>
                    </div>
                  )
                })}
              </div>
            </section>
          ))}
        </div>
      )}
    </div>
  )
}
