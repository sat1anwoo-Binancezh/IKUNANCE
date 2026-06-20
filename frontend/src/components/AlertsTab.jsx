import React from 'react'

export default function AlertsTab({ alertLog, setAlertLog, unreadCount, setUnreadCount, markAllRead }) {
  function clearLog() {
    setAlertLog([])
    setUnreadCount(0)
  }

  return (
    <div className="list-container alerts-tab">
      <div className="alerts-toolbar">
        <span>未读 {unreadCount || 0}</span>
        <button onClick={markAllRead}>全部已读</button>
        <button onClick={clearLog}>清空</button>
      </div>
      <table className="bn-table">
        <thead>
          <tr>
            <th>时间</th>
            <th>标的</th>
            <th>信号</th>
            <th>详情</th>
          </tr>
        </thead>
        <tbody>
          {(alertLog || []).length === 0 ? (
            <tr><td colSpan={4} style={{ textAlign: 'center', padding: 40, color: 'var(--text-secondary)' }}>暂无快讯</td></tr>
          ) : alertLog.map((item, index) => (
            <tr key={`${item.symbol || 'signal'}-${item.time || index}-${index}`}>
              <td>{item.time || item.trigger_time || '-'}</td>
              <td>{item.symbol || '-'}</td>
              <td>{item.signal || item.action || '-'}</td>
              <td>{item.detail || '-'}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}
