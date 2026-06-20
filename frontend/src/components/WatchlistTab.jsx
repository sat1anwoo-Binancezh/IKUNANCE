/**
 * WatchlistTab — 自选列表 Tab + 自建列表组 Tab
 *
 * watchlist 现在是对象数组: [{ key, symbol, exchangeId, display, exchangeLabel }]
 * key 格式: "BTC/USDT@okx"，是唯一标识
 */
import React from 'react'

const TV_EXCHANGE_PREFIX = {
  binance: 'BINANCE',
  okx: 'OKX',
  bybit: 'BYBIT',
  bitget: 'BITGET',
  gate: 'GATEIO',
  kucoin: 'KUCOIN',
}

function tvSymbolFor(wlItem) {
  const exchange = TV_EXCHANGE_PREFIX[wlItem.exchangeId] || 'BINANCE'
  const symbol = String(wlItem.symbol || '')
    .toUpperCase()
    .replace('/', '')
    .replace(/[^A-Z0-9]/g, '')
  return `${exchange}:${symbol}.P`
}

function TradingViewMiniChart({ wlItem }) {
  const tvSymbol = tvSymbolFor(wlItem)
  const frameId = `ikun_tv_${tvSymbol.replace(/[^A-Z0-9]/gi, '_')}`
  const src = `https://s.tradingview.com/widgetembed/?frameElementId=${frameId}&symbol=${encodeURIComponent(tvSymbol)}&interval=15&hidesidetoolbar=1&symboledit=1&saveimage=0&toolbarbg=151923&studies=[]&theme=dark&style=1&timezone=Asia%2FShanghai&withdateranges=1`

  return (
    <div style={{ width: '100%', height: 260, border: '1px solid var(--border-color)', borderRadius: 8, overflow: 'hidden', background: 'var(--bg-color)' }}>
      <iframe
        id={frameId}
        title={`${wlItem.symbol} TradingView`}
        src={src}
        loading="lazy"
        referrerPolicy="origin"
        style={{ width: '100%', height: '100%', border: 0, display: 'block' }}
      />
    </div>
  )
}

function SymbolName({ symbol, isOpen, onClick }) {
  const handleClick = (event) => {
    event.stopPropagation()
    onClick?.()
  }

  return (
    <button
      type="button"
      className="pair-name"
      onClick={handleClick}
      aria-expanded={isOpen}
      title="打开 TradingView K线图"
      style={{
        background: 'none',
        border: 'none',
        padding: 0,
        margin: 0,
        color: 'inherit',
        cursor: 'pointer',
        textAlign: 'left',
        fontFamily: 'inherit',
      }}
    >
      {symbol}
    </button>
  )
}

/* ── 趋势 + 信号 badge ── */
function TrendEl({ trend }) {
  const bull = trend === 'BULL'
  return bull
    ? <span className="trend-bull">
        <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"
          strokeLinecap="round" strokeLinejoin="round" style={{ marginRight: 4, verticalAlign: -2 }}>
          <polyline points="23 6 13.5 15.5 8.5 10.5 1 18"/>
          <polyline points="17 6 23 6 23 12"/>
        </svg>多头 Bull
      </span>
    : <span className="trend-bear">
        <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"
          strokeLinecap="round" strokeLinejoin="round" style={{ marginRight: 4, verticalAlign: -2 }}>
          <polyline points="23 18 13.5 8.5 8.5 13.5 1 6"/>
          <polyline points="17 18 23 18 23 12"/>
        </svg>空头 Bear
      </span>
}

function SignalBadge({ signal, action }) {
  if (signal === '-') return <span style={{ color: '#555' }}>-</span>
  return action === 'LONG'
    ? <span className="signal-badge badge-yellow">{signal}</span>
    : <span className="signal-badge badge-purple">{signal}</span>
}

/* ── 搜索框 + 下拉 ── */
// filteredSymbols: [{ key, symbol, display, exchangeId, exchangeLabel }]
function searchStatusText(status) {
  if (status === 'loading') return '\u6b63\u5728\u641c\u7d22'
  if (status === 'empty') return '\u65e0\u7ed3\u679c'
  if (status === 'error') return '\u8054\u7f51\u641c\u7d22\u5931\u8d25\uff0c\u8bf7\u91cd\u8bd5'
  return '\u672a\u627e\u5230\u5339\u914d\u7684\u6807\u7684'
}

function SymbolSearch({ wrapRef, value, onChange, onFocus, onKeyDown, showDropdown, filteredSymbols, inListFn, onAdd, searchStatus = 'idle' }) {
  return (
    <div className="main-search-container" ref={wrapRef}>
      <input
        className="main-search-input"
        placeholder="搜索永续合约/美股标的 (输入: BTC, AAPL, NVDA...)"
        value={value}
        onFocus={onFocus}
        onChange={onChange}
        onKeyDown={onKeyDown}
      />
      <span className="main-search-icon">
        <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
          <circle cx="11" cy="11" r="8"/><line x1="21" y1="21" x2="16.65" y2="16.65"/>
        </svg>
      </span>
      {showDropdown && value && (
        <div className="search-dropdown">
          {filteredSymbols.length === 0
            ? <div className="search-dd-item" style={{ color: 'var(--text-secondary)' }}>{searchStatusText(searchStatus)}</div>
            : filteredSymbols.map(item => {
                const already = inListFn(item.key)
                return (
                  <div key={item.key} className="search-dd-item">
                    <div className="dd-item-main">
                      <span className="dd-sym-name">{item.display}</span>
                      <span className="dd-sym-meta">
                        <span className="dd-tag dd-exchange">{item.exchangeLabel}</span>
                        <span className="dd-tag dd-type">{item.marketType === 'stock' ? '美股' : '合约'}</span>
                      </span>
                    </div>
                    {already
                      ? <span className="in-list">已添加</span>
                      : <button className="add-btn" onClick={e => { e.stopPropagation(); onAdd(item) }}>+ 添加</button>}
                  </div>
                )
              })
          }
        </div>
      )}
    </div>
  )
}

/* ── 交易对表格行 ── */
// wlItem: { key, symbol, exchangeId, display, exchangeLabel }
// scanItem: 后端返回的 { symbol, price, trend, signal, detail, action }
function SymbolTableRow({ wlItem, scanItem, removeBtn, showExLink = true, chartOpen = false, onToggleChart }) {
  const sym   = wlItem.symbol
  const eid   = wlItem.exchangeId
  const elabel = wlItem.exchangeLabel || eid.toUpperCase()

  // 各交易所合约页跳转
  const tradeUrl = {
    binance: `https://www.binance.com/zh-CN/futures/${sym.replace('/', '')}`,
    okx:     `https://www.okx.com/trade-swap/${sym.replace('/', '-').toLowerCase()}-swap`,
    bybit:   `https://www.bybit.com/trade/usdt/${sym.replace('/USDT', '')}`,
    bitget:  `https://www.bitget.com/futures/usdt/${sym.replace('/', '')}`,
    gate:    `https://www.gate.io/futures_trade/USDT/${sym.replace('/USDT', '_USDT')}`,
    kucoin:  `https://www.kucoin.com/futures/trade/${sym.replace('/USDT', 'USDTM')}`,
  }[eid]

  if (!scanItem) {
    return (
      <>
        <tr>
          <td>{removeBtn}</td>
          <td onClick={onToggleChart} title="打开 TradingView K线图" style={{ cursor: 'pointer' }}>
            <SymbolName symbol={sym} isOpen={chartOpen} onClick={onToggleChart} />
            <span style={{ fontSize: 12, color: '#888' }}>{elabel}</span>
          </td>
          <td style={{ color: 'var(--text-secondary)' }}>加载中</td>
          <td>-</td><td>-</td>
          <td style={{ textAlign: 'right' }}><span style={{ color: '#555', fontSize: 12 }}>等待数据</span></td>
        </tr>
        {chartOpen && (
          <tr>
            <td></td>
            <td colSpan={5} style={{ paddingTop: 0 }}>
              <TradingViewMiniChart wlItem={wlItem} />
            </td>
          </tr>
        )}
      </>
    )
  }

  let action = <span style={{ color: '#555', fontSize: 12 }}>观望 Wait</span>
  if (scanItem.signal !== '-') {
    if (scanItem.action === 'LONG') {
      action = (showExLink && tradeUrl)
        ? <a href={tradeUrl} target="_blank" rel="noreferrer" className="btn-action btn-long"
            style={{ textDecoration: 'none', display: 'inline-block', color: '#fff' }}>开多 LONG</a>
        : <span className="btn-action btn-long" style={{ display: 'inline-block', cursor: 'default' }}>开多 LONG</span>
    } else {
      action = (showExLink && tradeUrl)
        ? <a href={tradeUrl} target="_blank" rel="noreferrer" className="btn-action btn-short"
            style={{ textDecoration: 'none', display: 'inline-block', color: '#fff' }}>开空 SHORT</a>
        : <span className="btn-action btn-short" style={{ display: 'inline-block', cursor: 'default' }}>开空 SHORT</span>
    }
  }

  return (
    <>
      <tr>
        <td>{removeBtn}</td>
        <td onClick={onToggleChart} title="打开 TradingView K线图" style={{ cursor: 'pointer' }}>
          <SymbolName symbol={sym} isOpen={chartOpen} onClick={onToggleChart} />
          <span style={{ fontSize: 12, color: '#888' }}>{elabel}</span>
        </td>
        <td><TrendEl trend={scanItem.trend} /></td>
        <td><SignalBadge signal={scanItem.signal} action={scanItem.action} /></td>
        <td style={{ color: '#888', fontSize: 13 }}>{scanItem.detail}</td>
        <td style={{ textAlign: 'right' }}>{action}</td>
      </tr>
      {chartOpen && (
        <tr>
          <td></td>
          <td colSpan={5} style={{ paddingTop: 0 }}>
            <TradingViewMiniChart wlItem={wlItem} />
          </td>
        </tr>
      )}
    </>
  )
}

const StarBtn = ({ onClick }) => (
  <button style={{ background: 'none', border: 'none', cursor: 'pointer', fontSize: 16, color: 'var(--accent)', padding: '4px 8px' }} onClick={onClick}>
    <svg width="18" height="18" viewBox="0 0 24 24" fill="currentColor" stroke="none">
      <polygon points="12 2 15.09 8.26 22 9.27 17 14.14 18.18 21.02 12 17.77 5.82 21.02 7 14.14 2 9.27 8.91 8.26 12 2"/>
    </svg>
  </button>
)
const XBtn = ({ onClick }) => (
  <button style={{ background: 'none', border: 'none', cursor: 'pointer', fontSize: 16, color: 'var(--binance-red)', padding: '4px 8px' }} onClick={onClick}>×</button>
)

const TableHeader = () => (
  <thead>
    <tr>
      <th style={{ width: 40 }}></th>
      <th>交易标的 / 平台</th>
      <th>趋势概览 (EMA200)</th>
      <th>策略信号</th>
      <th>信号详情</th>
      <th style={{ textAlign: 'right' }}>操作建议</th>
    </tr>
  </thead>
)

const PushToggle = ({ enabled, onChange }) => (
  <label style={{
    display: 'inline-flex', alignItems: 'center', gap: 8,
    color: 'var(--text-secondary)', fontSize: 13, cursor: 'pointer',
    marginLeft: 'auto', whiteSpace: 'nowrap',
  }}>
    <input
      type="checkbox"
      checked={!!enabled}
      onChange={e => onChange?.(e.target.checked)}
      style={{ accentColor: 'var(--accent)', cursor: 'pointer' }}
    />
    参与信号推送
  </label>
)

/* ── 主自选列表 Tab ── */
// watchlist: [{ key, symbol, exchangeId, display, exchangeLabel }]
export function WatchlistTab({
  watchlist, scanData, exchangeLabel, watchlistMode,
  searchQ, setSearchQ, showDropdown, setShowDropdown, searchWrapRef,
  filteredSymbols, searchStatus, addSymbol, removeSymbol,
  showPushToggle = false, pushEnabled = false, onTogglePush,
}) {
  // 用 symbol 作为 scanMap key（后端返回的）
  const scanMap = {}
  scanData.forEach(d => { scanMap[d.symbol] = d })

  const isMovers = watchlistMode === 'movers'
  const [openChartKey, setOpenChartKey] = React.useState('')

  return (
    <div className="list-container">
      {!isMovers ? (
        <SymbolSearch
          wrapRef={searchWrapRef}
          value={searchQ}
          onChange={e => { setSearchQ(e.target.value); setShowDropdown(true) }}
          onFocus={() => setShowDropdown(true)}
          onKeyDown={e => {
            if (e.key !== 'Enter') return
            if (filteredSymbols.length > 0) addSymbol(filteredSymbols[0])
          }}
          showDropdown={showDropdown}
          filteredSymbols={filteredSymbols}
          searchStatus={searchStatus}
          inListFn={(key) => watchlist.some(it => it.key === key)}
          onAdd={addSymbol}
        />
      ) : (
        <div style={{ 
          padding: '12px 20px', 
          background: 'rgba(77, 184, 255, 0.1)', 
          border: '1px solid rgba(77, 184, 255, 0.2)',
          borderRadius: 8,
          marginBottom: 16,
          display: 'flex',
          alignItems: 'center',
          gap: 12
        }}>
          <div style={{ 
            width: 8, height: 8, borderRadius: '50%', background: 'var(--binance-green)',
            boxShadow: '0 0 10px var(--binance-green)'
          }}></div>
          <span style={{ color: 'var(--accent)', fontWeight: 600, fontSize: 14 }}>
            监控列表
          </span>
          <span style={{ color: 'var(--text-secondary)', fontSize: 12 }}>每 5 分钟自动更新</span>
          {showPushToggle && <PushToggle enabled={pushEnabled} onChange={onTogglePush} />}
        </div>
      )}

      <table className="bn-table">
        <TableHeader />
        <tbody>
          {watchlist.length === 0 ? (
            <tr>
              <td colSpan={6} style={{ textAlign: 'center', padding: 40 }}>
                <div style={{ color: 'var(--binance-green)', fontSize: 16, fontWeight: 600, marginBottom: 10 }}>
                  {exchangeLabel} 数据已加载完毕
                </div>
                <div style={{ color: 'var(--text-secondary)', fontSize: 14 }}>
                  {isMovers ? '正在加载异动标的...' : '请通过上方搜索框添加标的开始监控 (最多20个)'}
                </div>
              </td>
            </tr>
          ) : watchlist.map(wlItem => (
            <SymbolTableRow
              key={wlItem.key}
              wlItem={wlItem}
              scanItem={scanMap[wlItem.symbol]}
              showExLink
              chartOpen={openChartKey === wlItem.key}
              onToggleChart={() => setOpenChartKey(openChartKey === wlItem.key ? '' : wlItem.key)}
              removeBtn={!isMovers 
                ? <XBtn onClick={() => removeSymbol(wlItem.key)} /> 
                : <StarBtn onClick={() => addSymbol(wlItem)} />
              }
            />
          ))}
        </tbody>
      </table>
    </div>
  )
}

/* ── 自建列表组 Tab（symbols 仍是字符串数组，暂不改） ── */
export function GroupTab({
  group, scanData, exchangeLabel,
  groupSearchQ, setGroupSearchQ, groupShowDropdown, setGroupShowDropdown, groupSearchWrapRef,
  filteredGroupSymbols, groupSearchStatus, addSymbolToGroup, removeSymbolFromGroup,
  showPushToggle = false, pushEnabled = false, onTogglePush,
}) {
  const scanMap = {}
  scanData.forEach(d => { scanMap[d.symbol] = d })
  const symbols = group.symbols || []
  const [openChartKey, setOpenChartKey] = React.useState('')

  return (
    <div className="list-container">
      <SymbolSearch
        wrapRef={groupSearchWrapRef}
        value={groupSearchQ}
        onChange={e => { setGroupSearchQ(e.target.value); setGroupShowDropdown(true) }}
        onFocus={() => setGroupShowDropdown(true)}
        onKeyDown={e => { if (e.key === 'Enter' && filteredGroupSymbols.length > 0) addSymbolToGroup(group.id, filteredGroupSymbols[0]) }}
        showDropdown={groupShowDropdown}
        filteredSymbols={filteredGroupSymbols}
        searchStatus={groupSearchStatus}
        inListFn={(key) => {
          // GroupTab 里 symbols 还是字符串，从 key 提取 symbol 部分比对
          const sym = key.includes('@') ? key.split('@')[0] : key
          return symbols.includes(sym)
        }}
        onAdd={sym => addSymbolToGroup(group.id, sym)}
      />

      {showPushToggle && (
        <div style={{ display: 'flex', justifyContent: 'flex-end', margin: '-8px 0 14px' }}>
          <PushToggle enabled={pushEnabled} onChange={onTogglePush} />
        </div>
      )}

      <table className="bn-table">
        <TableHeader />
        <tbody>
          {symbols.length === 0 ? (
            <tr>
              <td colSpan={6} style={{ textAlign: 'center', padding: 40 }}>
                <div style={{ color: 'var(--binance-green)', fontSize: 16, fontWeight: 600, marginBottom: 10 }}>数据已加载完毕</div>
                <div style={{ color: 'var(--text-secondary)', fontSize: 14 }}>通过上方搜索框向「{group.name}」添加标的</div>
              </td>
            </tr>
          ) : symbols.map(sym => {
              // GroupTab 用虚拟 wlItem 兼容 SymbolTableRow
              const wlItem = { key: sym, symbol: sym, exchangeId: 'binance', display: sym, exchangeLabel }
              return (
                <SymbolTableRow
                  key={sym}
                  wlItem={wlItem}
                  scanItem={scanMap[sym]}
                  showExLink={false}
                  chartOpen={openChartKey === sym}
                  onToggleChart={() => setOpenChartKey(openChartKey === sym ? '' : sym)}
                  removeBtn={<XBtn onClick={() => removeSymbolFromGroup(group.id, sym)} />}
                />
              )
            })
          }
        </tbody>
      </table>
    </div>
  )
}
