/**
 * useWatchlist 鈥?鑷€夊垪琛?+ 鎵弿鏁版嵁 hook
 *
 * 鏁版嵁妯″瀷锛?
 *   watchlist 瀛樼殑姣忔潯鏄?{ key, symbol, exchangeId, display }
 *   key = symbol + '@' + exchangeId锛屼緥濡?"BTC/USDT@okx"
 *   鍚屼竴鏍囩殑鍦ㄤ笉鍚屼氦鏄撴墍鏄畬鍏ㄧ嫭绔嬬殑鏉＄洰锛屽彲浠ュ悓鏃剁洃鎺?
 *
 * 瀛樺偍锛氭瘡涓氦鏄撴墍鐙珛 key锛歩kun_wl_binance / ikun_wl_okx ...
 *       鍒囨崲浜ゆ槗鎵€鍙槸鍒囨崲"褰撳墠婵€娲讳氦鏄撴墍"锛屾墍鏈変氦鏄撴墍鐨勫垪琛ㄩ兘鍦ㄥ唴瀛橀噷
 *
 * 鎵弿锛歠etchData 鎶婃墍鏈変氦鏄撴墍鐨勬爣鐨勬寜浜ゆ槗鎵€鍒嗙粍锛屽垎鍒姹傚悗绔?
 */
import { useState, useEffect, useRef, useCallback } from 'react'

export const EXCHANGE_LABELS = {
  binance: 'Binance', okx: 'OKX', bybit: 'Bybit',
  bitget: 'Bitget', gate: 'Gate.io', kucoin: 'KuCoin',
  binance_stock: 'Binance Stock',
}

// 鈹€鈹€ 涓绘祦姘哥画鍚堢害鍐呯疆鍒楄〃锛坆ase 鍚嶇О锛夆攢鈹€
const BUILTIN_BASES = [
  'BTC','ETH','BNB','SOL','XRP','DOGE','ADA','AVAX','LINK','DOT',
  'MATIC','UNI','LTC','BCH','ATOM','ETC','XLM','ALGO','ICP','FIL',
  'APT','ARB','OP','SUI','SEI','TIA','INJ','WLD','BLUR','PEPE',
  'FLOKI','SHIB','BONE','LDO','AAVE','CRV','MKR','SNX','COMP','YFI',
  'RUNE','NEAR','FTM','ONE','VET','THETA','EOS','XTZ','ZEC','DASH',
  'SAND','MANA','AXS','ENJ','CHZ','GALA','IMX','RNDR','GRT','1INCH',
  'DYDX','GMX','PENDLE','PYTH','JTO','MEME','BONK','WIF','BOME','MYRO',
  'ORDI','SATS','RATS','LUNC','USTC','CFX','STX','AGIX','FET','OCEAN',
  'ONDO','JUP','STRK','ALT','PIXEL','PORTAL','MANTA','ZK','EIGEN','LISTA',
  'TON','NOT','DOGS','HMSTR','CATI','MAJOR','WEN','POPCAT','TURBO','NEIRO',
]

const HOT_BASES = [
  'PNUT','ACT','GOAT','MOODENG','PENGU','VIRTUAL','AIXBT','FARTCOIN','TRUMP','MELANIA',
  'BERA','KAITO','HYPE','PUMP','WAL','LAYER','PARTI','INIT','SIGN','SOPH','HUMA',
]

// 鈹€鈹€鈹€ 瀛樺偍杈呭姪 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€
const WATCHLIST_LIMIT = 20
const LEGACY_WL_KEY = (eid) => `ikun_wl_${eid || 'binance'}`
const safeScope = (scope) => String(scope || 'anon').replace(/[^a-zA-Z0-9_.-]/g, '_').slice(0, 80) || 'anon'
const WL_KEY = (eid, scope = 'anon') => `ikun_wl_${safeScope(scope)}_${eid || 'binance'}`

function apiHeaders(extra = {}) {
  const token = localStorage.getItem('ikun_token') || ''
  return token ? { ...extra, 'X-Token': token } : extra
}

// 姣忔潯璁板綍: { key, symbol, exchangeId, display }
function loadList(eid, scope = 'anon') {
  try {
    const raw = localStorage.getItem(WL_KEY(eid, scope))
    if (raw !== null) return JSON.parse(raw || '[]')
    if (safeScope(scope) === 'anon') return JSON.parse(localStorage.getItem(LEGACY_WL_KEY(eid)) || '[]')
    return []
  }
  catch { return [] }
}
function saveList(eid, list, scope = 'anon') {
  localStorage.setItem(WL_KEY(eid, scope), JSON.stringify(list))
}

function makeItem(base, eid, extra = {}) {
  const normalizedBase = normalizeBase(base)
  const exchangeId = eid || 'binance'
  const label = EXCHANGE_LABELS[exchangeId] || exchangeId.toUpperCase()
  const marketType = extra.marketType || (exchangeId === 'binance_stock' ? 'stock' : 'futures')
  return {
    key:        `${normalizedBase}/USDT@${exchangeId}`,   // 鍞竴鏍囪瘑锛屽彲璺ㄤ氦鏄撴墍閲嶅鍚屾爣鐨?
    symbol:     `${normalizedBase}/USDT`,
    exchangeId,
    display:    extra.display || `${normalizedBase}USDT.P`,
    exchangeLabel: extra.exchangeLabel || label,
    marketType,
  }
}

function makeSearchItem(candidate, fallbackExchangeId = 'binance') {
  if (!candidate) return null
  if (typeof candidate === 'string') return makeItem(candidate, fallbackExchangeId)
  const marketType = candidate.marketType || (candidate.exchangeId === 'binance_stock' ? 'stock' : 'futures')
  const exchangeId = candidate.exchangeId || candidate.exchange || (marketType === 'stock' ? 'binance_stock' : fallbackExchangeId)
  const base = normalizeBase(candidate.displayName || candidate.display || candidate.symbol || candidate.baseAsset)
  if (!base) return null
  return makeItem(base, exchangeId, {
    exchangeLabel: candidate.exchangeLabel,
    marketType,
  })
}

function mergeSearchItems(fallbackExchangeId, ...lists) {
  const merged = []
  const seen = new Set()
  lists.flat().forEach(candidate => {
    const item = makeSearchItem(candidate, fallbackExchangeId)
    if (!item || seen.has(item.key)) return
    seen.add(item.key)
    merged.push(item)
  })
  return merged
}

// 鈹€鈹€鈹€ Hook 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€
/**
 * @param {string} timeframe
 * @param {string} triggerMode
 * @param {function} showToast
 * @param {string} exchangeId  鈥?褰撳墠婵€娲荤殑浜ゆ槗鎵€锛堟悳绱?娣诲姞鏃剁敤锛?
 */
export function useWatchlist(timeframe, triggerMode, showToast, exchangeId = 'binance', storageScope = 'anon') {
  const scope = safeScope(storageScope)
  // 妯″紡锛歠avorites (鑷€? / movers (娑ㄨ穼姒?
  const [watchlistMode, setWatchlistMode] = useState('favorites')
  
  const [marketMovers, setMarketMovers] = useState([])
  const [moversScanData, setMoversScanData] = useState([])

  // 褰撳墠婵€娲讳氦鏄撴墍鐨勮嚜閫夊垪琛紙鏄剧ず鐢級
  const [watchlist, setWatchlist] = useState(() => loadList(exchangeId, scope))
  const [scanData,   setScanData]   = useState([])
  const [connStatus, setConnStatus] = useState('灏辩华')

  const [allSymbols, setAllSymbols] = useState(() => mergeSearchItems(exchangeId, BUILTIN_BASES, HOT_BASES))
  const [symbolsVerified, setSymbolsVerified] = useState(false)

  const prevExchangeRef = useRef(`${exchangeId}:${scope}`)
  const [searchQ,       setSearchQ]       = useState('')
  const [showDropdown,  setShowDropdown]  = useState(false)
  const [searchStatus, setSearchStatus] = useState('idle')
  const searchWrapRef                     = useRef(null)
  const [groupSearchQ,      setGroupSearchQ]      = useState('')
  const [groupShowDropdown, setGroupShowDropdown] = useState(false)
  const [groupSearchStatus, setGroupSearchStatus] = useState('idle')
  const groupSearchWrapRef                         = useRef(null)

  // 鈹€鈹€ 杞藉叆鍏ㄩ噺鏍囩殑 鈹€鈹€
  useEffect(() => {
    setSymbolsVerified(false)
    fetch(`/api/all_symbols?exchange=${encodeURIComponent(exchangeId)}&structured=1`).then(r => r.json()).then(d => {
      const records = Array.isArray(d) ? d : (Array.isArray(d?.data) ? d.data : d?.symbols)
      if (Array.isArray(records) && records.length > 0) {
        setAllSymbols(mergeSearchItems(exchangeId, records))
        setSymbolsVerified(true)
      }
    }).catch(() => {})
  }, [exchangeId])

  useEffect(() => {
    const timers = []
    const runSearch = (q, setStatus) => {
      const base = normalizeBase(q)
      if (base.length < 2) {
        setStatus('idle')
        return
      }
      setStatus('loading')
      const timer = setTimeout(() => {
        fetch(`/api/search_symbols?exchange=${encodeURIComponent(exchangeId)}&q=${encodeURIComponent(base)}`)
          .then(r => r.json())
          .then(res => {
            const data = Array.isArray(res) ? res : (Array.isArray(res?.records) ? res.records : res.data)
            if (Array.isArray(data) && data.length > 0) {
              setAllSymbols(prev => mergeSearchItems(exchangeId, prev, data))
              setStatus('done')
              return
            }
            if (res && res.status === 'error') setStatus('error')
            else if (res && res.liveChecked) setStatus('empty')
          })
          .catch(() => setStatus('error'))
      }, 220)
      timers.push(timer)
    }

    runSearch(searchQ, setSearchStatus)
    runSearch(groupSearchQ, setGroupSearchStatus)
    return () => timers.forEach(clearTimeout)
  }, [searchQ, groupSearchQ, exchangeId])

  const fetchMovers = useCallback(() => {}, [])

  // 鈹€鈹€ 鍒囨崲浜ゆ槗鎵€锛氬垏鎹㈠睍绀哄垪琛紝娓呯┖褰撳墠鎵弿缁撴灉 鈹€鈹€
  useEffect(() => {
    const key = `${exchangeId}:${scope}`
    if (prevExchangeRef.current === key) return
    prevExchangeRef.current = key
    setWatchlist(loadList(exchangeId, scope))
    setScanData([])
    setConnStatus('灏辩华')
    setSearchQ('')
    setShowDropdown(false)
  }, [exchangeId, scope])

  // 鈹€鈹€ 鍚庣 watchlist 鍚屾浜嬩欢锛氱櫥褰曞悗鏈嶅姟鍣ㄦ暟鎹啓鍏?localStorage锛屽埛鏂板垪琛?鈹€鈹€
  useEffect(() => {
    const handler = () => {
      setWatchlist(loadList(exchangeId, scope))
      setScanData([])
    }
    window.addEventListener('ikun_wl_sync', handler)
    return () => window.removeEventListener('ikun_wl_sync', handler)
  }, [exchangeId, scope])

  const lastFetchKey = useRef('')
  const exLabel = EXCHANGE_LABELS[exchangeId] || exchangeId.toUpperCase()

  // 鈹€鈹€ 鐐瑰嚮澶栭儴鏀惰捣涓嬫媺 鈹€鈹€
  useEffect(() => {
    const off = (e) => {
      if (searchWrapRef.current && !searchWrapRef.current.contains(e.target))
        setShowDropdown(false)
      if (groupSearchWrapRef.current && !groupSearchWrapRef.current.contains(e.target))
        setGroupShowDropdown(false)
    }
    document.addEventListener('mousedown', off)
    return () => document.removeEventListener('mousedown', off)
  }, [])

  // 鈹€鈹€ 瀹氭椂鎵弿 鈹€鈹€
  useEffect(() => {
    const timer = setInterval(() => {
      const activeList = watchlistMode === 'favorites' ? watchlist : marketMovers
      if (activeList.length === 0) return
      
      const now  = new Date()
      const min  = now.getMinutes(), sec = now.getSeconds(), hr = now.getHours()
      const iSec = { '15m': 900, '1h': 3600, '4h': 14400, '1d': 86400 }[timeframe] || 3600
      let elapsed = 0
      if      (timeframe === '15m') elapsed = (min % 15) * 60 + sec
      else if (timeframe === '1h')  elapsed = min * 60 + sec
      else if (timeframe === '4h')  elapsed = (hr % 4) * 3600 + min * 60 + sec
      else                          elapsed = hr * 3600 + min * 60 + sec
      const remain  = iSec - elapsed
      const scanKey = `${timeframe}:${Math.floor(Date.now() / (iSec * 1000))}`
      if (elapsed <= 10 && lastFetchKey.current !== scanKey) {
        lastFetchKey.current = scanKey
        fetchData()
      } else if (elapsed > 10) {
        const txt = remain > 60 ? `${Math.ceil(remain / 60)}分钟` : `${remain}秒`
        setConnStatus(`下次扫描: ${txt}后`)
      }
    }, 5000)
    return () => clearInterval(timer)
  }, [timeframe, triggerMode, watchlist, marketMovers, watchlistMode]) // eslint-disable-line

  // 鈹€鈹€ fetchData锛氭妸褰撳墠鍒楄〃鐨勬爣鐨勫彂缁欏悗绔壂鎻?鈹€鈹€
  const fetchData = useCallback(() => {
    const activeList = watchlistMode === 'favorites' ? watchlist : marketMovers
    const targetExchange = watchlistMode === 'favorites' ? exchangeId : 'binance'

    if (activeList.length === 0) {
      if (watchlistMode === 'favorites') setScanData([])
      else setMoversScanData([])
      setConnStatus('灏辩华'); return
    }
    setConnStatus('鎵弿涓?..')
    // 浼犲綋鍓嶄氦鏄撴墍 + 褰撳墠鍒楄〃閲岀殑瑁?symbol锛堝悗绔寜 exchange 璺敱鍒板搴斾氦鏄撴墍锛?
    const symbolsParam = activeList.map(it => it.symbol).join(',')
    fetch(`/api/scan?timeframe=${timeframe}&trigger=${triggerMode}&exchange=${targetExchange}&symbols=${encodeURIComponent(symbolsParam)}`)
      .then(r => r.json())
      .then(res => {
        const data = (res.data || []).sort((a, b) => {
          const ia = activeList.findIndex(it => it.symbol === a.symbol)
          const ib = activeList.findIndex(it => it.symbol === b.symbol)
          return (ia === -1 ? 9999 : ia) - (ib === -1 ? 9999 : ib)
        })
        if (watchlistMode === 'favorites') setScanData(data)
        else setMoversScanData(data)
        
        const label = watchlistMode === 'favorites' ? exLabel : 'Binance'
        setConnStatus(`宸茶繛鎺?${label}`)
      })
      .catch(() => setConnStatus('杩炴帴澶辫触'))
  }, [watchlist, marketMovers, watchlistMode, timeframe, triggerMode, exchangeId, exLabel])

  // 鈹€鈹€ addSymbol锛氬甫浜ゆ槗鎵€淇℃伅娣诲姞锛屽悓浜ゆ槗鎵€鍘婚噸锛岃法浜ゆ槗鎵€鍏佽 鈹€鈹€
  function addSymbol(item) {
    const eid = item.exchangeId || exchangeId
    let base
    if (typeof item === 'string') {
      base = item.toUpperCase().replace(/\/USDT.*/, '').replace('USDT', '').replace('.P', '').trim()
    } else {
      base = (item.base || item.symbol || '').toUpperCase()
        .replace(/\/USDT.*/, '').replace('USDT', '').replace('.P', '').trim()
    }
    if (!base) return

    const newItem = makeItem(base, eid)

    const existing = loadList(eid, scope)
    if (existing.find(it => it.key === newItem.key)) {
      showToast(`${newItem.display} 宸插湪 ${newItem.exchangeLabel} 鑷€変腑`); return
    }
    if (existing.length >= WATCHLIST_LIMIT) {
      showToast(`${newItem.exchangeLabel} 鑷€変笂闄?{WATCHLIST_LIMIT}涓紝璇峰厛绉婚櫎`); return
    }

    const next = [...existing, newItem]
    saveList(eid, next, scope)

    if (eid === exchangeId) {
      setWatchlist(next)
      setSearchQ('')
      setShowDropdown(false)
    }
    showToast(`宸叉坊鍔?${newItem.display} 路 ${newItem.exchangeLabel}`)

    // 閫氱煡鍚庣 鈫?鍚庣浼氱珛鍗冲惎鍔ㄥ搴?WebSocket 璁㈤槄
    fetch('/api/add_symbol', {
      method: 'POST',
      headers: apiHeaders({ 'Content-Type': 'application/json' }),
      body: JSON.stringify({ symbol: newItem.symbol, exchange: eid, timeframe })
    }).then(async (response) => {
      let payload = {}
      try { payload = await response.json() } catch {}
      if (!response.ok || payload.status === 'error') {
        saveList(eid, existing, scope)
        if (eid === exchangeId) setWatchlist(existing)
        showToast(payload.msg || `${newItem.display} 未通过官方标的校验`)
        return
      }
      if (Array.isArray(payload.watchlist)) {
        const synced = payload.watchlist
          .filter(row => (row.exchange || row.exchangeId || eid) === eid)
          .map(row => makeItem(row.symbol, row.exchange || row.exchangeId || eid))
        saveList(eid, synced, scope)
        if (eid === exchangeId) setWatchlist(synced)
      }
    }).catch(() => {})
  }

  function removeSymbol(key) {
    // key 鏍煎紡: "BTC/USDT@okx"
    const [symPart, eid] = key.includes('@') ? key.split('@') : [key, exchangeId]
    const existing = loadList(eid, scope)
    const next = existing.filter(it => it.key !== key)
    saveList(eid, next, scope)

    if (eid === exchangeId) {
      setWatchlist(next)
      setScanData(prev => prev.filter(x => x.symbol !== symPart))
    }
    showToast(`宸茬Щ闄?${symPart} 路 ${EXCHANGE_LABELS[eid] || eid}`)

    fetch('/api/remove_symbol', {
      method: 'POST',
      headers: apiHeaders({ 'Content-Type': 'application/json' }),
      body: JSON.stringify({ symbol: symPart, exchange: eid, timeframe })
    }).catch(() => {})
  }

  // 鈹€鈹€ 鎼滅储锛氫娇鐢ㄥ悗绔叏閲忔爣鐨?鈹€鈹€
  function buildResults(q) {
    if (!q) return []
    const upper = q.toUpperCase()
      .replace(/\/USDT.*/, '').replace('USDT', '').replace('.P', '').trim()
    if (!upper) return []
    
    const source = mergeSearchItems(
      exchangeId,
      symbolsVerified ? [] : BUILTIN_BASES,
      symbolsVerified ? [] : HOT_BASES,
      allSymbols,
      marketMovers,
    )
    
    return source
      .filter(item => {
        const base = normalizeBase(item.symbol)
        return base === upper || base.startsWith(upper) || base.includes(upper)
      })
      .sort((a, b) => symbolRank(normalizeBase(a.symbol), upper) - symbolRank(normalizeBase(b.symbol), upper))
      .slice(0, 20)
  }

  const filteredSymbols      = buildResults(searchQ)
  const filteredGroupSymbols = buildResults(groupSearchQ)

  return {
    watchlist, setWatchlist,
    marketMovers, moversScanData,
    watchlistMode, setWatchlistMode,
    scanData,  setScanData,
    connStatus, setConnStatus,
    searchQ, setSearchQ,
    showDropdown, setShowDropdown,
    searchStatus,
    searchWrapRef,
    groupSearchQ, setGroupSearchQ,
    groupShowDropdown, setGroupShowDropdown,
    groupSearchStatus,
    groupSearchWrapRef,
    filteredSymbols,
    filteredGroupSymbols,
    fetchData,
    addSymbol,
    removeSymbol,
    symbolsLoaded: true,
    allSymbols,
  }
}

function normalizeBase(value) {
  return String(value || '')
    .toUpperCase()
    .replace(/\/USDT.*/, '')
    .replace(/USDT.*/, '')
    .replace('.P', '')
    .trim()
}

function mergeBases(...lists) {
  const merged = []
  const seen = new Set()
  lists.flat().forEach(item => {
    const base = normalizeBase(item)
    if (!base || seen.has(base)) return
    seen.add(base)
    merged.push(base)
  })
  return merged
}

function symbolRank(base, query) {
  const upper = String(base || '').toUpperCase()
  if (upper === query) return 0
  if (upper.startsWith(query)) return 1
  return 2
}

