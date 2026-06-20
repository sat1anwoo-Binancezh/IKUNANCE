import { useEffect, useRef, useState } from 'react'

export function useSignalStream({ exchange, symbols, timeframe, enabled, onSignal }) {
  const [streamStatus, setStreamStatus] = useState('未订阅')
  const onSignalRef = useRef(onSignal)

  useEffect(() => {
    onSignalRef.current = onSignal
  }, [onSignal])

  useEffect(() => {
    if (!enabled || !Array.isArray(symbols) || symbols.length === 0) {
      setStreamStatus('未订阅')
      return undefined
    }

    const params = new URLSearchParams({
      exchange: exchange || 'binance',
      timeframe: timeframe || '15m',
      symbols: symbols.join(','),
    })
    const token = localStorage.getItem('ikun_token') || ''
    if (token) params.set('token', token)
    const source = new EventSource(`/api/stream?${params.toString()}`)
    setStreamStatus('连接中')

    source.onopen = () => setStreamStatus('实时监听')
    source.onerror = () => setStreamStatus('连接中断')
    source.onmessage = event => {
      try {
        const payload = JSON.parse(event.data)
        if (payload && payload.type !== 'heartbeat') {
          onSignalRef.current?.(payload)
        }
      } catch {
        // Ignore malformed stream frames; the next frame can still be valid.
      }
    }

    return () => {
      source.close()
      setStreamStatus('未订阅')
    }
  }, [enabled, exchange, timeframe, JSON.stringify(symbols || [])])

  return { streamStatus }
}
