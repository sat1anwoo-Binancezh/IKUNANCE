from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass
class MacdSignal:
    action: str
    signal: str
    detail: str
    price: float
    trend: str
    candle_time: float


@dataclass
class MacdState:
    symbol: str
    fast_len: int = 12
    slow_len: int = 26
    signal_len: int = 9
    trend_len: int = 200
    ema_fast: Optional[float] = None
    ema_slow: Optional[float] = None
    signal_line: Optional[float] = None
    trend_ema: Optional[float] = None
    hist_tail: List[float] = field(default_factory=list)
    close_tail: List[float] = field(default_factory=list)
    last_candle_time: Optional[float] = None
    closed_count: int = 0

    def _ema(self, previous: Optional[float], value: float, span: int) -> float:
        if previous is None:
            return value
        alpha = 2.0 / (span + 1)
        return alpha * value + (1 - alpha) * previous

    def update_closed_kline(self, row: List[float]) -> Optional[MacdSignal]:
        if not isinstance(row, (list, tuple)) or len(row) < 5:
            return None
        candle_time = float(row[0])
        close = float(row[4])
        if self.last_candle_time is not None and candle_time <= self.last_candle_time:
            return None

        self.ema_fast = self._ema(self.ema_fast, close, self.fast_len)
        self.ema_slow = self._ema(self.ema_slow, close, self.slow_len)
        macd_line = self.ema_fast - self.ema_slow
        self.signal_line = self._ema(self.signal_line, macd_line, self.signal_len)
        hist = macd_line - self.signal_line
        self.trend_ema = self._ema(self.trend_ema, close, self.trend_len)

        self.hist_tail.append(hist)
        self.hist_tail = self.hist_tail[-3:]
        self.close_tail.append(close)
        self.close_tail = self.close_tail[-3:]
        self.last_candle_time = candle_time
        self.closed_count += 1

        if len(self.hist_tail) < 3:
            return None
        return self.classify_current()

    def classify_current(self) -> Optional[MacdSignal]:
        if len(self.hist_tail) < 3 or not self.close_tail:
            return None
        prev2, prev1, curr = self.hist_tail[-3], self.hist_tail[-2], self.hist_tail[-1]
        price = self.close_tail[-1]
        trend = "BULL" if self.trend_ema is not None and price > self.trend_ema else "BEAR"

        if curr > prev1 and prev1 < prev2 and prev1 < 0:
            return MacdSignal("LONG", "Trend confirmation", "Red histogram shrinking", price, trend, self.last_candle_time or 0)
        if curr > 0 and curr > prev1 and prev1 < prev2:
            return MacdSignal("LONG", "Trend evolution", "Green histogram first strengthening", price, trend, self.last_candle_time or 0)
        if curr < prev1 and prev1 > prev2 and prev1 > 0:
            return MacdSignal("SHORT", "Trend confirmation", "Green histogram shrinking", price, trend, self.last_candle_time or 0)
        if curr < 0 and curr < prev1 and prev1 > prev2:
            return MacdSignal("SHORT", "Trend evolution", "Red histogram first strengthening", price, trend, self.last_candle_time or 0)
        return MacdSignal("-", "-", "-", price, trend, self.last_candle_time or 0)

    def bootstrap(self, rows: List[List[float]]) -> Optional[MacdSignal]:
        signal = None
        for row in sorted(rows or [], key=lambda item: item[0] if isinstance(item, (list, tuple)) and item else 0):
            signal = self.update_closed_kline(row)
        return signal

    def snapshot(self) -> Dict:
        return {
            "symbol": self.symbol,
            "warm": self.closed_count >= 50 and len(self.hist_tail) >= 3,
            "closedCount": self.closed_count,
            "lastCandleTime": self.last_candle_time,
            "emaFast": self.ema_fast,
            "emaSlow": self.ema_slow,
            "signalLine": self.signal_line,
            "trendEma": self.trend_ema,
            "histTail": list(self.hist_tail),
            "closeTail": list(self.close_tail),
        }


class MacdStateManager:
    def __init__(self):
        self._states: Dict[str, MacdState] = {}

    def get(self, symbol: str) -> MacdState:
        key = str(symbol or "").upper()
        if key not in self._states:
            self._states[key] = MacdState(symbol=key)
        return self._states[key]

    def bootstrap(self, symbol: str, rows: List[List[float]]) -> MacdState:
        state = MacdState(symbol=str(symbol or "").upper())
        state.bootstrap(rows)
        self._states[state.symbol] = state
        return state

    def health(self) -> Dict:
        warm = sum(1 for state in self._states.values() if state.snapshot()["warm"])
        return {
            "trackedSymbols": len(self._states),
            "warmSymbols": warm,
            "symbols": {symbol: state.snapshot() for symbol, state in self._states.items()},
        }
