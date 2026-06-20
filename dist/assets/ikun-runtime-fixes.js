(function () {
  var PATCH_FLAG = "__ikun_runtime_fixes_20260604__";
  if (window[PATCH_FLAG]) return;
  window[PATCH_FLAG] = true;

  var recentSignals = {};
  var recentOrder = [];

  function escapeHtml(value) {
    return String(value == null ? "" : value)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#39;");
  }

  function normalizeSymbol(value) {
    var raw = typeof value === "string" ? value : value && value.symbol;
    var symbol = String(raw || "").toUpperCase().trim();
    if (!symbol) return "";
    if (symbol.indexOf("@") !== -1) symbol = symbol.split("@")[0];
    if (symbol.indexOf(":") !== -1) symbol = symbol.split(":")[0];
    if (symbol.indexOf("/") === -1) {
      symbol = symbol.replace(/USDT.*/, "") + "/USDT";
    } else if (!/\/USDT$/.test(symbol)) {
      symbol = symbol.replace(/\/.*/, "") + "/USDT";
    }
    return symbol;
  }

  function signalType(signal) {
    return signal.signal || signal.type || "-";
  }

  function signalAction(signal) {
    return signal.action || (String(signalType(signal)).toUpperCase().indexOf("LONG") !== -1 ? "LONG" : "") || "-";
  }

  function signalKey(signal) {
    return [
      normalizeSymbol(signal),
      signalType(signal),
      signalAction(signal),
      signal.detail || "",
      signal.candle_time || signal.time || signal.trigger_time || "",
    ].join("|");
  }

  function exchangeFromRow(row) {
    var label = (row.querySelector("td:nth-child(2) span") || {}).textContent || "";
    label = label.toLowerCase();
    if (label.indexOf("okx") !== -1) return "okx";
    if (label.indexOf("bybit") !== -1) return "bybit";
    if (label.indexOf("bitget") !== -1) return "bitget";
    if (label.indexOf("gate") !== -1) return "gate";
    if (label.indexOf("kucoin") !== -1) return "kucoin";
    return "binance";
  }

  function tradeUrl(symbol, exchange) {
    var raw = symbol.replace("/", "");
    var base = symbol.replace("/USDT", "");
    if (exchange === "okx") return "https://www.okx.com/trade-swap/" + symbol.replace("/", "-").toLowerCase() + "-swap";
    if (exchange === "bybit") return "https://www.bybit.com/trade/usdt/" + base;
    if (exchange === "bitget") return "https://www.bitget.com/futures/usdt/" + raw;
    if (exchange === "gate") return "https://www.gate.io/futures_trade/USDT/" + base + "_USDT";
    if (exchange === "kucoin") return "https://www.kucoin.com/futures/trade/" + base + "USDTM";
    return "https://www.binance.com/zh-CN/futures/" + raw;
  }

  function trendHtml(trend, action) {
    var bullish = trend === "BULL" || action === "LONG";
    var bearish = trend === "BEAR" || action === "SHORT";
    if (!bullish && !bearish) return '<span style="color:#555">-</span>';
    var cls = bullish ? "trend-bull" : "trend-bear";
    var icon = bullish
      ? '<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" style="margin-right:4px;vertical-align:-2px"><polyline points="23 6 13.5 15.5 8.5 10.5 1 18"></polyline><polyline points="17 6 23 6 23 12"></polyline></svg>'
      : '<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" style="margin-right:4px;vertical-align:-2px"><polyline points="23 18 13.5 8.5 8.5 13.5 1 6"></polyline><polyline points="17 18 23 18 23 12"></polyline></svg>';
    return '<span class="' + cls + '">' + icon + (bullish ? "\u591a\u5934 Bull" : "\u7a7a\u5934 Bear") + "</span>";
  }

  function badgeHtml(type, action) {
    if (!type || type === "-") return '<span style="color:#555">-</span>';
    var cls = action === "LONG" ? "signal-badge badge-yellow" : "signal-badge badge-purple";
    return '<span class="' + cls + '">' + escapeHtml(type) + "</span>";
  }

  function actionHtml(signal, symbol, exchange) {
    var type = signalType(signal);
    var action = signalAction(signal);
    if (!type || type === "-" || (action !== "LONG" && action !== "SHORT")) {
      return '<span style="color:#555;font-size:12px">\u89c2\u671b Wait</span>';
    }
    var cls = action === "LONG" ? "btn-action btn-long" : "btn-action btn-short";
    var label = action === "LONG" ? "\u5f00\u591a LONG" : "\u5f00\u7a7a SHORT";
    return '<a href="' + escapeHtml(tradeUrl(symbol, exchange)) + '" target="_blank" rel="noreferrer" class="' + cls + '" style="text-decoration:none;display:inline-block;color:#fff">' + label + "</a>";
  }

  function findSignalPayload(payload) {
    if (!payload || typeof payload !== "object") return null;
    if (payload.symbol) return payload;
    if (payload.data && payload.data.symbol) return payload.data;
    if (payload.signal && typeof payload.signal === "object" && payload.signal.symbol) return payload.signal;
    return null;
  }

  function applySignalToRows(signal) {
    signal = findSignalPayload(signal);
    var symbol = normalizeSymbol(signal);
    if (!symbol) return false;

    var key = signalKey(signal);
    var type = signalType(signal);
    var action = signalAction(signal);
    var trend = signal.trend || (action === "LONG" ? "BULL" : action === "SHORT" ? "BEAR" : "-");
    var detail = signal.detail || "\u7b56\u7565\u4fe1\u53f7\u89e6\u53d1";
    var updated = false;

    document.querySelectorAll(".bn-table tbody tr").forEach(function (row) {
      var pair = row.querySelector("button.pair-name");
      if (!pair || normalizeSymbol(pair.textContent) !== symbol) return;
      if (row.dataset.ikunSignalKey === key) return;

      var cells = row.children;
      if (!cells || cells.length < 6) return;
      var exchange = exchangeFromRow(row);
      cells[2].innerHTML = trendHtml(trend, action);
      cells[3].innerHTML = badgeHtml(type, action);
      cells[4].textContent = detail;
      cells[4].style.color = "#888";
      cells[4].style.fontSize = "13px";
      cells[5].innerHTML = actionHtml(signal, symbol, exchange);
      cells[5].style.textAlign = "right";
      row.dataset.ikunSignalKey = key;
      updated = true;
    });

    return updated;
  }

  function rememberSignal(signal) {
    signal = findSignalPayload(signal);
    var symbol = normalizeSymbol(signal);
    if (!symbol) return;
    if (!Object.prototype.hasOwnProperty.call(recentSignals, symbol)) recentOrder.push(symbol);
    recentSignals[symbol] = signal;
    while (recentOrder.length > 50) delete recentSignals[recentOrder.shift()];
  }

  function scheduleSignal(signal) {
    signal = findSignalPayload(signal);
    if (!signal) return;
    rememberSignal(signal);
    [0, 80, 350, 1200].forEach(function (delay) {
      window.setTimeout(function () { applySignalToRows(signal); }, delay);
    });
  }

  function replayRecentSignals() {
    recentOrder.forEach(function (symbol) {
      if (recentSignals[symbol]) applySignalToRows(recentSignals[symbol]);
    });
  }

  function removeMarketMoversUi() {
    document.querySelectorAll("select").forEach(function (select) {
      Array.prototype.slice.call(select.options || []).forEach(function (option) {
        if (/今日涨跌榜|Market Movers/.test(option.textContent || "")) option.remove();
      });
      if (select.value === "movers") {
        select.value = "favorites";
        select.dispatchEvent(new Event("change", { bubbles: true }));
      }
    });

    document.querySelectorAll("button, [role='button'], option").forEach(function (node) {
      var text = (node.textContent || "").replace(/\s+/g, " ").trim();
      if (!/今日涨跌榜|Market Movers/.test(text)) return;
      if (node.tagName === "OPTION") node.remove();
      else {
        node.style.display = "none";
        node.setAttribute("aria-hidden", "true");
      }
    });

    var showingMovers = false;
    document.querySelectorAll("div, span").forEach(function (node) {
      var text = (node.textContent || "").replace(/\s+/g, " ").trim();
      if (/今日市场异动/.test(text)) showingMovers = true;
    });
    if (showingMovers) {
      var watchlistButton = Array.prototype.slice.call(document.querySelectorAll("button")).find(function (button) {
        return /自选列表/.test(button.textContent || "");
      });
      if (watchlistButton && watchlistButton.offsetParent !== null) watchlistButton.click();
    }
  }

  function closeDesktopSidebar() {
    if (window.innerWidth <= 768) return;
    var overlay = document.querySelector(".sidebar-overlay.show");
    if (overlay) {
      overlay.click();
      overlay.classList.remove("show");
    }
  }

  function installChartCellClick() {
    document.addEventListener("click", function (event) {
      if (event.defaultPrevented) return;
      if (!event.target || !event.target.closest) return;
      if (event.target.closest("button,a,input,select,textarea,label")) return;
      var cell = event.target.closest("td");
      if (!cell) return;
      var pairButton = cell.querySelector("button.pair-name");
      if (!pairButton) return;
      pairButton.click();
    });
  }

  function installEventSourcePatch() {
    var NativeEventSource = window.EventSource;
    if (!NativeEventSource || NativeEventSource.__ikunRuntimePatched) return;

    function PatchedEventSource(url, config) {
      var source = arguments.length > 1 ? new NativeEventSource(url, config) : new NativeEventSource(url);
      try {
        if (String(url || "").indexOf("/api/stream") !== -1) {
          source.addEventListener("message", function (event) {
            try { scheduleSignal(JSON.parse(event.data)); } catch (error) {}
          });
        }
      } catch (error) {}
      return source;
    }

    PatchedEventSource.prototype = NativeEventSource.prototype;
    ["CONNECTING", "OPEN", "CLOSED"].forEach(function (name) {
      try { PatchedEventSource[name] = NativeEventSource[name]; } catch (error) {}
    });
    PatchedEventSource.__ikunRuntimePatched = true;
    window.EventSource = PatchedEventSource;
  }

  function installMutationReplay() {
    if (!window.MutationObserver) return;
    var pending = false;
    var observer = new MutationObserver(function () {
      if (pending) return;
      pending = true;
      window.setTimeout(function () {
        pending = false;
        removeMarketMoversUi();
        replayRecentSignals();
      }, 120);
    });

    function start() {
      if (document.body) observer.observe(document.body, { childList: true, subtree: true });
    }
    if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", start);
    else start();
  }

  window.addEventListener("resize", closeDesktopSidebar);
  window.addEventListener("orientationchange", closeDesktopSidebar);
  window.addEventListener("ikun_runtime_signal", function (event) { scheduleSignal(event.detail); });
  window.__ikunApplySignalToRows = applySignalToRows;

  installEventSourcePatch();
  installChartCellClick();
  installMutationReplay();
  [0, 120, 500, 1200].forEach(function (delay) {
    window.setTimeout(removeMarketMoversUi, delay);
  });

  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", closeDesktopSidebar);
  else closeDesktopSidebar();
})();
