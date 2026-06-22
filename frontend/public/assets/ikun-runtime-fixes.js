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

  var orionMarketState = {
    loading: false,
    loaded: false,
    rankings: {},
    error: "",
    added: {},
  };
  var ORION_OFFICIAL_URL = "https://screener.orionterminal.com/";

  function safeRun(fn) {
    try { return fn && fn(); } catch (error) { return null; }
  }

  function installWhiteScreenGuard() {
    if (window.__ikunWhiteScreenGuardInstalled) return;
    window.__ikunWhiteScreenGuardInstalled = true;

    window.addEventListener("error", function (event) {
      window.__ikunLastFrontendError = event && (event.message || String(event.error || ""));
    });
    window.addEventListener("unhandledrejection", function (event) {
      window.__ikunLastFrontendError = event && event.reason ? String(event.reason.message || event.reason) : "unhandled rejection";
    });

    function rootIsBlank() {
      var root = document.getElementById("root");
      if (!root) return false;
      return root.children.length === 0 && !String(root.textContent || "").trim();
    }

    function renderRecovery() {
      var root = document.getElementById("root");
      if (!root || !rootIsBlank()) return;
      root.innerHTML = [
        '<div style="min-height:100vh;display:flex;align-items:center;justify-content:center;background:#030812;color:#dbeafe;font-family:DM Sans,Arial,sans-serif;">',
        '<div style="border:1px solid rgba(77,184,255,.28);background:rgba(5,13,30,.88);border-radius:10px;padding:18px 22px;text-align:center;max-width:360px;">',
        '<div style="font-family:Rajdhani,Arial,sans-serif;font-size:22px;font-weight:800;margin-bottom:8px;">IKUNANCE</div>',
        '<div style="font-size:13px;color:#9ca3af;margin-bottom:14px;">页面加载异常，请刷新重试。</div>',
        '<button type="button" onclick="location.reload()" style="border:1px solid rgba(77,184,255,.55);background:rgba(77,184,255,.12);color:#4db8ff;border-radius:8px;padding:8px 16px;font-weight:700;cursor:pointer;">刷新页面</button>',
        '</div></div>',
      ].join("");
    }

    function checkBlank() {
      if (!rootIsBlank()) return;
      try {
        if (!sessionStorage.getItem("ikun_white_screen_reloaded")) {
          sessionStorage.setItem("ikun_white_screen_reloaded", "1");
          location.reload();
          return;
        }
      } catch (error) {}
      renderRecovery();
    }

    [2200, 5200].forEach(function (delay) {
      window.setTimeout(checkBlank, delay);
    });
  }

  function installMarketPrehideStyles() {
    if (document.getElementById("ikun-market-prehide-css")) return;
    var style = document.createElement("style");
    style.id = "ikun-market-prehide-css";
    style.textContent = ".mkt-page .mkt-body{visibility:hidden!important;opacity:0!important;pointer-events:none!important;}";
    document.head.appendChild(style);
  }

  function fixSymbolSuffixValue(value) {
    return String(value == null ? "" : value)
      .replace(/\b([A-Z0-9]{1,40})\/USDTUSDT\.P\b/g, "$1USDT.P")
      .replace(/\b([A-Z0-9]{1,40})USDTUSDT\.P\b/g, "$1USDT.P");
  }

  function sanitizeStoredWatchlists() {
    try {
      Object.keys(localStorage).forEach(function (key) {
        if (key.indexOf("ikun_wl") !== 0) return;
        var raw = localStorage.getItem(key);
        if (!raw || raw.indexOf("USDTUSDT.P") === -1) return;
        var data = JSON.parse(raw);
        var changed = false;
        if (Array.isArray(data)) {
          data.forEach(function (item) {
            if (!item || typeof item !== "object") return;
            if (typeof item.display === "string") {
              var fixed = fixSymbolSuffixValue(item.display);
              if (fixed !== item.display) {
                item.display = fixed;
                changed = true;
              }
            }
          });
        }
        if (changed) localStorage.setItem(key, JSON.stringify(data));
      });
    } catch (error) {}
  }

  function fixVisibleSymbolSuffixes() {
    if (!document.body || !window.NodeFilter) return;
    var walker = document.createTreeWalker(document.body, NodeFilter.SHOW_TEXT, {
      acceptNode: function (node) {
        var parent = node.parentElement;
        if (!parent || /^(SCRIPT|STYLE|NOSCRIPT)$/.test(parent.tagName)) return NodeFilter.FILTER_REJECT;
        return node.nodeValue && node.nodeValue.indexOf("USDTUSDT.P") !== -1
          ? NodeFilter.FILTER_ACCEPT
          : NodeFilter.FILTER_REJECT;
      },
    });
    var node;
    while ((node = walker.nextNode())) {
      node.nodeValue = fixSymbolSuffixValue(node.nodeValue);
    }
  }

  function fixSymbolSuffixes() {
    sanitizeStoredWatchlists();
    fixVisibleSymbolSuffixes();
  }

  function ensureOrionMarketStyles() {
    if (document.getElementById("ikun-orion-market-css")) return;
    var style = document.createElement("style");
    style.id = "ikun-orion-market-css";
    style.textContent = [
      ".ikun-orion-market-body{padding:24px 28px 44px;min-height:calc(100vh - 76px);background:var(--bg-color,#030812);overflow:auto;}",
      ".ikun-orion-board-wrap{width:min(100%,1380px);margin:0 auto;}",
      ".ikun-orion-hero{display:flex;align-items:flex-end;justify-content:space-between;gap:18px;margin-bottom:18px;padding:2px 2px 0;}",
      ".ikun-orion-title{font-family:'Rajdhani',sans-serif;font-size:26px;font-weight:800;color:var(--text-primary,#e2eaf4);letter-spacing:.01em;line-height:1;}",
      ".ikun-orion-subtitle{margin-top:7px;font-family:'DM Sans',sans-serif;font-size:12px;color:var(--text-secondary,#9ca3af);}",
      ".ikun-orion-source{font-family:'Rajdhani',sans-serif;font-size:12px;font-weight:800;color:var(--accent,#4db8ff);border:1px solid rgba(77,184,255,.28);border-radius:8px;padding:6px 10px;background:rgba(77,184,255,.08);white-space:nowrap;}",
      ".ikun-orion-grid{display:grid;grid-template-columns:repeat(2,minmax(360px,1fr));gap:16px;align-items:start;}",
      ".ikun-orion-card{background:var(--card-bg,rgba(5,13,30,.86));border:1px solid rgba(148,163,184,.20);border-radius:14px;padding:18px 18px 12px;box-shadow:0 16px 38px rgba(0,0,0,.24);min-height:356px;}",
      ".ikun-orion-card-head{display:flex;align-items:flex-start;justify-content:space-between;gap:12px;margin-bottom:12px;}",
      ".ikun-orion-card-title{font-family:'Rajdhani',sans-serif;font-size:20px;font-weight:800;color:var(--text-primary,#e2eaf4);line-height:1.1;}",
      ".ikun-orion-card-sub{margin-top:4px;font-family:'DM Sans',sans-serif;font-size:11px;color:var(--text-secondary,#9ca3af);}",
      ".ikun-orion-pill{font-family:'Rajdhani',sans-serif;font-size:11px;font-weight:800;color:var(--accent,#4db8ff);border:1px solid rgba(77,184,255,.22);border-radius:999px;padding:3px 8px;background:rgba(77,184,255,.07);white-space:nowrap;}",
      ".ikun-orion-table{width:100%;border-collapse:collapse;font-family:'DM Sans',sans-serif;table-layout:fixed;}",
      ".ikun-orion-table th{padding:7px 0 10px;text-align:left;font-size:11px;font-weight:600;color:var(--text-secondary,#9ca3af);}",
      ".ikun-orion-table th:nth-child(1),.ikun-orion-table td:nth-child(1){width:30px;color:var(--text-secondary,#9ca3af);}",
      ".ikun-orion-table th:nth-child(3),.ikun-orion-table td:nth-child(3),.ikun-orion-table th:nth-child(4),.ikun-orion-table td:nth-child(4){text-align:right;}",
      ".ikun-orion-table th:nth-child(5),.ikun-orion-table td:nth-child(5){width:42px;text-align:right;}",
      ".ikun-orion-table td{padding:9px 0;border-top:1px solid rgba(148,163,184,.09);color:var(--text-primary,#e2eaf4);font-size:13px;vertical-align:middle;}",
      ".ikun-orion-name{font-family:'Rajdhani',sans-serif;font-size:14px;font-weight:800;color:var(--text-primary,#e2eaf4);white-space:nowrap;overflow:hidden;text-overflow:ellipsis;display:block;}",
      ".ikun-orion-symbol{display:block;margin-top:2px;font-family:'DM Sans',sans-serif;font-size:10px;color:var(--text-secondary,#9ca3af);white-space:nowrap;overflow:hidden;text-overflow:ellipsis;}",
      ".ikun-orion-price{font-family:'Rajdhani',sans-serif;font-size:14px;font-weight:700;color:var(--text-primary,#e2eaf4);}",
      ".ikun-orion-up{font-family:'Rajdhani',sans-serif;font-size:14px;font-weight:800;color:#0ecb81;}",
      ".ikun-orion-down{font-family:'Rajdhani',sans-serif;font-size:14px;font-weight:800;color:#f6465d;}",
      ".ikun-orion-neutral{font-family:'Rajdhani',sans-serif;font-size:14px;font-weight:800;color:var(--text-secondary,#9ca3af);}",
      ".ikun-orion-add{width:28px;height:28px;border-radius:50%;border:1px solid rgba(77,184,255,.42);background:rgba(77,184,255,.08);color:var(--accent,#4db8ff);font-family:'Rajdhani',sans-serif;font-size:18px;font-weight:800;line-height:24px;cursor:pointer;transition:transform .15s ease,border-color .15s ease,background .15s ease;color-scheme:dark;}",
      ".ikun-orion-add:hover{transform:translateY(-1px);border-color:rgba(77,184,255,.85);background:rgba(77,184,255,.16);}",
      ".ikun-orion-add.is-added{border-color:rgba(14,203,129,.55);background:rgba(14,203,129,.10);color:#0ecb81;font-size:14px;}",
      ".ikun-orion-add.is-busy{opacity:.58;pointer-events:none;}",
      ".ikun-orion-empty{padding:48px 0!important;text-align:center!important;color:var(--text-secondary,#9ca3af)!important;font-size:13px!important;}",
      ".ikun-orion-toast{position:fixed;right:22px;bottom:22px;z-index:9999;max-width:280px;padding:10px 13px;border:1px solid rgba(77,184,255,.28);border-radius:10px;background:rgba(5,13,30,.94);color:var(--text-primary,#e2eaf4);font-family:'DM Sans',sans-serif;font-size:12px;box-shadow:0 14px 32px rgba(0,0,0,.36);}",
      ".ikun-orion-official-body{padding:0!important;overflow:hidden!important;background:#030812!important;}",
      ".ikun-orion-official-shell{position:relative;width:100%;height:calc(100vh - 56px);min-height:640px;background:#030812;}",
      ".ikun-orion-official-frame{position:absolute;inset:0;width:100%;height:100%;border:0;background:#030812;color-scheme:dark;}",
      ".ikun-orion-official-fallback{position:absolute;right:18px;bottom:16px;z-index:2;font-family:'DM Sans',sans-serif;font-size:12px;color:rgba(226,234,244,.72);background:rgba(3,8,18,.74);border:1px solid rgba(77,184,255,.18);border-radius:9px;padding:8px 10px;backdrop-filter:blur(8px);}",
      ".ikun-orion-official-fallback a{color:var(--accent,#4db8ff);text-decoration:none;font-weight:700;margin-left:6px;}",
      "#ikun-orion-official-portal{position:fixed;left:0;right:0;bottom:0;top:60px;z-index:20;background:#030812;}",
      "#ikun-orion-official-portal .ikun-orion-official-shell{height:100%;min-height:0;}",
      "@media(max-width:980px){.ikun-orion-grid{grid-template-columns:1fr}.ikun-orion-market-body{padding:18px 14px 70px}.ikun-orion-hero{align-items:flex-start;flex-direction:column}.ikun-orion-card{border-radius:12px;padding:16px 14px 10px;min-height:320px}}",
      "@media(max-width:980px){.ikun-orion-official-shell{height:calc(100vh - 56px);min-height:560px}.ikun-orion-official-fallback{left:12px;right:12px;text-align:center}}",
    ].join("");
    document.head.appendChild(style);
  }

  function formatOrionPrice(value) {
    var num = Number(value || 0);
    if (!isFinite(num)) return "-";
    if (Math.abs(num) >= 1000) return num.toLocaleString("en-US", { maximumFractionDigits: 2 });
    if (Math.abs(num) >= 1) return num.toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 4 });
    if (Math.abs(num) >= 0.01) return num.toFixed(5).replace(/0+$/, "").replace(/\.$/, "");
    return num.toPrecision(4);
  }

  function formatOrionChange(value) {
    var num = Number(value || 0);
    if (!isFinite(num)) num = 0;
    return (num >= 0 ? "+" : "") + num.toFixed(2) + "%";
  }

  var ORION_RANKING_CONFIGS = [
    { key: "gainers", title: "\u6da8\u5e45\u699c", sub: "24h price change", metric: "\u6da8\u5e45", mode: "signed" },
    { key: "losers", title: "\u8dcc\u5e45\u699c", sub: "24h price change", metric: "\u8dcc\u5e45", mode: "signed" },
    { key: "oiMovers", title: "\u6301\u4ed3\u5f02\u52a8", sub: "1h OI CHG%", metric: "OI", mode: "signed" },
    { key: "volumeSurges", title: "\u91cf\u80fd\u53d8\u5316", sub: "1h VOL CHG%", metric: "\u91cf\u80fd", mode: "signed" },
  ];

  function orionRowSymbol(item) {
    return normalizeSymbol(item && (item.symbol || item.displayName || item.baseAsset));
  }

  function orionDisplayName(item) {
    var symbol = orionRowSymbol(item);
    var rawName = item && (item.displayName || item.name);
    if (rawName) return String(rawName).replace(/USDTUSDT\.P/g, "USDT.P");
    if (!symbol) return "-";
    return symbol.replace("/", "") + "\u6c38\u7eed\u5408\u7ea6";
  }

  function orionMetricClass(value) {
    var num = Number(value || 0);
    if (!isFinite(num) || num === 0) return "ikun-orion-neutral";
    return num > 0 ? "ikun-orion-up" : "ikun-orion-down";
  }

  function currentWatchlistScope() {
    try {
      var user = JSON.parse(localStorage.getItem("ikun_mock_user") || "{}");
      var raw = user.email || user.nickname || user.username || "anon";
      return String(raw).toLowerCase().replace(/[^a-z0-9_.@-]/g, "_");
    } catch (error) {
      return "anon";
    }
  }

  function watchlistStorageKeys() {
    return ["ikun_wl_" + currentWatchlistScope() + "_binance", "ikun_wl_binance"];
  }

  function isOrionAdded(symbol) {
    var normalized = normalizeSymbol(symbol);
    if (!normalized) return false;
    var key = normalized + "@binance";
    var found = false;
    watchlistStorageKeys().forEach(function (storageKey) {
      if (found) return;
      try {
        var data = JSON.parse(localStorage.getItem(storageKey) || "[]");
        found = Array.isArray(data) && data.some(function (item) {
          if (!item) return false;
          return item.key === key || normalizeSymbol(item.symbol || item.display || item.base) === normalized;
        });
      } catch (error) {}
    });
    return found || !!orionMarketState.added[normalized];
  }

  function tokenHeaders() {
    var headers = { "Content-Type": "application/json" };
    try {
      var token = localStorage.getItem("ikun_token") || localStorage.getItem("authToken") || "";
      if (token) {
        headers["X-Token"] = token;
        headers.Authorization = "Bearer " + token;
      }
    } catch (error) {}
    return headers;
  }

  function displaySymbol(symbol) {
    var normalized = normalizeSymbol(symbol);
    if (!normalized) return "";
    return normalized.replace("/", "") + ".P";
  }

  function syncLocalWatchlist(symbol) {
    var normalized = normalizeSymbol(symbol);
    if (!normalized) return { ok: false, msg: "\u6807\u7684\u65e0\u6548" };
    var key = normalized + "@binance";
    var item = {
      key: key,
      symbol: normalized,
      exchangeId: "binance",
      exchange: "binance",
      display: displaySymbol(normalized),
      exchangeLabel: "Binance",
      marketType: "futures",
    };
    var result = { ok: true, exists: false, full: false };
    watchlistStorageKeys().forEach(function (storageKey) {
      try {
        var data = JSON.parse(localStorage.getItem(storageKey) || "[]");
        if (!Array.isArray(data)) data = [];
        var exists = data.some(function (entry) {
          return entry && (entry.key === key || normalizeSymbol(entry.symbol || entry.display || entry.base) === normalized);
        });
        if (exists) {
          result.exists = true;
        } else if (data.length >= 20) {
          result.full = true;
        } else {
          data.push(item);
          localStorage.setItem(storageKey, JSON.stringify(data));
        }
      } catch (error) {}
    });
    return result;
  }

  function showOrionToast(message) {
    var old = document.querySelector(".ikun-orion-toast");
    if (old) old.remove();
    var node = document.createElement("div");
    node.className = "ikun-orion-toast";
    node.textContent = message;
    document.body.appendChild(node);
    window.setTimeout(function () { if (node.parentNode) node.remove(); }, 2200);
  }

  function isMarketMoverText(value) {
    var text = String(value || "").replace(/\s+/g, " ").trim();
    return /今日涨跌榜|今日市场异动|Market Movers|浠婃棩娑ㄨ穼姒|浠婃棩甯傚満寮傚姩/.test(text);
  }

  function isWatchlistText(value) {
    var text = String(value || "").replace(/\s+/g, " ").trim();
    return /自选列表|Favorites|鑷€夊垪琛/.test(text);
  }

  function copyText(text) {
    text = String(text || "");
    if (!text) return Promise.reject(new Error("empty text"));
    function fallbackCopy() {
      return new Promise(function (resolve, reject) {
        try {
          var input = document.createElement("textarea");
          input.value = text;
          input.setAttribute("readonly", "readonly");
          input.style.position = "fixed";
          input.style.left = "-9999px";
          document.body.appendChild(input);
          input.select();
          document.execCommand("copy");
          input.remove();
          resolve();
        } catch (error) {
          reject(error);
        }
      });
    }
    if (navigator.clipboard && navigator.clipboard.writeText) {
      try {
        return Promise.race([
          navigator.clipboard.writeText(text),
          new Promise(function (_, reject) {
            window.setTimeout(function () { reject(new Error("clipboard timeout")); }, 800);
          }),
        ]).catch(fallbackCopy);
      } catch (error) {
        return fallbackCopy();
      }
    }
    return fallbackCopy();
  }

  function symbolCopyCode(symbol) {
    var normalized = normalizeSymbol(symbol);
    return normalized ? normalized.replace("/", "") + ".P" : "";
  }

  function markOrionAdded(symbol) {
    var normalized = normalizeSymbol(symbol);
    if (!normalized) return;
    orionMarketState.added[normalized] = true;
    document.querySelectorAll('.ikun-orion-add[data-symbol="' + normalized.replace(/"/g, '\\"') + '"]').forEach(function (button) {
      button.classList.add("is-added");
      button.textContent = "\u2713";
      button.title = "\u5df2\u5728\u76d1\u63a7\u5217\u8868";
    });
  }

  function addOrionSymbol(symbol, button) {
    var normalized = normalizeSymbol(symbol);
    if (!normalized || !button) return;
    if (button.classList.contains("is-added")) {
      showOrionToast("\u5df2\u5728\u76d1\u63a7\u5217\u8868");
      return;
    }
    button.classList.add("is-busy");
    fetch("/api/add_symbol", {
      method: "POST",
      headers: tokenHeaders(),
      body: JSON.stringify({ symbol: normalized, exchange: "binance", exchangeId: "binance", timeframe: "15m" }),
    })
      .then(function (response) {
        return response.json().then(function (payload) {
          if (!response.ok || !payload || payload.status !== "success") {
            throw new Error((payload && (payload.msg || payload.error)) || "\u6dfb\u52a0\u5931\u8d25");
          }
          return payload;
        });
      })
      .then(function () {
        var local = syncLocalWatchlist(normalized);
        markOrionAdded(normalized);
        window.dispatchEvent(new Event("ikun_wl_sync"));
        showOrionToast(local.exists ? "\u5df2\u5728\u76d1\u63a7\u5217\u8868" : "\u5df2\u6dfb\u52a0\u5230\u76d1\u63a7\u5217\u8868");
      })
      .catch(function (error) {
        var msg = error && error.message ? error.message : "\u6dfb\u52a0\u5931\u8d25";
        if (/limit/i.test(msg)) msg = "\u81ea\u9009\u5217\u8868\u5df2\u6ee1";
        showOrionToast(msg);
      })
      .finally(function () {
        button.classList.remove("is-busy");
      });
  }

  function orionBoardRowsHtml(config) {
    var rows = Array.isArray(orionMarketState.rankings && orionMarketState.rankings[config.key])
      ? orionMarketState.rankings[config.key]
      : [];
    if (orionMarketState.loading && !rows.length) {
      return '<tr><td class="ikun-orion-empty" colspan="5">\u6b63\u5728\u52a0\u8f7d ORION \u6570\u636e...</td></tr>';
    }
    if (!rows.length) {
      var msg = orionMarketState.error || "\u6682\u65e0 ORION \u6570\u636e";
      return '<tr><td class="ikun-orion-empty" colspan="5">' + escapeHtml(msg) + "</td></tr>";
    }
    return rows.slice(0, 8).map(function (item, index) {
      var symbol = orionRowSymbol(item);
      var metric = Number(item.metricValue != null ? item.metricValue : item.changePercent);
      var added = isOrionAdded(symbol);
      return [
        "<tr>",
        "<td>" + (index + 1) + "</td>",
        '<td><span class="ikun-orion-name">' + escapeHtml(orionDisplayName(item)) + '</span><span class="ikun-orion-symbol">' + escapeHtml(symbol) + "</span></td>",
        '<td class="ikun-orion-price">' + escapeHtml(formatOrionPrice(item.price)) + "</td>",
        '<td class="' + orionMetricClass(metric) + '">' + escapeHtml(formatOrionChange(metric)) + "</td>",
        '<td><button type="button" class="ikun-orion-add' + (added ? " is-added" : "") + '" data-symbol="' + escapeHtml(symbol) + '" title="' + (added ? "\u5df2\u5728\u76d1\u63a7\u5217\u8868" : "\u6dfb\u52a0\u5230\u76d1\u63a7\u5217\u8868") + '">' + (added ? "\u2713" : "+") + "</button></td>",
        "</tr>",
      ].join("");
    }).join("");
  }

  function orionBoardsHtml() {
    return ORION_RANKING_CONFIGS.map(function (config) {
      return [
        '<section class="ikun-orion-card">',
        '<div class="ikun-orion-card-head">',
        "<div>",
        '<div class="ikun-orion-card-title">' + escapeHtml(config.title) + "</div>",
        '<div class="ikun-orion-card-sub">' + escapeHtml(config.sub) + "</div>",
        "</div>",
        '<div class="ikun-orion-pill">' + escapeHtml(config.metric) + "</div>",
        "</div>",
        '<table class="ikun-orion-table">',
        "<thead><tr><th></th><th>\u540d\u79f0</th><th>\u4ef7\u683c</th><th>\u6307\u6807</th><th></th></tr></thead>",
        '<tbody data-orion-board="' + escapeHtml(config.key) + '">',
        orionBoardRowsHtml(config),
        "</tbody></table></section>",
      ].join("");
    }).join("");
  }

  function updateOrionMarketRows() {
    document.querySelectorAll("[data-orion-board]").forEach(function (tbody) {
      var key = tbody.getAttribute("data-orion-board");
      var config = ORION_RANKING_CONFIGS.find(function (item) { return item.key === key; });
      if (config) tbody.innerHTML = orionBoardRowsHtml(config);
    });
  }

  function loadOrionMarketData() {
    if (orionMarketState.loading) return;
    orionMarketState.loading = true;
    updateOrionMarketRows();
    fetch("/api/orion/market-rankings?limit=8")
      .then(function (response) { return response.json(); })
      .then(function (payload) {
        orionMarketState.rankings = payload && payload.rankings && typeof payload.rankings === "object" ? payload.rankings : {};
        orionMarketState.error = payload && payload.status === "error" ? (payload.error || "\u52a0\u8f7d\u5931\u8d25") : "";
      })
      .catch(function (error) {
        orionMarketState.error = error && error.message ? error.message : "\u52a0\u8f7d\u5931\u8d25";
      })
      .finally(function () {
        orionMarketState.loading = false;
        orionMarketState.loaded = true;
        updateOrionMarketRows();
      });
  }

  function replaceMarketPage() {
    var page = document.querySelector(".mkt-page");
    var existing = document.getElementById("ikun-orion-official-portal");
    if (!page) {
      if (existing && existing.parentNode) existing.parentNode.removeChild(existing);
      return;
    }
    if (existing) return;
    ensureOrionMarketStyles();
    var portal = document.createElement("div");
    portal.id = "ikun-orion-official-portal";
    portal.innerHTML = [
      '<div class="ikun-orion-official-shell">',
      '<iframe class="ikun-orion-official-frame" src="' + ORION_OFFICIAL_URL + '" title="ORION Terminal Screener" loading="eager" referrerpolicy="no-referrer-when-downgrade"></iframe>',
      '<div class="ikun-orion-official-fallback">ORION \u5b98\u65b9\u6570\u636e\u9875\u9762<a href="' + ORION_OFFICIAL_URL + '" target="_blank" rel="noreferrer">\u65b0\u7a97\u53e3\u6253\u5f00</a></div>',
      "</div>",
    ].join("");
    document.body.appendChild(portal);
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
        if (isMarketMoverText(option.textContent || "")) {
          option.hidden = true;
          option.disabled = true;
          option.style.display = "none";
        }
      });
      if (select.value === "movers") {
        select.value = "favorites";
        select.dispatchEvent(new Event("change", { bubbles: true }));
      }
    });

    document.querySelectorAll("button, [role='button'], option").forEach(function (node) {
      var text = (node.textContent || "").replace(/\s+/g, " ").trim();
      if (!isMarketMoverText(text)) return;
      if (node.tagName === "OPTION") {
        node.hidden = true;
        node.disabled = true;
        node.style.display = "none";
      }
      else {
        node.style.display = "none";
        node.setAttribute("aria-hidden", "true");
      }
    });

    var showingMovers = false;
    document.querySelectorAll("div, span").forEach(function (node) {
      var text = (node.textContent || "").replace(/\s+/g, " ").trim();
      if (isMarketMoverText(text)) showingMovers = true;
    });
    if (showingMovers) {
      var watchlistButton = Array.prototype.slice.call(document.querySelectorAll("button")).find(function (button) {
        return isWatchlistText(button.textContent || "");
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

  function installOrionQuickAdd() {
    document.addEventListener("click", function (event) {
      if (!event.target || !event.target.closest) return;
      var button = event.target.closest(".ikun-orion-add");
      if (!button) return;
      event.preventDefault();
      event.stopPropagation();
      addOrionSymbol(button.getAttribute("data-symbol"), button);
    });
  }

  function installExchangeLabelCopy() {
    if (window.__ikunExchangeLabelCopyInstalled) return;
    window.__ikunExchangeLabelCopyInstalled = true;

    document.addEventListener("mouseover", function (event) {
      if (!event.target || !event.target.closest) return;
      var label = event.target.closest(".bn-table tbody tr td:nth-child(2) span");
      if (!label || String(label.textContent || "").trim() !== "Binance") return;
      var row = label.closest("tr");
      var pair = row && row.querySelector("button.pair-name");
      if (!pair) return;
      label.style.cursor = "copy";
      label.title = "\u70b9\u51fb\u590d\u5236 " + symbolCopyCode(pair.textContent);
    });

    document.addEventListener("click", function (event) {
      if (!event.target || !event.target.closest) return;
      var label = event.target.closest(".bn-table tbody tr td:nth-child(2) span");
      if (!label || String(label.textContent || "").trim() !== "Binance") return;
      var row = label.closest("tr");
      var pair = row && row.querySelector("button.pair-name");
      var code = pair ? symbolCopyCode(pair.textContent) : "";
      if (!code) return;
      event.preventDefault();
      event.stopPropagation();
      copyText(code)
        .then(function () { showOrionToast("\u5df2\u590d\u5236 " + code); })
        .catch(function () { showOrionToast("\u590d\u5236\u5931\u8d25\uff0c\u8bf7\u624b\u52a8\u590d\u5236"); });
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
      safeRun(removeMarketMoversUi);
      safeRun(fixSymbolSuffixes);
      safeRun(replaceMarketPage);
      safeRun(replayRecentSignals);
      if (pending) return;
      pending = true;
      window.setTimeout(function () {
        pending = false;
        safeRun(removeMarketMoversUi);
        safeRun(fixSymbolSuffixes);
        safeRun(replaceMarketPage);
        safeRun(replayRecentSignals);
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

  safeRun(installWhiteScreenGuard);
  safeRun(installMarketPrehideStyles);
  safeRun(installEventSourcePatch);
  safeRun(installChartCellClick);
  safeRun(installOrionQuickAdd);
  safeRun(installExchangeLabelCopy);
  safeRun(installMutationReplay);
  [0, 120, 500, 1200].forEach(function (delay) {
    window.setTimeout(function () {
      safeRun(removeMarketMoversUi);
      safeRun(fixSymbolSuffixes);
      safeRun(replaceMarketPage);
    }, delay);
  });

  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", function () {
    safeRun(closeDesktopSidebar);
    safeRun(fixSymbolSuffixes);
    safeRun(replaceMarketPage);
  });
  else {
    safeRun(closeDesktopSidebar);
    safeRun(fixSymbolSuffixes);
    safeRun(replaceMarketPage);
  }
})();
