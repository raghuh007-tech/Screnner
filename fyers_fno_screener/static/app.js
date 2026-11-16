let rows = [];
let latestScan = null;
let refreshTimer = null;
const $ = id => document.getElementById(id);
const fmt = (value, digits = 2) => value == null || value === '' || !Number.isFinite(Number(value))
  ? '—' : Number(value).toLocaleString('en-IN', {maximumFractionDigits: digits});
const esc = value => String(value ?? '').replace(/[&<>"']/g, c => ({
  '&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'
}[c]));
async function api(url) {
  const response = await fetch(url, {credentials:'same-origin'});
  const data = await response.json();
  if (!response.ok) throw new Error(data.error || 'Request failed');
  return data;
}
const STOCK_CAPITAL = 250000;
function tradePlanInline(ltp) {
  const price = Number(ltp);
  if (!Number.isFinite(price) || price <= 0) return '';
  const quantity = Math.floor(STOCK_CAPITAL / price);
  return `<span class="trade-plan"><b>Q:${fmt(quantity,0)}</b><span class="trade-sl">SL:${fmt(price * 0.99)}</span><span class="trade-tgt">TGT:${fmt(price * 1.02)}</span></span>`;
}
function sentimentClass(value) {
  return ({
    'Bullish':'sentiment-bullish',
    'Neutral':'sentiment-neutral',
    'Bearish':'sentiment-bearish',
    'Unavailable':'sentiment-neutral'
  })[value] || 'sentiment-neutral';
}
function formatChange(value) {
  if (value == null || !Number.isFinite(Number(value))) return '—';
  const n = Number(value);
  return `${n > 0 ? '+' : ''}${fmt(n)}%`;
}
async function runFyersDataTest() {
  const button = $('testFyersData');
  const output = $('fyersDataTest');
  if (!button || !output) return;
  button.disabled = true;
  button.textContent = 'Testing…';
  output.textContent = 'Checking quotes and 5-minute history…';
  output.className = 'status offline';
  try {
    const response = await fetch('/api/fyers-test-data', {
      method: 'POST',
      credentials: 'same-origin'
    });
    const data = await response.json();
    const quote = data.quote || {};
    const history = data.history || {};
    output.textContent = `Quotes: ${quote.ok ? 'PASS' : 'FAIL'} · History: ${history.ok ? 'PASS' : 'FAIL'}`;
    output.className = data.ok ? 'status online' : 'status offline';
    if (data.ok) {
      output.title = `SBIN LTP: ${quote.ltp} · ${history.candles} candles checked`;
    } else {
      output.title = [quote.message, history.message, data.message].filter(Boolean).join(' | ');
    }
  } catch (_) {
    output.textContent = 'Data test failed';
    output.title = 'Could not complete FYERS data test.';
    output.className = 'status offline';
  } finally {
    button.disabled = false;
    button.textContent = 'Test FYERS Data';
  }
}
const fyersTestButton = $('testFyersData');
if (fyersTestButton) fyersTestButton.addEventListener('click', runFyersDataTest);


async function runFyersDiagnostic() {
  const button = $('runFyersDiagnostic');
  const output = $('fyersDiagnosticResult');
  if (!button || !output) return;
  button.disabled = true;
  button.textContent = 'Checking…';
  output.textContent = 'Loading F&O universe and testing quote/history coverage…';
  output.className = 'status offline';
  try {
    const response = await fetch('/api/fyers-diagnostic', {method:'POST', credentials:'same-origin'});
    const data = await response.json();
    if (!response.ok || !data.universe_count) throw new Error(data.message || 'Diagnostic failed');
    const historyOk = (data.history_samples || []).filter(x => x.ok).length;
    output.textContent = `F&O ${data.universe_count} · Quotes ${data.quote_ok_count}/${data.universe_count} · History ${historyOk}/${data.history_samples.length}`;
    output.className = data.ok ? 'status online' : 'status offline';
    output.title = `Missing quotes: ${(data.missing_symbols || []).join(', ') || 'None'} · Failed batches: ${(data.quote_failed_batches || []).length} · Active broker: ${data.active_broker_unchanged}`;
  } catch (error) {
    output.textContent = 'F&O diagnostic failed';
    output.title = error.message || 'Check server logs.';
    output.className = 'status offline';
  } finally {
    button.disabled = false;
    button.textContent = 'Run F&O Diagnostic';
  }
}
document.addEventListener('click', event => {
  const trigger = event.target && event.target.closest ? event.target.closest('#runFyersDiagnostic') : null;
  if (trigger) runFyersDiagnostic();
});



async function updateStatus() {
  try {
    const data = await api('/api/status');
    const brokerName = data.broker === 'zerodha' ? 'Zerodha Kite' : 'FYERS';
    const fyersIndicator = $('fyersTestStatus');
    if (fyersIndicator) {
      try {
        const fyersData = await api('/api/fyers-test-status');
        fyersIndicator.textContent = fyersData.connected ? 'FYERS: Connected' : 'FYERS: Not connected';
        fyersIndicator.className = fyersData.connected ? 'status online' : 'status offline';
      } catch (_) {
        fyersIndicator.textContent = 'FYERS: Status unavailable';
        fyersIndicator.className = 'status offline';
      }
    }
    $('status').textContent = data.connected ? `${brokerName} connected` : `${brokerName} not connected`;
    $('status').className = data.connected ? 'status online' : 'status offline';
    if (data.error) $('feed').textContent = data.error;
    if (!data.connected) $('feed').textContent = `Waiting for ${brokerName} connection`;
    const storageNote = $('historyStorageText');
    if (storageNote) {
      if (data.history_cache_persistent_mount) {
        storageNote.textContent = `Persistent history disk active · ${data.history_cache_symbols} stocks saved`;
        storageNote.className = 'storage-note storage-ok';
      } else {
        storageNote.textContent = 'Temporary Render storage · backfill can repeat after sleep or restart. Attach a persistent disk at /var/data.';
        storageNote.className = 'storage-note storage-warning';
      }
    }
  } catch (error) {
    $('status').textContent = 'Connection unavailable';
    $('status').className = 'status offline';
  }
}
function filteredRows() {
  const filter = $('signalFilter').value;
  const sort = $('sort').value;
  const result = rows.filter(row => {
    const coreCount = Number(row.core_signal_count ?? row.signal_count ?? 0);
    // Default view remains the strict scanner: PDH +2% momentum + 3x volume.
    if (filter === 'all' && coreCount < 3) return false;
    // Multiple signals means at least two of the three core conditions.
    if (filter === 'multiple' && coreCount < 2) return false;
    if (filter !== 'all' && filter !== 'multiple' && !row.signals.includes(filter)) return false;
    return true;
  });
  result.sort((a,b) => {
    if (sort === 'week52_distance_pct') return (a.week52_distance_pct ?? a.ath_distance_pct ?? 999) - (b.week52_distance_pct ?? b.ath_distance_pct ?? 999);
    return Number(b[sort] ?? -999999) - Number(a[sort] ?? -999999);
  });
  return result;
}
function signalClass(signal) {
  if (signal === 'PDH BREAKOUT') return 'tag-pdh';
  if (signal === '+2% MOMENTUM') return 'tag-momentum';
  if (signal === 'NEAR 52W HIGH') return 'tag-ath';
  if (signal === '52W HIGH +2% + 3X') return 'tag-combo';
  if (signal === '3X VOLUME') return 'tag-volume';
  if (signal === 'IVF') return 'tag-ivf';
  return 'tag-ath';
}
function badges(signals) {
  return signals.map(signal => `<span class="tag ${signalClass(signal)}">${esc(signal)}</span>`).join('');
}
function highGap(row) {
  return row.ath_distance_pct == null ? '—' :
    (row.ath_distance_pct < 0 ? 'At/new high' : `${fmt(row.ath_distance_pct)}%`);
}
function render() {
  const matches = filteredRows();
  $('rows').innerHTML = matches.length ? matches.map(row => {
    const changeClass = Number(row.change_pct) >= 0 ? 'positive' : 'negative';
    return `<tr class="desktop-stock-row" data-symbol="${esc(row.symbol)}" title="Click to view intraday chart">
      <td class="stock-cell">
        <b>${esc(row.symbol)}</b>${tradePlanInline(row.ltp)}
        <div class="stock-meta">${esc(row.sector || 'Sector unavailable')}</div>
        <div class="tags">${badges(row.signals)}</div>
      </td>
      <td>${fmt(row.ltp)}</td>
      <td class="${changeClass}">${formatChange(row.change_pct)}</td>
      <td><span class="sentiment-tag ${sentimentClass(row.sentiment)}">${esc(row.sentiment || 'Neutral')}</span></td>
      <td class="vol-cell">${row.volume_ratio == null ? '—' : fmt(row.volume_ratio,2)+'×'}</td>
      <td class="ivf-cell">${row.ivf_volume_ratio == null ? '—' : fmt(row.ivf_volume_ratio,2)+'×'}${row.ivf_candle_time ? '<small class="ivf-time">'+esc(row.ivf_candle_time)+'</small>' : ''}</td>
      <td>${fmt(row.volume,0)}</td>
      <td>${fmt(row.prev_day_high)}</td>
      <td>${highGap(row)}</td>
    </tr>`;
  }).join('') : '<tr><td colspan="9" class="empty">No stocks currently match these filters.</td></tr>';

  $('mobileRows').innerHTML = matches.length ? matches.map(row => {
    const changeClass = Number(row.change_pct) >= 0 ? 'positive' : 'negative';
    const checks = [
      ['PDH BREAKOUT', 'PDH'],
      ['+2% MOMENTUM', '2%'],
      ['3X VOLUME', 'DAY 3×'],
      ['IVF', 'IVF 5m'],
      ['NEAR 52W HIGH', '52W']
    ];
    const active = checks.filter(([signal]) => (row.signals || []).includes(signal));
    const compactTags = active.map(([, label]) => `<span class="compact-confirm">${label}</span>`).join('');
    return `<button type="button" class="stock-row" data-symbol="${esc(row.symbol)}">
      <span class="stock-row-main"><b class="stock-row-name">${esc(row.symbol)}</b>${tradePlanInline(row.ltp)}<span class="stock-row-price">${fmt(row.ltp)}</span><span class="stock-row-change ${changeClass}">${formatChange(row.change_pct)}</span><b class="confirmation-count">${active.length}/5</b></span>
      <span class="stock-row-confirmations">${compactTags}</span>
    </button>`;
  }).join('') : '<div class="empty">No stocks currently match these filters.</div>';
}
function renderTopGainers(gainers) {
  const host = $('gainersRows'); if (!host) return;
  const list = (gainers || []).slice(0, 10);
  const renderColumn = (items, offset) => items.map((row, index) => {
    const rank = offset + index + 1;
    const level = row.confirmation_level || 'none';
    const label = level === 'strong' ? 'STRONG' : level === 'moderate' ? 'MODERATE' : level === 'limited' ? 'LIMITED' : 'NO CONFIRMATION';
    const isNew = Boolean(row.is_new_entry);
    const checks = (row.new_entry_confirmations || []).map(check =>
      '<span class="new-check ' + (check.passed ? 'passed' : 'pending') + '">' +
      (check.passed ? '✓ ' : '○ ') + esc(check.label) + '</span>').join('');
    const status = '<span class="gainer-status">' + (isNew
      ? '<span class="gainer-new-tag">NEW · ' + esc(row.new_entry_time || '') + '</span><span class="gainer-level new-score">' + (row.new_entry_confirmation_count || 0) + '/3</span>'
      : '<span class="gainer-level">' + label + ' · ' + (row.confirmation_count || 0) + '/4</span>') + '</span>';
    const details = isNew ? checks : (row.confirmations || []).map(esc).join(' · ') || 'No additional confirmation';
    return '<div class="gainer-row confirm-' + level + (isNew ? ' gainer-new-entry' : '') + '" data-symbol="' + esc(row.symbol) + '" role="button" tabindex="0" title="Click to view intraday chart">' +
      '<b class="gainer-rank">' + rank + '</b><b class="gainer-symbol">' + esc(row.symbol) + '</b>' + tradePlanInline(row.ltp) +
      '<span class="gainer-change positive">+' + fmt(row.change_pct) + '%</span>' + status +
      '<small class="gainer-signals">' + details + '</small></div>';
  }).join('');
  const left = list.slice(0, 5);
  const right = list.slice(5, 10);
  host.innerHTML = list.length
    ? '<div class="gainers-column">' + renderColumn(left, 0) + '</div><div class="gainers-column">' + renderColumn(right, 5) + '</div>'
    : '<div class="empty">No positive F&O gainers available yet.</div>';
}

function renderMarketContext(context) {
  const nifty = context?.nifty50 || {};
  $('niftyPrice').textContent = fmt(nifty.price);
  const niftyChange = $('niftyChange');
  niftyChange.textContent = nifty.change_pct == null ? 'Index quote unavailable' : formatChange(nifty.change_pct);
  niftyChange.className = nifty.change_pct > 0 ? 'positive' : nifty.change_pct < 0 ? 'negative' : '';

  const sector = context?.top_sector;
  $('topSector').textContent = sector?.name || 'Unavailable';
  const sectorChange = $('topSectorChange');
  sectorChange.textContent = sector ? `${formatChange(sector.change_pct)} · strongest available sector index` : 'Sector index quote unavailable';
  sectorChange.className = sector?.change_pct > 0 ? 'positive' : sector?.change_pct < 0 ? 'negative' : '';

  $('sectorCount') && ($('sectorCount').textContent = context?.sector_count ? `${context.sector_count} sector indices checked` : '');
}
function renderSentiment(data) {
  const sentiment = data || {};
  const overall = sentiment.overall || 'Unavailable';
  const overallEl = $('overallSentiment');
  overallEl.textContent = overall;
  overallEl.className = `sentiment-value ${sentimentClass(overall)}`;

  const counts = sentiment.counts || {};
  $('sentimentScore').textContent = sentiment.valid_stocks
    ? `${fmt(counts['Bullish'] || 0,0)} Bullish · ${fmt(counts['Neutral'] || 0,0)} Neutral · ${fmt(counts['Bearish'] || 0,0)} Bearish · ${fmt(sentiment.valid_stocks,0)} F&O stocks`
    : 'Waiting for NSE F&O stock breadth';
}
async function refresh() {
  $('feed').textContent = 'Scanning…';
  $('refresh').disabled = true;
  try {
    const data = await api('/api/scan');
    latestScan = data;
    rows = data.rows || [];
    renderTopGainers(data.top_gainers || []);
    $('universe').textContent = fmt(data.universe,0);
    $('pdhCount').textContent = fmt(data.pd_breakouts,0);
    $('momentumCount').textContent = fmt(data.momentum_count,0);
    $('athCount').textContent = fmt(data.near_52w,0);
    $('athMomentumCount').textContent = fmt(data.week52_momentum,0);
    renderMarketContext(data.market_context);
    renderSentiment(data.sentiment);
    $('feed').textContent = `Updated ${new Date(data.updated_at).toLocaleTimeString('en-IN')}`;
    const volumeWarning = data.coverage > 0 && data.quote_volume_nonzero === 0;
    $('coverage-note').textContent = volumeWarning
      ? `Warning: Kite returned zero day-volume for all ${data.coverage} quotes. Volume-based signals are not reliable yet.`
      : `${data.matched} unique matches from ${data.coverage}/${data.universe} F&O stocks. Non-zero quote volumes: ${data.quote_volume_nonzero}. Sector classification: ${data.sector_coverage}/${data.universe}.`;
    $('historyText').textContent = data.history_running
      ? `Loading/updating history: ${data.history_ready}/${data.universe} stocks ready. Historical calls are limited to initial backfill or daily updates.`
      : `Historical cache ready for ${data.history_ready}/${data.universe} stocks.`;
    $('subhead').textContent = `${data.matched} matching stocks · ${data.market_time} · auto-refresh every 60 seconds`;
    render();
  } catch (error) {
    $('feed').textContent = error.message;
    if (error.message.toLowerCase().includes('login')) location.href = '/login';
    $('rows').innerHTML = `<tr><td colspan="8" class="empty">${esc(error.message)}</td></tr>`;
    $('mobileRows').innerHTML = `<div class="empty">${esc(error.message)}</div>`;
  } finally {
    $('refresh').disabled = false;
  }
}
// Desktop table rows open the same chart modal as mobile stock cards.
$('rows').addEventListener('click', event => {
  const row = event.target.closest('tr[data-symbol]');
  if (row) openChart(row.dataset.symbol);
});
$('gainersRows').addEventListener('click', event => {
  const row = event.target.closest('.gainer-row[data-symbol]');
  if (row) openChart(row.dataset.symbol);
});
$('gainersRows').addEventListener('keydown', event => {
  if (event.key !== 'Enter' && event.key !== ' ') return;
  const row = event.target.closest('.gainer-row[data-symbol]');
  if (row) { event.preventDefault(); openChart(row.dataset.symbol); }
});
$('signalFilter').addEventListener('change', render);
$('sort').addEventListener('change', render);
$('refresh').addEventListener('click', refresh);
$('exportSymbols').addEventListener('click', async () => {
  if (!latestScan) {
    $('feed').textContent = 'Wait for the first successful scan before exporting symbols.';
    return;
  }
  const symbols = new Set();
  const add = value => {
    const symbol = String(value || '').trim().replace(/^(NSE:)/i, '').replace(/(-EQ)$/i, '').toUpperCase();
    if (symbol) symbols.add(symbol);
  };
  (latestScan.top_gainers || []).forEach(row => add(row.symbol));
  (latestScan.momentum?.fresh_momentum || []).forEach(row => add(row.symbol));
  (latestScan.rows || []).filter(row => ['Bullish','Bearish'].includes(row.sentiment)).forEach(row => add(row.symbol));
  const exportText = [...symbols].sort().join('\\n');
  if (!exportText) {
    $('feed').textContent = 'No scanned symbols available to export yet.';
    return;
  }
  try {
    await navigator.clipboard.writeText(exportText);
    $('feed').textContent = `Copied ${symbols.size} unique symbols. Paste manually into Kite watchlist.`;
  } catch (_) {
    $('feed').textContent = 'Clipboard access blocked. Allow clipboard permission and retry.';
  }
});
(async () => {
  if ('serviceWorker' in navigator) {
    try { await navigator.serviceWorker.register('/static/sw.js'); } catch (_) {}
  }
  await updateStatus();
  await refreshBacktestStatus();
  await refresh();
  refreshTimer = setInterval(refresh, 60000);
  setInterval(updateStatus, 30000);
  setInterval(refreshBacktestStatus, 30000);
})();

let chartInstance=null, activeChartSymbol='', activeTimeframe='5'; // Default: today's 5-minute intraday chart
function calcEma(points,n){let prev=null,k=2/(n+1);return points.map(p=>{prev=prev===null?p.value:p.value*k+prev*(1-k);return {time:p.time,value:prev};});}
function closeChart(){document.getElementById('chartModal').classList.remove('open');if(chartInstance){chartInstance.remove();chartInstance=null;}}
document.getElementById('closeChart').addEventListener('click',closeChart);
document.getElementById('chartModal').addEventListener('click',e=>{if(e.target.id==='chartModal')closeChart();});
document.getElementById('mobileRows').addEventListener('click',e=>{let r=e.target.closest('.stock-row');if(r)openChart(r.dataset.symbol);});
document.querySelectorAll('.chart-timeframes button').forEach(b=>b.addEventListener('click',()=>{document.querySelectorAll('.chart-timeframes button').forEach(x=>x.classList.toggle('active',x===b));activeTimeframe=b.dataset.tf;openChart(activeChartSymbol);}));
async function openChart(symbol){
 activeChartSymbol=symbol;document.getElementById('chartSymbol').textContent=symbol;document.getElementById('chartModal').classList.add('open');document.getElementById('chartMessage').textContent='Loading candles and volume…';
 if(chartInstance){chartInstance.remove();chartInstance=null;}
 try{
  let d=await api(`/api/chart/${encodeURIComponent(symbol)}?interval=${activeTimeframe}`), raw=d.candles||[];
  let cs=raw.map(c=>({time:Math.floor(c[0]),open:c[1],high:c[2],low:c[3],close:c[4]}));
  if(!cs.length){document.getElementById('chartMessage').textContent=d.message||'Intraday candles not available yet.';return;}
  let el=document.getElementById('stockChart');
  chartInstance=LightweightCharts.createChart(el,{width:el.clientWidth,height:el.clientHeight,layout:{background:{color:'#101a28'},textColor:'#a9bad0'},grid:{vertLines:{color:'#1d2b3d'},horzLines:{color:'#1d2b3d'}},timeScale:{timeVisible:true},rightPriceScale:{scaleMargins:{top:0.08,bottom:0.25}},leftPriceScale:{visible:false}});
  let candle=chartInstance.addCandlestickSeries({upColor:'#35c99a',downColor:'#f16f7a',borderVisible:false,wickUpColor:'#35c99a',wickDownColor:'#f16f7a'});candle.setData(cs);
  let volume=chartInstance.addHistogramSeries({priceScaleId:'volume',priceFormat:{type:'volume'},lastValueVisible:false,priceLineVisible:false});
  volume.priceScale().applyOptions({scaleMargins:{top:0.82,bottom:0}});
  volume.setData(raw.map(c=>({time:Math.floor(c[0]),value:Number(c[5])||0,color:Number(c[4])>=Number(c[1])?'#35c99a88':'#f16f7a88'})));
  chartInstance.timeScale().fitContent();document.getElementById('chartMessage').textContent='Volume bars · EMA lines hidden';
 }catch(e){document.getElementById('chartMessage').textContent=e.message;}
}


async function refreshBacktestStatus() {
  try {
    const data = await api('/api/backtest/status');
    const statusLabel = {
      not_started:'Not started', running:'Collecting data',
      partial:'Partial · retry needed', data_ready:'Historical data ready'
    }[data.status] || data.status || 'Unknown';
    $('btStatus').textContent = statusLabel;
    $('btStocks').textContent = fmt(data.symbols_with_5m || 0,0);
    $('btDailyStocks').textContent = fmt(data.symbols_with_daily || 0,0);
    $('btBars').textContent = fmt(data.intraday_bars || 0,0);
    $('btErrors').textContent = fmt(data.error_chunks ?? data.failed_calls ?? 0,0);
    const pct = data.total_calls ? Math.min(100, data.completed_calls / data.total_calls * 100) : 0;
    $('btProgressBar').style.width = pct + '%';
    $('btProgressText').textContent = data.total_calls
      ? fmt(data.completed_calls,0) + ' / ' + fmt(data.total_calls,0) + ' checked · ' +
        fmt(data.successful_calls || 0,0) + ' successful · ' + fmt(data.failed_calls || 0,0) +
        ' failed · ' + fmt(pct,1) + '%'
      : 'Historical collection has not started.';
    $('btDateRange').textContent = data.trade_start && data.trade_end
      ? data.trade_start + ' to ' + data.trade_end : '—';
    $('btMessage').textContent = data.message || 'Waiting for status.';
    $('startBacktest').disabled = Boolean(data.running);
    $('startBacktest').textContent = data.running ? 'Collection Running…' : 'Start / Resume Data Collection';
  } catch (error) {
    $('btMessage').textContent = 'Backtest status unavailable: ' + error.message;
  }
}

function money(v) { return '₹' + Number(v || 0).toLocaleString('en-IN',{maximumFractionDigits:0}); }
let strategyResultsLoaded = false;
let strategyV2ResultsLoaded = false;
async function refreshStrategyStatus() {
  try {
    const s = await api('/api/backtest/strategy/status');
    $('strategyMessage').textContent = s.message || s.status;
    $('runStrategy').disabled = s.status === 'running';
    $('runStrategy').textContent = s.status === 'running' ? 'Calculating…' : 'Run Strategy Backtest';
    if (s.status === 'running') strategyResultsLoaded = false;
    if (s.status === 'complete' && !strategyResultsLoaded) {
      const r = await api('/api/backtest/strategy/results');
      const x = r.summary || {};
      $('stTrades').textContent = fmt(x.total_trades || 0,0);
      $('stWinRate').textContent = fmt(x.win_rate_pct || 0,1) + '%';
      $('stGross').textContent = money(x.gross_pnl);
      $('stBrokerage').textContent = money(x.brokerage_total);
      $('stNet').textContent = money(x.net_pnl);
      $('stDD').textContent = money(x.max_drawdown);
      $('strategyTrades').innerHTML = (r.trades || []).slice(0,250).map(t =>
        '<tr><td>' + t.date + '<br>' + t.symbol + '</td><td>' + t.entry + '<br>' + t.entry_time + '</td><td>' + t.stop + '</td><td>' + t.exit + '<br>' + t.exit_time + '</td><td>' + t.qty + '</td><td>' + money(t.net_pnl) + '</td><td>' + t.exit_reason + '</td></tr>'
      ).join('') || '<tr><td colspan="7" class="empty">No qualifying trades found.</td></tr>';
      $('strategyMessage').textContent = x.data_note || s.message;
      strategyResultsLoaded = true;
    }
  } catch(e) { $('strategyMessage').textContent = 'Strategy status unavailable: ' + e.message; }
}
$('runStrategy').addEventListener('click', async () => {
  strategyResultsLoaded = false;
  const b = $('runStrategy'); b.disabled = true; b.textContent = 'Starting…';
  try {
    const response = await fetch('/api/backtest/strategy/run',{method:'POST',credentials:'same-origin'});
    const data = await response.json();
    if (!response.ok) throw new Error(data.error || 'Could not start strategy validation');
    $('strategyMessage').textContent = data.message || 'Strategy validation started.';
    await refreshStrategyStatus();
  } catch(e) { $('strategyMessage').textContent = e.message; b.disabled=false; b.textContent='Run Strategy Backtest'; }
});
setInterval(refreshStrategyStatus, 5000);
refreshStrategyStatus();


async function refreshStrategyV2Status() {
  try {
    const s=await api('/api/backtest/strategy/v2/status');
    $('strategyV2Message').textContent=s.message||s.status;
    $('runStrategyV2').disabled=s.status==='running'||s.status==='calculating';
    $('runStrategyV2').textContent=$('runStrategyV2').disabled?'Calculating…':'Run IVF V2 Backtest';
    if(s.status==='running'||s.status==='calculating') strategyV2ResultsLoaded=false;
    if(s.status==='complete'&&!strategyV2ResultsLoaded){
      const r=await api('/api/backtest/strategy/v2/results'),x=r.summary||{};
      $('v2Trades').textContent=fmt(x.total_trades||0,0); $('v2WinRate').textContent=fmt(x.win_rate_pct||0,1)+'%';
      $('v2Gross').textContent=money(x.gross_pnl); $('v2Brokerage').textContent=money(x.brokerage_total);
      $('v2Net').textContent=money(x.net_pnl); $('v2DD').textContent=money(x.max_drawdown);
      $('strategyV2Trades').innerHTML=(r.trades||[]).map(t=>'<tr><td>'+esc(t.date)+'<br>'+esc(t.symbol)+'</td><td>'+fmt(t.entry,2)+'<br>'+t.entry_time+'</td><td>'+fmt(t.stop,2)+'<br>'+fmt(t.stop_pct,2)+'%</td><td>'+fmt(t.exit,2)+'<br>'+t.exit_time+'</td><td>'+fmt(t.qty,0)+'</td><td>'+fmt(t.ivf_multiple,2)+'×</td><td>'+money(t.net_pnl)+'</td><td>'+esc(t.exit_reason)+'</td></tr>').join('')||'<tr><td colspan="8" class="empty">No qualifying IVF V2 trades found.</td></tr>';
      $('strategyV2Message').textContent=x.data_note||s.message;
       strategyV2ResultsLoaded=true;
    }
  }catch(e){$('strategyV2Message').textContent='IVF V2 status unavailable: '+e.message;}
}
$('runStrategyV2').addEventListener('click',async()=>{
  strategyV2ResultsLoaded=false;
  const b=$('runStrategyV2');b.disabled=true;b.textContent='Starting…';
  try{const response=await fetch('/api/backtest/strategy/v2/run',{method:'POST',credentials:'same-origin'});const data=await response.json();if(!response.ok)throw new Error(data.error||'Could not start IVF V2');$('strategyV2Message').textContent=data.message||'IVF V2 started.';await refreshStrategyV2Status();}
  catch(e){$('strategyV2Message').textContent=e.message;b.disabled=false;b.textContent='Run IVF V2 Backtest';}
});
setInterval(refreshStrategyV2Status,5000);
refreshStrategyV2Status();

$('startBacktest').addEventListener('click', async () => {
  const button = $('startBacktest');
  button.disabled = true;
  button.textContent = 'Starting…';
  try {
    const response = await fetch('/api/backtest/start', {method:'POST', credentials:'same-origin'});
    const data = await response.json();
    if (!response.ok) throw new Error(data.error || 'Could not start collection');
    $('btMessage').textContent = data.message || 'Historical collection started.';
    await refreshBacktestStatus();
  } catch (error) {
    $('btMessage').textContent = error.message;
    button.disabled = false;
    button.textContent = 'Start / Resume Data Collection';
  }
});


// Independent, read-only IVF V2 audit
async function refreshV2AuditStatus(){
  try{
    const s=await api('/api/backtest/strategy/v2/audit/status');
    const label=$('v2AuditStatus');
    const button=$('runV2Audit');
    if(!label||!button)return;
    label.textContent=s.message||s.status;
    button.disabled=s.status==='running';
    button.textContent=button.disabled?'Auditing…':'Independently Validate IVF V2';
    if(s.status==='complete'){
      const r=await api('/api/backtest/strategy/v2/audit/results');
      const x=r.report||{};
      $('v2AuditReport').textContent=[
        'Saved trades: '+x.saved_trade_count+' · Independently rebuilt: '+x.rebuilt_trade_count,
        'Matched: '+x.matched_trade_count+' · Mismatches: '+x.mismatch_count,
        'Missing saved trades: '+(x.missing_saved_trades||[]).length+' · Unexpected saved trades: '+(x.unexpected_saved_trades||[]).length,
        'Duplicate keys: '+(x.duplicate_saved_keys||[]).length+' saved / '+(x.duplicate_rebuilt_keys||[]).length+' rebuilt',
        'Data quality issues: '+x.data_quality_issue_count,
        'Rebuilt net P&L: '+money(x.rebuilt_summary?.net_pnl)+' · Rebuilt max DD: '+money(x.rebuilt_summary?.max_drawdown),
        'Summary differences: '+Object.keys(x.summary_differences||{}).join(', '),
        'Audit details available from the results endpoint.'
      ].join('\n');
    }
  }catch(e){if($('v2AuditStatus'))$('v2AuditStatus').textContent='Audit status unavailable: '+e.message;}
}
if($('runV2Audit')){
  $('runV2Audit').addEventListener('click',async()=>{
    const b=$('runV2Audit');b.disabled=true;b.textContent='Starting audit…';
    try{
      const response=await fetch('/api/backtest/strategy/v2/audit/run',{method:'POST',credentials:'same-origin'});
      const data=await response.json();
      if(!response.ok)throw new Error(data.error||'Could not start audit');
      $('v2AuditStatus').textContent=data.message||'Audit started.';
      await refreshV2AuditStatus();
    }catch(e){$('v2AuditStatus').textContent=e.message;b.disabled=false;b.textContent='Independently Validate IVF V2';}
  });
  setInterval(refreshV2AuditStatus,5000);
  refreshV2AuditStatus();
}
