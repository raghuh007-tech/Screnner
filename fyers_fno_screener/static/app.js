let rows = [];
let latestScan = null;
let activeGainerTab = '1-5';
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
async function updateStatus() {
  try {
    const data = await api('/api/status');
    const brokerName = data.broker === 'zerodha' ? 'Zerodha Kite' : 'FYERS';
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
    if (filter === 'multiple' && row.signals.length < 2) return false;
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
      <td>${fmt(row.volume,0)}</td>
      <td>${fmt(row.prev_day_high)}</td>
      <td>${highGap(row)}</td>
    </tr>`;
  }).join('') : '<tr><td colspan="8" class="empty">No stocks currently match these filters.</td></tr>';

  $('mobileRows').innerHTML = matches.length ? matches.map(row => {
    const changeClass = Number(row.change_pct) >= 0 ? 'positive' : 'negative';
    const checks = [
      ['PDH BREAKOUT', 'PDH'],
      ['+2% MOMENTUM', '2%'],
      ['3X VOLUME', '3× VOL'],
      ['NEAR 52W HIGH', '52W']
    ];
    const active = checks.filter(([signal]) => (row.signals || []).includes(signal));
    const compactTags = active.map(([, label]) => `<span class="compact-confirm">${label}</span>`).join('');
    return `<button type="button" class="stock-row" data-symbol="${esc(row.symbol)}">
      <span class="stock-row-main"><b class="stock-row-name">${esc(row.symbol)}</b>${tradePlanInline(row.ltp)}<span class="stock-row-price">${fmt(row.ltp)}</span><span class="stock-row-change ${changeClass}">${formatChange(row.change_pct)}</span><span class="row-chevron">›</span></span>
      <span class="stock-row-confirmations">${compactTags}<b class="confirmation-count">${active.length}/4</b></span>
    </button>`;
  }).join('') : '<div class="empty">No stocks currently match these filters.</div>';
}
function renderTopGainers(gainers) {
  const host = $('gainersRows'); if (!host) return;
  const list = gainers || [];
  const startIndex = activeGainerTab === '1-5' ? 0 : 5;
  const visible = list.slice(startIndex, startIndex + 5);
  const tabHost = $('gainersTabs');
  if (tabHost) {
    tabHost.innerHTML = `
      <button type="button" class="gainer-tab ${activeGainerTab === '1-5' ? 'active' : ''}" data-gainer-tab="1-5">1–5</button>
      <button type="button" class="gainer-tab ${activeGainerTab === '6-10' ? 'active' : ''}" data-gainer-tab="6-10">6–10</button>`;
  }
  host.innerHTML = visible.length ? visible.map((row, offset) => {
    const index = startIndex + offset;
    const level = row.confirmation_level || 'none';
    const label = level === 'strong' ? 'STRONG' : level === 'moderate' ? 'MODERATE' : level === 'limited' ? 'LIMITED' : 'NO CONFIRMATION';
    const isNew = Boolean(row.is_new_entry);
    const checks = (row.new_entry_confirmations || []).map(check =>
      '<span class="new-check ' + (check.passed ? 'passed' : 'pending') + '">' +
      (check.passed ? '✓ ' : '○ ') + esc(check.label) + '</span>').join('');
    const status = isNew
      ? '<span class="gainer-new-tag">NEW · ' + esc(row.new_entry_time || '') + '</span><span class="gainer-level new-score">' + (row.new_entry_confirmation_count || 0) + '/3</span>'
      : '<span class="gainer-level">' + label + ' · ' + (row.confirmation_count || 0) + '/4</span>';
    const details = isNew ? checks : (row.confirmations || []).map(esc).join(' · ') || 'No additional confirmation';
    return '<div class="gainer-row confirm-' + level + (isNew ? ' gainer-new-entry' : '') + '" data-symbol="' + esc(row.symbol) + '" role="button" tabindex="0" title="Click to view intraday chart">' +
      '<b class="gainer-rank">' + (index+1) + '</b><b class="gainer-symbol">' + esc(row.symbol) + '</b>' + tradePlanInline(row.ltp) +
      '<span class="gainer-change positive">+' + fmt(row.change_pct) + '%</span>' + status +
      '<small class="gainer-signals">' + details + '</small></div>';
  }).join('') : '<div class="empty">No gainers in this rank range yet.</div>';
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
$('gainersTabs').addEventListener('click', event => {
  const tab = event.target.closest('[data-gainer-tab]');
  if (!tab || tab.dataset.gainerTab === activeGainerTab) return;
  activeGainerTab = tab.dataset.gainerTab;
  renderTopGainers(latestScan?.top_gainers || []);
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
  await refresh();
  refreshTimer = setInterval(refresh, 60000);
  setInterval(updateStatus, 30000);
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
