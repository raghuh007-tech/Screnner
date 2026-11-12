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
    return `<tr>
      <td class="stock-cell">
        <b>${esc(row.symbol)}</b>
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
    return `<button type="button" class="stock-row" data-symbol="${esc(row.symbol)}"><span class="stock-row-name">${esc(row.symbol)}</span><span class="stock-row-signals">${badges(row.signals)}</span><span class="stock-row-price">${fmt(row.ltp)}</span><span class="stock-row-change ${changeClass}">${formatChange(row.change_pct)}</span><span class="row-chevron">›</span></button>`;
  }).join('') : '<div class="empty">No stocks currently match these filters.</div>';
}
function renderTopGainers(gainers) {
  const host = $('gainersRows'); if (!host) return;
  const list = gainers || [];
  host.innerHTML = list.length ? list.map((row, index) => {
    const level = row.confirmation_level || 'none';
    const label = level === 'strong' ? 'STRONG' : level === 'moderate' ? 'MODERATE' : level === 'limited' ? 'LIMITED' : 'NO CONFIRMATION';
    const signals = (row.confirmations || []).map(esc).join(' · ') || 'No additional confirmation';
    return '<div class="gainer-row confirm-' + level + '"><b class="gainer-rank">' + (index+1) + '</b><b class="gainer-symbol">' + esc(row.symbol) + '</b><span class="gainer-change positive">+' + fmt(row.change_pct) + '%</span><span class="gainer-level">' + label + ' · ' + (row.confirmation_count || 0) + '/4</span><small class="gainer-signals">' + signals + '</small></div>';
  }).join('') : '<div class="empty">No positive F&O gainers available yet.</div>';
}


function renderMomentum(data) {
  const host = $('freshMomentum');
  if (!host) return;
  const items = data?.fresh_momentum || [];
  host.innerHTML = items.length ? items.map((r,i) => {
    const checks = (r.confirmations || []).map(esc).join(' · ');
    return '<div class="momentum-row mom-strong"><span class="mom-rank">'+(i+1)+'</span><b class="mom-symbol">'+esc(r.symbol)+'</b><span class="mom-daily">'+formatChange(r.change_pct)+'</span><strong class="mom-move">+'+fmt(r.move_5m)+'%</strong><span class="mom-checks">'+checks+' · '+fmt(r.volume_ratio)+'×</span></div>';
  }).join('') : '<div class="empty">No stocks meet all four Fresh Momentum filters yet.</div>';
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
    renderMomentum(data.momentum || {});
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

let chartInstance=null, activeChartSymbol='', activeTimeframe='5';
function calcEma(points,n){let prev=null,k=2/(n+1);return points.map(p=>{prev=prev===null?p.value:p.value*k+prev*(1-k);return {time:p.time,value:prev};});}
function closeChart(){document.getElementById('chartModal').classList.remove('open');if(chartInstance){chartInstance.remove();chartInstance=null;}}
document.getElementById('closeChart').addEventListener('click',closeChart);
document.getElementById('chartModal').addEventListener('click',e=>{if(e.target.id==='chartModal')closeChart();});
document.getElementById('mobileRows').addEventListener('click',e=>{let r=e.target.closest('.stock-row');if(r)openChart(r.dataset.symbol);});
document.querySelectorAll('.chart-timeframes button').forEach(b=>b.addEventListener('click',()=>{document.querySelectorAll('.chart-timeframes button').forEach(x=>x.classList.toggle('active',x===b));activeTimeframe=b.dataset.tf;openChart(activeChartSymbol);}));
async function openChart(symbol){
 activeChartSymbol=symbol;document.getElementById('chartSymbol').textContent=symbol;document.getElementById('chartModal').classList.add('open');document.getElementById('chartMessage').textContent='Loading candles…';
 if(chartInstance){chartInstance.remove();chartInstance=null;}
 try{
  let d=await api(`/api/chart/${encodeURIComponent(symbol)}?interval=${activeTimeframe}`), cs=(d.candles||[]).map(c=>({time:Math.floor(c[0]),open:c[1],high:c[2],low:c[3],close:c[4]}));
  if(!cs.length){document.getElementById('chartMessage').textContent=d.message||'Intraday candles not available yet.';return;}
  let el=document.getElementById('stockChart');
  chartInstance=LightweightCharts.createChart(el,{width:el.clientWidth,height:el.clientHeight,layout:{background:{color:'#101a28'},textColor:'#a9bad0'},grid:{vertLines:{color:'#1d2b3d'},horzLines:{color:'#1d2b3d'}},timeScale:{timeVisible:true}});
  let candle=chartInstance.addCandlestickSeries({upColor:'#35c99a',downColor:'#f16f7a',borderVisible:false,wickUpColor:'#35c99a',wickDownColor:'#f16f7a'});candle.setData(cs);
  let closes=cs.map(c=>({time:c.time,value:c.close}));
  let e10=chartInstance.addLineSeries({color:'#f5c451',lineWidth:2});e10.setData(calcEma(closes,10));
  let e20=chartInstance.addLineSeries({color:'#65a9ff',lineWidth:2});e20.setData(calcEma(closes,20));
  chartInstance.timeScale().fitContent();document.getElementById('chartMessage').textContent='EMA 10 · EMA 20';
 }catch(e){document.getElementById('chartMessage').textContent=e.message;}
}
