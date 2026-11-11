let rows = [];
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
    'Mild Bullish':'sentiment-mild-bullish',
    'Neutral':'sentiment-neutral',
    'Mild Bearish':'sentiment-mild-bearish',
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
  const query = $('search').value.trim().toUpperCase();
  const filter = $('signalFilter').value;
  const sentimentFilter = $('sentimentFilter').value;
  const sort = $('sort').value;
  const result = rows.filter(row => {
    if (query && !row.symbol.toUpperCase().includes(query) && !(row.sector || '').toUpperCase().includes(query)) return false;
    if (sentimentFilter !== 'all' && row.sentiment !== sentimentFilter) return false;
    if (filter === 'multiple' && row.signals.length < 2) return false;
    if (filter !== 'all' && filter !== 'multiple' && !row.signals.includes(filter)) return false;
    return true;
  });
  result.sort((a,b) => {
    if (sort === 'ath_distance_pct') return (a.ath_distance_pct ?? 999) - (b.ath_distance_pct ?? 999);
    return Number(b[sort] ?? -999999) - Number(a[sort] ?? -999999);
  });
  return result;
}
function signalClass(signal) {
  if (signal === 'PDH BREAKOUT') return 'tag-pdh';
  if (signal === '+2% MOMENTUM') return 'tag-momentum';
  if (signal === 'NEAR 5Y HIGH') return 'tag-ath';
  if (signal === '5Y HIGH +2% + 3X') return 'tag-combo';
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
    return `<article class="stock-card">
      <div class="stock-top"><b>${esc(row.symbol)}</b><strong>${fmt(row.ltp)}</strong></div>
      <div class="stock-meta">${esc(row.sector || 'Sector unavailable')}</div>
      <div class="stock-card-tags">${badges(row.signals)}</div>
      <div class="stock-sentiment"><span class="sentiment-tag ${sentimentClass(row.sentiment)}">${esc(row.sentiment || 'Neutral')}</span><span class="${changeClass}">${formatChange(row.change_pct)}</span></div>
      <div class="stock-stats">
        <span>Volume <b>${row.volume_ratio == null ? '—' : fmt(row.volume_ratio)+'×'}</b></span>
        <span>Day vol <b>${fmt(row.volume,0)}</b></span>
        <span>PDH <b>${fmt(row.prev_day_high)}</b></span>
        <span>5Y high gap <b>${highGap(row)}</b></span>
      </div>
    </article>`;
  }).join('') : '<div class="empty">No stocks currently match these filters.</div>';
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
  $('sentimentScore').textContent = sentiment.score == null
    ? 'Market breadth score unavailable'
    : `F&O breadth score: ${(Number(sentiment.score) * 100).toFixed(0)} · ${sentiment.valid_stocks} stocks`;

  const counts = sentiment.counts || {};
  $('bullishCount').textContent = fmt(counts['Bullish'] || 0,0);
  $('mildBullishCount').textContent = fmt(counts['Mild Bullish'] || 0,0);
  $('neutralCount').textContent = fmt(counts['Neutral'] || 0,0);
  $('mildBearishCount').textContent = fmt(counts['Mild Bearish'] || 0,0);
  $('bearishCount').textContent = fmt(counts['Bearish'] || 0,0);
  $('sentimentCoverage').textContent = `${sentiment.valid_stocks || 0} of F&O stocks classified`;
}
async function refresh() {
  $('feed').textContent = 'Scanning…';
  $('refresh').disabled = true;
  try {
    const data = await api('/api/scan');
    rows = data.rows || [];
    $('universe').textContent = fmt(data.universe,0);
    $('pdhCount').textContent = fmt(data.pd_breakouts,0);
    $('momentumCount').textContent = fmt(data.momentum,0);
    $('athCount').textContent = fmt(data.near_ath,0);
    $('athMomentumCount').textContent = fmt(data.ath_momentum,0);
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
    $('subhead').textContent = `${data.matched} matching stocks · ${data.market_time} · auto-refresh every 20 seconds`;
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
$('search').addEventListener('input', render);
$('signalFilter').addEventListener('change', render);
$('sentimentFilter').addEventListener('change', render);
$('sort').addEventListener('change', render);
$('refresh').addEventListener('click', refresh);
(async () => {
  if ('serviceWorker' in navigator) {
    try { await navigator.serviceWorker.register('/static/sw.js'); } catch (_) {}
  }
  await updateStatus();
  await refresh();
  refreshTimer = setInterval(refresh, 20000);
  setInterval(updateStatus, 30000);
})();
