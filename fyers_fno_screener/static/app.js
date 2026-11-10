let rows = [];
let refreshTimer = null;
const $ = id => document.getElementById(id);
const fmt = (value, digits = 2) => value == null || value === '' || !Number.isFinite(Number(value)) ? '—' : Number(value).toLocaleString('en-IN', {maximumFractionDigits: digits});
const esc = value => String(value ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
async function api(url) { const response = await fetch(url, {credentials:'same-origin'}); const data = await response.json(); if (!response.ok) throw new Error(data.error || 'Request failed'); return data; }
async function updateStatus() {
  try {
    const data = await api('/api/status');
    const brokerName = data.broker === 'zerodha' ? 'Zerodha Kite' : 'FYERS';
    $('status').textContent = data.connected ? `${brokerName} connected` : `${brokerName} not connected`;
    $('status').className = data.connected ? 'status online' : 'status offline';
    if (data.error) $('feed').textContent = data.error;
    if (!data.connected) $('feed').textContent = `Waiting for ${brokerName} connection`; 
  } catch (error) { $('status').textContent = 'Connection unavailable'; $('status').className = 'status offline'; }
}
function filteredRows() {
  const query = $('search').value.trim().toUpperCase();
  const filter = $('signalFilter').value;
  const sort = $('sort').value;
  const result = rows.filter(row => {
    if (query && !row.symbol.includes(query)) return false;
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
function badges(signals) { return signals.map(signal => `<span class="tag ${signal === 'PDH BREAKOUT' ? 'tag-pdh' : signal === '+2% MOMENTUM' ? 'tag-momentum' : 'tag-ath'}">${esc(signal)}</span>`).join(''); }
function render() {
  const matches = filteredRows();
  $('rows').innerHTML = matches.length ? matches.map(row => {
    const changeClass = Number(row.change_pct) >= 0 ? 'positive' : 'negative';
    return `<tr><td class="stock-cell"><b>${esc(row.symbol)}</b><div class="tags">${badges(row.signals)}</div></td><td>${fmt(row.ltp)}</td><td class="${changeClass}">${Number(row.change_pct) > 0 ? '+' : ''}${fmt(row.change_pct)}%</td><td class="vol-cell">${row.volume_ratio == null ? '—' : fmt(row.volume_ratio,2)+'×'}</td><td>${fmt(row.volume,0)}</td><td>${fmt(row.prev_day_high)}</td><td>${row.ath_distance_pct == null ? '—' : (row.ath_distance_pct < 0 ? 'At/new high' : fmt(row.ath_distance_pct)+'%')}</td></tr>`;
  }).join('') : '<tr><td colspan="7" class="empty">No stocks currently match these filters.</td></tr>';
  $('mobileRows').innerHTML = matches.length ? matches.map(row => {
    const changeClass = Number(row.change_pct) >= 0 ? 'positive' : 'negative';
    return `<article class="stock-card"><div class="stock-top"><b>${esc(row.symbol)}</b><strong>${fmt(row.ltp)}</strong></div><div class="tags">${badges(row.signals)}</div><div class="stock-stats"><span>Change <b class="${changeClass}">${Number(row.change_pct)>0?'+':''}${fmt(row.change_pct)}%</b></span><span>Volume <b>${row.volume_ratio == null ? '—' : fmt(row.volume_ratio)+'×'}</b></span><span>PDH <b>${fmt(row.prev_day_high)}</b></span><span>ATH gap <b>${row.ath_distance_pct == null ? '—' : row.ath_distance_pct < 0 ? 'At/new high' : fmt(row.ath_distance_pct)+'%'}</b></span></div></article>`;
  }).join('') : '<div class="empty">No stocks currently match these filters.</div>';
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
    $('feed').textContent = `Updated ${new Date(data.updated_at).toLocaleTimeString('en-IN')}`;
    $('coverage-note').textContent = data.coverage > 0 && data.quote_volume_nonzero === 0
      ? `${data.matched} matches from `${data.coverage} quotes. Warning: Kite returned zero day-volume for all quotes; volume-based signals are not reliable yet.`
      : `${data.matched} matches from `${data.coverage}/`${data.universe} F&O stocks. Non-zero quote volumes: `${data.quote_volume_nonzero}.`;
    $('historyText').textContent = data.history_running ? `Building 20-session volume baselines and 5-year highs: ${data.history_ready}/${data.universe} stocks ready. Matches will populate as data arrives.` : `Historical baselines ready for ${data.history_ready}/${data.universe} stocks.`;
    $('subhead').textContent = `${data.matched} matching stocks · ${data.market_time} · auto-refresh every 20 seconds`;
    render();
  } catch (error) {
    $('feed').textContent = error.message;
    if (error.message.includes('login')) location.href = '/login';
    $('rows').innerHTML = `<tr><td colspan="7" class="empty">${esc(error.message)}</td></tr>`;
    $('mobileRows').innerHTML = `<div class="empty">${esc(error.message)}</div>`;
  } finally { $('refresh').disabled = false; }
}
$('search').addEventListener('input', render);
$('signalFilter').addEventListener('change', render);
$('sort').addEventListener('change', render);
$('refresh').addEventListener('click', refresh);
(async () => {
  if ('serviceWorker' in navigator) { try { await navigator.serviceWorker.register('/static/sw.js'); } catch (_) {} }
  await updateStatus();
  await refresh();
  refreshTimer = setInterval(refresh, 20000);
  setInterval(updateStatus, 30000);
})();
