const links = document.querySelector('#links');
const count = document.querySelector('#count');
const choices = document.querySelector('#choices');
const queue = document.querySelector('#queue');
const downloadAd = document.querySelector('#download-ad');
let analyzed = [];
let queuePoll;
let browserSession = '';
let storedSession = '';
try { storedSession = localStorage.getItem('linkdrop_session') || ''; } catch (_) { /* Cookie fallback. */ }
const sessionReady = fetch('/api/session', {
  credentials: 'same-origin',
  cache: 'no-store',
  headers: storedSession ? { 'X-LinkDrop-Session': storedSession } : {},
})
  .then(async response => {
    if (!response.ok) throw new Error('Could not connect to LinkDrop.');
    const body = await response.json();
    browserSession = body.session;
    try { localStorage.setItem('linkdrop_session', browserSession); } catch (_) { /* Cookie still works. */ }
  });

function urls() { return [...new Set(links.value.split(/\n|,|\s+/).map(x => x.trim()).filter(Boolean))]; }
function updateCount() { count.textContent = `${urls().length} of 8 links`; }
links.addEventListener('input', updateCount);

function availableQualities(video) {
  const byHeight = new Map();
  for (const format of video.formats || []) {
    const height = Number(format.height);
    if (!Number.isInteger(height) || height < 1) continue;
    const current = byHeight.get(height);
    if (!current || (format.ext === 'mp4' && current.ext !== 'mp4')) byHeight.set(height, format);
  }
  return [...byHeight.values()].sort((left, right) => right.height - left.height);
}

async function api(path, options = {}) {
  await sessionReady;
  const response = await fetch(path, {
    ...options,
    headers: { 'Content-Type': 'application/json', 'X-LinkDrop-Session': browserSession, ...options.headers },
  });
  const body = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(body.detail || 'Something went wrong.');
  return body;
}

document.querySelector('#analyze').addEventListener('click', async () => {
  const entries = urls();
  if (!entries.length) return alert('Paste at least one link first.');
  if (entries.length > 8) return alert('Please use no more than 8 links in one batch.');
  choices.classList.remove('hidden');
  choices.innerHTML = '<p>Checking available versions…</p>';
  try {
    analyzed = await Promise.all(entries.map(async url => ({ url, ...(await api('/api/analyze', { method: 'POST', body: JSON.stringify({ url }) })) })));
    choices.innerHTML = analyzed.map((video, index) => {
      const versions = availableQualities(video);
      const buttons = versions.map(format => `<button class="quality" data-quality="${format.height}">${format.height}p${format.ext === 'mp4' ? ' · MP4' : ''}</button>`).join('');
      const availability = versions.length
        ? `Available from this source: ${versions.map(format => `${format.height}p`).join(' · ')}`
        : 'This source did not report separate versions. Best available is the only option.';
      return `<article data-index="${index}"><h3>${escapeHtml(video.title)}</h3><p>${escapeHtml(video.url)}</p><small class="available-versions">${availability}</small><div class="choice-row"><button class="quality selected" data-quality="best">Best available</button>${buttons}</div></article>`;
    }).join('') + '<button class="panel-action" type="button">Add to download queue</button>';
  } catch (error) { choices.innerHTML = `<p>${escapeHtml(error.message)}</p>`; }
});

choices.addEventListener('click', async event => {
  const button = event.target.closest('.quality');
  if (button) { const article = button.closest('article'); article.querySelectorAll('.quality').forEach(x => x.classList.remove('selected')); button.classList.add('selected'); return; }
  if (!event.target.closest('.panel-action')) return;
  const items = [...choices.querySelectorAll('article')].map(article => ({ url: analyzed[article.dataset.index].url, quality: article.querySelector('.selected').dataset.quality }));
  try { await api('/api/jobs', { method: 'POST', body: JSON.stringify({ items }) }); choices.classList.add('hidden'); await loadQueue(); } catch (error) { alert(error.message); }
});

function escapeHtml(value = '') { const div = document.createElement('div'); div.textContent = value; return div.innerHTML; }
function label(job) { return job.title || new URL(job.source_url).hostname; }
function minutesRemaining(job) {
  if (!job.expires_at) return 'a few minutes';
  return `${Math.max(1, Math.ceil((job.expires_at - Date.now() / 1000) / 60))} min`;
}

function updateDownloadAd(waiting) {
  if (!waiting) {
    downloadAd.classList.add('hidden');
    return;
  }
  downloadAd.classList.remove('hidden');
  if (downloadAd.dataset.loaded) return;
  try {
    (window.adsbygoogle = window.adsbygoogle || []).push({});
    downloadAd.dataset.loaded = 'true';
  } catch (_) { /* An ad blocker or unavailable inventory must not affect downloads. */ }
}

async function loadQueue() {
  try {
    const data = await api('/api/jobs');
    window.clearTimeout(queuePoll);
    if (!data.jobs.length) { updateDownloadAd(false); queue.className = 'queue empty'; queue.textContent = 'Nothing in the queue yet.'; return; }
    queue.className = 'queue';
    queue.innerHTML = data.jobs.map(job => `<article class="job"><div class="job-head"><span class="job-title">${escapeHtml(label(job))}</span><span class="pill ${job.state}">${job.state === 'downloading' ? `downloading ${job.progress}%` : job.state}</span></div>${job.state === 'downloading' ? `<div class="bar"><i style="width:${job.progress}%"></i></div><small class="progress-text">Server download: ${job.progress}%</small>` : ''}${job.state === 'complete' ? `<div class="ready-download"><span>↓ Tap here to save your video</span><a class="download" href="/api/files/${job.id}" download>Download to device ↓</a></div><small class="save-help">Ready now — available for about ${minutesRemaining(job)}. On iPhone, tap the button once, then open Safari’s Downloads (↓) and choose Save to Files.</small>` : ''}${job.state === 'failed' ? `<small>${escapeHtml(job.error || 'Download failed')}</small>` : ''}</article>`).join('');
    const waiting = data.jobs.some(job => job.state === 'queued' || job.state === 'downloading');
    updateDownloadAd(waiting);
    const expiry = Math.min(...data.jobs.filter(job => job.state === 'complete' && job.expires_at).map(job => job.expires_at));
    if (waiting) {
      queuePoll = window.setTimeout(loadQueue, 2_000);
    } else if (Number.isFinite(expiry)) {
      const nextCheck = Math.max(1_000, Math.min(30_000, (expiry - Date.now() / 1000) * 1_000));
      queuePoll = window.setTimeout(loadQueue, nextCheck);
    }
  } catch (_) { /* Queue will refresh next time. */ }
}
document.querySelector('#refresh').addEventListener('click', loadQueue);
loadQueue();
