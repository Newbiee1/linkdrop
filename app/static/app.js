const links = document.querySelector('#links');
const count = document.querySelector('#count');
const choices = document.querySelector('#choices');
const queue = document.querySelector('#queue');
let analyzed = [];
let queuePoll;

function urls() { return [...new Set(links.value.split(/\n|,|\s+/).map(x => x.trim()).filter(Boolean))]; }
function updateCount() { count.textContent = `${urls().length} of 8 links`; }
links.addEventListener('input', updateCount);

async function api(path, options = {}) {
  const response = await fetch(path, { headers: { 'Content-Type': 'application/json' }, ...options });
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
    choices.innerHTML = analyzed.map((video, index) => `<article data-index="${index}"><h3>${escapeHtml(video.title)}</h3><p>${escapeHtml(video.url)}</p><div class="choice-row"><button class="quality selected" data-quality="best">Best available</button>${[1080,720,480,360].map(q => `<button class="quality" data-quality="${q}">${q}p</button>`).join('')}</div></article>`).join('') + '<button class="panel-action" type="button">Add to download queue</button>';
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
async function loadQueue() {
  try {
    const data = await api('/api/jobs');
    window.clearTimeout(queuePoll);
    if (!data.jobs.length) { queue.className = 'queue empty'; queue.textContent = 'Nothing in the queue yet.'; return; }
    queue.className = 'queue';
    queue.innerHTML = data.jobs.map(job => `<article class="job"><div class="job-head"><span class="job-title">${escapeHtml(label(job))}</span><span class="pill ${job.state}">${job.state}</span></div>${job.state === 'downloading' ? `<div class="bar"><i style="width:${job.progress}%"></i></div>` : ''}${job.state === 'complete' ? `<a class="download" href="/api/files/${job.id}" download>Save to device ↓</a><small class="save-help">On iPhone, tap this once, then open Safari’s Downloads (↓) and choose Save to Files.</small>` : ''}${job.state === 'failed' ? `<small>${escapeHtml(job.error || 'Download failed')}</small>` : ''}</article>`).join('');
    if (data.jobs.some(job => job.state === 'queued' || job.state === 'downloading')) {
      queuePoll = window.setTimeout(loadQueue, 2_000);
    }
  } catch (_) { /* Queue will refresh next time. */ }
}
document.querySelector('#refresh').addEventListener('click', loadQueue);
loadQueue();
