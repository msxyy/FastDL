const API = 'http://127.0.0.1:6800';
const list = document.getElementById('list');
const tasks = new Map();

const urlInput = document.getElementById('urlInput');
const addBtn = document.getElementById('addBtn');
const rlEnabled = document.getElementById('rlEnabled');
const rlValue = document.getElementById('rlValue');

// ========== 新建任务 ==========
async function submitUrl(url) {
  if (!url) return;
  const r = await fetch(`${API}/download`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ url }),
  });
  return r.json();
}

addBtn.addEventListener('click', async () => {
  const url = urlInput.value.trim();
  if (!url) return;
  try {
    await submitUrl(url);
    urlInput.value = '';
  } catch (e) { console.error(e); }
});

urlInput.addEventListener('keydown', (e) => {
  if (e.key === 'Enter') addBtn.click();
});

// ========== 限速开关 ==========
async function loadRateLimit() {
  try {
    const r = await fetch(`${API}/ratelimit`);
    const d = await r.json();
    rlEnabled.checked = d.enabled;
    if (d.limit_bps > 0) rlValue.value = Math.round(d.limit_bps / 1024);
  } catch {}
}

async function pushRateLimit() {
  const enabled = rlEnabled.checked;
  const kb = parseInt(rlValue.value, 10) || 2048;
  const bps = kb * 1024;
  try {
    await fetch(`${API}/ratelimit`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ enabled, limit_bps: bps }),
    });
  } catch (e) { console.error(e); }
}

rlEnabled.addEventListener('change', pushRateLimit);
rlValue.addEventListener('change', pushRateLimit);

// ========== 清理 ==========
document.getElementById('purge').addEventListener('click', async () => {
  try {
    await fetch(`${API}/tasks`, { method: 'DELETE' });
    for (const [id, t] of tasks) {
      const st = stateOf(t);
      if (['completed', 'failed', 'cancelled', 'interrupted'].includes(st)) tasks.delete(id);
    }
    render();
  } catch (e) { console.error(e); }
});

function fmtSize(b) {
  if (!b) return '0 B';
  const u = ['B', 'KB', 'MB', 'GB', 'TB'];
  let i = 0, v = b;
  while (v >= 1024 && i < u.length - 1) { v /= 1024; i++; }
  return `${v.toFixed(2)} ${u[i]}`;
}

function stateOf(t) {
  const s = t.info.state;
  return s.state || s;
}

async function action(id, kind) {
  try {
    const r = await fetch(`${API}/tasks/${id}/${kind}`, { method: 'POST' });
    console.log(kind, await r.json());
  } catch (e) { console.error(e); }
}

function render() {
  if (tasks.size === 0) {
    list.innerHTML = '<div class="empty">暂无任务</div>';
    return;
  }
  const arr = [...tasks.values()].sort((a, b) => b.info.created_at - a.info.created_at);
  list.innerHTML = arr.map(t => {
    const st = stateOf(t);
    const name = t.info.url.split('/').pop() || t.info.url;
    let progressHtml = '';
    if (t.progress && (st === 'running' || st === 'paused')) {
      const p = t.progress;
      const pct = p.total ? (p.downloaded / p.total * 100) : 0;
      progressHtml = `
        <div class="bar"><i style="width:${pct.toFixed(1)}%"></i></div>
        <div class="meta">
          <span>${fmtSize(p.downloaded)} / ${fmtSize(p.total)}</span>
          <span>${fmtSize(p.speed_bps)}/s</span>
        </div>`;
    }
    let buttons = '';
    if (st === 'running') {
      buttons = `<button data-action="pause" data-id="${t.info.id}">暂停</button>
                 <button class="danger" data-action="cancel" data-id="${t.info.id}">取消</button>`;
    } else if (st === 'paused') {
      buttons = `<button data-action="resume" data-id="${t.info.id}">恢复</button>
                 <button class="danger" data-action="cancel" data-id="${t.info.id}">取消</button>`;
    } else if (st === 'queued') {
      buttons = `<button class="danger" data-action="cancel" data-id="${t.info.id}">取消</button>`;
    }
    return `
      <div class="task">
        <div class="name">${name}</div>
        <div class="meta"><span class="badge state-${st}">${st}</span></div>
        ${progressHtml}
        ${buttons ? `<div class="actions">${buttons}</div>` : ''}
      </div>`;
  }).join('');
}

list.addEventListener('click', (e) => {
  const btn = e.target.closest('button[data-action]');
  if (!btn) return;
  action(btn.dataset.id, btn.dataset.action);
});

const es = new EventSource(`${API}/events`);
es.addEventListener('task', ev => {
  try {
    const e = JSON.parse(ev.data);
    if (e.type === 'created') {
      tasks.set(e.info.id, { info: e.info, progress: null });
    } else if (e.type === 'state_changed') {
      const t = tasks.get(e.id);
      if (t) { t.info.state = e.state; t.info.finished_at = e.finished_at; }
    } else if (e.type === 'progress') {
      const t = tasks.get(e.id);
      if (t) { t.progress = e; }
    } else if (e.type === 'speed_limit_changed') {
      rlEnabled.checked = e.enabled;
      if (e.limit_bps > 0) rlValue.value = Math.round(e.limit_bps / 1024);
    }
    render();
  } catch (err) { console.error(err); }
});
es.onerror = () => {
  list.innerHTML = '<div class="empty">引擎未启动或连接中断</div>';
};

loadRateLimit();