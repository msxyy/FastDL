const API = 'http://127.0.0.1:6800';
const ICON_URL = 'icon128.png';
const NATIVE_HOST = 'com.fastdl.launcher';

let runningCount = 0;
const runningTaskIds = new Set();
const notifiedTaskIds = new Set();
const taskUrlMap = new Map();

// ============================================================
// 点击扩展图标 → 打开侧边栏
// ============================================================
chrome.sidePanel
  .setPanelBehavior({ openPanelOnActionClick: true })
  .catch((e) => console.error('设置侧边栏行为失败:', e));

// ============================================================
// Native Messaging：确保引擎在跑
// ============================================================
function ensureEngine() {
  return new Promise((resolve) => {
    const timer = setTimeout(() => {
      console.warn('ensureEngine 超时 (8s)');
      resolve(false);
    }, 8000);

    try {
      chrome.runtime.sendNativeMessage(
        NATIVE_HOST,
        { action: 'ensure', ts: Date.now() },
        (response) => {
          clearTimeout(timer);
          if (chrome.runtime.lastError) {
            console.warn('Native host 未注册或错误:', chrome.runtime.lastError.message);
            resolve(false);
            return;
          }
          console.log('Launcher 响应:', response);
          resolve(
            response?.status === 'started' || response?.status === 'already_running'
          );
        }
      );
    } catch (e) {
      clearTimeout(timer);
      console.error('sendNativeMessage 异常:', e);
      resolve(false);
    }
  });
}

// ============================================================
// 拦截浏览器下载 → 唤醒引擎 → 提交任务
// ============================================================
chrome.downloads.onCreated.addListener(async (item) => {
  if (!item.url.startsWith('http') && !item.url.startsWith('magnet:')) return;

  // 取消浏览器自带下载
  chrome.downloads.cancel(item.id, () => {
    chrome.downloads.erase({ id: item.id });
  });

  // 唤醒引擎
  const ok = await ensureEngine();
  if (!ok) {
    chrome.notifications.create(`err-${Date.now()}`, {
      type: 'basic',
      iconUrl: ICON_URL,
      title: 'FastDL 无法启动',
      message: '请检查是否已安装 FastDL 桌面程序，或手动运行 fastdl.exe',
      priority: 2,
    });
    return;
  }

  // 提交任务
  try {
    const res = await fetch(`${API}/download`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        url: item.url,
        referer: item.referrer || null,
      }),
    });
    const data = await res.json();
    console.log('FastDL 任务已提交:', data);
  } catch (e) {
    console.error('提交失败:', e);
  }
});

// ============================================================
// 通知
// ============================================================
function notifyTaskStarted(taskId, url) {
  if (notifiedTaskIds.has(taskId)) return;
  notifiedTaskIds.add(taskId);

  const name = url.split('/').pop().split('?')[0] || url;
  chrome.notifications.create(`start-${taskId}`, {
    type: 'basic',
    iconUrl: ICON_URL,
    title: 'FastDL · 开始下载',
    message: name.length > 80 ? name.slice(0, 77) + '…' : name,
    priority: 1,
    silent: false,
  });
}

function notifyTaskFinished(taskId, url, kind) {
  const name = url.split('/').pop().split('?')[0] || url;
  const titleMap = {
    completed: 'FastDL · 下载完成',
    failed: 'FastDL · 下载失败',
    cancelled: 'FastDL · 已取消',
  };
  chrome.notifications.create(`done-${taskId}`, {
    type: 'basic',
    iconUrl: ICON_URL,
    title: titleMap[kind] || 'FastDL',
    message: name.length > 80 ? name.slice(0, 77) + '…' : name,
    priority: kind === 'failed' ? 2 : 1,
    silent: kind !== 'failed',
  });
}

// ============================================================
// 图标 badge
// ============================================================
function updateBadge() {
  if (runningCount > 0) {
    chrome.action.setBadgeText({ text: String(runningCount) });
    chrome.action.setBadgeBackgroundColor({ color: '#3b82f6' });
  } else {
    chrome.action.setBadgeText({ text: '' });
  }
}

// ============================================================
// [FIX] 与引擎校准状态
// 场景：SW 被浏览器回收后重启、SSE 断线重连、页面初次加载
// 都需要从 /tasks 拉一次全量状态，把内存里的 runningTaskIds 对齐
// ============================================================
async function recalibrate() {
  try {
    const res = await fetch(`${API}/tasks`);
    if (!res.ok) return;
    const list = await res.json();

    const newRunning = new Set();
    for (const t of list) {
      const st = t.state.state || t.state;
      if (st === 'running') {
        newRunning.add(t.id);
        // 补一次 URL 映射，避免通知里显示空
        if (!taskUrlMap.has(t.id)) taskUrlMap.set(t.id, t.url);
      } else if (['completed', 'failed', 'cancelled', 'interrupted'].includes(st)) {
        // 终态任务从内存映射里清掉
        taskUrlMap.delete(t.id);
        notifiedTaskIds.delete(t.id);
      }
    }

    runningTaskIds.clear();
    for (const id of newRunning) runningTaskIds.add(id);
    runningCount = runningTaskIds.size;
    updateBadge();
  } catch {
    /* 引擎可能还没起，忽略 */
  }
}

// ============================================================
// SSE 订阅
// ============================================================
function startSSE() {
  const es = new EventSource(`${API}/events`);

  // [FIX] 每次连接成功都校准一次，防止 SW 被回收后 badge 错乱
  es.onopen = () => {
    recalibrate();
  };

  es.addEventListener('task', (ev) => {
    try {
      const e = JSON.parse(ev.data);

      if (e.type === 'created') {
        taskUrlMap.set(e.info.id, e.info.url);
        const st = e.info.state.state || e.info.state;
        if (st === 'running') {
          runningTaskIds.add(e.info.id);
          runningCount = runningTaskIds.size;
          updateBadge();
          notifyTaskStarted(e.info.id, e.info.url);
        }
      } else if (e.type === 'state_changed') {
        const st = e.state.state || e.state;

        if (st === 'running') {
          const isNew = !runningTaskIds.has(e.id);
          runningTaskIds.add(e.id);
          runningCount = runningTaskIds.size;
          updateBadge();
          if (isNew) {
            const url = taskUrlMap.get(e.id) || '';
            notifyTaskStarted(e.id, url);
          }
        } else if (
          ['completed', 'failed', 'cancelled', 'interrupted'].includes(st)
        ) {
          const wasRunning = runningTaskIds.has(e.id);
          runningTaskIds.delete(e.id);
          runningCount = runningTaskIds.size;
          updateBadge();

          if (wasRunning && (st === 'completed' || st === 'failed')) {
            const url = taskUrlMap.get(e.id) || '';
            notifyTaskFinished(e.id, url, st);
          }
          taskUrlMap.delete(e.id);
          notifiedTaskIds.delete(e.id);
        }
      }
      // progress / speed_limit_changed 事件不影响 badge，交给侧边栏处理
    } catch (err) {
      console.error('SSE 解析失败:', err);
    }
  });

  es.onerror = () => {
    es.close();
    // [FIX] 断线重连延迟；重连成功后 onopen 会触发 recalibrate()
    setTimeout(startSSE, 5000);
  };
}

startSSE();
updateBadge();

// ============================================================
// 通知点击 → 打开侧边栏
// ============================================================
chrome.notifications.onClicked.addListener(async () => {
  try {
    const [tab] = await chrome.tabs.query({ active: true, currentWindow: true });
    if (tab && tab.windowId) {
      await chrome.sidePanel.open({ windowId: tab.windowId });
    }
  } catch (e) {
    console.warn('打开侧边栏失败:', e);
  }
});

// ============================================================
// [FIX] SW 首次唤醒时也校准一次
// （原来这段是重复的 IIFE，统一收敛到 recalibrate）
// ============================================================
recalibrate();