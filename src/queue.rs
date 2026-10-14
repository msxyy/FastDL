use anyhow::Result;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::{broadcast, Notify, OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "state", rename_all = "lowercase")]
pub enum TaskState {
    Queued,
    Running,
    Paused,
    Completed { path: String },
    Failed { error: String },
    Cancelled,
    Interrupted,
}

// [FIX] 统一 terminal 语义，避免 load_from_disk / set_state / purge_finished
// 三处判断不一致（之前 load_from_disk 漏了 Interrupted）
impl TaskState {
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            TaskState::Completed { .. }
                | TaskState::Failed { .. }
                | TaskState::Cancelled
                | TaskState::Interrupted
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskInfo {
    pub id: String,
    pub url: String,
    pub state: TaskState,
    pub created_at: u64,
    pub finished_at: Option<u64>,
    #[serde(default)]
    pub expected_sha256: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TaskEvent {
    Created {
        info: TaskInfo,
    },
    StateChanged {
        id: String,
        state: TaskState,
        finished_at: Option<u64>,
    },
    Progress {
        id: String,
        downloaded: u64,
        total: u64,
        speed_bps: u64,
    },
    SpeedLimitChanged {
        enabled: bool,
        limit_bps: u64,
    },
}

#[derive(Clone)]
pub struct EventBus {
    tx: broadcast::Sender<TaskEvent>,
}

impl EventBus {
    pub fn new(capacity: usize) -> Self {
        let (tx, _) = broadcast::channel(capacity);
        Self { tx }
    }
    pub fn subscribe(&self) -> broadcast::Receiver<TaskEvent> {
        self.tx.subscribe()
    }
    pub fn emit(&self, event: TaskEvent) {
        let _ = self.tx.send(event);
    }
}

pub struct PauseState {
    paused: AtomicBool,
    notify: Notify,
}

impl PauseState {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            paused: AtomicBool::new(false),
            notify: Notify::new(),
        })
    }
    pub fn is_paused(&self) -> bool {
        self.paused.load(Ordering::Relaxed)
    }
    pub fn pause(&self) {
        self.paused.store(true, Ordering::Relaxed);
    }
    pub fn resume(&self) {
        self.paused.store(false, Ordering::Relaxed);
        self.notify.notify_waiters();
    }
    pub async fn wait_resume_or_cancel(&self, cancel: &CancellationToken) -> bool {
        while self.paused.load(Ordering::Relaxed) {
            tokio::select! {
                _ = self.notify.notified() => {}
                _ = cancel.cancelled() => return false,
            }
        }
        true
    }
}

/// 全局限速器（令牌桶）
pub struct RateLimiter {
    enabled: AtomicBool,
    limit_bps: std::sync::atomic::AtomicU64,
    bucket: std::sync::Mutex<TokenBucket>,
}

struct TokenBucket {
    tokens: f64,
    last: std::time::Instant,
}

impl RateLimiter {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            enabled: AtomicBool::new(false),
            limit_bps: std::sync::atomic::AtomicU64::new(0),
            bucket: std::sync::Mutex::new(TokenBucket {
                tokens: 0.0,
                last: std::time::Instant::now(),
            }),
        })
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::Relaxed)
    }
    pub fn limit_bps(&self) -> u64 {
        self.limit_bps.load(Ordering::Relaxed)
    }

    /// 设置：enabled=false 表示不限速
    pub fn set(&self, enabled: bool, limit_bps: u64) {
        self.enabled.store(enabled, Ordering::Relaxed);
        self.limit_bps.store(limit_bps, Ordering::Relaxed);
        if let Ok(mut b) = self.bucket.lock() {
            b.tokens = 0.0;
            b.last = std::time::Instant::now();
        }
    }

    /// 消耗 n 字节，返回需要 sleep 的时长
    pub fn consume(&self, n: u64) -> std::time::Duration {
        if !self.enabled.load(Ordering::Relaxed) {
            return std::time::Duration::ZERO;
        }
        let limit = self.limit_bps.load(Ordering::Relaxed);
        if limit == 0 {
            return std::time::Duration::ZERO;
        }

        let mut b = match self.bucket.lock() {
            Ok(b) => b,
            Err(_) => return std::time::Duration::ZERO,
        };
        let now = std::time::Instant::now();
        let elapsed = now.duration_since(b.last).as_secs_f64();
        // 桶容量 = 半秒的突发量，避免启动瞬间太卡
        let burst_cap = (limit as f64) * 0.5;
        b.tokens = (b.tokens + elapsed * (limit as f64)).min(burst_cap);
        b.last = now;

        if b.tokens >= n as f64 {
            b.tokens -= n as f64;
            std::time::Duration::ZERO
        } else {
            let needed = n as f64 - b.tokens;
            b.tokens = 0.0;
            let wait_s = needed / (limit as f64);
            std::time::Duration::from_secs_f64(wait_s.min(1.0))
        }
    }
}

pub struct TaskHandle {
    pub info: TaskInfo,
    pub cancel: CancellationToken,
    pub pause: Arc<PauseState>,
}

pub struct TaskManager {
    tasks: DashMap<String, TaskHandle>,
    semaphore: Arc<Semaphore>,
    events: EventBus,
    persist_path: PathBuf,
}

impl TaskManager {
    pub fn new(max_concurrent: usize, persist_path: PathBuf) -> Self {
        let mgr = Self {
            tasks: DashMap::new(),
            semaphore: Arc::new(Semaphore::new(max_concurrent)),
            events: EventBus::new(1024),
            persist_path,
        };
        mgr.load_from_disk();
        mgr
    }

    pub fn events(&self) -> EventBus {
        self.events.clone()
    }
    pub fn subscribe(&self) -> broadcast::Receiver<TaskEvent> {
        self.events.subscribe()
    }

    fn load_from_disk(&self) {
        let path = &self.persist_path;
        let content = match std::fs::read_to_string(path) {
            Ok(c) => c,
            Err(_) => {
                tracing::info!("任务持久化文件不存在，跳过加载");
                return;
            }
        };
        let list: Vec<TaskInfo> = match serde_json::from_str(&content) {
            Ok(l) => l,
            Err(e) => {
                tracing::warn!("任务持久化文件解析失败: {}", e);
                return;
            }
        };
        let n = list.len();
        for mut info in list {
            // [FIX] 用统一方法判断终态；非终态统一标记为 Interrupted
            if !info.state.is_terminal() {
                info.state = TaskState::Interrupted;
                info.finished_at = Some(now());
            }
            self.tasks.insert(
                info.id.clone(),
                TaskHandle {
                    info,
                    cancel: CancellationToken::new(),
                    pause: PauseState::new(),
                },
            );
        }
        tracing::info!("从磁盘恢复了 {} 个任务", n);
    }

    fn persist(&self) {
        let list: Vec<TaskInfo> = self.tasks.iter().map(|h| h.info.clone()).collect();
        if let Ok(json) = serde_json::to_string_pretty(&list) {
            if let Err(e) = std::fs::write(&self.persist_path, json) {
                tracing::warn!("任务持久化失败: {}", e);
            }
        }
    }

    pub fn create(
        &self,
        url: String,
        expected_sha256: Option<String>,
    ) -> (String, CancellationToken, Arc<PauseState>) {
        let id = Uuid::new_v4().to_string();
        let token = CancellationToken::new();
        let pause = PauseState::new();
        let info = TaskInfo {
            id: id.clone(),
            url,
            state: TaskState::Queued,
            created_at: now(),
            finished_at: None,
            expected_sha256,
        };
        self.tasks.insert(
            id.clone(),
            TaskHandle {
                info: info.clone(),
                cancel: token.clone(),
                pause: pause.clone(),
            },
        );
        self.events.emit(TaskEvent::Created { info });
        self.persist();
        (id, token, pause)
    }

    pub fn set_state(&self, id: &str, state: TaskState) {
        let finished = if let Some(mut h) = self.tasks.get_mut(id) {
            // [FIX] 统一用 is_terminal()
            let terminal = state.is_terminal();
            h.info.state = state.clone();
            if terminal {
                let n = now();
                h.info.finished_at = Some(n);
            }
            h.info.finished_at
        } else {
            None
        };

        self.events.emit(TaskEvent::StateChanged {
            id: id.to_string(),
            state,
            finished_at: finished,
        });
        self.persist();
    }

    pub fn get(&self, id: &str) -> Option<TaskInfo> {
        self.tasks.get(id).map(|h| h.info.clone())
    }

    pub fn list(&self) -> Vec<TaskInfo> {
        let mut v: Vec<_> = self.tasks.iter().map(|h| h.info.clone()).collect();
        v.sort_by_key(|t| std::cmp::Reverse(t.created_at));
        v
    }

    pub fn cancel(&self, id: &str) -> bool {
        if let Some(h) = self.tasks.get(id) {
            h.cancel.cancel();
            h.pause.resume();
            true
        } else {
            false
        }
    }

    pub fn pause(&self, id: &str) -> bool {
        if let Some(h) = self.tasks.get(id) {
            if matches!(h.info.state, TaskState::Running | TaskState::Queued) {
                h.pause.pause();
                drop(h);
                self.set_state(id, TaskState::Paused);
                return true;
            }
        }
        false
    }

    pub fn resume(&self, id: &str) -> bool {
        if let Some(h) = self.tasks.get(id) {
            if matches!(h.info.state, TaskState::Paused) {
                h.pause.resume();
                drop(h);
                self.set_state(id, TaskState::Running);
                return true;
            }
        }
        false
    }

    pub fn purge_finished(&self) -> usize {
        let ids: Vec<String> = self
            .tasks
            .iter()
            .filter(|h| h.info.state.is_terminal()) // [FIX] 统一判断
            .map(|h| h.info.id.clone())
            .collect();
        let n = ids.len();
        for id in ids {
            self.tasks.remove(&id);
        }
        if n > 0 {
            self.persist();
        }
        n
    }

    pub async fn acquire_slot(&self) -> Result<OwnedSemaphorePermit> {
        Ok(self.semaphore.clone().acquire_owned().await?)
    }
}

fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
}