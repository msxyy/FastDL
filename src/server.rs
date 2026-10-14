use crate::downloader::{Downloader, ProgressReporter};
use crate::magnet::{self, TorrentSource};
use crate::queue::{PauseState, RateLimiter, TaskEvent, TaskInfo, TaskManager, TaskState};
use anyhow::{Context, Result};
use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::sse::{Event, KeepAlive, Sse},
    routing::{get, post},
    Json, Router,
};
use futures::stream::Stream;
use librqbit::Session;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::convert::Infallible;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::broadcast;
use tower_http::cors::{Any, CorsLayer};

#[derive(Clone)]
pub struct AppState {
    pub downloader: Arc<Downloader>,
    pub output_dir: Arc<String>,
    pub tasks: Arc<TaskManager>,
    pub rate_limiter: Arc<RateLimiter>,
    pub bt_session: Arc<Session>,
    // [aria2 兼容] 缓存最新进度，供 aria2.tellStatus 使用
    pub progress_cache: Arc<Mutex<HashMap<String, (u64, u64, u64)>>>,
}

#[derive(Deserialize)]
pub struct TaskRequest {
    pub url: String,
    #[serde(default)]
    pub referer: Option<String>,
    #[serde(default)]
    pub cookie: Option<String>,
    #[serde(default)]
    pub sha256: Option<String>,
}

#[derive(Serialize)]
pub struct TaskResponse {
    pub ok: bool,
    pub message: String,
    pub task_id: Option<String>,
    pub path: Option<String>,
}

#[derive(Deserialize)]
pub struct SpeedLimitRequest {
    pub enabled: bool,
    pub limit_bps: u64,
}

pub async fn run_server(
    port: u16,
    downloader: Downloader,
    output_dir: String,
    max_concurrent: usize,
    rate_limiter: Arc<RateLimiter>,
) -> Result<()> {
    let persist_path = PathBuf::from("tasks.json");

    let bt_session: Arc<Session> = Session::new(PathBuf::from(&output_dir))
        .await
        .context("创建 BT session 失败")?;

    let tasks = Arc::new(TaskManager::new(max_concurrent, persist_path));
    let progress_cache: Arc<Mutex<HashMap<String, (u64, u64, u64)>>> =
        Arc::new(Mutex::new(HashMap::new()));

    // 订阅 EventBus，把 progress 缓存起来给 aria2.tellStatus 用
    {
        let mut rx = tasks.subscribe();
        let cache = progress_cache.clone();
        tokio::spawn(async move {
            loop {
                match rx.recv().await {
                    Ok(TaskEvent::Progress {
                        id,
                        downloaded,
                        total,
                        speed_bps,
                    }) => {
                        cache
                            .lock()
                            .unwrap()
                            .insert(id, (downloaded, total, speed_bps));
                    }
                    Ok(_) => {}
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        });
    }

    let state = AppState {
        downloader: Arc::new(downloader),
        output_dir: Arc::new(output_dir),
        tasks,
        rate_limiter,
        bt_session,
        progress_cache,
    };

    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    let app = Router::new()
        .route("/", get(|| async { "FastDL is running" }))
        .route("/download", post(handle_download))
        .route("/tasks", get(list_tasks).delete(purge_tasks))
        .route("/tasks/:id", get(get_task))
        .route("/tasks/:id/cancel", post(cancel_task))
        .route("/tasks/:id/pause", post(pause_task))
        .route("/tasks/:id/resume", post(resume_task))
        .route("/ratelimit", get(get_ratelimit).post(set_ratelimit))
        .route("/events", get(sse_handler))
        // [aria2 兼容] 脚本猫等直链工具走这个端点
        .route("/jsonrpc", post(handle_jsonrpc))
        .layer(cors)
        .with_state(state);

    let addr = format!("127.0.0.1:{}", port);
    tracing::info!("FastDL 监听 {} (含 aria2 JSON-RPC 兼容端点)", addr);
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

// ============================================================
// 原有 API handlers
// ============================================================

async fn sse_handler(
    State(state): State<AppState>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let mut rx = state.tasks.subscribe();
    let snapshot: Vec<TaskEvent> = state
        .tasks
        .list()
        .into_iter()
        .map(|info| TaskEvent::Created { info })
        .collect();

    let rate_enabled = state.rate_limiter.is_enabled();
    let rate_bps = state.rate_limiter.limit_bps();

    let stream = async_stream::stream! {
        for ev in snapshot {
            if let Ok(data) = serde_json::to_string(&ev) {
                yield Ok::<_, Infallible>(Event::default().event("task").data(data));
            }
        }
        let init = TaskEvent::SpeedLimitChanged { enabled: rate_enabled, limit_bps: rate_bps };
        if let Ok(data) = serde_json::to_string(&init) {
            yield Ok(Event::default().event("task").data(data));
        }

        loop {
            match rx.recv().await {
                Ok(ev) => {
                    if let Ok(data) = serde_json::to_string(&ev) {
                        yield Ok(Event::default().event("task").data(data));
                    }
                }
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    tracing::warn!("SSE 客户端落后 {} 条事件", n);
                    continue;
                }
                Err(broadcast::error::RecvError::Closed) => break,
            }
        }
    };
    Sse::new(stream).keep_alive(KeepAlive::new().interval(Duration::from_secs(15)))
}

async fn handle_download(
    State(state): State<AppState>,
    Json(req): Json<TaskRequest>,
) -> (StatusCode, Json<TaskResponse>) {
    tracing::info!("收到任务: {}", req.url);

    let id = start_download_task(
        &state,
        req.url.clone(),
        req.referer.clone(),
        req.cookie.clone(),
        req.sha256.clone(),
    );

    (
        StatusCode::OK,
        Json(TaskResponse {
            ok: true,
            message: "任务已加入队列".into(),
            task_id: Some(id),
            path: None,
        }),
    )
}

/// 抽出公用逻辑：创建任务 + spawn 下载，返回 task_id
fn start_download_task(
    state: &AppState,
    url: String,
    referer: Option<String>,
    cookie: Option<String>,
    sha256: Option<String>,
) -> String {
    let (id, token, pause) = state.tasks.create(url.clone(), sha256.clone());
    let tasks = state.tasks.clone();
    let downloader = state.downloader.clone();
    let bt_session = state.bt_session.clone();
    let output_dir = state.output_dir.clone();
    let task_id = id.clone();
    let bus = state.tasks.events();

    tokio::spawn(async move {
        let _permit = match tasks.acquire_slot().await {
            Ok(p) => p,
            Err(_) => return,
        };
        tasks.set_state(&task_id, TaskState::Running);
        let progress = ProgressReporter {
            bus,
            task_id: task_id.clone(),
        };

        let result = run_task(
            downloader.clone(),
            bt_session,
            &url,
            &output_dir,
            referer.as_deref(),
            cookie.as_deref(),
            token.clone(),
            pause,
            Some(progress),
            sha256,
        )
        .await;

        match result {
            Ok(path) => {
                tracing::info!("任务 {} 完成: {}", task_id, path.display());
                tasks.set_state(
                    &task_id,
                    TaskState::Completed {
                        path: path.to_string_lossy().to_string(),
                    },
                );
            }
            Err(e) => {
                if token.is_cancelled() {
                    tracing::warn!("任务 {} 已取消", task_id);
                    tasks.set_state(&task_id, TaskState::Cancelled);
                } else {
                    tracing::error!("任务 {} 失败: {:#}", task_id, e);
                    tasks.set_state(
                        &task_id,
                        TaskState::Failed {
                            error: format!("{:#}", e),
                        },
                    );
                }
            }
        }
    });

    id
}

#[allow(clippy::too_many_arguments)]
async fn run_task(
    downloader: Arc<Downloader>,
    bt_session: Arc<Session>,
    url: &str,
    output_dir: &str,
    referer: Option<&str>,
    cookie: Option<&str>,
    cancel: tokio_util::sync::CancellationToken,
    pause: Arc<PauseState>,
    progress: Option<ProgressReporter>,
    sha256: Option<String>,
) -> Result<std::path::PathBuf> {
    if url.starts_with("magnet:") {
        return magnet::download_torrent(
            bt_session,
            TorrentSource::Magnet(url.to_string()),
            output_dir,
            cancel,
            pause,
            progress,
        )
        .await;
    }
    if url.ends_with(".torrent") && !url.starts_with("http") {
        let bytes = tokio::fs::read(url).await?;
        return magnet::download_torrent(
            bt_session,
            TorrentSource::File(bytes),
            output_dir,
            cancel,
            pause,
            progress,
        )
        .await;
    }
    if url.starts_with("http") && url.ends_with(".torrent") {
        let bytes = downloader
            .download_to_memory(url, referer, cookie, cancel.clone())
            .await?;
        tracing::info!("已获取种子文件 {} 字节", bytes.len());
        return magnet::download_torrent(
            bt_session,
            TorrentSource::File(bytes),
            output_dir,
            cancel,
            pause,
            progress,
        )
        .await;
    }
    downloader
        .download(url, output_dir, referer, cookie, cancel, pause, progress, sha256)
        .await
}

async fn list_tasks(State(state): State<AppState>) -> Json<Vec<TaskInfo>> {
    Json(state.tasks.list())
}

async fn get_task(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> (StatusCode, Json<Option<TaskInfo>>) {
    match state.tasks.get(&id) {
        Some(t) => (StatusCode::OK, Json(Some(t))),
        None => (StatusCode::NOT_FOUND, Json(None)),
    }
}

async fn cancel_task(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> (StatusCode, Json<TaskResponse>) {
    if state.tasks.cancel(&id) {
        (
            StatusCode::OK,
            Json(TaskResponse {
                ok: true,
                message: "取消信号已发送".into(),
                task_id: Some(id),
                path: None,
            }),
        )
    } else {
        (
            StatusCode::NOT_FOUND,
            Json(TaskResponse {
                ok: false,
                message: "任务不存在".into(),
                task_id: None,
                path: None,
            }),
        )
    }
}

async fn pause_task(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> (StatusCode, Json<TaskResponse>) {
    if state.tasks.pause(&id) {
        (
            StatusCode::OK,
            Json(TaskResponse {
                ok: true,
                message: "已暂停".into(),
                task_id: Some(id),
                path: None,
            }),
        )
    } else {
        (
            StatusCode::BAD_REQUEST,
            Json(TaskResponse {
                ok: false,
                message: "无法暂停".into(),
                task_id: None,
                path: None,
            }),
        )
    }
}

async fn resume_task(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> (StatusCode, Json<TaskResponse>) {
    if state.tasks.resume(&id) {
        (
            StatusCode::OK,
            Json(TaskResponse {
                ok: true,
                message: "已恢复".into(),
                task_id: Some(id),
                path: None,
            }),
        )
    } else {
        (
            StatusCode::BAD_REQUEST,
            Json(TaskResponse {
                ok: false,
                message: "无法恢复".into(),
                task_id: None,
                path: None,
            }),
        )
    }
}

async fn purge_tasks(State(state): State<AppState>) -> Json<serde_json::Value> {
    let n = state.tasks.purge_finished();
    Json(serde_json::json!({ "ok": true, "removed": n }))
}

async fn get_ratelimit(State(state): State<AppState>) -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "enabled": state.rate_limiter.is_enabled(),
        "limit_bps": state.rate_limiter.limit_bps(),
    }))
}

async fn set_ratelimit(
    State(state): State<AppState>,
    Json(req): Json<SpeedLimitRequest>,
) -> Json<serde_json::Value> {
    state.rate_limiter.set(req.enabled, req.limit_bps);
    state.tasks.events().emit(TaskEvent::SpeedLimitChanged {
        enabled: req.enabled,
        limit_bps: req.limit_bps,
    });
    tracing::info!("限速设置: enabled={}, limit={} B/s", req.enabled, req.limit_bps);
    Json(serde_json::json!({ "ok": true }))
}

// ============================================================
// [aria2 兼容] JSON-RPC 端点
// ============================================================
//
// 脚本猫 / Tampermonkey 的网盘直链脚本通常支持 "Aria2" 作为下载器。
// 它们会向 http://<host>:<port>/jsonrpc 发 JSON-RPC 请求。
// 我们在这里实现最常用的几个 method，其他忽略。
//
// 已实现：
//   aria2.addUri          → 新建下载任务
//   aria2.tellStatus      → 查询任务进度
//   aria2.getGlobalStat   → 全局统计
//   aria2.getVersion      → 版本信息（脚本用来测试连接）
//   aria2.remove          → 取消任务
//   aria2.forceRemove     → 强制取消任务
//   aria2.pause           → 暂停任务
//   aria2.unpause         → 恢复任务
//   aria2.changeGlobalOption → 设置限速
//   system.listMethods    → 支持的方法列表
//
// Token 参数会被忽略（无鉴权）。
// ------------------------------------------------------------

async fn handle_jsonrpc(
    State(state): State<AppState>,
    Json(req): Json<Value>,
) -> Json<Value> {
    let id = req.get("id").cloned().unwrap_or(Value::Null);
    let method = req.get("method").and_then(|v| v.as_str()).unwrap_or("");
    let params = req.get("params").cloned().unwrap_or(Value::Array(vec![]));

    let result = dispatch_jsonrpc(&state, method, &params).await;

    match result {
        Ok(v) => Json(json!({ "jsonrpc": "2.0", "id": id, "result": v })),
        Err(e) => {
            tracing::warn!("JSON-RPC {} 失败: {}", method, e);
            Json(json!({
                "jsonrpc": "2.0",
                "id": id,
                "error": { "code": -32603, "message": e.to_string() }
            }))
        }
    }
}

/// 从 params 里剥掉可能的 "token:xxx" 前缀，返回剩余的参数数组
fn strip_token(params: &Value) -> Vec<Value> {
    let arr = match params.as_array() {
        Some(a) => a.clone(),
        None => return vec![],
    };
    if arr.is_empty() {
        return arr;
    }
    // token:xxx 是字符串形式，出现在 params[0]
    if arr[0].is_string() {
        let s = arr[0].as_str().unwrap_or("");
        if s.starts_with("token:") {
            return arr[1..].to_vec();
        }
    }
    arr
}

async fn dispatch_jsonrpc(
    state: &AppState,
    method: &str,
    params: &Value,
) -> Result<Value> {
    let args = strip_token(params);
    match method {
        "aria2.addUri" => {
            // params: [[url1, url2, ...], {options}]
            let urls = args
                .get(0)
                .and_then(|v| v.as_array())
                .ok_or_else(|| anyhow::anyhow!("addUri: 缺少 url 数组"))?;
            let first_url = urls
                .iter()
                .filter_map(|v| v.as_str())
                .find(|u| u.starts_with("http") || u.starts_with("magnet:"))
                .ok_or_else(|| anyhow::anyhow!("addUri: 没有有效 url"))?;

            // 如果请求里带 referer / user-agent / header，一并提取
            let mut referer: Option<String> = None;
            let mut cookie: Option<String> = None;
            if let Some(opts) = args.get(1).and_then(|v| v.as_object()) {
                if let Some(r) = opts.get("referer").and_then(|v| v.as_str()) {
                    referer = Some(r.to_string());
                }
                // aria2 的 header 数组形如 ["Cookie: xxx", "User-Agent: yyy"]
                if let Some(headers) = opts.get("header").and_then(|v| v.as_array()) {
                    for h in headers {
                        if let Some(hs) = h.as_str() {
                            if let Some(rest) = hs.strip_prefix("Cookie:") {
                                cookie = Some(rest.trim().to_string());
                            }
                        }
                    }
                }
            }

            tracing::info!(
                "[aria2.addUri] url={} referer={:?} cookie={}",
                first_url,
                referer,
                cookie.as_ref().map(|c| c.len()).unwrap_or(0)
            );

            let gid = start_download_task(
                state,
                first_url.to_string(),
                referer,
                cookie,
                None,
            );
            Ok(json!(gid))
        }

        "aria2.tellStatus" => {
            let gid = args
                .get(0)
                .and_then(|v| v.as_str())
                .ok_or_else(|| anyhow::anyhow!("tellStatus: 缺少 gid"))?;

            let info = match state.tasks.get(gid) {
                Some(t) => t,
                None => {
                    return Err(anyhow::anyhow!("tellStatus: 未找到任务 {}", gid));
                }
            };

            let (downloaded, total, speed) = state
                .progress_cache
                .lock()
                .unwrap()
                .get(gid)
                .copied()
                .unwrap_or((0, 0, 0));

            let status = match &info.state {
                TaskState::Queued => "waiting",
                TaskState::Running => "active",
                TaskState::Paused => "paused",
                TaskState::Completed { .. } => "complete",
                TaskState::Failed { .. } => "error",
                TaskState::Cancelled => "removed",
                TaskState::Interrupted => "error",
            };

            Ok(json!({
                "gid": gid,
                "status": status,
                "totalLength": total.to_string(),
                "completedLength": downloaded.to_string(),
                "downloadSpeed": speed.to_string(),
                "uploadSpeed": "0",
                "connections": "0",
                "dir": state.output_dir.to_string(),
                "files": [{
                    "index": "1",
                    "path": "",
                    "length": total.to_string(),
                    "completedLength": downloaded.to_string(),
                    "selected": "true",
                    "uris": [{ "uri": info.url, "status": "used" }]
                }]
            }))
        }

        "aria2.getGlobalStat" => {
            let list = state.tasks.list();
            let num_active = list
                .iter()
                .filter(|t| matches!(t.state, TaskState::Running))
                .count();
            let num_waiting = list
                .iter()
                .filter(|t| matches!(t.state, TaskState::Queued))
                .count();
            let num_stopped = list
                .iter()
                .filter(|t| {
                    matches!(
                        t.state,
                        TaskState::Completed { .. }
                            | TaskState::Failed { .. }
                            | TaskState::Cancelled
                    )
                })
                .count();
            Ok(json!({
                "downloadSpeed": "0",
                "uploadSpeed": "0",
                "numActive": num_active.to_string(),
                "numWaiting": num_waiting.to_string(),
                "numStopped": num_stopped.to_string(),
                "numStoppedTotal": num_stopped.to_string(),
            }))
        }

        "aria2.getVersion" => Ok(json!({
            "version": "1.36.0",
            "enabledFeatures": [
                "Async DNS",
                "BitTorrent",
                "Firefox3 Cookie",
                "GZip",
                "HTTPS",
                "Message Digest",
                "Metalink",
                "XML-RPC",
                "SFTP"
            ]
        })),

        "aria2.remove" | "aria2.forceRemove" => {
            let gid = args
                .get(0)
                .and_then(|v| v.as_str())
                .ok_or_else(|| anyhow::anyhow!("remove: 缺少 gid"))?;
            state.tasks.cancel(gid);
            Ok(json!(gid))
        }

        "aria2.pause" => {
            let gid = args
                .get(0)
                .and_then(|v| v.as_str())
                .ok_or_else(|| anyhow::anyhow!("pause: 缺少 gid"))?;
            state.tasks.pause(gid);
            Ok(json!(gid))
        }

        "aria2.unpause" => {
            let gid = args
                .get(0)
                .and_then(|v| v.as_str())
                .ok_or_else(|| anyhow::anyhow!("unpause: 缺少 gid"))?;
            state.tasks.resume(gid);
            Ok(json!(gid))
        }

        "aria2.changeGlobalOption" => {
            if let Some(opts) = args.get(0).and_then(|v| v.as_object()) {
                if let Some(limit) = opts.get("max-overall-download-limit").and_then(|v| v.as_str()) {
                    if let Ok(bps) = limit.parse::<u64>() {
                        if bps == 0 {
                            state.rate_limiter.set(false, 0);
                        } else {
                            state.rate_limiter.set(true, bps);
                        }
                    }
                }
            }
            Ok(json!("OK"))
        }

        "aria2.getSessionInfo" => Ok(json!({ "sessionId": "fastdl" })),

        "system.listMethods" => Ok(json!([
            "aria2.addUri",
            "aria2.tellStatus",
            "aria2.getGlobalStat",
            "aria2.getVersion",
            "aria2.remove",
            "aria2.forceRemove",
            "aria2.pause",
            "aria2.unpause",
            "aria2.changeGlobalOption",
            "aria2.getSessionInfo",
            "system.listMethods"
        ])),

        other => Err(anyhow::anyhow!("不支持的 method: {}", other)),
    }
}