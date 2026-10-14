use crate::meta::{ChunkMeta, DownloadMeta};
use crate::queue::{EventBus, PauseState, RateLimiter};
use anyhow::{bail, Context, Result};
use futures::{future::join_all, StreamExt};
use indicatif::{ProgressBar, ProgressStyle};
use reqwest::Client;
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use tokio_util::sync::CancellationToken;

const PROBE_TIMEOUT_SECS: u64 = 15;
const PROBE_BYTES: u64 = 256 * 1024;

const PROBE_EARLY_WINDOW_SECS: u64 = 3;
const PROBE_MIN_SPEED_BPS: u64 = 200 * 1024;

const WRITE_BATCH: usize = 1024 * 1024;

// [SLOW-HOST] 强制降并发的域名，避免被服务端限流后 403
const SLOW_HOSTS: &[&str] = &[
    "d.pcs.baidu.com",
    "baidupcs.com",
    "pan.baidu.com",
    "dldir1.qq.com",           // 腾讯 CDN 对过高并发也偶有限制，视情况可去
];

// [SLOW-HOST] 慢速域名下使用的 worker 数
const SLOW_HOST_CONNECTIONS: usize = 2;

const GITHUB_MIRRORS: &[&str] = &[
    "https://ghp.ci/",
    "https://moeyy.cn/gh-proxy/",
    "https://ghproxy.homeboyc.cn/",
    "https://v6.gh-proxy.org/",
    "https://ghproxy.net/",
    "https://gh-proxy.com/",
];

#[derive(Clone)]
pub struct ProgressReporter {
    pub bus: EventBus,
    pub task_id: String,
}

#[derive(Clone)]
pub struct Downloader {
    client: Client,
    connections: usize,
    rate_limiter: Arc<RateLimiter>,
}

#[derive(Clone)]
struct Candidate {
    url: String,
    speed_bps: u64,
    total_size: Option<u64>,
    supports_range: bool,
}

#[derive(Clone)]
struct ProbeResult {
    candidates: Vec<Candidate>,
    total_size: Option<u64>,
    supports_range: bool,
    suggested_filename: Option<String>,
    late_candidates: Arc<tokio::sync::Mutex<Vec<Candidate>>>,
}

enum ChunkResult {
    Completed,
    Interrupted,
}

enum DownloadErr {
    Cancelled,
    Paused,
    Failed(anyhow::Error),
}

impl Downloader {
    pub fn new(connections: usize, rate_limiter: Arc<RateLimiter>) -> Result<Self> {
        let client = Client::builder()
            .user_agent(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) \
                 AppleWebKit/537.36 (KHTML, like Gecko) \
                 Chrome/124.0.0.0 Safari/537.36 FastDL/0.1",
            )
            .timeout(Duration::from_secs(120))
            .pool_max_idle_per_host((connections * 4).max(64))
            .build()?;
        Ok(Self {
            client,
            connections,
            rate_limiter,
        })
    }

    /// [SLOW-HOST] 根据原始 URL 决定这个任务实际用多少并发。
    /// chunk 划分仍按 self.connections 走，只有 worker 数变化。
    fn effective_connections(&self, url: &str) -> usize {
        // 环境变量覆盖：FASTDL_SLOW_HOSTS="host1,host2" FASTDL_SLOW_CONNECTIONS=4
        let (hosts, limit) = match (
            std::env::var("FASTDL_SLOW_HOSTS"),
            std::env::var("FASTDL_SLOW_CONNECTIONS"),
        ) {
            (Ok(h), Ok(n)) => {
                let hosts: Vec<String> = h
                    .split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect();
                let n = n.parse::<usize>().unwrap_or(SLOW_HOST_CONNECTIONS);
                (hosts, n.max(1))
            }
            _ => (
                SLOW_HOSTS.iter().map(|s| s.to_string()).collect(),
                SLOW_HOST_CONNECTIONS,
            ),
        };

        if hosts.iter().any(|h| url.contains(h.as_str())) {
            let capped = limit.min(self.connections).max(1);
            tracing::info!(
                "检测到慢速域名，worker 数从 {} 降为 {}（避免并发 403）",
                self.connections,
                capped
            );
            capped
        } else {
            self.connections
        }
    }

    fn candidate_urls(url: &str) -> Vec<String> {
        let mut urls = vec![url.to_string()];
        if !(url.contains("github.com") || url.contains("githubusercontent.com")) {
            return urls;
        }

        let mut mirrors: Vec<String> =
            GITHUB_MIRRORS.iter().map(|s| s.to_string()).collect();

        if let Ok(extra) = std::env::var("FASTDL_MIRRORS") {
            for m in extra.split(',') {
                let m = m.trim();
                if !m.is_empty() {
                    mirrors.insert(
                        0,
                        if m.ends_with('/') {
                            m.to_string()
                        } else {
                            format!("{}/", m)
                        },
                    );
                }
            }
        }

        for m in &mirrors {
            urls.push(format!("{}{}", m, url));
        }
        urls
    }

    async fn probe_url(
        client: &Client,
        url: &str,
        referer: Option<&str>,
        cookie: Option<&str>,
    ) -> Result<(Option<u64>, bool, u64, Option<String>)> {
        let range_end = PROBE_BYTES.saturating_sub(1);
        let range = format!("bytes=0-{}", range_end);

        let mut req = client
            .get(url)
            .header("Range", range)
            .header("Accept-Encoding", "identity");
        if let Some(r) = referer {
            req = req.header("Referer", r);
        }
        if let Some(c) = cookie {
            req = req.header("Cookie", c);
        }

        let start = Instant::now();
        let resp = req.send().await.context("探测请求失败")?;
        let status = resp.status();

        let filename = resp
            .headers()
            .get(reqwest::header::CONTENT_DISPOSITION)
            .and_then(|v| v.to_str().ok())
            .and_then(parse_content_disposition);

        let (total, supports_range) = if status == reqwest::StatusCode::PARTIAL_CONTENT {
            let size = resp
                .headers()
                .get("content-range")
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.split('/').nth(1))
                .and_then(|s| s.trim().parse::<u64>().ok());
            (size, true)
        } else if status.is_success() {
            (resp.content_length(), false)
        } else {
            bail!("HTTP {}", status);
        };

        let mut received: u64 = 0;
        let mut stream = resp.bytes_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.context("读探测数据失败")?;
            received += chunk.len() as u64;
            if received >= PROBE_BYTES {
                break;
            }
        }

        let elapsed = start.elapsed().as_secs_f64();
        let speed_bps = if received > 0 && elapsed > 0.0 {
            (received as f64 / elapsed) as u64
        } else {
            0
        };

        Ok((total, supports_range, speed_bps, filename))
    }

    async fn resolve(
        &self,
        url: &str,
        referer: Option<&str>,
        cookie: Option<&str>,
    ) -> ProbeResult {
        let candidates = Self::candidate_urls(url);
        let total_candidates = candidates.len();
        tracing::info!(
            "并发探测 {} 个候选源（软窗口 {}s / 阈值 {} / 采样 {}KB）...",
            total_candidates,
            PROBE_EARLY_WINDOW_SECS,
            human_speed(PROBE_MIN_SPEED_BPS),
            PROBE_BYTES / 1024
        );

        type ProbeMsg = (
            usize,
            String,
            Result<(Option<u64>, bool, u64, Option<String>)>,
        );
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<ProbeMsg>();

        for (i, candidate) in candidates.iter().enumerate() {
            let client = self.client.clone();
            let candidate = candidate.clone();
            let referer = referer.map(String::from);
            let cookie = cookie.map(String::from);
            let tx = tx.clone();

            tokio::spawn(async move {
                let probe = tokio::time::timeout(
                    Duration::from_secs(PROBE_TIMEOUT_SECS),
                    Self::probe_url(&client, &candidate, referer.as_deref(), cookie.as_deref()),
                )
                .await;
                let probe = match probe {
                    Ok(inner) => inner,
                    Err(_) => Err(anyhow::anyhow!(
                        "探测超时 ({}s)",
                        PROBE_TIMEOUT_SECS
                    )),
                };
                let _ = tx.send((i, candidate, probe));
            });
        }
        drop(tx);

        let mut results: Vec<Candidate> = Vec::new();
        let mut suggested_filename: Option<String> = None;

        let start = Instant::now();
        let early_deadline = start + Duration::from_secs(PROBE_EARLY_WINDOW_SECS);
        let hard_deadline = start + Duration::from_secs(PROBE_TIMEOUT_SECS + 1);

        let mut early_return_triggered = false;
        let mut received = 0usize;

        loop {
            let now = Instant::now();

            if now >= hard_deadline {
                tracing::info!(
                    "探测达到硬上限 ({}s)，用已收集的 {} 个结果",
                    PROBE_TIMEOUT_SECS + 1,
                    results.len()
                );
                break;
            }

            if now >= early_deadline && !results.is_empty() {
                tracing::info!(
                    "探测软窗口结束 ({}s)，收到 {}/{} 个结果，可用候选 {} 个",
                    PROBE_EARLY_WINDOW_SECS,
                    received,
                    total_candidates,
                    results.len()
                );
                break;
            }

            let wait_until = if results.is_empty() {
                hard_deadline
            } else {
                early_deadline
            };
            let remaining = wait_until.saturating_duration_since(now);
            if remaining.is_zero() {
                continue;
            }

            match tokio::time::timeout(remaining, rx.recv()).await {
                Ok(Some((i, url_c, probe))) => {
                    received += 1;
                    let tag = if i == 0 {
                        "原始 URL".to_string()
                    } else {
                        format!("镜像 #{}", i)
                    };

                    match probe {
                        Ok((size, range, speed, filename)) => {
                            tracing::info!(
                                "探测成功 [{}]: {} (速度: {}, 大小: {:?}, Range: {}, 文件名: {:?})",
                                tag,
                                url_c,
                                human_speed(speed),
                                size,
                                range,
                                filename
                            );
                            if suggested_filename.is_none() {
                                suggested_filename = filename;
                            }
                            let candidate = Candidate {
                                url: url_c,
                                speed_bps: speed,
                                total_size: size,
                                supports_range: range,
                            };
                            let hit = candidate.total_size.is_some()
                                && candidate.supports_range
                                && candidate.speed_bps >= PROBE_MIN_SPEED_BPS;
                            results.push(candidate);
                            if hit {
                                tracing::info!(
                                    "触发快速启动：候选速度 >= {}，立即开始下载（节省了最多 {}s 等待）",
                                    human_speed(PROBE_MIN_SPEED_BPS),
                                    PROBE_TIMEOUT_SECS as i64 - now.elapsed().as_secs() as i64
                                );
                                early_return_triggered = true;
                                break;
                            }
                        }
                        Err(e) => {
                            tracing::warn!("探测失败 [{}]: {} — {:#}", tag, url_c, e);
                        }
                    }
                }
                Ok(None) => {
                    tracing::info!("所有探测任务完成，可用候选 {} 个", results.len());
                    break;
                }
                Err(_) => {
                    continue;
                }
            }
        }

        let late_pool: Arc<tokio::sync::Mutex<Vec<Candidate>>> =
            Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let late_pool_inner = late_pool.clone();
        tokio::spawn(async move {
            while let Some((i, url_c, probe)) = rx.recv().await {
                if let Ok((size, range, speed, _)) = probe {
                    let tag = if i == 0 {
                        "原始 URL".to_string()
                    } else {
                        format!("镜像 #{}", i)
                    };
                    tracing::info!(
                        "[后台追加] 候选 [{}]: {} (速度: {}, 大小: {:?}, Range: {})",
                        tag,
                        url_c,
                        human_speed(speed),
                        size,
                        range
                    );
                    late_pool_inner.lock().await.push(Candidate {
                        url: url_c,
                        speed_bps: speed,
                        total_size: size,
                        supports_range: range,
                    });
                }
            }
        });

        if results.is_empty() {
            let fallback = candidates
                .last()
                .cloned()
                .unwrap_or_else(|| url.to_string());
            tracing::warn!("所有候选都探测失败，降级单线程，使用: {}", fallback);
            return ProbeResult {
                candidates: vec![Candidate {
                    url: fallback,
                    speed_bps: 0,
                    total_size: None,
                    supports_range: false,
                }],
                total_size: None,
                supports_range: false,
                suggested_filename,
                late_candidates: late_pool,
            };
        }

        results.sort_by_key(|r| {
            let has_size = r.total_size.is_some() && r.supports_range;
            (std::cmp::Reverse(has_size), std::cmp::Reverse(r.speed_bps))
        });

        let min_speed = 100 * 1024;
        let multi_candidates: Vec<Candidate> = results
            .iter()
            .filter(|r| r.total_size.is_some() && r.supports_range && r.speed_bps >= min_speed)
            .cloned()
            .collect();

        let best = &results[0];
        let chosen = if multi_candidates.is_empty() {
            vec![best.clone()]
        } else {
            multi_candidates
        };

        tracing::info!(
            "最终选定 {} 个候选源，首选: {} (速度: {}){}",
            chosen.len(),
            best.url,
            human_speed(best.speed_bps),
            if early_return_triggered {
                " [early-return]"
            } else {
                ""
            }
        );

        ProbeResult {
            candidates: chosen,
            total_size: best.total_size,
            supports_range: best.supports_range,
            suggested_filename,
            late_candidates: late_pool,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn download(
        &self,
        url: &str,
        output_dir: &str,
        referer: Option<&str>,
        cookie: Option<&str>,
        cancel: CancellationToken,
        pause: Arc<PauseState>,
        progress: Option<ProgressReporter>,
        expected_sha256: Option<String>,
    ) -> Result<PathBuf> {
        tokio::fs::create_dir_all(output_dir).await?;

        let probe = self.resolve(url, referer, cookie).await;
        let candidates = probe.candidates.clone();
        let total_size = probe.total_size;
        let supports_range = probe.supports_range;
        let late_pool = probe.late_candidates.clone();

        let filename = probe
            .suggested_filename
            .as_deref()
            .map(sanitize)
            .unwrap_or_else(|| guess_filename(url));
        let path = Path::new(output_dir).join(&filename);

        // [SLOW-HOST] 按 URL 决定本次任务实际用多少 worker
        let effective_connections = self.effective_connections(url);

        tracing::info!(
            "文件: {} | 大小: {:?} | 支持 Range: {} | worker 数: {}",
            filename,
            total_size,
            supports_range,
            effective_connections
        );

        let result = match (supports_range, total_size) {
            (true, Some(size)) if size > 1024 * 1024 => {
                self.multi_thread(
                    url,
                    &candidates,
                    &path,
                    size,
                    referer,
                    cookie,
                    cancel.clone(),
                    pause,
                    progress,
                    late_pool,
                    effective_connections,
                )
                .await
            }
            _ => {
                let url0 = candidates[0].url.clone();
                self.single_thread(
                    &url0,
                    &path,
                    referer,
                    cookie,
                    cancel.clone(),
                    pause,
                    progress,
                )
                .await
            }
        };

        if result.is_err() && cancel.is_cancelled() {
            tracing::info!("任务已取消，保留已下载数据以便续传: {}", filename);
            cleanup_legacy_part_files(&path).await;
        }

        result?;

        if let Some(expected) = expected_sha256 {
            tracing::info!("开始 SHA-256 校验...");
            let actual = compute_sha256(&path).await?;
            if actual.eq_ignore_ascii_case(&expected) {
                tracing::info!("SHA-256 校验通过");
            } else {
                tracing::error!("SHA-256 不匹配！期望 {}，实际 {}", expected, actual);
                let bad_path = path.with_extension("bad");
                if tokio::fs::rename(&path, &bad_path).await.is_ok() {
                    tracing::warn!("已保留损坏文件: {}", bad_path.display());
                } else {
                    tokio::fs::remove_file(&path).await.ok();
                }
                bail!("SHA-256 校验失败");
            }
        }

        Ok(path)
    }

    pub async fn download_to_memory(
        &self,
        url: &str,
        referer: Option<&str>,
        cookie: Option<&str>,
        cancel: CancellationToken,
    ) -> Result<Vec<u8>> {
        let mut req = self.client.get(url).header("Accept-Encoding", "identity");
        if let Some(r) = referer {
            req = req.header("Referer", r);
        }
        if let Some(c) = cookie {
            req = req.header("Cookie", c);
        }
        let resp = req.send().await?;
        if !resp.status().is_success() {
            bail!("HTTP {}", resp.status());
        }

        let mut buf = Vec::new();
        let mut stream = resp.bytes_stream();
        while let Some(chunk) = stream.next().await {
            if cancel.is_cancelled() {
                bail!("任务已取消");
            }
            let chunk = chunk?;
            if buf.len() + chunk.len() > 100 * 1024 * 1024 {
                bail!("内存下载超过 100MB 上限，拒绝继续");
            }
            buf.extend_from_slice(&chunk);
        }
        Ok(buf)
    }

    #[allow(clippy::too_many_arguments)]
    async fn multi_thread(
        &self,
        original_url: &str,
        candidates: &[Candidate],
        path: &Path,
        total_size: u64,
        referer: Option<&str>,
        cookie: Option<&str>,
        cancel: CancellationToken,
        pause: Arc<PauseState>,
        progress: Option<ProgressReporter>,
        late_candidates: Arc<tokio::sync::Mutex<Vec<Candidate>>>,
        // [SLOW-HOST] 本次任务实际 worker 数（可能低于 self.connections）
        effective_connections: usize,
    ) -> Result<()> {
        // 目标文件已存在且大小完全匹配 → 视为已完成
        if let Ok(m) = tokio::fs::metadata(path).await {
            if m.len() == total_size {
                tracing::info!(
                    "目标文件已存在且大小完全匹配 ({} 字节)，跳过下载",
                    total_size
                );
                return Ok(());
            }
        }

        let mut meta = match DownloadMeta::load(path).await? {
            Some(mut m) if m.url == original_url && m.total_size == total_size => {
                if m.completed.len() != m.chunks.len() {
                    m.completed = vec![false; m.chunks.len()];
                }
                if m.completed_bytes() == 0 {
                    tokio::fs::remove_file(path).await.ok();
                }
                tracing::info!("发现未完成任务，从断点继续...");
                m
            }
            Some(_) => {
                tracing::warn!("元数据不匹配，重新开始");
                remove_or_rename(path).await?;
                // [SLOW-HOST] chunk 划分仍用 self.connections，保证 meta 一致
                let m = DownloadMeta::new(original_url, total_size, self.connections);
                m.save(path).await?;
                m
            }
            None => {
                remove_or_rename(path).await?;
                let m = DownloadMeta::new(original_url, total_size, self.connections);
                m.save(path).await?;
                m
            }
        };

        let file = Arc::new(
            std::fs::OpenOptions::new()
                .create(true)
                .write(true)
                .read(true)
                .open(path)
                .with_context(|| format!("打开输出文件失败: {}", path.display()))?,
        );

        let pb = Arc::new(build_progress(total_size));
        let resume_bytes = meta.completed_bytes();
        if resume_bytes > 0 {
            pb.inc(resume_bytes);
            tracing::info!(
                "已恢复 {} 字节 ({:.2}%)",
                resume_bytes,
                resume_bytes as f64 / total_size as f64 * 100.0
            );
        }

        let mut urls: Vec<String> = candidates.iter().map(|c| c.url.clone()).collect();
        let chunks_arc = Arc::new(meta.chunks.clone());
        let total_chunks = meta.chunks.len();
        // [SLOW-HOST] 用 effective_connections 而不是 self.connections
        let workers = effective_connections.min(total_chunks).max(1);
        tracing::info!(
            "worker 池模式（偏移写）：起始 {} 个源，{} 块分片，{} 个 worker",
            urls.len(),
            total_chunks,
            workers
        );

        let progress_stop = CancellationToken::new();
        if let Some(rep) = progress.clone() {
            let pb = pb.clone();
            let stop = progress_stop.clone();
            let cancel2 = cancel.clone();
            tokio::spawn(async move {
                let mut last = pb.position();
                let mut tick = tokio::time::interval(Duration::from_millis(800));
                loop {
                    tokio::select! {
                        _ = stop.cancelled() => break,
                        _ = cancel2.cancelled() => break,
                        _ = tick.tick() => {
                            let current = pb.position();
                            let speed = current.saturating_sub(last) * 1000 / 800;
                            last = current;
                            rep.bus.emit(crate::queue::TaskEvent::Progress {
                                id: rep.task_id.clone(),
                                downloaded: current,
                                total: total_size,
                                speed_bps: speed,
                            });
                            if current >= total_size { break; }
                        }
                    }
                }
            });
        }

        loop {
            if pause.is_paused() {
                tracing::info!("任务已暂停，等待恢复...");
                if !pause.wait_resume_or_cancel(&cancel).await {
                    bail!("任务已取消");
                }
                tracing::info!("任务已恢复，继续下载");
            }

            // 从后台池补充新候选
            {
                let mut pool = late_candidates.lock().await;
                if !pool.is_empty() {
                    let drained: Vec<Candidate> = pool.drain(..).collect();
                    drop(pool);
                    for c in drained {
                        if !urls.contains(&c.url) {
                            tracing::info!(
                                "追加候选源（换源池 +1，当前 {} 个）: {} ({})",
                                urls.len() + 1,
                                c.url,
                                human_speed(c.speed_bps)
                            );
                            urls.push(c.url);
                        }
                    }
                }
            }

            let remaining: Vec<usize> = (0..total_chunks)
                .filter(|&i| !meta.completed[i])
                .collect();

            if remaining.is_empty() {
                break;
            }

            let remaining = Arc::new(remaining);
            let cursor = Arc::new(AtomicUsize::new(0));
            let completed = Arc::new(std::sync::Mutex::new(Vec::<usize>::new()));
            let active_workers = workers.min(remaining.len()).max(1);

            let mut tasks = Vec::with_capacity(active_workers);
            for _ in 0..active_workers {
                let client = self.client.clone();
                let urls = urls.clone();
                let chunks = chunks_arc.clone();
                let remaining = remaining.clone();
                let cursor = cursor.clone();
                let completed = completed.clone();
                let file = file.clone();
                let pb = pb.clone();
                let referer = referer.map(String::from);
                let cookie = cookie.map(String::from);
                let cancel = cancel.clone();
                let pause = pause.clone();
                let rate = self.rate_limiter.clone();

                tasks.push(tokio::spawn(async move {
                    loop {
                        if cancel.is_cancelled() {
                            bail!("任务已取消");
                        }
                        if pause.is_paused() {
                            return Ok::<_, anyhow::Error>(());
                        }

                        let slot = cursor.fetch_add(1, Ordering::Relaxed);
                        if slot >= remaining.len() {
                            break;
                        }
                        let chunk_idx = remaining[slot];
                        let chunk = chunks[chunk_idx].clone();
                        let start_url = urls[chunk_idx % urls.len()].clone();

                        let result = download_chunk_with_retry(
                            client.clone(),
                            urls.clone(),
                            start_url,
                            file.clone(),
                            chunk.clone(),
                            referer.clone(),
                            cookie.clone(),
                            cancel.clone(),
                            pause.clone(),
                            rate.clone(),
                            5,
                        )
                        .await?;

                        match result {
                            ChunkResult::Completed => {
                                completed.lock().unwrap().push(chunk_idx);
                                pb.inc(chunk.len());
                            }
                            ChunkResult::Interrupted => {
                                return Ok(());
                            }
                        }
                    }
                    Ok(())
                }));
            }

            for r in join_all(tasks).await {
                r.context("worker panic")??;
            }

            {
                let mut c = completed.lock().unwrap();
                for idx in c.drain(..) {
                    meta.completed[idx] = true;
                }
            }
            meta.save(path).await?;

            if meta.completed.iter().all(|&c| c) {
                break;
            }
            if cancel.is_cancelled() {
                bail!("任务已取消");
            }
            if !pause.is_paused() {
                bail!("分片下载异常中断（非暂停原因）");
            }
        }

        progress_stop.cancel();

        tracing::info!("下载完成，清理元数据...");
        DownloadMeta::cleanup(path).await;

        pb.finish_with_message("下载完成");
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn single_thread(
        &self,
        url: &str,
        path: &Path,
        referer: Option<&str>,
        cookie: Option<&str>,
        cancel: CancellationToken,
        pause: Arc<PauseState>,
        progress: Option<ProgressReporter>,
    ) -> Result<()> {
        let pb = Arc::new(build_progress(0));

        let progress_stop = CancellationToken::new();
        if let Some(rep) = progress.clone() {
            let pb = pb.clone();
            let stop = progress_stop.clone();
            let cancel2 = cancel.clone();
            tokio::spawn(async move {
                let mut last = pb.position();
                let mut tick = tokio::time::interval(Duration::from_millis(800));
                loop {
                    tokio::select! {
                        _ = stop.cancelled() => break,
                        _ = cancel2.cancelled() => break,
                        _ = tick.tick() => {
                            let current = pb.position();
                            let speed = current.saturating_sub(last) * 1000 / 800;
                            last = current;
                            let total = pb.length().unwrap_or(0);
                            rep.bus.emit(crate::queue::TaskEvent::Progress {
                                id: rep.task_id.clone(),
                                downloaded: current,
                                total,
                                speed_bps: speed,
                            });
                        }
                    }
                }
            });
        }

        let mut known_total: u64 = 0;

        loop {
            if cancel.is_cancelled() {
                bail!("任务已取消");
            }

            if pause.is_paused() {
                tracing::info!("单线程任务已暂停，等待恢复...");
                if !pause.wait_resume_or_cancel(&cancel).await {
                    bail!("任务已取消");
                }
                tracing::info!("单线程任务已恢复");
            }

            let current = if path.exists() {
                tokio::fs::metadata(path).await?.len()
            } else {
                0
            };

            if known_total > 0 && current >= known_total {
                break;
            }

            let mut req = self.client.get(url).header("Accept-Encoding", "identity");
            if current > 0 {
                req = req.header("Range", format!("bytes={}-", current));
            }
            if let Some(r) = referer {
                req = req.header("Referer", r);
            }
            if let Some(c) = cookie {
                req = req.header("Cookie", c);
            }

            let resp = req.send().await?;
            let status = resp.status();

            let is_partial = status == reqwest::StatusCode::PARTIAL_CONTENT;

            if !is_partial && current > 0 {
                tokio::fs::remove_file(path).await.ok();
            }

            if !is_partial && !status.is_success() {
                bail!("HTTP {}", status);
            }

            known_total = if is_partial {
                resp.headers()
                    .get("content-range")
                    .and_then(|v| v.to_str().ok())
                    .and_then(|s| s.split('/').nth(1))
                    .and_then(|s| s.trim().parse().ok())
                    .unwrap_or(0)
            } else {
                resp.content_length().unwrap_or(0)
            };

            if known_total > 0 && current > known_total {
                tracing::warn!(
                    "本地文件 ({}B) 超过远端 total ({}B)，清空重下",
                    current,
                    known_total
                );
                tokio::fs::remove_file(path).await.ok();
                known_total = 0;
                continue;
            }

            pb.set_length(known_total);

            let mut file = tokio::fs::OpenOptions::new()
                .create(true)
                .write(true)
                .append(true)
                .open(path)
                .await?;

            let mut stream = resp.bytes_stream();
            let mut paused_this_round = false;

            loop {
                tokio::select! {
                    _ = cancel.cancelled() => {
                        file.flush().await.ok();
                        bail!("任务已取消");
                    }
                    chunk = stream.next() => {
                        let Some(c) = chunk else { break; };
                        if pause.is_paused() {
                            file.flush().await.ok();
                            paused_this_round = true;
                            break;
                        }
                        let c = c?;
                        file.write_all(&c).await?;
                        pb.inc(c.len() as u64);
                        let wait = self.rate_limiter.consume(c.len() as u64);
                        if !wait.is_zero() {
                            tokio::time::sleep(wait).await;
                        }
                    }
                }
            }

            if !paused_this_round {
                break;
            }
        }

        progress_stop.cancel();
        pb.finish_with_message("下载完成");
        Ok(())
    }
}

// ============================================================
// 底层偏移写
// ============================================================

fn sync_write_at(file: &std::fs::File, buf: &[u8], offset: u64) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::FileExt;
        file.write_all_at(buf, offset)
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::FileExt;
        let mut written = 0usize;
        while written < buf.len() {
            let n = file.seek_write(&buf[written..], offset + written as u64)?;
            if n == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::WriteZero,
                    "seek_write returned 0",
                ));
            }
            written += n;
        }
        Ok(())
    }
}

async fn write_at(file: Arc<std::fs::File>, buf: Vec<u8>, offset: u64) -> Result<()> {
    tokio::task::spawn_blocking(move || sync_write_at(&file, &buf, offset))
        .await
        .context("write_at 任务 panic")??;
    Ok(())
}

// ============================================================
// chunk 下载
// ============================================================

async fn download_chunk_with_retry(
    client: Client,
    urls: Vec<String>,
    start_url: String,
    file: Arc<std::fs::File>,
    chunk: ChunkMeta,
    referer: Option<String>,
    cookie: Option<String>,
    cancel: CancellationToken,
    pause: Arc<PauseState>,
    rate: Arc<RateLimiter>,
    max_retry: usize,
) -> Result<ChunkResult> {
    let mut attempt = 0;
    let mut current_url = start_url;

    loop {
        if cancel.is_cancelled() {
            bail!("任务已取消");
        }
        if pause.is_paused() {
            return Ok(ChunkResult::Interrupted);
        }

        match download_chunk(
            &client,
            &current_url,
            &file,
            &chunk,
            referer.as_deref(),
            cookie.as_deref(),
            &cancel,
            &pause,
            &rate,
        )
        .await
        {
            Ok(_) => return Ok(ChunkResult::Completed),
            Err(DownloadErr::Cancelled) => bail!("任务已取消"),
            Err(DownloadErr::Paused) => return Ok(ChunkResult::Interrupted),
            Err(DownloadErr::Failed(e)) if attempt < max_retry => {
                attempt += 1;
                if urls.len() > 1 {
                    current_url = urls[attempt % urls.len()].clone();
                }
                tracing::warn!(
                    "分片 #{} 第 {} 次重试 (换源 {}): {}",
                    chunk.index,
                    attempt,
                    current_url,
                    e
                );
                tokio::time::sleep(Duration::from_millis(500 * attempt as u64)).await;
            }
            Err(DownloadErr::Failed(e)) => {
                return Err(e).context(format!("分片 #{} 失败", chunk.index));
            }
        }
    }
}

async fn download_chunk(
    client: &Client,
    url: &str,
    file: &Arc<std::fs::File>,
    chunk: &ChunkMeta,
    referer: Option<&str>,
    cookie: Option<&str>,
    cancel: &CancellationToken,
    pause: &Arc<PauseState>,
    rate: &Arc<RateLimiter>,
) -> Result<(), DownloadErr> {
    let range = format!("bytes={}-{}", chunk.start, chunk.end);
    let mut req = client
        .get(url)
        .header("Range", range)
        .header("Accept-Encoding", "identity");
    if let Some(r) = referer {
        req = req.header("Referer", r);
    }
    if let Some(c) = cookie {
        req = req.header("Cookie", c);
    }

    let resp = match req.send().await {
        Ok(r) => r,
        Err(e) => return Err(DownloadErr::Failed(e.into())),
    };

    let status = resp.status();
    if status != reqwest::StatusCode::PARTIAL_CONTENT {
        return Err(DownloadErr::Failed(anyhow::anyhow!(
            "服务器未返回 206（忽略 Range）：HTTP {}",
            status
        )));
    }

    let mut stream = resp.bytes_stream();
    let mut buf: Vec<u8> = Vec::with_capacity(WRITE_BATCH);
    let mut offset = chunk.start;

    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                return Err(DownloadErr::Cancelled);
            }
            chunk_opt = stream.next() => {
                let Some(c) = chunk_opt else { break; };
                if pause.is_paused() {
                    return Err(DownloadErr::Paused);
                }
                let c = match c {
                    Ok(c) => c,
                    Err(e) => {
                        return Err(DownloadErr::Failed(e.into()));
                    }
                };
                buf.extend_from_slice(&c);
                let wait = rate.consume(c.len() as u64);
                if !wait.is_zero() {
                    tokio::time::sleep(wait).await;
                }
                if buf.len() >= WRITE_BATCH {
                    let owned = std::mem::replace(&mut buf, Vec::with_capacity(WRITE_BATCH));
                    let len = owned.len() as u64;
                    if let Err(e) = write_at(file.clone(), owned, offset).await {
                        return Err(DownloadErr::Failed(e));
                    }
                    offset += len;
                }
            }
        }
    }

    if !buf.is_empty() {
        if let Err(e) = write_at(file.clone(), buf, offset).await {
            return Err(DownloadErr::Failed(e));
        }
    }

    Ok(())
}

// ============================================================
// 工具
// ============================================================

async fn remove_or_rename(path: &Path) -> Result<()> {
    if !path.exists() {
        return Ok(());
    }

    match tokio::fs::remove_file(path).await {
        Ok(_) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(_) => {
            let base_ext = path
                .extension()
                .and_then(|s| s.to_str())
                .unwrap_or("");
            let mut old_path = path.with_extension(format!("{}.old", base_ext));
            if old_path.exists() {
                let ts = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_secs())
                    .unwrap_or(0);
                old_path = path.with_extension(format!("{}.old.{}", base_ext, ts));
            }

            tokio::fs::rename(path, &old_path).await.with_context(|| {
                format!(
                    "旧文件被占用且无法重命名: {} → {}",
                    path.display(),
                    old_path.display()
                )
            })?;
            tracing::warn!(
                "旧文件被占用，已重命名为 {} 后继续",
                old_path.display()
            );
            Ok(())
        }
    }
}

async fn cleanup_legacy_part_files(final_path: &Path) {
    let parent = match final_path.parent() {
        Some(p) if !p.as_os_str().is_empty() => p.to_path_buf(),
        _ => PathBuf::from("."),
    };
    let stem = match final_path.file_stem() {
        Some(s) => s.to_string_lossy().to_string(),
        None => return,
    };
    let prefix = format!("{}.fastdl.part", stem);
    let merging = format!("{}.fastdl.merging", stem);

    let mut entries = match tokio::fs::read_dir(&parent).await {
        Ok(e) => e,
        Err(_) => return,
    };

    while let Ok(Some(entry)) = entries.next_entry().await {
        let name = entry.file_name();
        let name_str = name.to_string_lossy();
        if name_str.starts_with(&prefix) || name_str == merging {
            if let Err(e) = tokio::fs::remove_file(entry.path()).await {
                tracing::warn!("清理旧临时文件失败 {}: {}", name_str, e);
            }
        }
    }
}

async fn compute_sha256(path: &Path) -> Result<String> {
    use tokio::io::AsyncReadExt;
    let mut file = File::open(path).await?;
    let mut hasher = Sha256::new();
    let mut buf = vec![0u8; 65536];
    loop {
        let n = file.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(hex::encode(hasher.finalize()))
}

fn parse_content_disposition(value: &str) -> Option<String> {
    for part in value.split(';') {
        let part = part.trim();
        if let Some(rest) = part.strip_prefix("filename*=") {
            if let Some(idx) = rest.find("''") {
                let encoded = &rest[idx + 2..];
                if let Some(decoded) = percent_decode(encoded) {
                    if !decoded.is_empty() {
                        return Some(decoded);
                    }
                }
            }
            let cleaned = rest.trim().trim_matches('"').trim();
            if !cleaned.is_empty() {
                return Some(cleaned.to_string());
            }
        }
    }
    for part in value.split(';') {
        let part = part.trim();
        if let Some(rest) = part.strip_prefix("filename=") {
            let cleaned = rest.trim().trim_matches('"').trim();
            if !cleaned.is_empty() {
                return Some(cleaned.to_string());
            }
        }
    }
    None
}

fn percent_decode(s: &str) -> Option<String> {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            let hi = (bytes[i + 1] as char).to_digit(16)?;
            let lo = (bytes[i + 2] as char).to_digit(16)?;
            out.push((hi * 16 + lo) as u8);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out).ok()
}

fn guess_filename(url: &str) -> String {
    url.split('?')
        .next()
        .unwrap_or(url)
        .split('/')
        .last()
        .filter(|s| !s.is_empty())
        .map(sanitize)
        .unwrap_or_else(|| "download.bin".to_string())
}

fn sanitize(name: &str) -> String {
    let s: String = name
        .replace(['/', '\\', ':', '*', '?', '"', '<', '>', '|'], "_")
        .trim()
        .to_string();
    if s.is_empty() || s == "." || s == ".." {
        return "download.bin".to_string();
    }
    s
}

fn human_speed(bps: u64) -> String {
    const UNITS: &[&str] = &["B/s", "KB/s", "MB/s", "GB/s"];
    let mut v = bps as f64;
    let mut i = 0;
    while v >= 1024.0 && i < UNITS.len() - 1 {
        v /= 1024.0;
        i += 1;
    }
    format!("{:.2} {}", v, UNITS[i])
}

fn build_progress(total: u64) -> ProgressBar {
    let pb = ProgressBar::new(total);
    pb.set_style(
        ProgressStyle::with_template(
            "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] \
             {bytes}/{total_bytes} ({bytes_per_sec}, ETA {eta})",
        )
        .unwrap()
        .progress_chars("█▓░"),
    );
    pb
}