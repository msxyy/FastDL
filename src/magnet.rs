use crate::downloader::ProgressReporter;
use crate::queue::PauseState;
use anyhow::{bail, Context, Result};
use librqbit::{AddTorrent, AddTorrentOptions, AddTorrentResponse, Session};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

pub enum TorrentSource {
    Magnet(String),
    File(Vec<u8>),
}

#[allow(clippy::too_many_arguments)]
pub async fn download_torrent(
    // [FIX] 接受调用方传入的 Session，多个 BT 任务共享 DHT / tracker
    session: Arc<Session>,
    source: TorrentSource,
    output_dir: &str,
    cancel: CancellationToken,
    _pause: Arc<PauseState>,
    progress: Option<ProgressReporter>,
) -> Result<PathBuf> {
    tokio::fs::create_dir_all(output_dir).await?;
    let out_path = PathBuf::from(output_dir);

    let add = match source {
        TorrentSource::Magnet(m) => {
            if !m.starts_with("magnet:") {
                bail!("不是合法的磁力链接");
            }
            AddTorrent::from_url(m)
        }
        TorrentSource::File(bytes) => {
            if bytes.is_empty() {
                bail!("种子文件为空");
            }
            AddTorrent::from_bytes(bytes)
        }
    };

    let opts = AddTorrentOptions::default();
    let response = session
        .add_torrent(add, Some(opts))
        .await
        .context("添加种子任务失败")?;

    let handle = match response {
        AddTorrentResponse::Added(_, h) => h,
        AddTorrentResponse::AlreadyManaged(_, h) => h,
        _ => bail!("未知的添加响应"),
    };

    let name = handle
        .name()
        .unwrap_or_else(|| "(获取元数据中...)".to_string());
    tracing::info!("BT 任务已启动: {}", name);

    let mut last_bytes = 0u64;
    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                tracing::warn!("任务被取消，正在停止: {}", name);
                bail!("任务已取消");
            }
            _ = tokio::time::sleep(Duration::from_secs(2)) => {
                let stats = handle.stats();
                let total = stats.total_bytes;
                let done = stats.progress_bytes;
                let speed = done.saturating_sub(last_bytes) / 2;
                last_bytes = done;

                // [FIX] total 为 0（元数据还没拿到 / magnet 阶段）就不要算百分比、也不要发进度
                if total > 0 {
                    let pct = done as f64 / total as f64 * 100.0;
                    tracing::info!(
                        "{}: {:.2}% ({}/{}) {}/s",
                        name,
                        pct,
                        human(done),
                        human(total),
                        human(speed)
                    );
                    if let Some(rep) = &progress {
                        rep.bus.emit(crate::queue::TaskEvent::Progress {
                            id: rep.task_id.clone(),
                            downloaded: done,
                            total,
                            speed_bps: speed,
                        });
                    }
                } else {
                    tracing::info!("{}: 获取元数据中...", name);
                }

                if stats.finished {
                    tracing::info!("BT 下载完成: {}", name);
                    break;
                }
            }
        }
    }
    Ok(out_path)
}

fn human(bytes: u64) -> String {
    const UNITS: &[&str] = &["B", "KB", "MB", "GB", "TB"];
    let mut v = bytes as f64;
    let mut i = 0;
    while v >= 1024.0 && i < UNITS.len() - 1 {
        v /= 1024.0;
        i += 1;
    }
    format!("{:.2} {}", v, UNITS[i])
}