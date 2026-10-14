use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkMeta {
    pub index: usize,
    pub start: u64,
    pub end: u64,
}

impl ChunkMeta {
    pub fn len(&self) -> u64 {
        self.end - self.start + 1
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DownloadMeta {
    pub url: String,
    pub total_size: u64,
    pub chunk_size: u64,
    pub chunks: Vec<ChunkMeta>,
    /// [模式B] 每个 chunk 是否已完成。
    /// 老版本 meta 没有这个字段，serde 默认给空 Vec，
    /// 加载后由调用方检查长度并补全。
    #[serde(default)]
    pub completed: Vec<bool>,
}

impl DownloadMeta {
    pub fn new(url: &str, total_size: u64, connections: usize) -> Self {
        let min_chunk: u64 = 1024 * 1024;
        let by_conn = total_size / (connections as u64 * 8).max(1);
        let chunk_size = min_chunk.max(by_conn).max(1);

        let n = total_size.div_ceil(chunk_size) as usize;
        let mut chunks = Vec::with_capacity(n);
        for i in 0..n {
            let start = i as u64 * chunk_size;
            if start >= total_size {
                break;
            }
            let end = ((i as u64 + 1) * chunk_size - 1).min(total_size - 1);
            chunks.push(ChunkMeta {
                index: i,
                start,
                end,
            });
        }
        let completed = vec![false; chunks.len()];
        Self {
            url: url.to_string(),
            total_size,
            chunk_size,
            chunks,
            completed,
        }
    }

    /// 已完成的字节总数，用于进度条恢复
    pub fn completed_bytes(&self) -> u64 {
        self.chunks
            .iter()
            .zip(self.completed.iter())
            .filter(|(_, &c)| c)
            .map(|(chunk, _)| chunk.len())
            .sum()
    }

    /// [模式B] 只删 meta 文件——不再有 part 文件需要清理
    pub fn meta_path(final_path: &Path) -> PathBuf {
        final_path.with_extension("fastdl.meta")
    }

    pub async fn load(final_path: &Path) -> Result<Option<Self>> {
        let path = Self::meta_path(final_path);
        if !path.exists() {
            return Ok(None);
        }
        let data = tokio::fs::read_to_string(&path)
            .await
            .with_context(|| format!("读取元数据失败: {}", path.display()))?;
        Ok(Some(serde_json::from_str(&data)?))
    }

    pub async fn save(&self, final_path: &Path) -> Result<()> {
        let path = Self::meta_path(final_path);
        let data = serde_json::to_string_pretty(self)?;
        tokio::fs::write(&path, data).await?;
        Ok(())
    }

    /// [模式B] 完成后只删 meta，输出文件保留
    pub async fn cleanup(final_path: &Path) {
        let _ = tokio::fs::remove_file(Self::meta_path(final_path)).await;
    }
}