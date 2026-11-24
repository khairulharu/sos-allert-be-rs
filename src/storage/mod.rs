//! Incident media storage.
//!
//! The default backend is a local directory (`storage.local_dir`), the same
//! shape as the Go `LocalStorage`: the caller supplies a base name, the path is
//! confined to the root on read, and an upload whose metadata insert failed is
//! deleted again.

use std::path::{Path, PathBuf};

use async_trait::async_trait;

use crate::error::{AppError, AppResult};

#[async_trait]
pub trait MediaStorage: Send + Sync {
    /// Write the bytes and return the storage path recorded in the database.
    async fn save(&self, name: &str, bytes: &[u8]) -> AppResult<String>;
    async fn open(&self, path: &str) -> AppResult<Vec<u8>>;
    async fn delete(&self, path: &str) -> AppResult<()>;
}

pub struct LocalStorage {
    dir: PathBuf,
}

impl LocalStorage {
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        Self { dir: dir.into() }
    }

    /// Reject anything that could escape the storage root.
    async fn resolve(&self, path: &str) -> AppResult<PathBuf> {
        let candidate = Path::new(path);
        let joined = if candidate.is_absolute() {
            candidate.to_path_buf()
        } else {
            self.dir.join(candidate)
        };
        let root = tokio::fs::canonicalize(&self.dir)
            .await
            .unwrap_or_else(|_| self.dir.clone());
        let target = joined
            .canonicalize()
            .map_err(|e| AppError::NotFound(format!("media file: {e}")))?;
        if !target.starts_with(&root) {
            return Err(AppError::Forbidden("media path escapes storage".into()));
        }
        Ok(target)
    }
}

#[async_trait]
impl MediaStorage for LocalStorage {
    async fn save(&self, name: &str, bytes: &[u8]) -> AppResult<String> {
        let base = Path::new(name)
            .file_name()
            .and_then(|value| value.to_str())
            .ok_or_else(|| AppError::BadRequest("invalid media filename".into()))?;
        if base == "." || base == "/" || base.contains("..") {
            return Err(AppError::BadRequest("invalid media filename".into()));
        }

        tokio::fs::create_dir_all(&self.dir)
            .await
            .map_err(|e| AppError::Unavailable(format!("create storage dir: {e}")))?;
        let path = self.dir.join(base);
        tokio::fs::write(&path, bytes)
            .await
            .map_err(|e| AppError::Unavailable(format!("write media: {e}")))?;
        Ok(path.to_string_lossy().into_owned())
    }

    async fn open(&self, path: &str) -> AppResult<Vec<u8>> {
        let target = self.resolve(path).await?;
        tokio::fs::read(&target)
            .await
            .map_err(|e| AppError::NotFound(format!("media file: {e}")))
    }

    async fn delete(&self, path: &str) -> AppResult<()> {
        if let Ok(target) = self.resolve(path).await {
            let _ = tokio::fs::remove_file(target).await;
        }
        Ok(())
    }
}
