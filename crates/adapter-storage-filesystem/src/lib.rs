//! Filesystem-backed storage adapter for Rustcoon blob ports.

use std::io::ErrorKind;
use std::path::{Path, PathBuf};

use async_trait::async_trait;
use rustcoon_storage::{
    BlobDeleteStore, BlobKey, BlobMetadata, BlobReadRange, BlobReadStore, BlobReader,
    BlobWritePrecondition, BlobWriteRequest, BlobWriteSession, BlobWriteStore, StorageError,
    StorageOperation,
};
use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, SeekFrom};
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct FilesystemBlobStore {
    root: PathBuf,
}

impl FilesystemBlobStore {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    fn blob_path(&self, key: &BlobKey) -> PathBuf {
        self.root.join(key.as_str())
    }

    async fn ensure_parent_dir(&self, key: &BlobKey) -> Result<PathBuf, StorageError> {
        let path = self.blob_path(key);
        let parent = path
            .parent()
            .expect("blob path should always have a parent");
        fs::create_dir_all(parent)
            .await
            .map_err(|err| classify_io_error(StorageOperation::BeginWrite, key.clone(), err))?;
        Ok(path)
    }

    async fn open_file(
        &self,
        key: &BlobKey,
        operation: StorageOperation,
    ) -> Result<fs::File, StorageError> {
        let path = self.blob_path(key);
        fs::File::open(&path)
            .await
            .map_err(|err| classify_io_error(operation, key.clone(), err))
    }
}

struct FilesystemWriteSession {
    key: BlobKey,
    final_path: PathBuf,
    staging_path: Option<PathBuf>,
    file: Option<fs::File>,
    precondition: BlobWritePrecondition,
}

impl FilesystemWriteSession {
    fn file_mut(&mut self) -> &mut fs::File {
        self.file
            .as_mut()
            .expect("staged file should be present before commit or abort")
    }

    async fn sync_staged_file(&mut self) -> Result<(), StorageError> {
        if let Some(file) = self.file.as_mut() {
            file.sync_all().await.map_err(|err| {
                classify_io_error(StorageOperation::Commit, self.key.clone(), err)
            })?;
        }
        Ok(())
    }

    fn take_staging_path(&mut self) -> PathBuf {
        self.staging_path
            .take()
            .expect("staging path should exist before commit")
    }

    async fn commit_create_new(&self, staging_path: &Path) -> Result<(), StorageError> {
        match fs::hard_link(staging_path, &self.final_path).await {
            Ok(()) => {
                fs::remove_file(staging_path).await.map_err(|err| {
                    classify_io_error(StorageOperation::Commit, self.key.clone(), err)
                })?;
                Ok(())
            }
            Err(err) if err.kind() == ErrorKind::AlreadyExists => {
                let _ = fs::remove_file(staging_path).await;
                Err(StorageError::already_exists(self.key.clone()))
            }
            Err(err) => {
                let _ = fs::remove_file(staging_path).await;
                Err(classify_io_error(
                    StorageOperation::Commit,
                    self.key.clone(),
                    err,
                ))
            }
        }
    }

    async fn commit_replace(
        &self,
        staging_path: &Path,
        must_exist: bool,
    ) -> Result<(), StorageError> {
        if must_exist {
            match fs::metadata(&self.final_path).await {
                Ok(_) => {}
                Err(err) if err.kind() == ErrorKind::NotFound => {
                    let _ = fs::remove_file(staging_path).await;
                    return Err(StorageError::not_found(self.key.clone()));
                }
                Err(err) => {
                    let _ = fs::remove_file(staging_path).await;
                    return Err(classify_io_error(
                        StorageOperation::Commit,
                        self.key.clone(),
                        err,
                    ));
                }
            }
        }

        if let Err(err) = rename_overwriting(&self.final_path, staging_path, &self.key).await {
            let _ = fs::remove_file(staging_path).await;
            return Err(err);
        }

        Ok(())
    }
}

impl Drop for FilesystemWriteSession {
    fn drop(&mut self) {
        if let Some(path) = self.staging_path.as_ref() {
            let _ = std::fs::remove_file(path);
        }
    }
}

#[async_trait]
impl BlobWriteSession for FilesystemWriteSession {
    async fn write_chunk(&mut self, chunk: &[u8]) -> Result<(), StorageError> {
        self.file_mut()
            .write_all(chunk)
            .await
            .map_err(|err| classify_io_error(StorageOperation::WriteChunk, self.key.clone(), err))
    }

    async fn commit(mut self: Box<Self>) -> Result<(), StorageError> {
        self.sync_staged_file().await?;
        self.file.take();

        let staging_path = self.take_staging_path();
        match self.precondition {
            BlobWritePrecondition::MustNotExist => self.commit_create_new(&staging_path).await,
            BlobWritePrecondition::MustExist => self.commit_replace(&staging_path, true).await,
            BlobWritePrecondition::None => self.commit_replace(&staging_path, false).await,
        }
    }

    async fn abort(mut self: Box<Self>) -> Result<(), StorageError> {
        self.file.take();

        let Some(staging_path) = self.staging_path.take() else {
            return Ok(());
        };

        match fs::remove_file(staging_path).await {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == ErrorKind::NotFound => Ok(()),
            Err(err) => Err(classify_io_error(
                StorageOperation::Abort,
                self.key.clone(),
                err,
            )),
        }
    }
}

#[async_trait]
impl BlobReadStore for FilesystemBlobStore {
    async fn head(&self, key: &BlobKey) -> Result<BlobMetadata, StorageError> {
        let path = self.blob_path(key);
        let metadata = fs::metadata(path)
            .await
            .map_err(|err| classify_io_error(StorageOperation::Head, key.clone(), err))?;

        Ok(BlobMetadata {
            key: key.clone(),
            size_bytes: metadata.len(),
            content_type: None,
            version: None,
            created_at: metadata.created().ok(),
            updated_at: metadata.modified().ok(),
        })
    }

    async fn open(&self, key: &BlobKey) -> Result<BlobReader, StorageError> {
        let file = self.open_file(key, StorageOperation::Open).await?;
        Ok(Box::new(file))
    }

    async fn open_range(
        &self,
        key: &BlobKey,
        range: BlobReadRange,
    ) -> Result<BlobReader, StorageError> {
        let metadata = self.head(key).await?;
        if range.offset > metadata.size_bytes {
            return Err(StorageError::InvalidRange);
        }

        if let Some(length) = range.length
            && range.offset.saturating_add(length) > metadata.size_bytes
        {
            return Err(StorageError::InvalidRange);
        }

        let mut file = self.open_file(key, StorageOperation::OpenRange).await?;
        file.seek(SeekFrom::Start(range.offset))
            .await
            .map_err(|err| classify_io_error(StorageOperation::OpenRange, key.clone(), err))?;

        if let Some(length) = range.length {
            Ok(Box::new(file.take(length)))
        } else {
            Ok(Box::new(file))
        }
    }
}

#[async_trait]
impl BlobWriteStore for FilesystemBlobStore {
    async fn begin_write(
        &self,
        request: BlobWriteRequest,
    ) -> Result<Box<dyn BlobWriteSession>, StorageError> {
        let final_path = self.ensure_parent_dir(&request.key).await?;
        let parent = final_path
            .parent()
            .expect("blob path should always have a parent directory");
        let staging_path = parent.join(format!(
            ".{}.{}.staging",
            final_path
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("blob"),
            Uuid::new_v4()
        ));

        let file = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&staging_path)
            .await
            .map_err(|err| {
                classify_io_error(StorageOperation::BeginWrite, request.key.clone(), err)
            })?;

        Ok(Box::new(FilesystemWriteSession {
            key: request.key,
            final_path,
            staging_path: Some(staging_path),
            file: Some(file),
            precondition: request.precondition,
        }))
    }
}

#[async_trait]
impl BlobDeleteStore for FilesystemBlobStore {
    async fn delete(&self, key: &BlobKey) -> Result<(), StorageError> {
        let path = self.blob_path(key);
        match fs::remove_file(path).await {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == ErrorKind::NotFound => Ok(()),
            Err(err) => Err(classify_io_error(
                StorageOperation::Delete,
                key.clone(),
                err,
            )),
        }
    }
}

async fn rename_overwriting(
    final_path: &Path,
    staging_path: &Path,
    key: &BlobKey,
) -> Result<(), StorageError> {
    match fs::rename(staging_path, final_path).await {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == ErrorKind::AlreadyExists => {
            let backup_path = final_path.with_extension(format!("bak.{}", Uuid::new_v4()));
            fs::rename(final_path, &backup_path)
                .await
                .map_err(|rename_err| {
                    classify_io_error(StorageOperation::Commit, key.clone(), rename_err)
                })?;

            if let Err(rename_err) = fs::rename(staging_path, final_path).await {
                let _ = fs::rename(&backup_path, final_path).await;
                return Err(classify_io_error(
                    StorageOperation::Commit,
                    key.clone(),
                    rename_err,
                ));
            }

            let _ = fs::remove_file(backup_path).await;
            Ok(())
        }
        Err(err) => Err(classify_io_error(
            StorageOperation::Commit,
            key.clone(),
            err,
        )),
    }
}

fn classify_io_error(
    operation: StorageOperation,
    key: BlobKey,
    err: std::io::Error,
) -> StorageError {
    match err.kind() {
        ErrorKind::NotFound => StorageError::NotFound {
            key,
            source: Some(Box::new(err)),
        },
        ErrorKind::PermissionDenied => StorageError::permission_denied(err),
        ErrorKind::TimedOut
        | ErrorKind::Interrupted
        | ErrorKind::WouldBlock
        | ErrorKind::ConnectionAborted
        | ErrorKind::ConnectionRefused
        | ErrorKind::ConnectionReset
        | ErrorKind::NotConnected
        | ErrorKind::BrokenPipe
        | ErrorKind::UnexpectedEof => StorageError::unavailable(true, err),
        ErrorKind::Unsupported => StorageError::Unsupported {
            capability: capability_label(operation),
            source: Some(Box::new(err)),
        },
        _ => StorageError::backend("filesystem", operation, err),
    }
}

const fn capability_label(operation: StorageOperation) -> &'static str {
    match operation {
        StorageOperation::Head => "head",
        StorageOperation::Open => "open",
        StorageOperation::OpenRange => "open_range",
        StorageOperation::BeginWrite => "begin_write",
        StorageOperation::WriteChunk => "write_chunk",
        StorageOperation::Commit => "commit",
        StorageOperation::Abort => "abort",
        StorageOperation::Delete => "delete",
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use rustcoon_storage::{BlobDeleteStore, BlobKey, StorageError};
    use rustcoon_storage::{
        BlobReadRange, BlobReadStore, BlobWritePrecondition, BlobWriteRequest, BlobWriteSession,
        BlobWriteStore,
    };
    use tempfile::tempdir;
    use tokio::io::AsyncReadExt;

    use super::{
        FilesystemBlobStore, FilesystemWriteSession, capability_label, rename_overwriting,
    };

    #[tokio::test]
    async fn write_read_and_range_round_trip() {
        let dir = tempdir().expect("tempdir");
        let store = FilesystemBlobStore::new(dir.path());
        let key = BlobKey::new("images/study/object.dcm").expect("valid key");

        let mut write = store
            .begin_write(BlobWriteRequest::new(key.clone()))
            .await
            .expect("begin write");
        write.write_chunk(b"abcdef").await.expect("write chunk");
        write.commit().await.expect("commit");

        let mut reader = store.open(&key).await.expect("open");
        let mut all = Vec::new();
        reader.read_to_end(&mut all).await.expect("read full");
        assert_eq!(all, b"abcdef");

        let mut range_reader = store
            .open_range(&key, BlobReadRange::bounded(2, 3))
            .await
            .expect("open range");
        let mut part = Vec::new();
        range_reader
            .read_to_end(&mut part)
            .await
            .expect("read range");
        assert_eq!(part, b"cde");

        let mut tail_reader = store
            .open_range(&key, BlobReadRange::from_offset(3))
            .await
            .expect("open tail range");
        let mut tail = Vec::new();
        tail_reader
            .read_to_end(&mut tail)
            .await
            .expect("read tail range");
        assert_eq!(tail, b"def");

        let metadata = store.head(&key).await.expect("head");
        assert_eq!(metadata.key, key);
        assert_eq!(metadata.size_bytes, 6);
    }

    #[tokio::test]
    async fn write_preconditions_are_enforced() {
        let dir = tempdir().expect("tempdir");
        let store = FilesystemBlobStore::new(dir.path());
        let key = BlobKey::new("cache/item.bin").expect("valid key");

        let mut create = store
            .begin_write(
                BlobWriteRequest::new(key.clone())
                    .with_precondition(BlobWritePrecondition::MustNotExist),
            )
            .await
            .expect("begin create");
        create.write_chunk(b"one").await.expect("write");
        create.commit().await.expect("commit");

        let mut second = store
            .begin_write(
                BlobWriteRequest::new(key.clone())
                    .with_precondition(BlobWritePrecondition::MustNotExist),
            )
            .await
            .expect("begin second create");
        second.write_chunk(b"two").await.expect("write");
        let err = second.commit().await.expect_err("commit should fail");
        assert!(matches!(err, StorageError::AlreadyExists { .. }));

        let missing = BlobKey::new("cache/missing.bin").expect("valid key");
        let mut replace = store
            .begin_write(
                BlobWriteRequest::new(missing.clone())
                    .with_precondition(BlobWritePrecondition::MustExist),
            )
            .await
            .expect("begin replace");
        replace.write_chunk(b"x").await.expect("write");
        let err = replace.commit().await.expect_err("commit should fail");
        assert!(matches!(err, StorageError::NotFound { .. }));
    }

    #[tokio::test]
    async fn delete_is_idempotent() {
        let dir = tempdir().expect("tempdir");
        let store = FilesystemBlobStore::new(dir.path());
        let key = BlobKey::new("cache/item.bin").expect("valid key");

        store.delete(&key).await.expect("delete missing");
    }

    #[tokio::test]
    async fn unconditional_and_must_exist_writes_replace_existing_payload() {
        let dir = tempdir().expect("tempdir");
        let store = FilesystemBlobStore::new(dir.path());
        let key = BlobKey::new("images/object.dcm").expect("valid key");

        let mut create = store
            .begin_write(
                BlobWriteRequest::new(key.clone())
                    .with_precondition(BlobWritePrecondition::MustNotExist),
            )
            .await
            .expect("begin create");
        create.write_chunk(b"old").await.expect("write");
        create.commit().await.expect("commit");

        let mut replace = store
            .begin_write(BlobWriteRequest::new(key.clone()))
            .await
            .expect("begin unconditional write");
        replace.write_chunk(b"new").await.expect("write");
        replace.commit().await.expect("commit");

        let mut reader = store.open(&key).await.expect("open");
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf).await.expect("read");
        assert_eq!(buf, b"new");

        let mut strict_replace = store
            .begin_write(
                BlobWriteRequest::new(key.clone())
                    .with_precondition(BlobWritePrecondition::MustExist),
            )
            .await
            .expect("begin strict replace");
        strict_replace.write_chunk(b"latest").await.expect("write");
        strict_replace.commit().await.expect("commit");

        let mut reader = store.open(&key).await.expect("open");
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf).await.expect("read");
        assert_eq!(buf, b"latest");
    }

    #[tokio::test]
    async fn open_and_head_missing_and_invalid_ranges_return_expected_errors() {
        let dir = tempdir().expect("tempdir");
        let store = FilesystemBlobStore::new(dir.path());
        let missing = BlobKey::new("images/missing.dcm").expect("valid key");

        assert!(matches!(
            store.head(&missing).await,
            Err(StorageError::NotFound { .. })
        ));
        assert!(matches!(
            store.open(&missing).await,
            Err(StorageError::NotFound { .. })
        ));
        assert!(matches!(
            store
                .open_range(&missing, BlobReadRange::from_offset(0))
                .await,
            Err(StorageError::NotFound { .. })
        ));

        let key = BlobKey::new("images/object.dcm").expect("valid key");
        let mut write = store
            .begin_write(BlobWriteRequest::new(key.clone()))
            .await
            .expect("begin write");
        write.write_chunk(b"abc").await.expect("write");
        write.commit().await.expect("commit");

        assert!(matches!(
            store.open_range(&key, BlobReadRange::from_offset(4)).await,
            Err(StorageError::InvalidRange)
        ));
        assert!(matches!(
            store.open_range(&key, BlobReadRange::bounded(2, 2)).await,
            Err(StorageError::InvalidRange)
        ));
    }

    #[tokio::test]
    async fn abort_discards_staged_write_and_manual_empty_abort_is_ok() {
        let dir = tempdir().expect("tempdir");
        let store = FilesystemBlobStore::new(dir.path());
        let key = BlobKey::new("cache/abort.bin").expect("valid key");

        let mut write = store
            .begin_write(BlobWriteRequest::new(key.clone()))
            .await
            .expect("begin write");
        write.write_chunk(b"temp").await.expect("write");
        write.abort().await.expect("abort");

        assert!(matches!(
            store.open(&key).await,
            Err(StorageError::NotFound { .. })
        ));

        let manual = FilesystemWriteSession {
            key: key.clone(),
            final_path: PathBuf::from("/tmp/unused"),
            staging_path: None,
            file: None,
            precondition: BlobWritePrecondition::None,
        };
        Box::new(manual).abort().await.expect("manual abort");
    }

    #[tokio::test]
    async fn helper_paths_cover_remaining_internal_branches() {
        let dir = tempdir().expect("tempdir");
        let key = BlobKey::new("images/object.dcm").expect("valid key");
        let final_path = dir.path().join("final.bin");
        let staging_path = dir.path().join("staging.bin");
        tokio::fs::write(&staging_path, b"abc")
            .await
            .expect("write staging");

        rename_overwriting(&final_path, &staging_path, &key)
            .await
            .expect("rename should succeed");
        assert_eq!(
            tokio::fs::read(&final_path).await.expect("read final"),
            b"abc"
        );

        assert_eq!(
            capability_label(rustcoon_storage::StorageOperation::Head),
            "head"
        );
        assert_eq!(
            capability_label(rustcoon_storage::StorageOperation::Open),
            "open"
        );
        assert_eq!(
            capability_label(rustcoon_storage::StorageOperation::OpenRange),
            "open_range"
        );
        assert_eq!(
            capability_label(rustcoon_storage::StorageOperation::BeginWrite),
            "begin_write"
        );
        assert_eq!(
            capability_label(rustcoon_storage::StorageOperation::WriteChunk),
            "write_chunk"
        );
        assert_eq!(
            capability_label(rustcoon_storage::StorageOperation::Commit),
            "commit"
        );
        assert_eq!(
            capability_label(rustcoon_storage::StorageOperation::Abort),
            "abort"
        );
        assert_eq!(
            capability_label(rustcoon_storage::StorageOperation::Delete),
            "delete"
        );
    }

    #[test]
    fn classify_io_error_maps_expected_variants() {
        let key = BlobKey::new("images/object.dcm").expect("valid key");

        assert!(matches!(
            super::classify_io_error(
                rustcoon_storage::StorageOperation::Open,
                key.clone(),
                std::io::Error::new(std::io::ErrorKind::NotFound, "missing"),
            ),
            StorageError::NotFound { .. }
        ));

        assert!(matches!(
            super::classify_io_error(
                rustcoon_storage::StorageOperation::Open,
                key.clone(),
                std::io::Error::new(std::io::ErrorKind::PermissionDenied, "denied"),
            ),
            StorageError::PermissionDenied { .. }
        ));

        assert!(matches!(
            super::classify_io_error(
                rustcoon_storage::StorageOperation::Open,
                key.clone(),
                std::io::Error::new(std::io::ErrorKind::TimedOut, "timeout"),
            ),
            StorageError::Unavailable {
                transient: true,
                ..
            }
        ));

        assert!(matches!(
            super::classify_io_error(
                rustcoon_storage::StorageOperation::Open,
                key.clone(),
                std::io::Error::new(std::io::ErrorKind::Unsupported, "unsupported"),
            ),
            StorageError::Unsupported {
                capability: "open",
                ..
            }
        ));

        assert!(matches!(
            super::classify_io_error(
                rustcoon_storage::StorageOperation::Commit,
                key,
                std::io::Error::other("boom"),
            ),
            StorageError::Backend {
                backend: "filesystem",
                operation: rustcoon_storage::StorageOperation::Commit,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn internal_helpers_cover_remaining_non_platform_specific_error_paths() {
        let dir = tempdir().expect("tempdir");
        let key = BlobKey::new("images/object.dcm").expect("valid key");

        let mut session = FilesystemWriteSession {
            key: key.clone(),
            final_path: dir.path().join("final.bin"),
            staging_path: Some(dir.path().join("missing-staging.bin")),
            file: None,
            precondition: BlobWritePrecondition::None,
        };
        session.sync_staged_file().await.expect("sync without file");

        let missing_staging = dir.path().join("missing-create.bin");
        assert!(matches!(
            session.commit_create_new(&missing_staging).await,
            Err(StorageError::NotFound { .. })
        ));

        let existing_final = dir.path().join("existing-final.bin");
        tokio::fs::write(&existing_final, b"old")
            .await
            .expect("write final");
        let missing_replace = dir.path().join("missing-replace.bin");
        let replace_session = FilesystemWriteSession {
            key: key.clone(),
            final_path: existing_final,
            staging_path: Some(missing_replace.clone()),
            file: None,
            precondition: BlobWritePrecondition::None,
        };
        assert!(matches!(
            replace_session
                .commit_replace(&missing_replace, false)
                .await,
            Err(StorageError::NotFound { .. })
        ));

        let staging_to_drop = dir.path().join("drop-staging.bin");
        std::fs::write(&staging_to_drop, b"temp").expect("write drop staging");
        let drop_session = FilesystemWriteSession {
            key,
            final_path: dir.path().join("unused-final.bin"),
            staging_path: Some(staging_to_drop.clone()),
            file: None,
            precondition: BlobWritePrecondition::None,
        };
        drop(drop_session);
        assert!(!staging_to_drop.exists());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn filesystem_permission_failures_cover_begin_write_and_replace_metadata_paths() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempdir().expect("tempdir");
        let key = BlobKey::new("images/object.dcm").expect("valid key");

        let blocked_parent = dir.path().join("blocked");
        std::fs::create_dir_all(&blocked_parent).expect("create blocked parent");
        let mut perms = std::fs::metadata(&blocked_parent)
            .expect("metadata")
            .permissions();
        perms.set_mode(0o555);
        std::fs::set_permissions(&blocked_parent, perms).expect("set readonly perms");

        let blocked_store = FilesystemBlobStore::new(dir.path());
        assert!(matches!(
            blocked_store
                .begin_write(BlobWriteRequest::new(
                    BlobKey::new("blocked/object.dcm").expect("valid blocked key"),
                ))
                .await,
            Err(StorageError::PermissionDenied { .. } | StorageError::Backend { .. })
        ));

        let restricted_parent = dir.path().join("restricted");
        std::fs::create_dir_all(&restricted_parent).expect("create restricted");
        let final_path = restricted_parent.join("final.bin");
        std::fs::write(&final_path, b"old").expect("write final");
        let staging_path = dir.path().join("staging.bin");
        std::fs::write(&staging_path, b"new").expect("write staging");

        let mut perms = std::fs::metadata(&restricted_parent)
            .expect("metadata")
            .permissions();
        perms.set_mode(0o000);
        std::fs::set_permissions(&restricted_parent, perms).expect("remove perms");

        let session = FilesystemWriteSession {
            key,
            final_path: final_path.clone(),
            staging_path: Some(staging_path.clone()),
            file: None,
            precondition: BlobWritePrecondition::MustExist,
        };
        assert!(matches!(
            session.commit_replace(&staging_path, true).await,
            Err(StorageError::PermissionDenied { .. } | StorageError::Backend { .. })
        ));

        let mut perms = std::fs::metadata(&restricted_parent)
            .expect("metadata")
            .permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(&restricted_parent, perms).expect("restore perms");
        let mut perms = std::fs::metadata(&blocked_parent)
            .expect("metadata")
            .permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(&blocked_parent, perms).expect("restore perms");
    }

    #[tokio::test]
    async fn begin_write_reports_error_when_staging_filename_is_too_long() {
        let dir = tempdir().expect("tempdir");
        let store = FilesystemBlobStore::new(dir.path());
        let oversized_name = "a".repeat(300);
        let key = BlobKey::new(format!("images/{oversized_name}")).expect("valid key");

        assert!(matches!(
            store.begin_write(BlobWriteRequest::new(key)).await,
            Err(StorageError::Backend { .. } | StorageError::PermissionDenied { .. })
        ));
    }

    #[tokio::test]
    async fn begin_write_reports_error_when_root_is_a_file() {
        let dir = tempdir().expect("tempdir");
        let root_file = dir.path().join("root-file");
        tokio::fs::write(&root_file, b"x")
            .await
            .expect("write root file");

        let store = FilesystemBlobStore::new(&root_file);
        let key = BlobKey::new("images/object.dcm").expect("valid key");
        assert!(matches!(
            store.begin_write(BlobWriteRequest::new(key)).await,
            Err(StorageError::Backend { .. } | StorageError::PermissionDenied { .. })
        ));
    }
}
