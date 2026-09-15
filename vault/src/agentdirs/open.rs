//! Opening transcript files without following symlinks.
//!
//! Transcripts are sensitive, and a symlink planted inside an agent's session
//! directory must never cause an unrelated file to be uploaded. Every read of a
//! session file goes through this module.

use std::fs::{File, Metadata, OpenOptions};

use crate::util::path_error;
use crate::{Error, Result};

#[cfg(unix)]
pub fn open_regular_file_nofollow(path: &str) -> Result<(File, Metadata)> {
    use std::os::unix::fs::OpenOptionsExt;

    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)
        .map_err(|e| Error::from(path_error("open", path, &e)))?;
    let info = file.metadata().map_err(|e| Error::from(path_error("stat", path, &e)))?;
    if !info.is_file() {
        return Err(format!("{path} is not a regular file").into());
    }
    Ok((file, info))
}

#[cfg(windows)]
pub fn open_regular_file_nofollow(path: &str) -> Result<(File, Metadata)> {
    use std::os::windows::fs::{MetadataExt, OpenOptionsExt};

    const FILE_FLAG_OPEN_REPARSE_POINT: u32 = 0x0020_0000;
    const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x0000_0400;

    let file = OpenOptions::new()
        .read(true)
        .custom_flags(FILE_FLAG_OPEN_REPARSE_POINT)
        .open(path)
        .map_err(|e| Error::from(path_error("open", path, &e)))?;
    let info = file.metadata().map_err(|e| Error::from(path_error("stat", path, &e)))?;
    if info.file_type().is_symlink() {
        return Err(format!("{path} is a symlink").into());
    }
    if info.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
        return Err(format!("{path} is a reparse point").into());
    }
    if !info.is_file() {
        return Err(format!("{path} is not a regular file").into());
    }
    Ok((file, info))
}

pub fn regular_file_info_nofollow(path: &str) -> Result<Metadata> {
    let (_file, info) = open_regular_file_nofollow(path)?;
    Ok(info)
}
