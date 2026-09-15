//! Directory traversal with the semantics of Go's `filepath.WalkDir`:
//! lexical (sorted) order, symlinks reported but never followed, and errors
//! delivered to the callback instead of aborting the walk.

use std::fs;
use std::io;

use crate::gopath;

#[derive(Debug, Clone)]
pub struct WalkEntry {
    pub name: String,
    pub is_dir: bool,
    pub is_symlink: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Flow {
    Continue,
    SkipDir,
}

pub type WalkFn<'a> = dyn FnMut(&str, Option<&WalkEntry>, Option<io::Error>) -> Flow + 'a;

/// Walk `root`, calling `f` for every entry. The callback receives the path,
/// the entry (absent only when the root itself could not be stat'ed), and the
/// error for that entry, if any.
pub fn walk_dir(root: &str, f: &mut WalkFn<'_>) {
    match fs::symlink_metadata(root) {
        Err(e) => {
            f(root, None, Some(e));
        }
        Ok(md) => {
            let entry = WalkEntry {
                name: gopath::base(root),
                is_dir: md.is_dir(),
                is_symlink: md.file_type().is_symlink(),
            };
            walk_rec(root, &entry, f);
        }
    }
}

fn walk_rec(path: &str, d: &WalkEntry, f: &mut WalkFn<'_>) -> Flow {
    let flow = f(path, Some(d), None);
    if !d.is_dir {
        return flow;
    }
    if flow == Flow::SkipDir {
        return Flow::Continue;
    }
    let (children, read_err) = read_dir_sorted(path);
    if let Some(err) = read_err {
        // Second call, to report the ReadDir error.
        if f(path, Some(d), Some(err)) == Flow::SkipDir {
            return Flow::Continue;
        }
    }
    for child in children {
        let child_path = gopath::join(&[path, &child.name]);
        if walk_rec(&child_path, &child, f) == Flow::SkipDir {
            break;
        }
    }
    Flow::Continue
}

/// Read a directory in name order, returning the entries read before any
/// error together with that error (mirroring `os.ReadDir`).
fn read_dir_sorted(path: &str) -> (Vec<WalkEntry>, Option<io::Error>) {
    let iter = match fs::read_dir(path) {
        Ok(iter) => iter,
        Err(e) => return (Vec::new(), Some(e)),
    };
    let mut raw: Vec<(std::ffi::OsString, WalkEntry)> = Vec::new();
    let mut first_err = None;
    for item in iter {
        let entry = match item {
            Ok(entry) => entry,
            Err(e) => {
                first_err = Some(e);
                break;
            }
        };
        let os_name = entry.file_name();
        let Some(name) = os_name.to_str().map(str::to_string) else {
            first_err = Some(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("{}: file name is not valid UTF-8", os_name.to_string_lossy()),
            ));
            break;
        };
        let (is_dir, is_symlink) = match entry.file_type() {
            Ok(ft) => (ft.is_dir(), ft.is_symlink()),
            Err(e) => {
                first_err = Some(e);
                break;
            }
        };
        raw.push((os_name, WalkEntry { name, is_dir, is_symlink }));
    }
    raw.sort_by(|a, b| a.0.cmp(&b.0));
    (raw.into_iter().map(|(_, e)| e).collect(), first_err)
}
