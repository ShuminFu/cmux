//! Lexical path helpers that reproduce Go's `path/filepath` semantics.
//!
//! The Go implementation manipulated paths as strings and relied on
//! `filepath.Clean`, `Join`, and `Rel` for its relative-path contract (the
//! `relPath` stored in cmux Vault). Reproducing those algorithms exactly keeps
//! the on-disk state keys and the server-side identifiers stable across the
//! rewrite.

pub const SEP: char = std::path::MAIN_SEPARATOR;

#[inline]
fn is_sep(b: u8) -> bool {
    #[cfg(windows)]
    {
        b == b'\\' || b == b'/'
    }
    #[cfg(not(windows))]
    {
        b == b'/'
    }
}

/// Port of `filepath.Clean` (without Windows volume-name handling).
#[must_use]
pub fn clean(path: &str) -> String {
    let bytes = path.as_bytes();
    if bytes.is_empty() {
        return ".".to_string();
    }
    let rooted = is_sep(bytes[0]);
    let n = bytes.len();
    let mut out: Vec<u8> = Vec::with_capacity(n);
    let mut r = 0usize;
    let mut dotdot = 0usize;
    if rooted {
        out.push(SEP as u8);
        r = 1;
        dotdot = 1;
    }
    while r < n {
        if is_sep(bytes[r]) {
            // Empty path element.
            r += 1;
        } else if bytes[r] == b'.' && (r + 1 == n || is_sep(bytes[r + 1])) {
            // `.` element.
            r += 1;
        } else if bytes[r] == b'.' && bytes[r + 1] == b'.' && (r + 2 == n || is_sep(bytes[r + 2])) {
            // `..` element: remove to last separator.
            r += 2;
            if out.len() > dotdot {
                // Can backtrack.
                let mut removed = out.pop();
                while out.len() > dotdot && !removed.is_some_and(is_sep) {
                    removed = out.pop();
                }
            } else if !rooted {
                // Cannot backtrack, but not rooted, so append `..` element.
                if !out.is_empty() {
                    out.push(SEP as u8);
                }
                out.extend_from_slice(b"..");
                dotdot = out.len();
            }
        } else {
            // Real path element. Add separator if needed.
            if (rooted && out.len() != 1) || (!rooted && !out.is_empty()) {
                out.push(SEP as u8);
            }
            while r < n && !is_sep(bytes[r]) {
                out.push(bytes[r]);
                r += 1;
            }
        }
    }
    if out.is_empty() {
        out.push(b'.');
    }
    // Only ASCII bytes were removed or inserted, so UTF-8 validity is preserved.
    String::from_utf8(out).expect("clean preserves UTF-8")
}

/// Port of `filepath.Join`: joins the non-empty elements and cleans the result.
#[must_use]
pub fn join(elems: &[&str]) -> String {
    let mut buf = String::new();
    for elem in elems {
        if elem.is_empty() {
            continue;
        }
        if !buf.is_empty() {
            buf.push(SEP);
        }
        buf.push_str(elem);
    }
    if buf.is_empty() {
        return String::new();
    }
    clean(&buf)
}

/// Port of `filepath.Rel` (without Windows volume-name handling).
pub fn rel(basepath: &str, targpath: &str) -> Result<String, String> {
    let base = clean(basepath);
    let targ = clean(targpath);
    if targ == base {
        return Ok(".".to_string());
    }
    let base = if base == "." { String::new() } else { base };
    let base_slashed = base.as_bytes().first().is_some_and(|b| is_sep(*b));
    let targ_slashed = targ.as_bytes().first().is_some_and(|b| is_sep(*b));
    if base_slashed != targ_slashed {
        return Err(format!("Rel: can't make {targpath} relative to {basepath}"));
    }
    let b = base.as_bytes();
    let t = targ.as_bytes();
    let (bl, tl) = (b.len(), t.len());
    let (mut b0, mut bi, mut t0, mut ti) = (0usize, 0usize, 0usize, 0usize);
    loop {
        while bi < bl && !is_sep(b[bi]) {
            bi += 1;
        }
        while ti < tl && !is_sep(t[ti]) {
            ti += 1;
        }
        if t[t0..ti] != b[b0..bi] {
            break;
        }
        if bi < bl {
            bi += 1;
        }
        if ti < tl {
            ti += 1;
        }
        b0 = bi;
        t0 = ti;
    }
    if &b[b0..bi] == b".." {
        return Err(format!("Rel: can't make {targpath} relative to {basepath}"));
    }
    if b0 != bl {
        // Base elements left. Must go up before going down.
        let seps = b[b0..bl].iter().filter(|c| is_sep(**c)).count();
        let mut buf = String::from("..");
        for _ in 0..seps {
            buf.push(SEP);
            buf.push_str("..");
        }
        if t0 != tl {
            buf.push(SEP);
            buf.push_str(&targ[t0..]);
        }
        return Ok(buf);
    }
    Ok(targ[t0..].to_string())
}

/// Port of `filepath.Base`.
#[must_use]
pub fn base(path: &str) -> String {
    if path.is_empty() {
        return ".".to_string();
    }
    let mut end = path.len();
    let bytes = path.as_bytes();
    while end > 0 && is_sep(bytes[end - 1]) {
        end -= 1;
    }
    let trimmed = &path[..end];
    if trimmed.is_empty() {
        return SEP.to_string();
    }
    let start = trimmed.bytes().rposition(is_sep).map_or(0, |i| i + 1);
    trimmed[start..].to_string()
}

/// Port of `filepath.Dir`.
#[must_use]
pub fn dir(path: &str) -> String {
    let bytes = path.as_bytes();
    let mut i = bytes.len();
    while i > 0 && !is_sep(bytes[i - 1]) {
        i -= 1;
    }
    clean(&path[..i])
}

/// Port of `filepath.Ext`.
#[must_use]
pub fn ext(path: &str) -> &str {
    let bytes = path.as_bytes();
    let mut i = bytes.len();
    while i > 0 && !is_sep(bytes[i - 1]) {
        if bytes[i - 1] == b'.' {
            return &path[i - 1..];
        }
        i -= 1;
    }
    ""
}

/// Port of `filepath.IsAbs`.
#[must_use]
pub fn is_abs(path: &str) -> bool {
    #[cfg(windows)]
    {
        std::path::Path::new(path).is_absolute()
    }
    #[cfg(not(windows))]
    {
        path.starts_with('/')
    }
}

/// Port of `filepath.ToSlash`.
#[must_use]
pub fn to_slash(path: &str) -> String {
    if SEP == '/' { path.to_string() } else { path.replace(SEP, "/") }
}

/// Port of `filepath.FromSlash`.
#[must_use]
pub fn from_slash(path: &str) -> String {
    if SEP == '/' { path.to_string() } else { path.replace('/', &SEP.to_string()) }
}

#[cfg(all(test, not(windows)))]
mod tests {
    use super::*;

    #[test]
    fn clean_matches_go_vectors() {
        let cases = [
            ("", "."),
            ("abc", "abc"),
            ("abc/def", "abc/def"),
            ("a/b/c", "a/b/c"),
            (".", "."),
            ("..", ".."),
            ("../..", "../.."),
            ("../../abc", "../../abc"),
            ("/abc", "/abc"),
            ("/", "/"),
            ("abc/", "abc"),
            ("abc/def/", "abc/def"),
            ("a/b/c/", "a/b/c"),
            ("./", "."),
            ("../", ".."),
            ("../../", "../.."),
            ("/abc/", "/abc"),
            ("abc//def//ghi", "abc/def/ghi"),
            ("//abc", "/abc"),
            ("///abc", "/abc"),
            ("//abc//", "/abc"),
            ("abc//", "abc"),
            ("abc/./def", "abc/def"),
            ("/./abc/def", "/abc/def"),
            ("abc/.", "abc"),
            ("abc/def/ghi/../jkl", "abc/def/jkl"),
            ("abc/def/../ghi/../jkl", "abc/jkl"),
            ("abc/def/..", "abc"),
            ("abc/def/../..", "."),
            ("/abc/def/../..", "/"),
            ("abc/def/../../..", ".."),
            ("/abc/def/../../..", "/"),
            ("abc/def/../../../ghi/jkl/../../../mno", "../../mno"),
            ("/../abc", "/abc"),
            ("a/../b:/../../c", "../c"),
            ("abc/./../def", "def"),
            ("abc//./../def", "def"),
            ("abc/../../././../def", "../../def"),
        ];
        for (input, want) in cases {
            assert_eq!(clean(input), want, "clean({input:?})");
        }
    }

    #[test]
    fn rel_matches_go_vectors() {
        let ok = [
            ("a/b", "a/b", "."),
            ("a/b/.", "a/b", "."),
            ("a/b", "a/b/.", "."),
            ("./a/b", "a/b", "."),
            ("a/b", "./a/b", "."),
            ("ab/cd", "ab/cde", "../cde"),
            ("ab/cd", "ab/c", "../c"),
            ("a/b", "a/b/c/d", "c/d"),
            ("a/b", "a/b/../c", "../c"),
            ("a/b/../c", "a/b", "../b"),
            ("a/b/c", "a/c/d", "../../c/d"),
            ("a/b", "c/d", "../../c/d"),
            ("a/b/c/d", "a/b", "../.."),
            ("a/b/c/d", "a/b/", "../.."),
            ("a/b/c/d/", "a/b", "../.."),
            ("a/b/c/d/", "a/b/", "../.."),
            ("../../a/b", "../../a/b/c/d", "c/d"),
            ("/a/b", "/a/b", "."),
            ("/a/b/.", "/a/b", "."),
            ("/a/b", "/a/b/.", "."),
            ("/ab/cd", "/ab/cde", "../cde"),
            ("/ab/cd", "/ab/c", "../c"),
            ("/a/b", "/a/b/c/d", "c/d"),
            ("/a/b", "/a/b/../c", "../c"),
            ("/a/b/../c", "/a/b", "../b"),
            ("/a/b/c", "/a/c/d", "../../c/d"),
            ("/a/b", "/c/d", "../../c/d"),
            ("/a/b/c/d", "/a/b", "../.."),
            ("/a/b/c/d", "/a/b/", "../.."),
            ("/a/b/c/d/", "/a/b", "../.."),
            ("/a/b/c/d/", "/a/b/", "../.."),
            ("/../../a/b", "/../../a/b/c/d", "c/d"),
            (".", "a/b", "a/b"),
            (".", "..", ".."),
        ];
        for (base, targ, want) in ok {
            assert_eq!(rel(base, targ).as_deref(), Ok(want), "rel({base:?}, {targ:?})");
        }
        for (base, targ) in [("..", "."), ("..", "a"), ("../..", ".."), ("a", "/a"), ("/a", "a")] {
            assert!(rel(base, targ).is_err(), "rel({base:?}, {targ:?}) should fail");
        }
    }

    #[test]
    fn join_base_dir_ext() {
        assert_eq!(join(&["a", "b"]), "a/b");
        assert_eq!(join(&["a", "", "b"]), "a/b");
        assert_eq!(join(&["", ""]), "");
        assert_eq!(join(&["/", "a", "..", "b"]), "/b");
        assert_eq!(base(""), ".");
        assert_eq!(base("/"), "/");
        assert_eq!(base("a/b/"), "b");
        assert_eq!(base("a/b.jsonl"), "b.jsonl");
        assert_eq!(dir("/a/b/c"), "/a/b");
        assert_eq!(dir("a"), ".");
        assert_eq!(dir("/"), "/");
        assert_eq!(dir("a/b/"), "a/b");
        assert_eq!(ext("x.jsonl"), ".jsonl");
        assert_eq!(ext("x.tar.gz"), ".gz");
        assert_eq!(ext("dir.d/x"), "");
        assert_eq!(ext("x"), "");
        assert!(is_abs("/x"));
        assert!(!is_abs("x"));
    }
}
