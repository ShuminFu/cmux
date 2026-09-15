//! Local sync bookkeeping: `~/.local/state/cmux-vault/state.json`.

use std::collections::{BTreeMap, HashMap};

use serde::{Deserialize, Serialize};

use crate::gopath;
use crate::util::{nullable, path_error, write_file_atomic};
use crate::{Error, Result};

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct Entry {
    #[serde(rename = "sizeBytes", deserialize_with = "nullable")]
    pub size_bytes: i64,
    #[serde(rename = "mtimeUnixNs", deserialize_with = "nullable")]
    pub mtime_unix_ns: i64,
    #[serde(rename = "sha256", deserialize_with = "nullable")]
    pub sha256: String,
    #[serde(rename = "remoteSha256", deserialize_with = "nullable")]
    pub remote_sha256: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct File {
    #[serde(deserialize_with = "nullable")]
    pub entries: BTreeMap<String, Entry>,
}

#[derive(Debug)]
pub struct Store {
    path: String,
    pub entries: BTreeMap<String, Entry>,
}

pub fn default_dir(home: &str, vars: &HashMap<String, String>) -> Result<String> {
    if let Some(dir) = vars.get("CMUX_VAULT_STATE_DIR").map(|v| v.trim()).filter(|v| !v.is_empty())
    {
        return Ok(dir.to_string());
    }
    if home.trim().is_empty() {
        return Err("home directory is empty".into());
    }
    Ok(gopath::join(&[home, ".local", "state", "cmux-vault"]))
}

pub fn load(home: &str, vars: &HashMap<String, String>) -> Result<Store> {
    let dir = default_dir(home, vars)?;
    let path = gopath::join(&[&dir, "state.json"]);
    let mut store = Store { path: path.clone(), entries: BTreeMap::new() };
    let data = match std::fs::read(&path) {
        Ok(data) => data,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(store),
        Err(e) => return Err(path_error("open", &path, &e).into()),
    };
    if data.is_empty() {
        return Ok(store);
    }
    let file: File = serde_json::from_slice(&data).map_err(|e| Error::from(e.to_string()))?;
    store.entries = file.entries;
    Ok(store)
}

/// State key for a session: agent and relative path separated by NUL.
#[must_use]
pub fn key(agent: &str, rel_path: &str) -> String {
    format!("{}\u{0}{}", agent.trim(), rel_path.trim())
}

impl Store {
    #[must_use]
    pub fn path(&self) -> &str {
        &self.path
    }

    pub fn save(&self) -> Result<()> {
        let file = File { entries: self.entries.clone() };
        let mut data = serde_json::to_vec_pretty(&file).map_err(|e| Error::from(e.to_string()))?;
        data.push(b'\n');
        write_file_atomic(&self.path, &data, ".state-", ".tmp")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn store_round_trip_and_atomic_write() {
        let tmp = tempfile::tempdir().unwrap();
        let home = tmp.path().to_string_lossy().to_string();
        let state_dir = format!("{home}/state");
        let vars = HashMap::from([("CMUX_VAULT_STATE_DIR".to_string(), state_dir.clone())]);
        let mut store = load(&home, &vars).unwrap();
        store.entries.insert(
            key("codex", "sessions/a.jsonl"),
            Entry {
                size_bytes: 12,
                mtime_unix_ns: 34,
                sha256: "abc".into(),
                remote_sha256: "abc".into(),
            },
        );
        store.save().unwrap();
        store.entries.insert(
            key("codex", "sessions/a.jsonl"),
            Entry {
                size_bytes: 56,
                mtime_unix_ns: 78,
                sha256: "def".into(),
                remote_sha256: "def".into(),
            },
        );
        store.save().unwrap();

        let loaded = load(&home, &vars).unwrap();
        let got = &loaded.entries[&key("codex", "sessions/a.jsonl")];
        assert_eq!(
            got,
            &Entry {
                size_bytes: 56,
                mtime_unix_ns: 78,
                sha256: "def".into(),
                remote_sha256: "def".into()
            }
        );
        let leftovers: Vec<String> = std::fs::read_dir(&state_dir)
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().to_string())
            .filter(|n| n.starts_with(".state-"))
            .collect();
        assert!(leftovers.is_empty(), "temporary files left behind: {leftovers:?}");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode =
                std::fs::metadata(format!("{state_dir}/state.json")).unwrap().permissions().mode()
                    & 0o777;
            assert_eq!(mode, 0o600);
        }
        let raw = std::fs::read_to_string(format!("{state_dir}/state.json")).unwrap();
        let expected_prefix = "{\n  \"entries\": {\n    \"codex\\u0000sessions/a.jsonl\": {\n      \"sizeBytes\": 56,";
        assert!(raw.starts_with(expected_prefix), "{raw}");
        assert!(raw.ends_with("}\n"));
    }

    #[test]
    fn load_tolerates_empty_and_null_entries() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().to_string_lossy().to_string();
        let vars = HashMap::from([("CMUX_VAULT_STATE_DIR".to_string(), dir.clone())]);
        std::fs::write(format!("{dir}/state.json"), "").unwrap();
        assert!(load("/h", &vars).unwrap().entries.is_empty());
        std::fs::write(format!("{dir}/state.json"), r#"{"entries":null}"#).unwrap();
        assert!(load("/h", &vars).unwrap().entries.is_empty());
        std::fs::write(
            format!("{dir}/state.json"),
            "{\"entries\":{\"a\\u0000b\":{\"sizeBytes\":1}}}",
        )
        .unwrap();
        let store = load("/h", &vars).unwrap();
        assert_eq!(store.entries[&key("a", "b")], Entry { size_bytes: 1, ..Entry::default() });
        std::fs::write(format!("{dir}/state.json"), "{").unwrap();
        assert!(load("/h", &vars).is_err());
        assert_eq!(default_dir("/h", &HashMap::new()).unwrap(), "/h/.local/state/cmux-vault");
    }
}
