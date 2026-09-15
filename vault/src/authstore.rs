//! Stack Auth token storage: `~/.config/cmux-vault/auth.json`, mode `0600`,
//! written atomically.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::gopath;
use crate::util::{nullable, path_error, write_file_atomic};
use crate::{Error, Result};

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct Tokens {
    #[serde(rename = "accessToken", deserialize_with = "nullable")]
    pub access_token: String,
    #[serde(rename = "refreshToken", deserialize_with = "nullable")]
    pub refresh_token: String,
}

pub fn default_dir(home: &str, vars: &HashMap<String, String>) -> Result<String> {
    if let Some(dir) = vars.get("CMUX_VAULT_CONFIG_DIR").map(|v| v.trim()).filter(|v| !v.is_empty())
    {
        return Ok(dir.to_string());
    }
    if home.trim().is_empty() {
        return Err("home directory is empty".into());
    }
    Ok(gopath::join(&[home, ".config", "cmux-vault"]))
}

fn auth_path(home: &str, vars: &HashMap<String, String>) -> Result<String> {
    Ok(gopath::join(&[&default_dir(home, vars)?, "auth.json"]))
}

/// Load stored tokens. Returns `None` when no usable token file exists.
pub fn load(home: &str, vars: &HashMap<String, String>) -> Result<Option<Tokens>> {
    let path = auth_path(home, vars)?;
    let data = match std::fs::read(&path) {
        Ok(data) => data,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(path_error("open", &path, &e).into()),
    };
    let tokens: Tokens = serde_json::from_slice(&data).map_err(|e| Error::from(e.to_string()))?;
    if tokens.access_token.trim().is_empty() || tokens.refresh_token.trim().is_empty() {
        return Ok(None);
    }
    Ok(Some(tokens))
}

pub fn save(home: &str, vars: &HashMap<String, String>, tokens: &Tokens) -> Result<()> {
    let path = auth_path(home, vars)?;
    let mut data = serde_json::to_vec_pretty(tokens).map_err(|e| Error::from(e.to_string()))?;
    data.push(b'\n');
    write_file_atomic(&path, &data, ".auth-", ".tmp")
}

pub fn delete(home: &str, vars: &HashMap<String, String>) -> Result<()> {
    let path = auth_path(home, vars)?;
    match std::fs::remove_file(&path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(path_error("remove", &path, &e).into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vars(dir: &str) -> HashMap<String, String> {
        HashMap::from([("CMUX_VAULT_CONFIG_DIR".to_string(), dir.to_string())])
    }

    #[test]
    fn round_trip_and_private_mode() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("cfg").to_string_lossy().to_string();
        let vars = vars(&dir);
        assert_eq!(load("/home/x", &vars).unwrap(), None);
        let tokens = Tokens { access_token: "a".into(), refresh_token: "r".into() };
        save("/home/x", &vars, &tokens).unwrap();
        assert_eq!(load("/home/x", &vars).unwrap(), Some(tokens));
        let raw = std::fs::read_to_string(format!("{dir}/auth.json")).unwrap();
        assert_eq!(raw, "{\n  \"accessToken\": \"a\",\n  \"refreshToken\": \"r\"\n}\n");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode =
                std::fs::metadata(format!("{dir}/auth.json")).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode, 0o600);
        }
        let leftovers = std::fs::read_dir(&dir)
            .unwrap()
            .filter(|e| e.as_ref().unwrap().file_name().to_string_lossy().starts_with(".auth-"))
            .count();
        assert_eq!(leftovers, 0);
        delete("/home/x", &vars).unwrap();
        delete("/home/x", &vars).unwrap();
        assert_eq!(load("/home/x", &vars).unwrap(), None);
    }

    #[test]
    fn incomplete_or_null_tokens_are_not_logged_in() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().to_string_lossy().to_string();
        let vars = vars(&dir);
        std::fs::write(format!("{dir}/auth.json"), r#"{"accessToken":"a","refreshToken":null}"#)
            .unwrap();
        assert_eq!(load("/home/x", &vars).unwrap(), None);
        std::fs::write(
            format!("{dir}/auth.json"),
            r#"{"accessToken":" ","refreshToken":"r","extra":1}"#,
        )
        .unwrap();
        assert_eq!(load("/home/x", &vars).unwrap(), None);
        std::fs::write(format!("{dir}/auth.json"), "not json").unwrap();
        assert!(load("/home/x", &vars).is_err());
    }

    #[test]
    fn default_dir_uses_home() {
        assert_eq!(default_dir("/home/u", &HashMap::new()).unwrap(), "/home/u/.config/cmux-vault");
        assert!(default_dir("  ", &HashMap::new()).is_err());
        assert_eq!(default_dir("", &vars(" /x ")).unwrap(), "/x");
    }
}
