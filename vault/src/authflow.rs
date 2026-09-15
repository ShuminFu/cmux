//! Device-code login against the cmux web API.

use std::time::{Duration, Instant};

use crate::api::Client;
use crate::authstore::Tokens;
use crate::util::quote;
use crate::{Printer, Result};

pub fn login(client: &Client, out: &dyn Printer) -> Result<Tokens> {
    let start = client.start_auth()?;

    out.print(&format!("Open this URL to approve cmux-vault:\n  {}\n\n", start.verification_url));
    out.print(&format!("Code: {}\n", start.user_code));
    #[cfg(target_os = "macos")]
    {
        let _ = std::process::Command::new("open")
            .arg(&start.verification_url)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn();
    }

    let interval = if start.interval_seconds > 0 {
        Duration::from_secs(start.interval_seconds.unsigned_abs())
    } else {
        Duration::from_secs(3)
    };
    let expires_in = if start.expires_in_seconds > 0 {
        Duration::from_secs(start.expires_in_seconds.unsigned_abs())
    } else {
        Duration::from_secs(15 * 60)
    };
    let deadline = Instant::now() + expires_in;

    loop {
        let now = Instant::now();
        let tick_at = now + interval;
        if tick_at >= deadline {
            std::thread::sleep(deadline.saturating_duration_since(now));
            return Err("login expired before approval".into());
        }
        std::thread::sleep(interval);
        let poll = client.poll_auth(&start.device_code)?;
        match poll.status.as_str() {
            "pending" => {}
            "approved" => {
                if poll.access_token.is_empty() || poll.refresh_token.is_empty() {
                    return Err("server approved login without tokens".into());
                }
                return Ok(Tokens {
                    access_token: poll.access_token,
                    refresh_token: poll.refresh_token,
                });
            }
            status @ ("expired" | "denied" | "claimed" | "unknown") => {
                return Err(format!("login {status}").into());
            }
            other => return Err(format!("unexpected login status {}", quote(other)).into()),
        }
    }
}
