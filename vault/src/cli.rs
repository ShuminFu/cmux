//! Command dispatch for the `cmux-vault` binary. Exit codes, output streams,
//! and message text follow the original implementation: `2` for usage
//! errors, `1` for runtime failures, `0` on success.

use std::io::Write;

use serde::Serialize;

use crate::Printer;
use crate::agentdirs;
use crate::api::{self, Client};
use crate::authflow;
use crate::authstore;
use crate::environ::Environ;
use crate::flags::{FlagSet, ParseError};
use crate::resume;
use crate::state;
use crate::syncer;
use crate::util::{quote, rfc3339_local};

pub const VERSION: &str = match option_env!("CMUX_VAULT_VERSION") {
    Some(v) => v,
    None => env!("CARGO_PKG_VERSION"),
};

const USAGE: &str = "Usage: cmux-vault [--api-base URL] [--json] <command> [options]

Commands:
  login      Start device-code login
  logout     Delete local auth tokens
  scan       Discover local agent sessions
  sync       Upload changed sessions
  resume     Restore a missing session from cmux Vault and print the resume command
  status     Show auth and local sync state
  version    Print version
";

struct StdoutPrinter;

impl Printer for StdoutPrinter {
    fn print(&self, text: &str) {
        let mut out = std::io::stdout().lock();
        let _ = out.write_all(text.as_bytes());
        let _ = out.flush();
    }
}

struct StderrPrinter;

impl Printer for StderrPrinter {
    fn print(&self, text: &str) {
        let mut err = std::io::stderr().lock();
        let _ = err.write_all(text.as_bytes());
    }
}

fn out(text: &str) {
    StdoutPrinter.print(text);
}

fn err(text: &str) {
    StderrPrinter.print(text);
}

fn write_json<T: Serialize>(value: &T) -> i32 {
    let Ok(mut data) = serde_json::to_vec_pretty(value) else {
        return 1;
    };
    data.push(b'\n');
    let mut stdout = std::io::stdout().lock();
    if stdout.write_all(&data).is_err() || stdout.flush().is_err() {
        return 1;
    }
    0
}

/// Run the CLI with `args` (excluding the program name) and return the exit
/// code. Writes to the process's stdout and stderr.
pub fn run(args: &[String]) -> i32 {
    let code = run_inner(args);
    let _ = std::io::stdout().flush();
    code
}

fn run_inner(args: &[String]) -> i32 {
    let mut global = FlagSet::new("cmux-vault");
    global.string("api-base", &default_api_base(), "cmux web API base URL");
    global.bool("json", false, "write JSON output where supported");
    let remaining = match global.parse(args) {
        Ok(rest) => rest,
        Err(parse_err) => {
            err(&parse_err.render());
            return 2;
        }
    };
    if remaining.is_empty() {
        err(USAGE);
        return 2;
    }
    let api_base = global.get_string("api-base");
    let json_output = global.get_bool("json");

    let env = match Environ::real() {
        Ok(env) => env,
        Err(message) => {
            err(&format!("cmux-vault: {message}\n"));
            return 1;
        }
    };
    let cmd = remaining[0].as_str();
    let cmd_args = &remaining[1..];
    let code = dispatch(cmd, cmd_args, &env, &api_base, json_output);
    let _ = std::io::stdout().flush();
    for warning in env.take_warnings() {
        err(&format!("warning: {warning}\n"));
    }
    code
}

fn dispatch(
    cmd: &str,
    cmd_args: &[String],
    env: &Environ,
    api_base: &str,
    json_output: bool,
) -> i32 {
    match cmd {
        "version" => {
            if json_output {
                return write_json(&VersionOutput { version: VERSION });
            }
            out(&format!("{VERSION}\n"));
            0
        }
        "login" => {
            let client = Client::new(api_base, None);
            // In JSON mode stdout must stay machine-readable, so the interactive
            // approval URL/code prompt goes to stderr instead.
            let prompt: &dyn Printer = if json_output { &StderrPrinter } else { &StdoutPrinter };
            let tokens = match authflow::login(&client, prompt) {
                Ok(tokens) => tokens,
                Err(e) => {
                    err(&format!("login failed: {e}\n"));
                    return 1;
                }
            };
            if let Err(e) = authstore::save(&env.home_dir, &env.vars, &tokens) {
                err(&format!("saving tokens failed: {e}\n"));
                return 1;
            }
            if json_output {
                return write_json(&OkOutput { ok: true });
            }
            out("Logged in.\n");
            0
        }
        "logout" => {
            if let Err(e) = authstore::delete(&env.home_dir, &env.vars) {
                err(&format!("logout failed: {e}\n"));
                return 1;
            }
            if json_output {
                return write_json(&OkOutput { ok: true });
            }
            out("Logged out.\n");
            0
        }
        "scan" => run_scan(cmd_args, env, json_output),
        "sync" => run_sync(cmd_args, env, api_base, json_output),
        "resume" => run_resume(cmd_args, env, api_base, json_output),
        "status" => run_status(cmd_args, env, json_output),
        "help" | "-h" | "--help" => {
            out(USAGE);
            0
        }
        other => {
            err(&format!("cmux-vault: unknown command {}\n", quote(other)));
            err(USAGE);
            2
        }
    }
}

/// Parse subcommand flags; both `-h` and malformed flags print the flag
/// usage to stderr and exit with status 2, as Go's `flag` package does.
fn parse_or_exit(fs: &mut FlagSet, args: &[String]) -> Result<Vec<String>, i32> {
    fs.parse(args).map_err(|parse_err: ParseError| {
        err(&parse_err.render());
        2
    })
}

fn run_scan(args: &[String], env: &Environ, json_output: bool) -> i32 {
    let mut fs = FlagSet::new("scan");
    fs.string("agent", "", "agent to scan (claude, codex, pi)");
    fs.bool("json", json_output, "write JSON output");
    if let Err(code) = parse_or_exit(&mut fs, args) {
        return code;
    }
    let sessions = match agentdirs::discover_all(env, &fs.get_string("agent")) {
        Ok(sessions) => sessions,
        Err(e) => {
            err(&format!("scan failed: {e}\n"));
            return 1;
        }
    };
    if fs.get_bool("json") {
        return write_json(&ScanOutput { sessions: &sessions });
    }
    let mut stdout = std::io::stdout().lock();
    for session in &sessions {
        let line = format!(
            "{}\t{}\t{}\t{}\t{}\n",
            session.agent_name,
            session.agent_session_id,
            session.size_bytes,
            rfc3339_local(session.mod_time),
            session.abs_path
        );
        if stdout.write_all(line.as_bytes()).is_err() {
            return 1;
        }
    }
    let _ = stdout.flush();
    0
}

fn run_sync(args: &[String], env: &Environ, api_base: &str, json_output: bool) -> i32 {
    let mut fs = FlagSet::new("sync");
    fs.string("agent", "", "agent to sync (claude, codex, pi)");
    fs.bool("dry-run", false, "scan and diff without uploading");
    fs.int("limit", 0, "maximum changed sessions to upload");
    fs.bool("json", json_output, "write JSON output");
    if let Err(code) = parse_or_exit(&mut fs, args) {
        return code;
    }
    let dry_run = fs.get_bool("dry-run");
    let local_json = fs.get_bool("json");
    let tokens = match authstore::load(&env.home_dir, &env.vars) {
        Ok(tokens) => tokens,
        Err(e) => {
            err(&format!("loading auth failed: {e}\n"));
            return 1;
        }
    };
    if tokens.is_none() && !dry_run {
        err("not logged in; run cmux-vault login\n");
        return 1;
    }
    let mut store = match state::load(&env.home_dir, &env.vars) {
        Ok(store) => store,
        Err(e) => {
            err(&format!("loading state failed: {e}\n"));
            return 1;
        }
    };
    let client = Client::new(api_base, tokens);
    let mut engine = syncer::Engine {
        env,
        state: &mut store,
        client: &client,
        temp_dir: String::new(),
        out: if local_json { None } else { Some(&StdoutPrinter) },
    };
    let agent = fs.get_string("agent");
    let (summary, sync_err) =
        engine.sync(syncer::Options { agent: &agent, dry_run, limit: fs.get_int("limit") });
    if local_json {
        let code = write_json(&summary);
        if code != 0 {
            return code;
        }
    }
    if let Some(e) = sync_err {
        err(&format!("sync failed: {e}\n"));
        return 1;
    }
    if !local_json {
        out(&format!(
            "summary: uploaded={} skipped={} failed={} bytes={} compressedBytes={}\n",
            summary.uploaded,
            summary.skipped,
            summary.failed,
            summary.bytes_uploaded,
            summary.compressed_bytes_uploaded
        ));
    }
    0
}

fn run_resume(args: &[String], env: &Environ, api_base: &str, json_output: bool) -> i32 {
    let mut fs = FlagSet::new("resume");
    fs.string("agent", "", "agent to resume (claude, codex, pi)");
    fs.bool("force", false, "overwrite an existing local transcript");
    fs.bool("json", json_output, "write JSON output");
    let positional = match parse_or_exit(&mut fs, args) {
        Ok(rest) => rest,
        Err(code) => return code,
    };
    if positional.len() != 1 {
        err("resume requires a session id\n");
        return 2;
    }
    let local_json = fs.get_bool("json");
    let tokens = match authstore::load(&env.home_dir, &env.vars) {
        Ok(tokens) => tokens,
        Err(e) => {
            err(&format!("loading auth failed: {e}\n"));
            return 1;
        }
    };
    let Some(tokens) = tokens else {
        err("not logged in; run cmux-vault login\n");
        return 1;
    };
    let client = Client::new(api_base, Some(tokens));
    let restorer = resume::Restorer {
        env,
        client: &client,
        out: if local_json { None } else { Some(&StdoutPrinter) },
    };
    let agent = fs.get_string("agent");
    match restorer
        .resume(&positional[0], resume::Options { agent: &agent, force: fs.get_bool("force") })
    {
        Ok(hint) => {
            if local_json {
                return write_json(&HintOutput { hint: &hint });
            }
            0
        }
        Err(e) => {
            err(&format!("resume failed: {e}\n"));
            1
        }
    }
}

fn run_status(args: &[String], env: &Environ, json_output: bool) -> i32 {
    let mut fs = FlagSet::new("status");
    fs.bool("json", json_output, "write JSON output");
    if let Err(code) = parse_or_exit(&mut fs, args) {
        return code;
    }
    let tokens = match authstore::load(&env.home_dir, &env.vars) {
        Ok(tokens) => tokens,
        Err(e) => {
            err(&format!("loading auth failed: {e}\n"));
            return 1;
        }
    };
    let store = match state::load(&env.home_dir, &env.vars) {
        Ok(store) => store,
        Err(e) => {
            err(&format!("loading state failed: {e}\n"));
            return 1;
        }
    };
    let logged_in = tokens.is_some();
    let tracked = store.entries.len();
    if fs.get_bool("json") {
        return write_json(&StatusOutput { logged_in, tracked_files: tracked });
    }
    out(if logged_in { "Logged in.\n" } else { "Not logged in.\n" });
    out(&format!("Tracked files: {tracked}\n"));
    0
}

fn default_api_base() -> String {
    match std::env::var("CMUX_VAULT_API_BASE") {
        Ok(value) if !value.trim().is_empty() => value.trim().to_string(),
        _ => api::DEFAULT_BASE_URL.to_string(),
    }
}

#[derive(Serialize)]
struct VersionOutput {
    version: &'static str,
}

#[derive(Serialize)]
struct OkOutput {
    ok: bool,
}

#[derive(Serialize)]
struct HintOutput<'a> {
    hint: &'a str,
}

#[derive(Serialize)]
struct ScanOutput<'a> {
    sessions: &'a [agentdirs::Session],
}

#[derive(Serialize)]
struct StatusOutput {
    #[serde(rename = "loggedIn")]
    logged_in: bool,
    #[serde(rename = "trackedFiles")]
    tracked_files: usize,
}
