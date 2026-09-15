use std::io;

fn main() {
    let argv: Vec<String> = std::env::args().collect();
    let argv0 = argv.first().cloned().unwrap_or_default();
    let args: Vec<String> = argv.into_iter().skip(1).collect();
    if cmuxd_remote::daemon::should_run_cli_for_invocation(&argv0, &args) {
        let mut stdout = io::stdout();
        let mut stderr = io::stderr();
        let mut cli_io = cmuxd_remote::cli::CliIo {
            stdout: &mut stdout,
            stderr: &mut stderr,
        };
        std::process::exit(cmuxd_remote::cli::run_cli(&args, &mut cli_io));
    }
    let code = cmuxd_remote::daemon::run(
        &args,
        Box::new(io::stdin()),
        Box::new(io::stdout()),
        Box::new(io::stderr()),
    );
    std::process::exit(code);
}
