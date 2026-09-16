fn main() {
    let argv: Vec<String> = std::env::args_os().map(|a| a.to_string_lossy().into_owned()).collect();
    let argv0 = argv.first().cloned().unwrap_or_default();
    let args: Vec<String> = argv.into_iter().skip(1).collect();
    let code = cmuxd_remote::serve::main_entry(&argv0, &args);
    std::process::exit(code);
}
