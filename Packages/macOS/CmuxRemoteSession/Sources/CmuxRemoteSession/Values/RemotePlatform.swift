/// The remote host's platform tuple, kept in Go's `GOOS`/`GOARCH` spelling
/// (release asset names and remote install paths use it), as probed via
/// `uname`, lifted one-for-one from the legacy controller's nested type.
struct RemotePlatform {
    let goOS: String
    let goArch: String
}
