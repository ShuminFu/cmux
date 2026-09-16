//! PTY allocation and child spawning.

use std::fs::File;
use std::io;
use std::os::fd::{AsFd, OwnedFd};
use std::os::unix::process::CommandExt;
use std::process::{Child, Command, Stdio};

use rustix::fs::{Mode, OFlags};
use rustix::pty::OpenptFlags;
use rustix::termios::Winsize;

use crate::util::io_error_text;

pub struct PtyPair {
    pub master: OwnedFd,
    pub slave: OwnedFd,
}

/// Allocates a PTY master/slave pair. The production implementation opens
/// `/dev/ptmx`; tests substitute one that simulates a hardened devpts.
pub type PtyOpener = dyn Fn() -> io::Result<PtyPair> + Send + Sync;

pub fn open_pty() -> io::Result<PtyPair> {
    #[cfg(target_os = "linux")]
    let flags = OpenptFlags::RDWR | OpenptFlags::NOCTTY | OpenptFlags::CLOEXEC;
    #[cfg(not(target_os = "linux"))]
    let flags = OpenptFlags::RDWR | OpenptFlags::NOCTTY;
    let master = rustix::pty::openpt(flags).map_err(|e| {
        io::Error::new(e.kind(), format!("open /dev/ptmx: {}", io_error_text(&e.into())))
    })?;
    #[cfg(not(target_os = "linux"))]
    rustix::io::fcntl_setfd(&master, rustix::io::FdFlags::CLOEXEC).map_err(io::Error::from)?;
    rustix::pty::grantpt(&master).map_err(io::Error::from)?;
    rustix::pty::unlockpt(&master).map_err(io::Error::from)?;
    let name = rustix::pty::ptsname(&master, Vec::new()).map_err(io::Error::from)?;
    let slave =
        rustix::fs::open(&*name, OFlags::RDWR | OFlags::NOCTTY | OFlags::CLOEXEC, Mode::empty())
            .map_err(|e| {
                io::Error::new(
                    e.kind(),
                    format!("open {}: {}", name.to_string_lossy(), io_error_text(&e.into())),
                )
            })?;
    Ok(PtyPair { master, slave })
}

pub fn set_winsize(fd: &impl AsFd, cols: usize, rows: usize) -> io::Result<()> {
    let ws = Winsize {
        ws_row: u16::try_from(rows).unwrap_or(u16::MAX),
        ws_col: u16::try_from(cols).unwrap_or(u16::MAX),
        ws_xpixel: 0,
        ws_ypixel: 0,
    };
    rustix::termios::tcsetwinsize(fd, ws).map_err(io::Error::from)
}

pub fn get_winsize(fd: &impl AsFd) -> io::Result<(usize, usize)> {
    let ws = rustix::termios::tcgetwinsize(fd).map_err(io::Error::from)?;
    Ok((usize::from(ws.ws_col), usize::from(ws.ws_row)))
}

/// Spawn `cmd` with the slave as its controlling terminal on stdin, stdout,
/// and stderr, in a new session.
#[allow(unsafe_code)]
pub fn spawn_with_controlling_tty(mut cmd: Command, slave: &OwnedFd) -> io::Result<Child> {
    let stdin = File::from(slave.try_clone()?);
    let stdout = File::from(slave.try_clone()?);
    let stderr = File::from(slave.try_clone()?);
    cmd.stdin(Stdio::from(stdin)).stdout(Stdio::from(stdout)).stderr(Stdio::from(stderr));
    // SAFETY: the pre-exec closure only performs async-signal-safe syscalls
    // (setsid and one ioctl on the already-installed stdin) and touches no
    // heap or locks between fork and exec.
    unsafe {
        cmd.pre_exec(|| {
            rustix::process::setsid()?;
            rustix::process::ioctl_tiocsctty(io::stdin())?;
            Ok(())
        });
    }
    cmd.spawn()
}

/// Spawn a detached child in its own session (the persistent daemon). If
/// `ready_fd` is given, the child sees that descriptor as fd 3.
#[allow(unsafe_code)]
pub fn spawn_detached(mut cmd: Command, ready_fd: Option<OwnedFd>) -> io::Result<Child> {
    use std::os::fd::AsRawFd;

    let ready_raw = ready_fd.as_ref().map(AsRawFd::as_raw_fd);
    // SAFETY: only setsid and dup2/fcntl on a descriptor owned by this call
    // run between fork and exec; no allocation or locking happens.
    unsafe {
        cmd.pre_exec(move || {
            rustix::process::setsid()?;
            if let Some(raw) = ready_raw {
                let target = 3;
                if raw == target {
                    if libc::fcntl(raw, libc::F_SETFD, 0) < 0 {
                        return Err(io::Error::last_os_error());
                    }
                } else if libc::dup2(raw, target) < 0 {
                    return Err(io::Error::last_os_error());
                }
            }
            Ok(())
        });
    }
    let child = cmd.spawn();
    drop(ready_fd);
    child
}
