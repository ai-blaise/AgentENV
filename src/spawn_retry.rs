//! Spawning a program some other thread may be in the middle of writing.
//!
//! `exec` refuses, with `ETXTBSY`, to run a file that any process holds open
//! for writing. In a multithreaded process that is not a rare condition: if
//! one thread holds a write handle at the instant another thread forks, the
//! child inherits the handle until it execs, and every spawn of that file in
//! between is refused. The window is microseconds wide and nothing is wrong —
//! the writer closes, and the next attempt works.
//!
//! It matters here because the programs this server shells out to are ones it
//! installs itself, or ones a host package manager may be replacing
//! underneath it. Both make the race reachable on exactly the kind of busy
//! host this runs on, where an outright failure would surface as image
//! resolution or registry authentication breaking for no visible reason.

use std::time::Duration;

/// How many times a spawn is attempted before the error is the caller's.
pub(crate) const SPAWN_RETRY_ATTEMPTS: usize = 5;

/// Pause between attempts. One `fork`-to-`exec` gap wide.
pub(crate) const SPAWN_RETRY_DELAY: Duration = Duration::from_millis(20);

/// Whether a spawn failure is that race rather than a real problem.
pub(crate) fn is_text_file_busy(error: &std::io::Error) -> bool {
    error.raw_os_error() == Some(nix::libc::ETXTBSY)
}

/// Spawns a child, retrying a spawn that lost the race with a concurrent fork.
///
/// Only the spawn is retried. Once the child is running, its own failures are
/// the caller's to interpret — this cannot know whether re-running the program
/// is safe.
pub(crate) fn spawn_retrying_busy_text<T>(
    program: &str,
    mut spawn: impl FnMut() -> std::io::Result<T>,
) -> std::io::Result<T> {
    for attempt in 1..SPAWN_RETRY_ATTEMPTS {
        match spawn() {
            Err(error) if is_text_file_busy(&error) => {
                tracing::debug!(program, attempt, "program was busy being written; retrying");
                std::thread::sleep(SPAWN_RETRY_DELAY);
            }
            other => return other,
        }
    }
    spawn()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;

    fn busy() -> std::io::Error {
        std::io::Error::from_raw_os_error(nix::libc::ETXTBSY)
    }

    #[test]
    fn a_spawn_that_succeeds_is_not_retried() {
        let calls = Cell::new(0);
        let result = spawn_retrying_busy_text("prog", || {
            calls.set(calls.get() + 1);
            Ok(7)
        });
        assert_eq!(result.unwrap(), 7);
        assert_eq!(calls.get(), 1);
    }

    #[test]
    fn a_busy_spawn_is_retried_until_it_takes() {
        let calls = Cell::new(0);
        let result = spawn_retrying_busy_text("prog", || {
            calls.set(calls.get() + 1);
            if calls.get() < 3 {
                return Err(busy());
            }
            Ok(7)
        });
        assert_eq!(result.unwrap(), 7);
        assert_eq!(calls.get(), 3);
    }

    /// A file that stays busy is a real failure, and the caller has to see the
    /// original error rather than a retry-flavoured one.
    #[test]
    fn a_spawn_that_stays_busy_gives_up_with_the_real_error() {
        let calls = Cell::new(0);
        let result = spawn_retrying_busy_text("prog", || {
            calls.set(calls.get() + 1);
            Err::<(), _>(busy())
        });
        assert_eq!(result.unwrap_err().raw_os_error(), Some(nix::libc::ETXTBSY));
        assert_eq!(calls.get(), SPAWN_RETRY_ATTEMPTS);
    }

    /// Any other error is the program's own answer and must not be retried:
    /// a missing binary does not become present by waiting.
    #[test]
    fn an_unrelated_failure_is_returned_at_once() {
        let calls = Cell::new(0);
        let result = spawn_retrying_busy_text("prog", || {
            calls.set(calls.get() + 1);
            Err::<(), _>(std::io::Error::from_raw_os_error(nix::libc::ENOENT))
        });
        assert_eq!(result.unwrap_err().raw_os_error(), Some(nix::libc::ENOENT));
        assert_eq!(calls.get(), 1);
    }
}
