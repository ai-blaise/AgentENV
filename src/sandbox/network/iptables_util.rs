use std::fmt::Write as _;
use std::io::Write as IoWrite;
use std::process::{Command, Stdio};

use anyhow::{anyhow, Context, Result};
use tracing::{debug, warn};

pub(super) enum IptablesRestoreCommand {
    NewChain {
        table: &'static str,
        chain: &'static str,
    },
    FlushChain {
        table: &'static str,
        chain: &'static str,
    },
    Insert {
        table: &'static str,
        chain: &'static str,
        position: i32,
        rule: String,
    },
    Append {
        table: &'static str,
        chain: &'static str,
        rule: String,
    },
    Delete {
        table: &'static str,
        chain: &'static str,
        rule: String,
    },
}

impl IptablesRestoreCommand {
    fn table(&self) -> &'static str {
        match self {
            Self::NewChain { table, .. }
            | Self::FlushChain { table, .. }
            | Self::Insert { table, .. }
            | Self::Append { table, .. }
            | Self::Delete { table, .. } => table,
        }
    }
}

#[derive(Clone, Copy)]
pub(super) enum OpenFailurePolicy {
    ReturnErr,
    WarnAndIgnore(&'static str),
}

/// Builds an `iptables-restore` script from the given rules.
///
/// Rules **must** be grouped by table (all rules for one table appear consecutively).
/// A `debug_assert` fires in dev/test builds if this invariant is violated.
pub(super) fn build_restore_script(commands: &[IptablesRestoreCommand]) -> String {
    debug_assert!(
        commands.windows(2).all(|w| w[0].table() <= w[1].table()),
        "iptables restore commands must be grouped by table; got interleaved tables"
    );

    let mut script = String::new();
    let mut current_table: Option<&str> = None;

    for command in commands {
        let table = command.table();
        if current_table != Some(table) {
            if current_table.is_some() {
                script.push_str("COMMIT\n");
            }
            writeln!(&mut script, "*{table}").expect("writing to String cannot fail");
            current_table = Some(table);
        }
        match command {
            IptablesRestoreCommand::NewChain { chain, .. } => {
                writeln!(&mut script, "-N {chain}").expect("writing to String cannot fail");
            }
            IptablesRestoreCommand::FlushChain { chain, .. } => {
                writeln!(&mut script, "-F {chain}").expect("writing to String cannot fail");
            }
            IptablesRestoreCommand::Insert {
                chain,
                position,
                rule,
                ..
            } => {
                writeln!(&mut script, "-I {chain} {position} {rule}")
                    .expect("writing to String cannot fail");
            }
            IptablesRestoreCommand::Append { chain, rule, .. } => {
                writeln!(&mut script, "-A {chain} {rule}").expect("writing to String cannot fail");
            }
            IptablesRestoreCommand::Delete { chain, rule, .. } => {
                writeln!(&mut script, "-D {chain} {rule}").expect("writing to String cannot fail");
            }
        }
    }

    if current_table.is_some() {
        script.push_str("COMMIT\n");
    }

    script
}

/// How `iptables-restore` is told to wait for the xtables lock.
///
/// Only the legacy backend takes /run/xtables.lock at all — the nft backend
/// never opens it — and since 1.8 the legacy `iptables-restore` blocks for the
/// lock whether or not `--wait` is given. Passing it makes the wait bounded and
/// explicit rather than indefinite, which is what the pool refill loop needs:
/// slot setup runs one `iptables-restore` per namespace, and the loop abandons
/// its fill on any error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct IptablesWaitConfig {
    wait_secs: u64,
    wait_interval_usec: u64,
}

impl IptablesWaitConfig {
    fn from_global_config() -> Self {
        let config = &crate::cfg::ConfigManager::global_config().network.iptables;
        Self {
            wait_secs: config.wait_secs,
            wait_interval_usec: config.wait_interval_usec,
        }
    }
}

fn configured_wait() -> IptablesWaitConfig {
    static WAIT: std::sync::OnceLock<IptablesWaitConfig> = std::sync::OnceLock::new();
    *WAIT.get_or_init(IptablesWaitConfig::from_global_config)
}

/// The full argument vector for one `iptables-restore` invocation.
///
/// The lock options are `=`-bound deliberately. `-w`'s argument is
/// `optional_argument` in iptables' own getopt table, so `--wait 5` binds
/// nothing and leaves the `5` to be read as an input filename; the same is true
/// of `-w 5`. Only the `=` form passes a value.
fn restore_argv(wait: Option<IptablesWaitConfig>) -> Vec<String> {
    let mut argv = vec!["--noflush".to_string()];
    let Some(wait) = wait.filter(|wait| wait.wait_secs > 0) else {
        return argv;
    };
    argv.push(format!("--wait={}", wait.wait_secs));
    if wait.wait_interval_usec > 0 {
        argv.push(format!("--wait-interval={}", wait.wait_interval_usec));
    }
    argv
}

/// Which iptables implementation the host's binaries front.
///
/// It decides whether the lock options mean anything: `xtables-nft-restore`
/// accepts `--wait`/`--wait-interval` and discards them, because an nft restore
/// is one kernel netlink transaction and never opens /run/xtables.lock. On that
/// backend a lock-contention warning describes a lock nothing takes, so the
/// backend belongs in the record beside any lock measurement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum IptablesBackend {
    Legacy,
    NfTables,
    Unknown,
}

impl IptablesBackend {
    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::Legacy => "legacy",
            Self::NfTables => "nf_tables",
            Self::Unknown => "unknown",
        }
    }
}

/// Reads the backend out of `iptables --version` output.
///
/// The tag is parenthesised at the end of the single version line, for example
/// `iptables v1.8.10 (nf_tables)`. Binaries older than the nft split print no
/// tag at all and are legacy by construction.
fn parse_iptables_backend(version_output: &str) -> IptablesBackend {
    let line = version_output.lines().next().unwrap_or_default();
    if line.contains("(nf_tables)") {
        IptablesBackend::NfTables
    } else if line.contains("(legacy)") {
        IptablesBackend::Legacy
    } else {
        IptablesBackend::Unknown
    }
}

/// The host's iptables backend, asked once.
pub(super) fn iptables_backend() -> IptablesBackend {
    static BACKEND: std::sync::OnceLock<IptablesBackend> = std::sync::OnceLock::new();
    *BACKEND.get_or_init(|| {
        let Ok(output) = Command::new("iptables").arg("--version").output() else {
            return IptablesBackend::Unknown;
        };
        if !output.status.success() {
            return IptablesBackend::Unknown;
        }
        parse_iptables_backend(&String::from_utf8_lossy(&output.stdout))
    })
}

/// The invocation the `--wait` capability probe runs.
///
/// `--noflush` over an empty table body parses and commits nothing, so the
/// probe is safe to take at any point. It carries the same lock options the
/// real calls use, so a binary that understands `--wait` but not
/// `--wait-interval` reports unsupported rather than failing every later call.
fn wait_probe_command() -> Command {
    let mut command = Command::new("iptables-restore");
    command
        .args(restore_argv(Some(configured_wait())))
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    command
}

/// Whether the installed `iptables-restore` accepts `--wait`.
///
/// Asked by running it, not by reading `--help`. The nft backend — the default
/// on the shipped runtime image — accepts `--wait` and does not advertise it,
/// so the help text answers "no" for a binary that means "yes", and every node
/// logged a lock warning for a lock its backend never takes.
///
/// The probe exists so an unexpectedly old binary degrades to the previous
/// behavior instead of failing every call.
fn iptables_restore_supports_wait() -> bool {
    static SUPPORTS_WAIT: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *SUPPORTS_WAIT.get_or_init(|| wait_supported(probe_iptables_restore_wait()))
}

/// Turns the probe's answer into the decision, and is the whole decision.
///
/// Split out so a test can pin it without running iptables. The answer is the
/// probe and nothing else: the help text cannot be consulted here, which is
/// the defect this replaced -- `iptables-restore --help` on the nf_tables
/// backend prints no `--wait` at all, so reading it reported "unsupported" on
/// every modern host while the flag in fact works.
fn wait_supported(probe: bool) -> bool {
    if !probe {
        debug!(
            "iptables-restore did not accept --wait; falling back to the backend's own lock handling"
        );
    }
    probe
}

fn probe_iptables_restore_wait() -> bool {
    let Ok(mut child) = wait_probe_command().spawn() else {
        return false;
    };
    if let Some(stdin) = child.stdin.as_mut() {
        if stdin.write_all(b"*filter\nCOMMIT\n").is_err() {
            return false;
        }
    }
    child
        .wait_with_output()
        .is_ok_and(|output| output.status.success())
}

fn apply_restore_script(script: &str) -> Result<()> {
    crate::privileges::run_with_scoped_capabilities(&[crate::privileges::CAP_NET_ADMIN], || {
        let mut command = Command::new("iptables-restore");
        command.args(restore_argv(
            iptables_restore_supports_wait().then(configured_wait),
        ));
        let mut child = command
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .context("Failed to spawn iptables-restore")?;

        {
            let stdin = child
                .stdin
                .as_mut()
                .context("Failed to open stdin for iptables-restore")?;
            stdin
                .write_all(script.as_bytes())
                .context("Failed to write iptables-restore script")?;
        }

        let output = child
            .wait_with_output()
            .context("Failed to wait for iptables-restore")?;
        if output.status.success() {
            return Ok(());
        }

        let stderr = String::from_utf8_lossy(&output.stderr);
        // Distinguish lock contention from a malformed ruleset: the first is a
        // capacity signal that scales with concurrency, the second is a bug.
        let contended =
            stderr.contains("xtables lock") || stderr.contains("another app is currently holding");
        if contended {
            metrics::counter!("agentenv_network_iptables_lock_contention_total").increment(1);
        }
        Err(anyhow!("iptables-restore failed: {}", stderr.trim()))
    })
}

fn handle_restore_failure(err: anyhow::Error, policy: OpenFailurePolicy) -> Result<()> {
    match policy {
        OpenFailurePolicy::ReturnErr => Err(err),
        OpenFailurePolicy::WarnAndIgnore(message) => {
            warn!(error = %err, "{message}");
            Ok(())
        }
    }
}

/// Modify iptables rules through `iptables-restore`.
///
/// Normal rule sets are applied in one atomic batch. Cleanup consists only of
/// delete commands, which are tried as one batch first and re-tried one rule at
/// a time only if that batch fails: a rule that is already absent aborts the
/// whole script, and per-rule retry is what keeps teardown idempotent without
/// parsing rule strings back into argv.
pub(super) fn apply_iptables_commands(
    commands: &[IptablesRestoreCommand],
    open_failure_policy: OpenFailurePolicy,
) -> Result<()> {
    apply_iptables_commands_with(commands, open_failure_policy, &mut apply_restore_script)
}

/// The body of [`apply_iptables_commands`], with the executor passed in.
///
/// The seam exists so the batching decisions can be observed without a host
/// firewall; production supplies [`apply_restore_script`] and nothing else.
fn apply_iptables_commands_with(
    commands: &[IptablesRestoreCommand],
    open_failure_policy: OpenFailurePolicy,
    run_script: &mut dyn FnMut(&str) -> Result<()>,
) -> Result<()> {
    if commands.is_empty() {
        return Ok(());
    }

    if commands
        .iter()
        .all(|command| matches!(command, IptablesRestoreCommand::Delete { .. }))
    {
        let batched = build_restore_script(commands);
        let Err(batch_error) = run_script(&batched) else {
            return Ok(());
        };
        debug!(
            error = %batch_error,
            rule_count = commands.len(),
            "batched iptables delete failed; retrying one rule at a time"
        );

        for command in commands {
            let script = build_restore_script(std::slice::from_ref(command));
            if let Err(err) = run_script(&script) {
                if is_missing_iptables_rule_error(&err.to_string()) {
                    continue;
                }
                match open_failure_policy {
                    OpenFailurePolicy::ReturnErr => return Err(err),
                    OpenFailurePolicy::WarnAndIgnore(message) => {
                        // Best-effort teardown must continue so one stale or
                        // externally removed rule does not leak later rules.
                        warn!(error = %err, "{message}");
                    }
                }
            }
        }
        return Ok(());
    }

    let script = build_restore_script(commands);
    run_script(&script).or_else(|err| handle_restore_failure(err, open_failure_policy))
}

/// Groups commands by table while preserving each table's own order.
///
/// [`build_restore_script`] requires table-grouped input, and merging two rule
/// sets that each span `filter` and `nat` interleaves them. The sort is stable,
/// so within a table the rules keep the exact order — and therefore the exact
/// chain positions — they had when the two sets were applied back to back.
pub(super) fn group_commands_by_table(
    mut commands: Vec<IptablesRestoreCommand>,
) -> Vec<IptablesRestoreCommand> {
    commands.sort_by_key(|command| command.table());
    commands
}

fn is_missing_iptables_rule_error(error: &str) -> bool {
    let error = error.trim();
    let contains_ci = |needle: &str| {
        error
            .as_bytes()
            .windows(needle.len())
            .any(|w| w.eq_ignore_ascii_case(needle.as_bytes()))
    };
    contains_ci("bad rule")
        || contains_ci("does a matching rule exist")
        || contains_ci("no chain/target/match by that name")
        || contains_ci("rule does not exist")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn delete(chain: &'static str, rule: &str) -> IptablesRestoreCommand {
        IptablesRestoreCommand::Delete {
            table: "filter",
            chain,
            rule: rule.to_string(),
        }
    }

    /// Records every script it is handed and answers from a scripted list of
    /// results, so the batching decisions are observable without a firewall.
    struct ScriptedRunner {
        scripts: Vec<String>,
        results: std::collections::VecDeque<Result<()>>,
    }

    impl ScriptedRunner {
        fn new(results: Vec<Result<()>>) -> Self {
            Self {
                scripts: Vec::new(),
                results: results.into(),
            }
        }

        fn run(&mut self, script: &str) -> Result<()> {
            self.scripts.push(script.to_string());
            self.results.pop_front().unwrap_or(Ok(()))
        }
    }

    /// `-w`'s argument is `optional_argument`, so a space-separated value is
    /// never bound to it: `--wait 5` waits for the getopt default and then
    /// tries to open `5` as an input file. Only the `=` form passes a value,
    /// and `--wait-interval` is the half that keeps a busy lock from costing a
    /// full second per retry.
    #[test]
    fn the_lock_options_are_equals_bound_and_carry_the_interval() {
        assert_eq!(
            restore_argv(Some(IptablesWaitConfig {
                wait_secs: 5,
                wait_interval_usec: 20_000,
            })),
            vec![
                "--noflush".to_string(),
                "--wait=5".to_string(),
                "--wait-interval=20000".to_string(),
            ]
        );
    }

    /// The documented off switch: `wait_secs = 0` passes neither option and
    /// leaves the binary's own lock behavior in place.
    #[test]
    fn a_zero_wait_passes_no_lock_options_at_all() {
        assert_eq!(
            restore_argv(Some(IptablesWaitConfig {
                wait_secs: 0,
                wait_interval_usec: 20_000,
            })),
            vec!["--noflush".to_string()]
        );
        assert_eq!(restore_argv(None), vec!["--noflush".to_string()]);
    }

    /// The tag decides whether a lock wait means anything at all, so it has to
    /// be read from the version line rather than assumed.
    #[test]
    fn the_backend_is_read_from_the_version_tag() {
        assert_eq!(
            parse_iptables_backend("iptables v1.8.10 (nf_tables)\n"),
            IptablesBackend::NfTables
        );
        assert_eq!(
            parse_iptables_backend("iptables v1.8.7 (legacy)\n"),
            IptablesBackend::Legacy
        );
        assert_eq!(
            parse_iptables_backend("iptables v1.4.21\n"),
            IptablesBackend::Unknown
        );
    }

    /// Deleting N rules used to cost N forks and N lock acquisitions. The
    /// batch is one script, and one invocation, when the rules are all there.
    #[test]
    fn a_delete_set_that_applies_cleanly_costs_one_invocation() {
        let mut runner = ScriptedRunner::new(vec![Ok(())]);
        apply_iptables_commands_with(
            &[
                delete("INPUT", "-i veth-+ -j REJECT"),
                delete("FORWARD", "-i veth-+ -j ACCEPT"),
            ],
            OpenFailurePolicy::ReturnErr,
            &mut |script| runner.run(script),
        )
        .expect("a clean delete batch should succeed");

        assert_eq!(
            runner.scripts.len(),
            1,
            "expected one batched invocation, saw {:?}",
            runner.scripts
        );
        assert_eq!(
            runner.scripts[0],
            "*filter\n-D INPUT -i veth-+ -j REJECT\n-D FORWARD -i veth-+ -j ACCEPT\nCOMMIT\n"
        );
    }

    /// A rule that is already gone aborts the whole script, which is why the
    /// per-rule retry exists: the batch is the fast path, not the only path,
    /// and an absent rule still has to stay idempotent.
    #[test]
    fn a_failed_delete_batch_retries_rule_by_rule_and_tolerates_absent_rules() {
        let mut runner = ScriptedRunner::new(vec![
            Err(anyhow!(
                "iptables-restore failed: Bad rule (does a matching rule exist in that chain?)"
            )),
            Ok(()),
            Err(anyhow!(
                "iptables-restore failed: Bad rule (does a matching rule exist in that chain?)"
            )),
        ]);
        apply_iptables_commands_with(
            &[
                delete("INPUT", "-i veth-+ -j REJECT"),
                delete("FORWARD", "-i veth-+ -j ACCEPT"),
            ],
            OpenFailurePolicy::ReturnErr,
            &mut |script| runner.run(script),
        )
        .expect("an absent rule must stay idempotent");

        assert_eq!(
            runner.scripts.len(),
            3,
            "expected one batch then one script per rule, saw {:?}",
            runner.scripts
        );
        assert!(runner.scripts[1].contains("-D INPUT"));
        assert!(runner.scripts[2].contains("-D FORWARD"));
    }

    /// Merging two rule sets that each span `filter` and `nat` interleaves the
    /// tables, which `build_restore_script` cannot render. Grouping must not
    /// disturb the order within a table: these rules are inserted and appended
    /// at positions that depend on it.
    #[test]
    fn grouping_by_table_keeps_each_table_in_its_original_order() {
        let grouped = group_commands_by_table(vec![
            IptablesRestoreCommand::Append {
                table: "filter",
                chain: "FORWARD",
                rule: "first".to_string(),
            },
            IptablesRestoreCommand::Append {
                table: "nat",
                chain: "POSTROUTING",
                rule: "second".to_string(),
            },
            IptablesRestoreCommand::Append {
                table: "filter",
                chain: "FORWARD",
                rule: "third".to_string(),
            },
        ]);

        assert_eq!(
            build_restore_script(&grouped),
            "*filter\n-A FORWARD first\n-A FORWARD third\nCOMMIT\n*nat\n-A POSTROUTING second\nCOMMIT\n"
        );
    }

    /// The probe has to ask `iptables-restore` to wait, not ask it what it can
    /// do. `iptables-restore v1.8.10 (nf_tables)` — the binary on the shipped
    /// runtime image — accepts `--wait` and omits it from `--help`, so a
    /// help-text probe answers "unsupported" on every node and the flag is
    /// never passed.
    /// The decision must be the probe's answer, never the help text. On the
    /// nf_tables backend `iptables-restore --help` prints no `--wait` even
    /// though the flag works, so a help-reading implementation reports
    /// "unsupported" on every modern host and silently drops the lock wait.
    #[test]
    fn wait_support_follows_the_probe_and_never_the_help_text() {
        assert!(
            wait_supported(true),
            "a probe that succeeded must be believed"
        );
        assert!(
            !wait_supported(false),
            "a probe that failed must be believed"
        );
    }

    #[test]
    fn build_restore_script_renders_commands_in_order() {
        let script = build_restore_script(&[
            IptablesRestoreCommand::NewChain {
                table: "filter",
                chain: "AGENTENV-EGRESS",
            },
            IptablesRestoreCommand::NewChain {
                table: "filter",
                chain: "AGENTENV-USER-EGRESS",
            },
            IptablesRestoreCommand::Insert {
                table: "filter",
                chain: "FORWARD",
                position: 1,
                rule: "-i tap0 -o vpeer -j AGENTENV-EGRESS".to_string(),
            },
            IptablesRestoreCommand::FlushChain {
                table: "filter",
                chain: "AGENTENV-EGRESS",
            },
            IptablesRestoreCommand::Append {
                table: "filter",
                chain: "AGENTENV-EGRESS",
                rule: "-i tap0 -o vpeer -j AGENTENV-USER-EGRESS".to_string(),
            },
        ]);

        assert_eq!(
            script,
            "\
*filter
-N AGENTENV-EGRESS
-N AGENTENV-USER-EGRESS
-I FORWARD 1 -i tap0 -o vpeer -j AGENTENV-EGRESS
-F AGENTENV-EGRESS
-A AGENTENV-EGRESS -i tap0 -o vpeer -j AGENTENV-USER-EGRESS
COMMIT
"
        );
    }

    #[test]
    fn build_restore_script_groups_commands_by_table() {
        let script = build_restore_script(&[
            IptablesRestoreCommand::Append {
                table: "filter",
                chain: "FORWARD",
                rule: "-i tap0 -j ACCEPT".to_string(),
            },
            IptablesRestoreCommand::Delete {
                table: "nat",
                chain: "POSTROUTING",
                rule: "-o vpeer -j MASQUERADE".to_string(),
            },
        ]);

        assert_eq!(
            script,
            "\
*filter
-A FORWARD -i tap0 -j ACCEPT
COMMIT
*nat
-D POSTROUTING -o vpeer -j MASQUERADE
COMMIT
"
        );
    }

    #[test]
    fn build_restore_script_preserves_quoted_rule_arguments() {
        let script = build_restore_script(&[IptablesRestoreCommand::Append {
            table: "filter",
            chain: "FORWARD",
            rule: r#"-m comment --comment "sandbox traffic" -j ACCEPT"#.to_string(),
        }]);

        assert_eq!(
            script,
            "*filter\n-A FORWARD -m comment --comment \"sandbox traffic\" -j ACCEPT\nCOMMIT\n"
        );
    }
}
