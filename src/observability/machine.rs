use std::fs;
use std::path::PathBuf;

use super::MachineInfo;

/// Detects mostly-static machine descriptors for inclusion in node snapshots.
///
/// This is evaluated once during observability service construction rather than
/// on every API request.
pub(crate) fn detect_machine_info(backend: crate::sandbox::SandboxBackendKind) -> MachineInfo {
    let cpuinfo = fs::read_to_string("/proc/cpuinfo").unwrap_or_default();

    MachineInfo {
        cpu_family: first_cpuinfo_value(&cpuinfo, &["cpu family", "CPU architecture"])
            .unwrap_or_else(|| "unknown".to_string()),
        cpu_model: first_cpuinfo_value(&cpuinfo, &["model", "CPU part"])
            .unwrap_or_else(|| "unknown".to_string()),
        cpu_model_name: first_cpuinfo_value(&cpuinfo, &["model name", "Hardware"])
            .unwrap_or_else(|| "unknown".to_string()),
        cpu_architecture: std::env::consts::ARCH.to_string(),
        cpu_config_json: None,
        sandbox_backend: backend.to_string(),
    }
}

/// Runs `cpu-template-helper template dump` and returns the JSON output.
/// Returns `None` if the binary is missing, not executable, or fails.
pub(super) async fn dump_cpu_config(path: PathBuf) -> Option<String> {
    tokio::task::spawn_blocking(move || {
        let output = std::process::Command::new(&path)
            .args(["template", "dump", "-o", "/dev/stdout"])
            .output()
            .ok()?;
        if !output.status.success() {
            return None;
        }
        String::from_utf8(output.stdout)
            .ok()
            .filter(|s| !s.is_empty())
    })
    .await
    .ok()
    .flatten()
}

pub(crate) fn first_cpuinfo_value(cpuinfo: &str, keys: &[&str]) -> Option<String> {
    cpuinfo.lines().find_map(|line| {
        let (key, value) = line.split_once(':')?;
        keys.iter()
            .any(|candidate| key.trim() == *candidate)
            .then(|| value.trim().to_string())
            .filter(|value| !value.is_empty())
    })
}

#[cfg(test)]
mod tests {
    use super::first_cpuinfo_value;

    #[test]
    fn cpuinfo_parser_returns_first_matching_key() {
        let cpuinfo = "model name\t: Intel(R)\ncpu family\t: 6\n";
        assert_eq!(
            first_cpuinfo_value(cpuinfo, &["cpu family"]).as_deref(),
            Some("6")
        );
        assert_eq!(
            first_cpuinfo_value(cpuinfo, &["model name"]).as_deref(),
            Some("Intel(R)")
        );
    }
}
