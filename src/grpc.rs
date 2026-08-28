use std::fs;

use anyhow::{bail, Context, Result};
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity};

use crate::cfg::ClusterConfig;

/// Build the single authenticated scheduler transport used by node reporting
/// and P2P discovery. Production is fail-closed: plaintext must be explicitly
/// enabled, while HTTPS requires a CA and client identity.
pub(crate) fn scheduler_channel(config: &ClusterConfig) -> Result<Channel> {
    let raw_endpoint = config
        .scheduler_endpoint
        .as_deref()
        .context("scheduler endpoint is not configured")?;
    let url = url::Url::parse(raw_endpoint)
        .with_context(|| format!("invalid scheduler endpoint: {raw_endpoint}"))?;
    let mut endpoint = Endpoint::from_shared(raw_endpoint.to_string())
        .with_context(|| format!("invalid scheduler endpoint: {raw_endpoint}"))?;

    let tls_paths = [
        config.scheduler_tls_ca_path.as_ref(),
        config.scheduler_tls_cert_path.as_ref(),
        config.scheduler_tls_key_path.as_ref(),
    ];
    let tls_count = tls_paths.iter().filter(|path| path.is_some()).count();
    if tls_count != 0 && tls_count != tls_paths.len() {
        bail!("scheduler TLS CA, certificate, and private key must be configured together");
    }

    match url.scheme() {
        "https" => {
            let (Some(ca_path), Some(cert_path), Some(key_path)) = (
                config.scheduler_tls_ca_path.as_ref(),
                config.scheduler_tls_cert_path.as_ref(),
                config.scheduler_tls_key_path.as_ref(),
            ) else {
                bail!("HTTPS scheduler endpoints require a CA and client certificate identity");
            };
            let ca = fs::read(ca_path)
                .with_context(|| format!("read scheduler TLS CA {}", ca_path.display()))?;
            let cert = fs::read(cert_path).with_context(|| {
                format!(
                    "read scheduler TLS client certificate {}",
                    cert_path.display()
                )
            })?;
            let key = fs::read(key_path).with_context(|| {
                format!(
                    "read scheduler TLS client private key {}",
                    key_path.display()
                )
            })?;
            let domain_name = config
                .scheduler_tls_domain_name
                .as_deref()
                .filter(|name| !name.is_empty())
                .or_else(|| url.host_str())
                .context("HTTPS scheduler endpoint has no TLS server name")?;
            endpoint = endpoint.tls_config(
                ClientTlsConfig::new()
                    .ca_certificate(Certificate::from_pem(ca))
                    .identity(Identity::from_pem(cert, key))
                    .domain_name(domain_name),
            )?;
        }
        "http" if config.scheduler_allow_insecure_transport && tls_count == 0 => {}
        "http" if tls_count != 0 => {
            bail!("scheduler TLS credentials cannot be used with a plaintext HTTP endpoint");
        }
        "http" => {
            bail!(
                "plaintext scheduler transport is disabled; configure HTTPS mTLS or set \
                 AENV_SCHEDULER_ALLOW_INSECURE_TRANSPORT=true only on a trusted test network"
            );
        }
        scheme => bail!("unsupported scheduler endpoint scheme {scheme:?}"),
    }

    Ok(endpoint.connect_lazy())
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    fn config(endpoint: &str, allow_insecure: bool) -> ClusterConfig {
        ClusterConfig {
            scheduler_endpoint: Some(endpoint.to_string()),
            scheduler_tls_ca_path: None,
            scheduler_tls_cert_path: None,
            scheduler_tls_key_path: None,
            scheduler_tls_domain_name: None,
            scheduler_allow_insecure_transport: allow_insecure,
            require_route_generation: false,
        }
    }

    #[tokio::test]
    async fn plaintext_transport_requires_an_explicit_escape_hatch() {
        assert!(scheduler_channel(&config("http://scheduler:9090", false)).is_err());
        assert!(scheduler_channel(&config("http://scheduler:9090", true)).is_ok());
    }

    #[test]
    fn tls_identity_is_all_or_nothing() {
        let mut config = config("https://scheduler:9090", false);
        config.scheduler_tls_ca_path = Some(PathBuf::from("ca.pem"));
        let error = scheduler_channel(&config).unwrap_err().to_string();
        assert!(error.contains("must be configured together"));
    }

    #[test]
    fn https_requires_mutual_tls_identity() {
        let error = scheduler_channel(&config("https://scheduler:9090", false))
            .unwrap_err()
            .to_string();
        assert!(error.contains("require a CA and client certificate"));
    }
}
