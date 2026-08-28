use super::{ApiImpl, Claims};
use crate::{api::proxy, cfg::ConfigManager, types::SandboxId};
use agentenv_http_server::apis;
use async_trait::async_trait;
use axum::{
    body::Body,
    extract::{Request, State},
    http::{header::HeaderMap, HeaderValue, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
};

pub(crate) const API_KEY_HEADER: &str = "x-api-key";
pub(crate) const TRAFFIC_ACCESS_TOKEN_HEADER: &str = "e2b-traffic-access-token";
pub(crate) const ENVD_ACCESS_TOKEN_HEADER: &str = "x-access-token";

fn single_header<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a HeaderValue> {
    let mut values = headers.get_all(name).iter();
    let value = values.next()?;
    values.next().is_none().then_some(value)
}

fn route_generation(headers: &HeaderMap) -> Result<Option<u64>, StatusCode> {
    let values = headers
        .get_all(proxy::ROUTE_GENERATION_HEADER)
        .iter()
        .collect::<Vec<_>>();
    match values.as_slice() {
        [] => Ok(None),
        [value] => value
            .to_str()
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .filter(|generation| *generation > 0)
            .map(Some)
            .ok_or(StatusCode::BAD_REQUEST),
        _ => Err(StatusCode::BAD_REQUEST),
    }
}

fn generation_is_authorized(
    headers: &HeaderMap,
    expected: u64,
    required: bool,
) -> Result<bool, StatusCode> {
    match route_generation(headers)? {
        Some(actual) => Ok(actual == expected),
        None => Ok(!required),
    }
}

fn control_path_sandbox_id(path: &str) -> Option<SandboxId> {
    let mut segments = path.trim_start_matches('/').split('/');
    if segments.next()? != "sandboxes" {
        return None;
    }
    SandboxId::parse_str(segments.next()?).ok()
}

impl ApiImpl {
    pub(crate) fn has_valid_api_key(&self, headers: &HeaderMap) -> bool {
        single_header(headers, API_KEY_HEADER)
            .is_some_and(|value| self.api_key.matches(value.as_bytes()))
    }

    pub(crate) fn traffic_access_token(&self, sandbox_id: SandboxId) -> String {
        self.orchestrator.traffic_access_token(sandbox_id)
    }

    fn has_valid_traffic_access_token(&self, headers: &HeaderMap, sandbox_id: SandboxId) -> bool {
        single_header(headers, TRAFFIC_ACCESS_TOKEN_HEADER)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|candidate| {
                self.orchestrator
                    .validate_traffic_access_token(sandbox_id, candidate)
            })
    }
}

pub(crate) async fn require_auth<I>(
    State(api_impl): State<I>,
    mut request: Request,
    next: Next,
) -> Response<Body>
where
    I: AsRef<ApiImpl> + Clone + Send + Sync + 'static,
{
    let proxy_request =
        proxy::is_sandbox_proxy_request(&request, api_impl.as_ref().sandbox_proxy_domains());
    if matches!(request.uri().path(), "/health" | "/metrics") && !proxy_request {
        return next.run(request).await;
    }

    let api_impl = api_impl.as_ref();
    if !proxy_request {
        if !api_impl.has_valid_api_key(request.headers()) {
            return StatusCode::UNAUTHORIZED.into_response();
        }
        if let Some(sandbox_id) = control_path_sandbox_id(request.uri().path()) {
            let metadata = match api_impl.orchestrator().get_sandbox(&sandbox_id).await {
                Ok(Some(metadata)) => metadata,
                // Let the generated handler retain its endpoint-specific 404.
                Ok(None) => return next.run(request).await,
                Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
            };
            let required = ConfigManager::global_config()
                .cluster
                .require_route_generation;
            match generation_is_authorized(request.headers(), metadata.route_generation, required) {
                Ok(true) => {}
                Ok(false) => return StatusCode::CONFLICT.into_response(),
                Err(status) => return status.into_response(),
            }
        }
        return next.run(request).await;
    }

    let has_api_key = api_impl.has_valid_api_key(request.headers());

    let Some((sandbox_id, target_port)) =
        proxy::route_for_auth(&request, api_impl.sandbox_proxy_domains())
    else {
        request.headers_mut().remove(ENVD_ACCESS_TOKEN_HEADER);
        return if proxy::has_proxy_prefix(request.uri().path()) || has_api_key {
            next.run(request).await
        } else {
            StatusCode::UNAUTHORIZED.into_response()
        };
    };
    if has_api_key {
        request.headers_mut().remove(API_KEY_HEADER);
    }
    let metadata = match api_impl.orchestrator().get_sandbox(&sandbox_id).await {
        Ok(Some(metadata)) => metadata,
        Ok(None) => {
            request.headers_mut().remove(ENVD_ACCESS_TOKEN_HEADER);
            return proxy::sandbox_not_found_response(sandbox_id);
        }
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };
    let required = ConfigManager::global_config()
        .cluster
        .require_route_generation;
    match generation_is_authorized(request.headers(), metadata.route_generation, required) {
        Ok(true) => {}
        Ok(false) => return StatusCode::CONFLICT.into_response(),
        Err(status) => return status.into_response(),
    }

    let envd_request = target_port == proxy::effective_envd_port(&metadata);
    let envd_authorized = envd_request
        && metadata.secure
        && single_header(request.headers(), ENVD_ACCESS_TOKEN_HEADER)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|candidate| {
                api_impl
                    .orchestrator()
                    .validate_envd_access_token(sandbox_id, candidate)
            });
    let authorized = if envd_request {
        !metadata.secure || envd_authorized
    } else {
        metadata.network_policy.allow_public_traffic
            || api_impl.has_valid_traffic_access_token(request.headers(), sandbox_id)
    };

    if !authorized {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    if !envd_authorized {
        request.headers_mut().remove(ENVD_ACCESS_TOKEN_HEADER);
    }

    next.run(request).await
}

#[async_trait]
impl apis::ApiKeyAuthHeader for ApiImpl {
    type Claims = Claims;

    async fn extract_claims_from_header(
        &self,
        headers: &HeaderMap,
        _key: &str,
    ) -> Option<Self::Claims> {
        self.has_valid_api_key(headers).then_some(Claims)
    }
}

#[async_trait]
impl apis::ApiAuthBasic for ApiImpl {
    type Claims = Claims;

    async fn extract_claims_from_auth_header(
        &self,
        _kind: apis::BasicAuthKind,
        headers: &HeaderMap,
        _key: &str,
    ) -> Option<Self::Claims> {
        // The outer middleware is authoritative. This adapter keeps the
        // E2B-compatible generated router from rejecting its API-key request.
        self.has_valid_api_key(headers).then_some(Claims)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn route_generation_is_single_nonzero_u64() {
        let mut headers = HeaderMap::new();
        assert_eq!(route_generation(&headers).unwrap(), None);
        headers.insert(
            proxy::ROUTE_GENERATION_HEADER,
            HeaderValue::from_static("7"),
        );
        assert_eq!(route_generation(&headers).unwrap(), Some(7));

        headers.insert(
            proxy::ROUTE_GENERATION_HEADER,
            HeaderValue::from_static("0"),
        );
        assert_eq!(route_generation(&headers), Err(StatusCode::BAD_REQUEST));
        headers.append(
            proxy::ROUTE_GENERATION_HEADER,
            HeaderValue::from_static("8"),
        );
        assert_eq!(route_generation(&headers), Err(StatusCode::BAD_REQUEST));
    }

    #[test]
    fn stale_generations_are_rejected_even_before_enforcement_is_required() {
        let mut headers = HeaderMap::new();
        assert!(generation_is_authorized(&headers, 7, false).unwrap());
        assert!(!generation_is_authorized(&headers, 7, true).unwrap());

        headers.insert(
            proxy::ROUTE_GENERATION_HEADER,
            HeaderValue::from_static("6"),
        );
        assert!(!generation_is_authorized(&headers, 7, false).unwrap());
        headers.insert(
            proxy::ROUTE_GENERATION_HEADER,
            HeaderValue::from_static("7"),
        );
        assert!(generation_is_authorized(&headers, 7, true).unwrap());
    }

    #[test]
    fn only_existing_sandbox_control_paths_have_a_fence_subject() {
        let sandbox_id = SandboxId::new();
        assert_eq!(
            control_path_sandbox_id(&format!("/sandboxes/{sandbox_id}/pause")),
            Some(sandbox_id)
        );
        assert_eq!(control_path_sandbox_id("/sandboxes"), None);
        assert_eq!(control_path_sandbox_id("/templates/id"), None);
        assert_eq!(control_path_sandbox_id("/sandboxes/not-a-uuid"), None);
    }
}
