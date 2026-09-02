use std::path::PathBuf;

use anyhow::{anyhow, Context, Result};
use http_body_util::{BodyExt, Full};
use hyper::body::Bytes;
use hyper::Uri;
use hyper_util::client::legacy::Client as HyperClient;
use hyper_util::rt::TokioExecutor;
use serde::de::DeserializeOwned;
use serde::Serialize;

use super::connector::UnixConnector;

#[derive(Clone)]
pub(super) struct UnixSocketClient {
    client: HyperClient<UnixConnector, Full<Bytes>>,
}

impl UnixSocketClient {
    pub fn new(socket_path: PathBuf) -> Self {
        let connector = UnixConnector { path: socket_path };
        let client = HyperClient::builder(TokioExecutor::new()).build(connector);
        Self { client }
    }

    pub async fn request<B, R>(
        &self,
        method: hyper::Method,
        path: &str,
        body: Option<&B>,
    ) -> Result<R>
    where
        B: Serialize + ?Sized,
        R: DeserializeOwned,
    {
        let uri: Uri = format!("http://localhost{}", path)
            .parse()
            .context("Failed to parse URI")?;

        let body_bytes = if let Some(b) = body {
            serde_json::to_vec(b).context("Failed to serialize body")?
        } else {
            vec![]
        };

        let req = hyper::Request::builder()
            .method(method)
            .uri(uri)
            .header(hyper::header::CONTENT_TYPE, "application/json")
            .header(hyper::header::ACCEPT, "application/json")
            .body(Full::new(Bytes::from(body_bytes)))
            .context("Failed to build request")?;

        let res = self.client.request(req).await.context("Request failed")?;
        let status = res.status();

        if !status.is_success() {
            let bytes = res
                .collect()
                .await
                .context("Failed to read error body")?
                .to_bytes();
            let error_msg = String::from_utf8_lossy(&bytes);
            return Err(anyhow!("Request failed: {} - {}", status, error_msg));
        }

        let bytes = res
            .collect()
            .await
            .context("Failed to read response body")?
            .to_bytes();

        // Handle empty response
        if bytes.is_empty() {
            return serde_json::from_str("null")
                .context("Failed to deserialize null for empty response");
        }

        let response_body: R =
            serde_json::from_slice(&bytes).context("Failed to deserialize response body")?;
        Ok(response_body)
    }

    // Helper for requests without response body
    pub async fn request_no_content<B>(
        &self,
        method: hyper::Method,
        path: &str,
        body: Option<&B>,
    ) -> Result<()>
    where
        B: Serialize + ?Sized,
    {
        self.request::<B, serde::de::IgnoredAny>(method, path, body)
            .await?;
        Ok(())
    }
}

/// A recording stand-in for the Firecracker API socket.
///
/// Firecracker's control surface is a plain HTTP server on a Unix socket, so
/// everything the sandbox does to a microVM before the guest is involved can be
/// exercised without a hypervisor: the request order, the JSON bodies, and the
/// behaviour when a call fails or never answers.
#[cfg(test)]
pub(super) mod fake {
    use std::path::Path;
    use std::sync::{Arc, Mutex};

    use anyhow::Result;
    use http_body_util::{BodyExt, Full};
    use hyper::body::{Bytes, Incoming};
    use hyper::server::conn::http1;
    use hyper::service::service_fn;
    use hyper::{Request, Response, StatusCode};
    use hyper_util::rt::TokioIo;
    use tokio::net::UnixListener;

    #[derive(Clone, Debug)]
    pub(crate) struct RecordedRequest {
        pub method: String,
        pub path: String,
        pub body: Vec<u8>,
    }

    /// What the fake answers for one request.
    pub(crate) struct FakeReply {
        pub status: StatusCode,
        pub body: Vec<u8>,
    }

    impl FakeReply {
        pub fn no_content() -> Self {
            Self {
                status: StatusCode::NO_CONTENT,
                body: Vec::new(),
            }
        }

        pub fn json(value: serde_json::Value) -> Self {
            Self {
                status: StatusCode::OK,
                body: value.to_string().into_bytes(),
            }
        }

        pub fn bad_request(message: &str) -> Self {
            Self {
                status: StatusCode::BAD_REQUEST,
                body: message.as_bytes().to_vec(),
            }
        }
    }

    pub(crate) struct FakeFirecracker {
        requests: Arc<Mutex<Vec<RecordedRequest>>>,
        accept_task: tokio::task::JoinHandle<()>,
    }

    impl FakeFirecracker {
        /// Serve `socket_path` until dropped, answering each request with
        /// `responder(method, path)`.
        pub fn spawn<F>(socket_path: &Path, responder: F) -> Result<Self>
        where
            F: Fn(&str, &str) -> FakeReply + Send + Sync + 'static,
        {
            let listener = UnixListener::bind(socket_path)?;
            let requests = Arc::new(Mutex::new(Vec::new()));
            let responder = Arc::new(responder);

            let accept_requests = Arc::clone(&requests);
            let accept_task = tokio::spawn(async move {
                loop {
                    let Ok((stream, _)) = listener.accept().await else {
                        return;
                    };
                    let requests = Arc::clone(&accept_requests);
                    let responder = Arc::clone(&responder);
                    tokio::spawn(async move {
                        let _ = http1::Builder::new()
                            .serve_connection(
                                TokioIo::new(stream),
                                service_fn(move |req: Request<Incoming>| {
                                    let requests = Arc::clone(&requests);
                                    let responder = Arc::clone(&responder);
                                    async move {
                                        let method = req.method().to_string();
                                        let path = req.uri().path().to_string();
                                        let body = req
                                            .collect()
                                            .await
                                            .map(|collected| collected.to_bytes().to_vec())
                                            .unwrap_or_default();
                                        let reply = responder(&method, &path);
                                        requests
                                            .lock()
                                            .expect("fake firecracker request log poisoned")
                                            .push(RecordedRequest { method, path, body });
                                        Ok::<_, std::convert::Infallible>(
                                            Response::builder()
                                                .status(reply.status)
                                                .header(
                                                    hyper::header::CONTENT_TYPE,
                                                    "application/json",
                                                )
                                                .body(Full::new(Bytes::from(reply.body)))
                                                .expect("build fake response"),
                                        )
                                    }
                                }),
                            )
                            .await;
                    });
                }
            });

            Ok(Self {
                requests,
                accept_task,
            })
        }

        pub fn requests(&self) -> Vec<RecordedRequest> {
            self.requests
                .lock()
                .expect("fake firecracker request log poisoned")
                .clone()
        }

        /// Request paths in the order they arrived, with consecutive repeats of
        /// the same path collapsed so a poll loop does not swamp the sequence.
        pub fn path_sequence(&self) -> Vec<String> {
            let mut sequence: Vec<String> = Vec::new();
            for request in self.requests() {
                if sequence.last() != Some(&request.path) {
                    sequence.push(request.path);
                }
            }
            sequence
        }

        pub fn body_of(&self, path: &str) -> Option<serde_json::Value> {
            self.requests()
                .into_iter()
                .find(|request| request.path == path)
                .map(|request| serde_json::from_slice(&request.body).expect("fake request body"))
        }
    }

    impl Drop for FakeFirecracker {
        fn drop(&mut self) {
            self.accept_task.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::convert::Infallible;

    use http_body_util::Full;
    use hyper::body::Incoming;
    use hyper::server::conn::http1;
    use hyper::service::service_fn;
    use hyper::{Method, Request, Response, StatusCode};
    use hyper_util::rt::TokioIo;
    use serde::{Deserialize, Serialize};
    use tempfile::tempdir;
    use tokio::net::UnixListener;

    #[derive(Debug, Deserialize, PartialEq, Eq)]
    struct JsonReply {
        answer: u32,
    }

    #[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
    struct JsonRequest {
        hello: String,
    }

    #[tokio::test]
    async fn request_serializes_body_and_deserializes_response() -> Result<()> {
        let temp = tempdir()?;
        let socket_path = temp.path().join("firecracker.sock");
        let listener = UnixListener::bind(&socket_path)?;

        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept connection");
            http1::Builder::new()
                .keep_alive(false)
                .serve_connection(
                    TokioIo::new(stream),
                    service_fn(|req: Request<Incoming>| async move {
                        assert_eq!(req.method(), Method::PUT);
                        assert_eq!(req.uri().path(), "/machine-config");
                        let bytes = req
                            .collect()
                            .await
                            .expect("collect request body")
                            .to_bytes();
                        let parsed: JsonRequest =
                            serde_json::from_slice(&bytes).expect("decode request body");
                        assert_eq!(
                            parsed,
                            JsonRequest {
                                hello: "world".to_string(),
                            }
                        );
                        Ok::<_, Infallible>(
                            Response::builder()
                                .status(StatusCode::OK)
                                .header(hyper::header::CONTENT_TYPE, "application/json")
                                .body(Full::new(Bytes::from_static(br#"{"answer":42}"#)))
                                .expect("build response"),
                        )
                    }),
                )
                .await
                .expect("serve connection");
        });

        let client = UnixSocketClient::new(socket_path);
        let reply: JsonReply = client
            .request(
                Method::PUT,
                "/machine-config",
                Some(&JsonRequest {
                    hello: "world".to_string(),
                }),
            )
            .await?;

        assert_eq!(reply, JsonReply { answer: 42 });
        server.await.expect("server task");
        Ok(())
    }

    #[tokio::test]
    async fn request_no_content_accepts_empty_success_body() -> Result<()> {
        let temp = tempdir()?;
        let socket_path = temp.path().join("firecracker.sock");
        let listener = UnixListener::bind(&socket_path)?;

        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept connection");
            http1::Builder::new()
                .keep_alive(false)
                .serve_connection(
                    TokioIo::new(stream),
                    service_fn(|_req: Request<Incoming>| async move {
                        Ok::<_, Infallible>(
                            Response::builder()
                                .status(StatusCode::NO_CONTENT)
                                .body(Full::new(Bytes::new()))
                                .expect("build response"),
                        )
                    }),
                )
                .await
                .expect("serve connection");
        });

        let client = UnixSocketClient::new(socket_path);
        client
            .request_no_content::<serde_json::Value>(Method::PATCH, "/vm", None)
            .await?;

        server.await.expect("server task");
        Ok(())
    }

    #[tokio::test]
    async fn request_surfaces_http_error_status_and_body() {
        let temp = tempdir().expect("tempdir");
        let socket_path = temp.path().join("firecracker.sock");
        let listener = UnixListener::bind(&socket_path).expect("bind unix socket");

        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept connection");
            http1::Builder::new()
                .keep_alive(false)
                .serve_connection(
                    TokioIo::new(stream),
                    service_fn(|_req: Request<Incoming>| async move {
                        Ok::<_, Infallible>(
                            Response::builder()
                                .status(StatusCode::BAD_REQUEST)
                                .header(hyper::header::CONTENT_TYPE, "text/plain")
                                .body(Full::new(Bytes::from_static(b"bad request body")))
                                .expect("build response"),
                        )
                    }),
                )
                .await
                .expect("serve connection");
        });

        let client = UnixSocketClient::new(socket_path);
        let err = client
            .request::<serde_json::Value, JsonReply>(Method::GET, "/bad", None)
            .await
            .expect_err("non-success response should fail");

        assert!(err.to_string().contains("400 Bad Request"));
        assert!(err.to_string().contains("bad request body"));
        server.await.expect("server task");
    }

    #[tokio::test]
    async fn request_reports_invalid_json_responses() {
        let temp = tempdir().expect("tempdir");
        let socket_path = temp.path().join("firecracker.sock");
        let listener = UnixListener::bind(&socket_path).expect("bind unix socket");

        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept connection");
            http1::Builder::new()
                .keep_alive(false)
                .serve_connection(
                    TokioIo::new(stream),
                    service_fn(|_req: Request<Incoming>| async move {
                        Ok::<_, Infallible>(
                            Response::builder()
                                .status(StatusCode::OK)
                                .header(hyper::header::CONTENT_TYPE, "application/json")
                                .body(Full::new(Bytes::from_static(b"not-json")))
                                .expect("build response"),
                        )
                    }),
                )
                .await
                .expect("serve connection");
        });

        let client = UnixSocketClient::new(socket_path);
        let err = client
            .request::<serde_json::Value, JsonReply>(Method::GET, "/vm", None)
            .await
            .expect_err("invalid json should fail");

        assert!(err.to_string().contains("deserialize response body"));
        server.await.expect("server task");
    }
}
