// Copyright (c) 2026 Chris Corbyn <chris@zizq.io>
// Licensed under the MIT License. See LICENSE file for details.

use std::convert::Infallible;
use std::sync::Arc;

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::{TokioExecutor, TokioIo};
use tokio::net::TcpListener;
use tokio::sync::Mutex;

// Fields are read selectively per test binary; allow dead_code so
// the lifecycle binary doesn't complain about content_type/accept.
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct CapturedRequest {
    pub method: String,
    pub path: String,
    pub content_type: Option<String>,
    pub accept: Option<String>,
    pub body: Vec<u8>,
}

#[derive(Clone)]
struct Response_ {
    status: u16,
    content_type: &'static str,
    body: Bytes,
}

#[derive(Clone, Copy)]
enum Protocol {
    Http2,
    Http1,
}

/// Tiny in-process test server. Captures incoming requests and serves
/// a configurable canned response. Bound to 127.0.0.1 on a random
/// port; the assigned URL is available on `MockServer::url`. Speaks
/// either HTTP/2 (h2c) or HTTP/1.1 depending on which `start` variant
/// is used.
pub struct MockServer {
    pub url: String,
    captured: Arc<Mutex<Vec<CapturedRequest>>>,
    response: Arc<Mutex<Response_>>,
}

impl MockServer {
    /// Start a server speaking HTTP/2 with prior knowledge (h2c).
    /// Used by the request/response endpoint tests.
    #[allow(dead_code)]
    pub async fn start() -> Self {
        Self::start_with(Protocol::Http2).await
    }

    /// Start a server speaking HTTP/1.1. Used by the streaming
    /// `/jobs/take` tests since the client uses its HTTP/1.1 pool
    /// for that endpoint.
    #[allow(dead_code)]
    pub async fn start_http1() -> Self {
        Self::start_with(Protocol::Http1).await
    }

    async fn start_with(protocol: Protocol) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let url = format!("http://{addr}");

        let captured: Arc<Mutex<Vec<CapturedRequest>>> = Arc::new(Mutex::new(Vec::new()));
        let response = Arc::new(Mutex::new(Response_ {
            status: 200,
            content_type: "application/msgpack",
            body: Bytes::new(),
        }));

        let captured_for_task = captured.clone();
        let response_for_task = response.clone();

        tokio::spawn(async move {
            loop {
                let (stream, _) = match listener.accept().await {
                    Ok(pair) => pair,
                    Err(_) => continue,
                };
                let io = TokioIo::new(stream);
                let captured = captured_for_task.clone();
                let response = response_for_task.clone();

                tokio::spawn(async move {
                    let svc = service_fn(move |req: Request<Incoming>| {
                        let captured = captured.clone();
                        let response = response.clone();
                        async move {
                            let (parts, incoming) = req.into_parts();
                            let bytes = incoming.collect().await.unwrap().to_bytes();

                            let content_type = parts
                                .headers
                                .get(hyper::header::CONTENT_TYPE)
                                .and_then(|v| v.to_str().ok())
                                .map(str::to_string);
                            let accept = parts
                                .headers
                                .get(hyper::header::ACCEPT)
                                .and_then(|v| v.to_str().ok())
                                .map(str::to_string);

                            let path = parts
                                .uri
                                .path_and_query()
                                .map(|pq| pq.as_str().to_string())
                                .unwrap_or_else(|| parts.uri.path().to_string());

                            captured.lock().await.push(CapturedRequest {
                                method: parts.method.to_string(),
                                path,
                                content_type,
                                accept,
                                body: bytes.to_vec(),
                            });

                            let r = response.lock().await.clone();
                            let resp = Response::builder()
                                .status(StatusCode::from_u16(r.status).unwrap())
                                .header("content-type", r.content_type)
                                .body(Full::new(r.body))
                                .unwrap();
                            Ok::<_, Infallible>(resp)
                        }
                    });

                    match protocol {
                        Protocol::Http2 => {
                            let _ = hyper::server::conn::http2::Builder::new(TokioExecutor::new())
                                .serve_connection(io, svc)
                                .await;
                        }
                        Protocol::Http1 => {
                            let _ = hyper::server::conn::http1::Builder::new()
                                .serve_connection(io, svc)
                                .await;
                        }
                    }
                });
            }
        });

        Self {
            url,
            captured,
            response,
        }
    }

    #[allow(dead_code)]
    pub async fn set_response_msgpack<T: serde::Serialize>(&self, status: u16, value: &T) {
        let mut buf = Vec::new();
        let mut ser = rmp_serde::Serializer::new(&mut buf)
            .with_struct_map()
            .with_human_readable();
        value.serialize(&mut ser).unwrap();
        *self.response.lock().await = Response_ {
            status,
            content_type: "application/msgpack",
            body: Bytes::from(buf),
        };
    }

    pub async fn set_response_json(&self, status: u16, value: serde_json::Value) {
        *self.response.lock().await = Response_ {
            status,
            content_type: "application/json",
            body: Bytes::from(serde_json::to_vec(&value).unwrap()),
        };
    }

    pub async fn set_response_raw(&self, status: u16, content_type: &'static str, body: Vec<u8>) {
        *self.response.lock().await = Response_ {
            status,
            content_type,
            body: Bytes::from(body),
        };
    }

    #[allow(dead_code)]
    pub async fn requests(&self) -> Vec<CapturedRequest> {
        self.captured.lock().await.clone()
    }

    pub async fn last_request(&self) -> CapturedRequest {
        self.captured
            .lock()
            .await
            .last()
            .cloned()
            .expect("no requests captured")
    }
}
