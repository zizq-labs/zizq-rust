// Copyright (c) 2026 Chris Corbyn <chris@zizq.io>
// Licensed under the MIT License. See LICENSE file for details.

// Each file in tests/ compiles to its own integration-test binary, so
// methods used by one binary appear "dead" to another. The lint adds
// no value for shared test scaffolding — silence it module-wide here
// rather than peppering individual items.
#![allow(dead_code)]

use std::collections::VecDeque;
use std::convert::Infallible;
use std::sync::Arc;

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto;
use tokio::net::TcpListener;
use tokio::sync::Mutex;

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

#[derive(Clone)]
struct Route {
    method: String,
    path: String,
    response: Response_,
}

/// A sequence of responses tied to a (method, path). Each matching
/// request pops the next response from the queue; once empty the mock
/// falls through to the static routes / default.
struct SequenceRoute {
    method: String,
    path: String,
    responses: VecDeque<Response_>,
}

/// Tiny in-process test server. Captures incoming requests and serves
/// a configurable canned response. Bound to 127.0.0.1 on a random
/// port; the assigned URL is available on `MockServer::url`. Speaks
/// both HTTP/2 (h2c) and HTTP/1.1 via protocol auto-detection on the
/// initial bytes of each connection, so a single mock can handle a
/// client that uses different transports on different endpoints.
pub struct MockServer {
    pub url: String,
    captured: Arc<Mutex<Vec<CapturedRequest>>>,
    response: Arc<Mutex<Response_>>,
    routes: Arc<Mutex<Vec<Route>>>,
    sequences: Arc<Mutex<Vec<SequenceRoute>>>,
}

impl MockServer {
    pub async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let url = format!("http://{addr}");

        let captured: Arc<Mutex<Vec<CapturedRequest>>> = Arc::new(Mutex::new(Vec::new()));
        let response = Arc::new(Mutex::new(Response_ {
            status: 200,
            content_type: "application/msgpack",
            body: Bytes::new(),
        }));
        let routes: Arc<Mutex<Vec<Route>>> = Arc::new(Mutex::new(Vec::new()));
        let sequences: Arc<Mutex<Vec<SequenceRoute>>> = Arc::new(Mutex::new(Vec::new()));

        let captured_for_task = captured.clone();
        let response_for_task = response.clone();
        let routes_for_task = routes.clone();
        let sequences_for_task = sequences.clone();

        tokio::spawn(async move {
            loop {
                let (stream, _) = match listener.accept().await {
                    Ok(pair) => pair,
                    Err(_) => continue,
                };
                let io = TokioIo::new(stream);
                let captured = captured_for_task.clone();
                let response = response_for_task.clone();
                let routes = routes_for_task.clone();
                let sequences = sequences_for_task.clone();

                tokio::spawn(async move {
                    let svc = service_fn(move |req: Request<Incoming>| {
                        let captured = captured.clone();
                        let response = response.clone();
                        let routes = routes.clone();
                        let sequences = sequences.clone();
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

                            let path_only = parts.uri.path().to_string();
                            let path = parts
                                .uri
                                .path_and_query()
                                .map(|pq| pq.as_str().to_string())
                                .unwrap_or_else(|| path_only.clone());
                            let method = parts.method.to_string();

                            captured.lock().await.push(CapturedRequest {
                                method: method.clone(),
                                path,
                                content_type,
                                accept,
                                body: bytes.to_vec(),
                            });

                            // Resolution order:
                            //   1. response sequences (pop next),
                            //   2. static routes,
                            //   3. default response.
                            let from_sequence = {
                                let mut seqs = sequences.lock().await;
                                seqs.iter_mut()
                                    .find(|s| s.method == method && s.path == path_only)
                                    .and_then(|s| s.responses.pop_front())
                            };
                            let r = match from_sequence {
                                Some(r) => r,
                                None => {
                                    let routes = routes.lock().await.clone();
                                    let from_routes = routes
                                        .iter()
                                        .find(|r| r.method == method && r.path == path_only)
                                        .map(|r| r.response.clone());
                                    match from_routes {
                                        Some(r) => r,
                                        None => response.lock().await.clone(),
                                    }
                                }
                            };

                            let resp = Response::builder()
                                .status(StatusCode::from_u16(r.status).unwrap())
                                .header("content-type", r.content_type)
                                .body(Full::new(r.body))
                                .unwrap();
                            Ok::<_, Infallible>(resp)
                        }
                    });

                    // Auto-detect HTTP/1.1 vs HTTP/2 from the first
                    // bytes of the connection so a single mock can
                    // serve clients that use different transports on
                    // different endpoints.
                    let _ = auto::Builder::new(TokioExecutor::new())
                        .serve_connection(io, svc)
                        .await;
                });
            }
        });

        Self {
            url,
            captured,
            response,
            routes,
            sequences,
        }
    }

    /// Queue a sequence of responses for a (method, path). Each
    /// matching request pops and uses the next response; once the
    /// queue is empty, the mock falls through to static routes or
    /// the default response. Useful for testing retry behaviour
    /// (first call returns 500, second returns 204, etc).
    pub async fn set_response_sequence_for(
        &self,
        method: &str,
        path: &str,
        responses: Vec<(u16, &'static str, Vec<u8>)>,
    ) {
        let responses: VecDeque<Response_> = responses
            .into_iter()
            .map(|(status, content_type, body)| Response_ {
                status,
                content_type,
                body: Bytes::from(body),
            })
            .collect();
        self.sequences.lock().await.push(SequenceRoute {
            method: method.to_string(),
            path: path.to_string(),
            responses,
        });
    }

    /// Configure a response for a specific (method, path) pair. When
    /// a request arrives, configured routes are checked first; if
    /// none match, the default response (from
    /// `set_response_*`) is used.
    pub async fn set_response_for(
        &self,
        method: &str,
        path: &str,
        status: u16,
        content_type: &'static str,
        body: Vec<u8>,
    ) {
        let route = Route {
            method: method.to_string(),
            path: path.to_string(),
            response: Response_ {
                status,
                content_type,
                body: Bytes::from(body),
            },
        };
        self.routes.lock().await.push(route);
    }

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

    pub async fn requests(&self) -> Vec<CapturedRequest> {
        self.captured.lock().await.clone()
    }

    pub async fn requests_for(&self, method: &str, path: &str) -> Vec<CapturedRequest> {
        self.captured
            .lock()
            .await
            .iter()
            .filter(|r| r.method == method && r.path.split('?').next() == Some(path))
            .cloned()
            .collect()
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
