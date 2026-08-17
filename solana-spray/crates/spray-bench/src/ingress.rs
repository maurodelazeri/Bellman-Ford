//! A minimal, fast HTTP/2 client for the Restate ingress.
//!
//! The load generator has to not be the bottleneck, and it has to measure only
//! what it means to measure. So: a fixed pool of HTTP/2 connections with
//! request multiplexing (one TCP connection carries hundreds of concurrent
//! submissions), no TLS, no redirect handling, no JSON round trip on the
//! response path beyond what is needed.
//!
//! HTTP/2 multiplexing is the point. Submitting 10k transactions over 10k TCP
//! connections would measure the kernel; over 8 multiplexed connections it
//! measures Restate.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::{Request, StatusCode};
use hyper_util::rt::{TokioExecutor, TokioIo};

#[derive(Clone)]
pub struct IngressClient {
    conns: Arc<Vec<hyper::client::conn::http2::SendRequest<Full<Bytes>>>>,
    next: Arc<AtomicUsize>,
    authority: String,
}

impl IngressClient {
    /// Open `n` HTTP/2 connections to the ingress and keep them hot.
    pub async fn connect(addr: &str, n: usize) -> anyhow::Result<Self> {
        let mut conns = Vec::with_capacity(n);
        for _ in 0..n {
            let stream = tokio::net::TcpStream::connect(addr).await?;
            stream.set_nodelay(true)?;
            let io = TokioIo::new(stream);
            let (sender, conn) = hyper::client::conn::http2::Builder::new(TokioExecutor::new())
                .initial_stream_window_size(Some(1 << 20))
                .initial_connection_window_size(Some(4 << 20))
                .max_concurrent_reset_streams(0)
                .handshake(io)
                .await?;
            tokio::spawn(async move {
                if let Err(e) = conn.await {
                    eprintln!("ingress connection closed: {e}");
                }
            });
            conns.push(sender);
        }
        Ok(IngressClient {
            conns: Arc::new(conns),
            next: Arc::new(AtomicUsize::new(0)),
            authority: addr.to_string(),
        })
    }

    fn pick(&self) -> hyper::client::conn::http2::SendRequest<Full<Bytes>> {
        let i = self.next.fetch_add(1, Ordering::Relaxed) % self.conns.len();
        self.conns[i].clone()
    }

    /// POST a JSON body to an ingress path. Returns status and body.
    pub async fn post(
        &self,
        path: &str,
        body: Bytes,
        idempotency_key: Option<&str>,
    ) -> anyhow::Result<(StatusCode, Bytes)> {
        let mut builder = Request::builder()
            .method("POST")
            .uri(path)
            .header("host", &self.authority)
            .header("content-type", "application/json");
        if let Some(k) = idempotency_key {
            builder = builder.header("idempotency-key", k);
        }
        let req = builder.body(Full::new(body))?;

        let mut sender = self.pick();
        sender.ready().await?;
        let resp = sender.send_request(req).await?;
        let status = resp.status();
        let bytes = resp.into_body().collect().await?.to_bytes();
        Ok((status, bytes))
    }

}

/// Plain HTTP/1.1 one-shot GET, for scraping the service stats port and the
/// Restate admin API without holding pooled connections open against them.
pub async fn http1_get(addr: &str, path: &str) -> anyhow::Result<Bytes> {
    let stream = tokio::net::TcpStream::connect(addr).await?;
    stream.set_nodelay(true)?;
    let io = TokioIo::new(stream);
    let (mut sender, conn) = hyper::client::conn::http1::handshake(io).await?;
    tokio::spawn(async move {
        let _ = conn.await;
    });
    let req = Request::builder()
        .method("GET")
        .uri(path)
        .header("host", addr)
        .body(Full::new(Bytes::new()))?;
    let resp = sender.send_request(req).await?;
    Ok(resp.into_body().collect().await?.to_bytes())
}
