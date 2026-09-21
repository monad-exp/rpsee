use crate::{
    Settings, WsconnMessage,
    balancer::{
        format::{incoming_to_value, replace_block_tags, validate_request},
        processing::{CacheArgs, cache_query, hash_request, update_rpc_latency},
        selection::select::pick,
    },
    cache_error,
    database::types::GenericBytes,
    db_get, no_rpc_available, print_cache_error,
    rpc::types::Rpc,
    rpc_response, timed_out,
    websocket::{
        server::serve_websocket,
        types::{IncomingResponse, SubscriptionData},
    },
};

use tokio::sync::{broadcast, mpsc, watch};

use serde_json::Value;

use http_body_util::Full;
use hyper::{Request, body::Bytes, header::HeaderValue};
use hyper_tungstenite::{is_upgrade_request, upgrade};

use tokio::time::timeout;

use std::{
    convert::Infallible,
    sync::{Arc, RwLock},
    time::{Duration, Instant},
};

/// `ConnectionParams` contains the necessary data needed for rpsee
/// to fulfil an incoming request.
#[derive(Clone)]
pub struct ConnectionParams {
    rpc_list: Arc<RwLock<Vec<Rpc>>>,
    channels: RequestChannels,
    sub_data: Arc<SubscriptionData>,
    config: Arc<RwLock<Settings>>,
}

impl ConnectionParams {
    pub fn new(
        rpc_list_rwlock: &Arc<RwLock<Vec<Rpc>>>,
        channels: RequestChannels,
        sub_data: &Arc<SubscriptionData>,
        config: &Arc<RwLock<Settings>>,
    ) -> Self {
        ConnectionParams {
            rpc_list: rpc_list_rwlock.clone(),
            channels,
            sub_data: sub_data.clone(),
            config: config.clone(),
        }
    }
}

pub struct RequestParams {
    pub ttl: u128,
    pub max_retries: u32,
    pub header_check: bool,
}

#[derive(Debug)]
pub struct RequestChannels {
    pub finalized_rx: Arc<watch::Receiver<u64>>,
    pub incoming_tx: mpsc::UnboundedSender<WsconnMessage>,
    pub outgoing_rx: broadcast::Receiver<IncomingResponse>,
}

impl RequestChannels {
    pub fn new(
        finalized_rx: Arc<watch::Receiver<u64>>,
        incoming_tx: mpsc::UnboundedSender<WsconnMessage>,
        outgoing_rx: broadcast::Receiver<IncomingResponse>,
    ) -> Self {
        Self {
            finalized_rx,
            incoming_tx,
            outgoing_rx,
        }
    }
}

impl Clone for RequestChannels {
    fn clone(&self) -> Self {
        Self {
            finalized_rx: Arc::clone(&self.finalized_rx),
            incoming_tx: self.incoming_tx.clone(),
            outgoing_rx: self.outgoing_rx.resubscribe(),
        }
    }
}

/// Macros for accepting requests
#[macro_export]
macro_rules! accept {
    (
        $io:expr,
        $cache_args:expr,
        $connection_params:expr
    ) => {
        // Bind the incoming connection to our service
        if let Err(err) = http1::Builder::new()
            // `service_fn` converts our function in a `Service`
            .serve_connection(
                $io,
                service_fn(|req| {
                    let response =
                        accept_request(req, $cache_args.clone(), $connection_params.clone());
                    response
                }),
            )
            .with_upgrades()
            .await
        {
            tracing::error!(?err, "Error serving connection");
        }
    };
}

/// Macro for getting responses from either the cache or RPC nodes
macro_rules! get_response {
    (
        $tx:expr,
        $cache_args:expr,
        $tx_hash:expr,
        $rpc_position:expr,
        $id:expr,
        $con_params:expr,
        $ttl:expr,
        $max_retries:expr
    ) => {
        match db_get!($cache_args.cache, $tx_hash.as_bytes().to_owned().into()).map(|bytes| {
            bytes.and_then(|mut bytes| {
                simd_json::serde::from_slice::<Value>(&mut bytes)
                    .ok()
                    .filter(Value::is_object)
            })
        }) {
            Ok(Some(mut cached)) => {
                $rpc_position = None;
                // Reconstruct ID
                cached["id"] = $id.clone();
                cached.to_string()
            }
            Ok(_) => {
                fetch_from_rpc!(
                    $tx,
                    $cache_args,
                    $tx_hash,
                    $rpc_position,
                    $id,
                    $con_params,
                    $ttl,
                    $max_retries
                )
            }
            Err(_) => {
                // If anything errors send an rpc request and see if it works, if not then gg
                print_cache_error!();
                $rpc_position = None;
                return (cache_error!($id.clone()), $rpc_position);
            }
        }
    };
}

macro_rules! fetch_from_rpc {
    (
        $tx:expr,
        $cache_args:expr,
        $tx_hash:expr,
        $rpc_position:expr,
        $id:expr,
        $con_params:expr,
        $ttl:expr,
        $max_retries:expr
    ) => {{
        // Kinda jank but set the id back to what it was before
        $tx["id"] = $id.clone();

        // Loop until we get a response
        let rx;
        let mut retries = 0;
        loop {
            // Get the next Rpc in line.
            let rpc;
            {
                let mut rpc_list_guard = $con_params.rpc_list.write().unwrap_or_else(|e| {
                    // Handle the case where the RwLock is poisoned
                    e.into_inner()
                });

                (rpc, $rpc_position) = pick(&mut rpc_list_guard);
            }
            tracing::info!(rpc.name, "Forwarding to");

            // Check if we have any RPCs in the list, if not return error
            if $rpc_position == None {
                return (no_rpc_available!($id.clone()), None);
            }

            // Send the request. And return a timeout if it takes too long
            //
            // Check if it contains any errors or if its `latest` and insert it if it isn't
            match timeout(
                Duration::from_millis($ttl.try_into().unwrap()),
                rpc.send_request($tx.clone()),
            )
            .await
            {
                Ok(Ok(response)) => {
                    rx = response;
                    break;
                }
                Ok(Err(error)) => {
                    tracing::warn!(%error, "RPC request failed, retrying");
                }
                Err(_) => {
                    tracing::warn!("An RPC request has timed out, picking new RPC and retrying.");
                }
            };

            if retries >= $max_retries {
                return (timed_out!($id.clone()), $rpc_position);
            }
            retries += 1;
        }

        // Don't cache responses that contain errors or missing trie nodes
        cache_query(&rx, $tx, $tx_hash, &$cache_args).await;

        rx
    }};
}

/// Pick RPC and send request to it. In case the result is cached,
/// read and return from the cache.
pub async fn forward_body<K, V>(
    tx: Request<hyper::body::Incoming>,
    con_params: &ConnectionParams,
    cache_args: CacheArgs<K, V>,
    params: RequestParams,
) -> (
    Result<hyper::Response<Full<Bytes>>, Infallible>,
    Option<usize>,
)
where
    K: GenericBytes + From<[u8; 32]>,
    V: GenericBytes + From<Vec<u8>>,
{
    // TODO: do content type validation more upstream
    // Check if body has application/json
    //
    // Can be toggled via the config. Should be on if we want rpsee to be JSON-RPC compliant.
    if params.header_check
        && tx.headers().get("content-type") != Some(&HeaderValue::from_static("application/json"))
    {
        return (
            Ok(hyper::Response::builder()
                .status(400)
                .body(Full::new(Bytes::from("Improper content-type header")))
                .unwrap()),
            None,
        );
    }

    let request = match incoming_to_value(tx).await {
        Ok(request) => validate_request(&request).map(|()| request),
        Err(_) => Err(serde_json::json!({
            "jsonrpc": "2.0", "id": null,
            "error": { "code": -32700, "message": "Parse error" },
        })),
    };
    let mut tx = match request {
        Ok(request) => request,
        Err(error) => {
            return (
                Ok(hyper::Response::builder()
                    .status(400)
                    .header("Content-Type", "application/json")
                    .body(Full::new(Bytes::from(error.to_string())))
                    .unwrap()),
                None,
            );
        }
    };

    // Get the id of the request and set it to null for caching
    //
    // We're doing this ID gymnastics because we're hashing the
    // whole request and we don't want the ID as it's arbitrary
    // and does not impact the request result.
    let id = tx["id"].take();

    let mut tx = replace_block_tags(&mut tx, &cache_args.named_numbers);
    let tx_hash = hash_request(&tx);
    let mut rpc_position;

    // Get the response from either the DB or from a RPC. If it timeouts, retry.
    let rax = get_response!(
        tx,
        cache_args,
        tx_hash,
        rpc_position,
        id,
        con_params,
        params.ttl,
        params.max_retries
    );

    // Convert rx to bytes and but it in a Buf
    let body = hyper::body::Bytes::from(rax);

    // Put it in a http_body_util::Full
    let body = Full::new(body);

    // Build the response
    let res = hyper::Response::builder()
        .status(200)
        .header("Content-Type", "application/json")
        .header("Access-Control-Allow-Origin", "*")
        .body(body)
        .unwrap();

    (Ok(res), rpc_position)
}

/// Forward the request to *a* RPC picked by the algo set by the user.
/// Measures the time needed for a request, and updates the respective
/// RPC lself.
/// In case of a timeout, returns an error.
pub async fn accept_request<K, V>(
    mut tx: Request<hyper::body::Incoming>,
    connection_params: ConnectionParams,
    cache_args: CacheArgs<K, V>,
) -> Result<hyper::Response<Full<Bytes>>, Infallible>
where
    K: GenericBytes + From<[u8; 32]> + 'static,
    V: GenericBytes + From<Vec<u8>> + 'static,
{
    // Check if the request is a websocket upgrade request.
    if is_upgrade_request(&tx) {
        tracing::info!("Received WS upgrade request");

        if !connection_params.config.read().unwrap().is_ws {
            return rpc_response!(
                500,
                Full::new(Bytes::from(
                    "{code:-32005, message:\"error: WebSockets are disabled!\"}".to_string(),
                ))
            );
        }

        let (response, websocket) = match upgrade(&mut tx, None) {
            Ok((response, websocket)) => (response, websocket),
            Err(e) => {
                tracing::error!(?e, "Websocket upgrade error");
                return rpc_response!(500, Full::new(Bytes::from(
                    "{code:-32004, message:\"error: Websocket upgrade error! Try again later...\"}"
                        .to_string(),
                )));
            }
        };

        // Spawn a task to handle the websocket connection.
        tokio::task::spawn(async move {
            if let Err(e) = serve_websocket(
                websocket,
                connection_params.channels.incoming_tx,
                connection_params.channels.outgoing_rx,
                connection_params.sub_data.clone(),
                cache_args.to_owned(),
            )
            .await
            {
                tracing::error!(?e, "Websocket connection error");
            }
        });

        // Return the response so the spawned future can continue.
        return Ok(response);
    }

    // Send request
    let response: Result<hyper::Response<Full<Bytes>>, Infallible>;
    let rpc_position: Option<usize>;

    // RequestParams from config
    let params = {
        let config_guard = connection_params.config.read().unwrap();
        RequestParams {
            ttl: config_guard.ttl,
            max_retries: config_guard.max_retries,
            header_check: config_guard.header_check,
        }
    };

    // Check if we have the response hashed, and if not forward it
    // to the best available RPC.
    //
    // Also handle cache insertions.
    let time = Instant::now();
    (response, rpc_position) = forward_body(tx, &connection_params, cache_args, params).await;

    let time = time.elapsed();
    tracing::info!(?time, "Request time");

    // `rpc_position` is an Option<> that either contains the index of the RPC
    // we forwarded our request to, or is None if the result was cached.
    //
    // Here, we update the latency of the RPC that was used to process the request
    // if `rpc_position` is Some.
    if let Some(rpc_position) = rpc_position {
        update_rpc_latency(&connection_params.rpc_list, rpc_position, time);
    }

    response
}

#[cfg(test)]
mod tests {
    use super::*;
    use hyper::{server::conn::http1, service::service_fn};
    use hyper_util::rt::TokioIo;
    use serde_json::json;
    use tokio::net::TcpListener;

    async fn proxy(
        rpc: Rpc,
        cache_args: CacheArgs<[u8; 32], Vec<u8>>,
    ) -> (String, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = format!("http://{}", listener.local_addr().unwrap());
        let channels = RequestChannels::new(
            Arc::new(watch::channel(0).1),
            mpsc::unbounded_channel().0,
            broadcast::channel(1).1,
        );
        let params = ConnectionParams::new(
            &Arc::new(RwLock::new(vec![rpc])),
            channels,
            &Arc::new(SubscriptionData::new()),
            &Arc::new(RwLock::new(Settings {
                max_retries: 0,
                ..Settings::default()
            })),
        );
        let task = tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                let params = params.clone();
                let cache_args = cache_args.clone();
                tokio::spawn(async move {
                    http1::Builder::new()
                        .serve_connection(
                            TokioIo::new(stream),
                            service_fn(move |request| {
                                accept_request(request, params.clone(), cache_args.clone())
                            }),
                        )
                        .await
                        .unwrap();
                });
            }
        });
        (address, task)
    }

    #[cfg(not(feature = "no-cache"))]
    #[tokio::test]
    async fn cached_http_calls_preserve_ids_and_follow_head() {
        use http_body_util::BodyExt;
        use hyper::Response;
        use std::sync::atomic::{AtomicUsize, Ordering};

        let upstream = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let upstream_url = format!("http://{}", upstream.local_addr().unwrap());
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_clone = calls.clone();
        let upstream_task = tokio::spawn(async move {
            while let Ok((stream, _)) = upstream.accept().await {
                let calls = calls_clone.clone();
                tokio::spawn(async move {
                    http1::Builder::new().serve_connection(TokioIo::new(stream), service_fn(move |request: Request<hyper::body::Incoming>| {
                        let calls = calls.clone();
                        async move {
                            calls.fetch_add(1, Ordering::SeqCst);
                            let body = request.collect().await.unwrap().to_bytes();
                            let request: Value = serde_json::from_slice(&body).unwrap();
                            let response = json!({
                                "jsonrpc": "2.0", "id": request["id"],
                                "result": format!("{}:\n\"quoted\"", request["params"][1].as_str().unwrap()),
                            });
                            Ok::<_, Infallible>(Response::new(Full::new(Bytes::from(response.to_string()))))
                        }
                    })).await.unwrap();
                });
            }
        });
        let cache_args = CacheArgs::default();
        cache_args.named_numbers.write().unwrap().latest = 16;
        let rpc = Rpc::new(upstream_url.parse().unwrap(), None, 1, 0, 1.0);
        let (address, proxy_task) = proxy(rpc, cache_args.clone()).await;
        let client = reqwest::Client::new();
        for (id, head, expected_calls) in [
            (json!("first"), 16, 1),
            (json!(-7), 16, 1),
            (json!("third"), 17, 2),
        ] {
            cache_args.named_numbers.write().unwrap().latest = head;
            let response: Value = client.post(&address).timeout(Duration::from_secs(5)).json(&json!({
                "jsonrpc": "2.0", "id": id, "method": "eth_getBalance", "params": ["0x1", "latest"],
            })).send().await.unwrap().json().await.unwrap();
            assert_eq!(response["id"], id);
            assert_eq!(response["result"], format!("0x{head:x}:\n\"quoted\""));
            assert_eq!(calls.load(Ordering::SeqCst), expected_calls);
        }
        proxy_task.abort();
        upstream_task.abort();
    }

    #[tokio::test]
    async fn invalid_http_requests_return_structured_errors() {
        let rpc = Rpc::new("http://127.0.0.1:1".parse().unwrap(), None, 1, 0, 1.0);
        let (address, task) = proxy(rpc, CacheArgs::default()).await;
        let client = reqwest::Client::new();
        for (body, code, id) in [
            (b"{".as_slice(), -32700, json!(null)),
            (b"\xff".as_slice(), -32700, json!(null)),
            (b"[]".as_slice(), -32600, json!(null)),
            (b"null".as_slice(), -32600, json!(null)),
            (b"42".as_slice(), -32600, json!(null)),
            (b"{}".as_slice(), -32600, json!(null)),
            (
                br#"{"jsonrpc":"2.0","id":"original","method":false}"#.as_slice(),
                -32600,
                json!("original"),
            ),
            (
                br#"{"jsonrpc":"2.0","id":true,"method":"eth_blockNumber"}"#.as_slice(),
                -32600,
                json!(null),
            ),
        ] {
            let response = client
                .post(&address)
                .timeout(Duration::from_secs(5))
                .header("Content-Type", "application/json")
                .body(body.to_vec())
                .send()
                .await
                .unwrap();
            assert_eq!(response.status(), 400);
            let response: Value = response.json().await.unwrap();
            assert_eq!(response["jsonrpc"], "2.0");
            assert_eq!(response["error"]["code"], code);
            assert_eq!(response["id"], id);
            assert!(response.get("result").is_none());
        }
        task.abort();
    }

    #[tokio::test]
    async fn upstream_connection_failure_returns_a_response_without_retrying_forever() {
        let upstream = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = format!("http://{}", upstream.local_addr().unwrap());
        drop(upstream);
        let rpc = Rpc::new(address.parse().unwrap(), None, 1, 0, 1.0);
        let (address, task) = proxy(rpc, CacheArgs::default()).await;
        let response = reqwest::Client::new()
            .post(address)
            .timeout(Duration::from_secs(5))
            .json(&json!({
                "jsonrpc": "2.0", "id": 1, "method": "eth_blockNumber", "params": [],
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 408);
        let error: Value = response.json().await.unwrap();
        assert_eq!(error["id"], 1);
        assert_eq!(error["error"]["code"], -32001);
        task.abort();
    }
}
