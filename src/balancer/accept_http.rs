use crate::{
    Settings, WsconnMessage,
    balancer::{
        format::{incoming_to_value, replace_block_tags, validate_request},
        processing::{CacheArgs, cache_query, hash_request, update_rpc_latency},
        selection::select::pick,
    },
    database::types::GenericBytes,
    db_get,
    rpc::types::Rpc,
    rpc_response,
    websocket::{
        server::serve_websocket,
        types::{IncomingResponse, SubscriptionData},
    },
};

use tokio::sync::{broadcast, mpsc, watch};

use serde_json::Value;

use futures_util::{StreamExt, stream};
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

#[derive(Clone, Copy)]
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

fn response(status: u16, value: Option<Value>) -> hyper::Response<Full<Bytes>> {
    hyper::Response::builder()
        .status(if value.is_some() { status } else { 204 })
        .header("Content-Type", "application/json")
        .header("Access-Control-Allow-Origin", "*")
        .body(Full::new(Bytes::from(
            value.map(|value| value.to_string()).unwrap_or_default(),
        )))
        .unwrap()
}

fn request_error(id: Value, code: i32, message: &str) -> Value {
    serde_json::json!({"jsonrpc": "2.0", "id": id, "error": {"code": code, "message": message}})
}

async fn forward_call<K, V>(
    mut request: Value,
    connection: &ConnectionParams,
    cache: &CacheArgs<K, V>,
    params: RequestParams,
) -> (u16, Option<Value>)
where
    K: GenericBytes + From<[u8; 32]>,
    V: GenericBytes + From<Vec<u8>>,
{
    if let Err(error) = validate_request(&request) {
        return (400, Some(error));
    }
    let notification = request.get("id").is_none();
    let id = request
        .get_mut("id")
        .map(Value::take)
        .unwrap_or(Value::Null);
    let mut request = replace_block_tags(&mut request, &cache.named_numbers);
    let hash = hash_request(&request);

    if !notification {
        match db_get!(cache.cache, hash.into()) {
            Ok(Some(mut bytes)) => {
                if let Ok(Value::Object(mut cached)) =
                    simd_json::serde::from_slice::<Value>(&mut bytes)
                {
                    cached.insert("id".into(), id);
                    return (200, Some(Value::Object(cached)));
                }
            }
            Ok(None) => {}
            Err(_) => return (500, Some(request_error(id, -32003, "Cache error"))),
        }
        request["id"] = id.clone();
    }

    for attempt in 0..=params.max_retries {
        let (rpc, position) = pick(
            &mut connection
                .rpc_list
                .write()
                .unwrap_or_else(|e| e.into_inner()),
        );
        let Some(position) = position else {
            return (
                500,
                (!notification).then(|| request_error(id, -32002, "No working RPC available")),
            );
        };
        let started = Instant::now();
        let result = timeout(
            Duration::from_millis(params.ttl.try_into().unwrap()),
            rpc.send_request(request.clone()),
        )
        .await;
        update_rpc_latency(&connection.rpc_list, position, started.elapsed());
        match result {
            Ok(Ok(body)) => {
                if notification {
                    return (204, None);
                }
                let result = match serde_json::from_str::<Value>(&body) {
                    Ok(value) if value.is_object() => value,
                    _ => {
                        return (
                            502,
                            Some(request_error(id, -32603, "Invalid upstream response")),
                        );
                    }
                };
                cache_query(&body, request, hash, cache).await;
                return (200, Some(result));
            }
            Ok(Err(error)) => tracing::warn!(%error, "RPC request failed"),
            Err(_) => tracing::warn!("RPC request timed out"),
        }
        if attempt == params.max_retries {
            break;
        }
    }
    (
        408,
        (!notification).then(|| request_error(id, -32001, "Request timed out")),
    )
}

/// Process each batch member through the same routing and cache path as individual calls.
pub async fn forward_body<K, V>(
    tx: Request<hyper::body::Incoming>,
    connection: &ConnectionParams,
    cache: CacheArgs<K, V>,
    params: RequestParams,
) -> Result<hyper::Response<Full<Bytes>>, Infallible>
where
    K: GenericBytes + From<[u8; 32]>,
    V: GenericBytes + From<Vec<u8>>,
{
    if params.header_check
        && tx.headers().get("content-type") != Some(&HeaderValue::from_static("application/json"))
    {
        return Ok(response(
            400,
            Some(request_error(
                Value::Null,
                -32600,
                "Improper content-type header",
            )),
        ));
    }
    let request = match incoming_to_value(tx).await {
        Ok(request) => request,
        Err(_) => {
            return Ok(response(
                400,
                Some(request_error(Value::Null, -32700, "Parse error")),
            ));
        }
    };
    if let Value::Array(requests) = request {
        if requests.is_empty() {
            return Ok(response(
                400,
                Some(request_error(Value::Null, -32600, "Invalid Request")),
            ));
        }
        let responses = stream::iter(
            requests
                .into_iter()
                .map(|request| forward_call(request, connection, &cache, params)),
        )
        .buffered(16)
        .filter_map(|(_, response)| async move { response })
        .collect::<Vec<_>>()
        .await;
        return Ok(response(
            200,
            (!responses.is_empty()).then_some(Value::Array(responses)),
        ));
    }
    let (status, value) = forward_call(request, connection, &cache, params).await;
    Ok(response(status, value))
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

        let ttl = Duration::from_millis(
            connection_params
                .config
                .read()
                .unwrap()
                .ttl
                .try_into()
                .unwrap(),
        );
        // Spawn a task to handle the websocket connection.
        tokio::task::spawn(async move {
            if let Err(e) = serve_websocket(
                websocket,
                connection_params.channels.incoming_tx,
                connection_params.channels.outgoing_rx,
                connection_params.sub_data.clone(),
                cache_args.to_owned(),
                ttl,
            )
            .await
            {
                tracing::error!(?e, "Websocket connection error");
            }
        });

        // Return the response so the spawned future can continue.
        return Ok(response);
    }

    // RequestParams from config
    let params = {
        let config_guard = connection_params.config.read().unwrap();
        RequestParams {
            ttl: config_guard.ttl,
            max_retries: config_guard.max_retries,
            header_check: config_guard.header_check,
        }
    };

    forward_body(tx, &connection_params, cache_args, params).await
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
                                "result": format!("{}:\n\"quoted\"", request["params"][if request["method"] == "eth_getBlockByNumber" { 0 } else { 1 }].as_str().unwrap()),
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
        for (case, (method, tag)) in [
            ("eth_getBalance", "latest"),
            ("eth_getBlockByNumber", "latest"),
            ("eth_getBlockByNumber", "finalized"),
        ]
        .into_iter()
        .enumerate()
        {
            for (id, head, expected_calls) in [
                (json!("first"), 16, 1),
                (json!(-7), 16, 1),
                (json!("third"), 17, 2),
            ] {
                {
                    let mut named_numbers = cache_args.named_numbers.write().unwrap();
                    named_numbers.latest = head + case as u64 * 10;
                    named_numbers.finalized = named_numbers.latest;
                }
                let head = head + case as u64 * 10;
                let params = if method == "eth_getBlockByNumber" {
                    json!([tag, false])
                } else {
                    json!(["0x1", tag])
                };
                let response: Value = client
                    .post(&address)
                    .timeout(Duration::from_secs(5))
                    .json(&json!({
                        "jsonrpc": "2.0", "id": id, "method": method, "params": params,
                    }))
                    .send()
                    .await
                    .unwrap()
                    .json()
                    .await
                    .unwrap();
                assert_eq!(response["id"], id);
                assert_eq!(response["result"], format!("0x{head:x}:\n\"quoted\""));
                assert_eq!(calls.load(Ordering::SeqCst), case * 2 + expected_calls);
            }
        }
        proxy_task.abort();
        upstream_task.abort();
    }

    #[tokio::test]
    async fn http_batches_preserve_requests_omit_notifications_and_bound_concurrency() {
        use http_body_util::BodyExt;
        use std::sync::atomic::{AtomicUsize, Ordering};

        let upstream = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let upstream_url = format!("http://{}", upstream.local_addr().unwrap());
        let active = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let notifications = Arc::new(AtomicUsize::new(0));
        let (active_clone, peak_clone, notifications_clone) =
            (active.clone(), peak.clone(), notifications.clone());
        let upstream_task = tokio::spawn(async move {
            while let Ok((stream, _)) = upstream.accept().await {
                let (active, peak, notifications) = (
                    active_clone.clone(),
                    peak_clone.clone(),
                    notifications_clone.clone(),
                );
                tokio::spawn(async move {
                    http1::Builder::new().serve_connection(TokioIo::new(stream), service_fn(move |request: Request<hyper::body::Incoming>| {
                        let (active, peak, notifications) = (active.clone(), peak.clone(), notifications.clone());
                        async move {
                            let request: Value = serde_json::from_slice(&request.collect().await.unwrap().to_bytes()).unwrap();
                            peak.fetch_max(active.fetch_add(1, Ordering::SeqCst) + 1, Ordering::SeqCst);
                            tokio::time::sleep(Duration::from_millis(10)).await;
                            active.fetch_sub(1, Ordering::SeqCst);
                            let value = if request.get("id").is_none() {
                                notifications.fetch_add(1, Ordering::SeqCst);
                                None
                            } else {
                                Some(json!({"jsonrpc":"2.0", "id":request["id"], "result":request["params"]}))
                            };
                            Ok::<_, Infallible>(response(200, value))
                        }
                    })).await.unwrap();
                });
            }
        });
        let rpc = Rpc::new(upstream_url.parse().unwrap(), None, 1, 0, 1.0);
        let (address, proxy_task) = proxy(rpc, CacheArgs::default()).await;
        let client = reqwest::Client::new();
        let mut batch = vec![
            json!({"jsonrpc":"2.0", "id":"estimate", "method":"eth_estimateGas", "params":[{"data":"0x1234"}]}),
            json!({"jsonrpc":"2.0", "id":-7, "method":"eth_sendRawTransaction", "params":["0x1234"]}),
            json!({"jsonrpc":"2.0", "id":null, "method":"eth_chainId", "params":[]}),
            json!({"jsonrpc":"2.0", "method":"notify", "params":[]}),
            json!(5),
            json!([]),
        ];
        batch
            .extend((0..32).map(
                |id| json!({"jsonrpc":"2.0", "id":id, "method":"eth_chainId", "params":[id]}),
            ));
        let result: Value = client
            .post(&address)
            .timeout(Duration::from_secs(5))
            .json(&batch)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let responses = result.as_array().unwrap();
        assert_eq!(responses.len(), batch.len() - 1);
        for (response, request) in responses
            .iter()
            .zip(batch.iter().filter(|request| request["method"] != "notify"))
        {
            if request.is_object() {
                assert_eq!(response["id"], request["id"]);
                assert_eq!(response["result"], request["params"]);
            } else {
                assert_eq!(response["error"]["code"], -32600);
                assert!(response["id"].is_null());
            }
        }
        assert!((2..=16).contains(&peak.load(Ordering::SeqCst)));
        for payload in [
            json!({"jsonrpc":"2.0", "method":"notify"}),
            json!([{"jsonrpc":"2.0", "method":"notify"}, {"jsonrpc":"2.0", "method":"notify"}]),
        ] {
            let result = client.post(&address).json(&payload).send().await.unwrap();
            assert_eq!(result.status(), 204);
            assert!(result.bytes().await.unwrap().is_empty());
        }
        assert_eq!(notifications.load(Ordering::SeqCst), 4);
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
