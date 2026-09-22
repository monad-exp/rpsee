use crate::{
    balancer::{
        format::{replace_block_tags, validate_request},
        processing::{CacheArgs, cache_query, hash_request},
        selection::select::pick,
    },
    database::types::GenericBytes,
    db_get,
    rpc::{method::EthRpcMethod, types::Rpc},
    websocket::{
        error::WsError,
        types::{IncomingResponse, SubscriptionData, WsChannelErr, WsconnMessage},
    },
};

use std::{
    sync::{Arc, RwLock},
    time::Instant,
};

use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use simd_json::from_slice;

use tokio::sync::{broadcast, mpsc};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};

pub(crate) struct WsConnection {
    rpc: Rpc,
    sender: Option<mpsc::UnboundedSender<Value>>,
}

/// Routes requests to upstream WebSocket connections.
pub async fn ws_conn_manager(
    rpc_list: Arc<RwLock<Vec<Rpc>>>,
    ws_handles: Arc<RwLock<Vec<WsConnection>>>,
    mut incoming_rx: mpsc::UnboundedReceiver<WsconnMessage>,
    broadcast_tx: broadcast::Sender<IncomingResponse>,
    ws_error_tx: mpsc::UnboundedSender<WsChannelErr>,
    ttl: u128,
) {
    let deadline = std::time::Duration::from_millis(u64::try_from(ttl).unwrap_or(u64::MAX));
    update_ws_connections(
        &rpc_list,
        &ws_handles,
        &broadcast_tx,
        &ws_error_tx,
        deadline,
    )
    .await;

    while let Some(message) = incoming_rx.recv().await {
        match message {
            WsconnMessage::Message(incoming, specified_index) => {
                handle_incoming_message(
                    &ws_handles,
                    &rpc_list,
                    incoming,
                    specified_index,
                    &broadcast_tx,
                );
            }
            WsconnMessage::Reconnect() => {
                update_ws_connections(
                    &rpc_list,
                    &ws_handles,
                    &broadcast_tx,
                    &ws_error_tx,
                    deadline,
                )
                .await;
            }
        }
    }
    ws_handles.write().unwrap().clear();
}

async fn update_ws_connections(
    rpc_list: &Arc<RwLock<Vec<Rpc>>>,
    ws_handles: &Arc<RwLock<Vec<WsConnection>>>,
    broadcast_tx: &broadcast::Sender<IncomingResponse>,
    ws_error_tx: &mpsc::UnboundedSender<WsChannelErr>,
    deadline: std::time::Duration,
) {
    let rpcs = rpc_list.read().unwrap().clone();
    let mut reconnect = Vec::new();
    {
        let mut handles = ws_handles.write().unwrap();
        for (index, connection) in handles.iter_mut().enumerate() {
            if !rpcs.iter().any(|rpc| rpc.same_endpoint(&connection.rpc)) {
                if connection
                    .sender
                    .as_ref()
                    .is_some_and(|sender| !sender.is_closed())
                {
                    let _ = ws_error_tx.send(WsChannelErr::Closed(index, connection.rpc.clone()));
                }
                connection.sender = None;
            }
        }
        for rpc in rpcs {
            let index = if let Some(index) = handles
                .iter()
                .position(|connection| connection.rpc.same_endpoint(&rpc))
            {
                index
            } else {
                handles.push(WsConnection {
                    rpc: rpc.clone(),
                    sender: None,
                });
                handles.len() - 1
            };
            if handles[index]
                .sender
                .as_ref()
                .is_none_or(|sender| sender.is_closed())
            {
                reconnect.push((index, rpc));
            }
        }
    }
    let connections =
        futures_util::future::join_all(reconnect.into_iter().map(|(index, rpc)| async move {
            let (sender, receiver) = mpsc::unbounded_channel();
            let connected = ws_conn(
                rpc.clone(),
                rpc_list.clone(),
                receiver,
                broadcast_tx.clone(),
                ws_error_tx.clone(),
                index,
                deadline,
            )
            .await;
            (index, rpc, connected.then_some(sender))
        }))
        .await;
    let rpcs = rpc_list.read().unwrap();
    let mut handles = ws_handles.write().unwrap();
    for (index, rpc, sender) in connections {
        if rpcs.iter().any(|active| active.same_endpoint(&rpc)) {
            handles[index].sender = sender;
        }
    }
}

fn handle_incoming_message(
    ws_handles: &Arc<RwLock<Vec<WsConnection>>>,
    rpc_list: &Arc<RwLock<Vec<Rpc>>>,
    incoming: Value,
    specified_index: Option<usize>,
    broadcast_tx: &broadcast::Sender<IncomingResponse>,
) {
    let rpc_position = specified_index.or_else(|| {
        let mut rpcs = rpc_list.write().unwrap_or_else(|e| e.into_inner());
        let handles = ws_handles.read().unwrap();
        let positions: Vec<_> = rpcs
            .iter()
            .enumerate()
            .filter_map(|(rpc_index, rpc)| {
                handles
                    .iter()
                    .position(|connection| {
                        connection.rpc.same_endpoint(rpc)
                            && connection
                                .sender
                                .as_ref()
                                .is_some_and(|sender| !sender.is_closed())
                    })
                    .map(|slot| (rpc_index, slot))
            })
            .collect();
        let mut candidates: Vec<_> = positions
            .iter()
            .map(|(index, _)| rpcs[*index].clone())
            .collect();
        let selected = pick(&mut candidates).1;
        for ((index, _), candidate) in positions.iter().zip(candidates) {
            rpcs[*index] = candidate;
        }
        selected.map(|index| positions[index].1)
    });
    let sender = rpc_position.and_then(|index| {
        ws_handles
            .read()
            .unwrap()
            .get(index)
            .and_then(|connection| connection.sender.clone())
    });
    let id = incoming["id"].clone();
    if sender.is_some_and(|sender| sender.send(incoming).is_ok()) {
        return;
    }
    let _ = broadcast_tx.send(IncomingResponse {
        node_id: rpc_position.unwrap_or(usize::MAX),
        content: serde_json::json!({
            "jsonrpc": "2.0", "id": id,
            "error": {"code": -32000, "message": "No upstream WebSocket connection available"},
        }),
    });
}

/// Owns both halves of a connection so replacing its sender closes the socket.
pub async fn ws_conn(
    rpc: Rpc,
    rpc_list: Arc<RwLock<Vec<Rpc>>>,
    mut incoming_rx: mpsc::UnboundedReceiver<Value>,
    broadcast_tx: broadcast::Sender<IncomingResponse>,
    ws_error_tx: mpsc::UnboundedSender<WsChannelErr>,
    index: usize,
    deadline: std::time::Duration,
) -> bool {
    let Some(url) = rpc.ws_url.as_ref() else {
        return false;
    };
    let mut stream = match tokio::time::timeout(deadline, connect_async(url.as_str())).await {
        Ok(Ok((stream, _))) => stream,
        _ => {
            tracing::warn!(
                rpc_name = rpc.name,
                "Unable to connect to upstream WebSocket"
            );
            return false;
        }
    };

    tokio::spawn(async move {
        loop {
            tokio::select! {
                incoming = incoming_rx.recv() => {
                    let Some(incoming) = incoming else { return; };
                    if !matches!(tokio::time::timeout(deadline,
                        stream.send(Message::Text(incoming.to_string().into()))).await, Ok(Ok(()))) {
                        break;
                    }
                }
                message = stream.next() => {
                    let mut bytes = match message {
                        Some(Ok(Message::Text(message))) => message.as_bytes().to_vec(),
                        Some(Ok(Message::Binary(message))) => message.to_vec(),
                        Some(Ok(Message::Ping(message))) => {
                            if !matches!(tokio::time::timeout(deadline,
                                stream.send(Message::Pong(message))).await, Ok(Ok(()))) {
                                break;
                            }
                            continue;
                        }
                        Some(Ok(Message::Pong(_))) => continue,
                        _ => break,
                    };
                    let time = Instant::now();
                    let content = match from_slice(&mut bytes) {
                        Ok(content) => content,
                        Err(error) => {
                            tracing::warn!(?error, "Invalid upstream WebSocket JSON");
                            continue;
                        }
                    };
                    let _ = broadcast_tx.send(IncomingResponse { node_id: index, content });
                    if let Some(active) = rpc_list.write().unwrap().iter_mut().find(|active| active.same_endpoint(&rpc)) {
                        active.update_latency(time.elapsed().as_nanos() as f64);
                    }
                }
            }
        }
        let _ = ws_error_tx.send(WsChannelErr::Closed(index, rpc));
    });
    true
}

/// Execute batch members sequentially to keep per-connection request work bounded.
pub async fn execute_ws_request<K, V>(
    request: Value,
    user_id: u32,
    incoming_tx: &mpsc::UnboundedSender<WsconnMessage>,
    broadcast_rx: broadcast::Receiver<IncomingResponse>,
    sub_data: &Arc<SubscriptionData>,
    cache_args: &CacheArgs<K, V>,
    ttl: std::time::Duration,
) -> Option<String>
where
    K: GenericBytes + From<[u8; 32]>,
    V: GenericBytes + From<Vec<u8>>,
{
    let batch = request.is_array();
    let requests = match request {
        Value::Array(requests) if requests.is_empty() => {
            return Some(serde_json::json!({"jsonrpc":"2.0", "id":null, "error":{"code":-32600, "message":"Invalid Request"}}).to_string());
        }
        Value::Array(requests) => requests,
        request => vec![request],
    };
    let mut responses = Vec::new();
    for call in requests {
        let notification = validate_request(&call).is_ok() && call.get("id").is_none();
        let id = call["id"].clone();
        let result = tokio::time::timeout(
            ttl,
            execute_ws_call(
                call,
                user_id,
                incoming_tx,
                broadcast_rx.resubscribe(),
                sub_data,
                cache_args,
            ),
        )
        .await;
        if notification {
            continue;
        }
        let response = match result {
            Ok(Ok(response)) => serde_json::from_str(&response).unwrap_or_else(|_| {
                serde_json::json!({"jsonrpc":"2.0", "id":id, "error":{"code":-32603, "message":"Invalid upstream response"}})
            }),
            Ok(Err(error)) => serde_json::json!({"jsonrpc":"2.0", "id":id, "error":{"code":-32603, "message":error.to_string()}}),
            Err(_) => serde_json::json!({"jsonrpc":"2.0", "id":id, "error":{"code":-32001, "message":"Request timed out"}}),
        };
        responses.push(response);
    }
    if responses.is_empty() {
        None
    } else if batch {
        Some(Value::Array(responses).to_string())
    } else {
        Some(responses.remove(0).to_string())
    }
}

/// Processes an individual RPC request received via WebSockets.
///
/// Contains logic for retreiving from cache, sending to the internal
/// WS pipeline, retreiving and returning received responses.
pub async fn execute_ws_call<K, V>(
    mut call: Value,
    user_id: u32,
    incoming_tx: &mpsc::UnboundedSender<WsconnMessage>,
    broadcast_rx: broadcast::Receiver<IncomingResponse>,
    sub_data: &Arc<SubscriptionData>,
    cache_args: &CacheArgs<K, V>,
) -> Result<String, WsError>
where
    K: GenericBytes + From<[u8; 32]>,
    V: GenericBytes + From<Vec<u8>>,
{
    tracing::debug!(
        "Received incoming WS call from user_id {}: {:?}",
        user_id,
        call
    );

    if let Err(error) = validate_request(&call) {
        return Ok(error.to_string());
    }
    if call.get("id").is_none() {
        if call["method"].eq(&EthRpcMethod::Unsubscribe) {
            if let Some(subscription_id) = call["params"][0].as_str() {
                sub_data.unsubscribe_user(user_id, subscription_id.to_owned());
            }
        } else {
            incoming_tx.send(WsconnMessage::Message(call, None))?;
        }
        return Ok(String::new());
    }
    let id = call["id"].take();
    let is_subscription = call["method"].eq(&EthRpcMethod::Subscribe);
    if !is_subscription {
        call = replace_block_tags(&mut call, &cache_args.named_numbers);
    }
    let tx_hash = hash_request(&call);

    if let Ok(Some(mut bytes)) = db_get!(cache_args.cache, tx_hash.as_bytes().to_owned().into())
        && let Ok(Value::Object(mut cached)) = from_slice::<Value>(&mut bytes)
    {
        cached.insert("id".into(), id);
        return Ok(Value::Object(cached).to_string());
    }

    // Remove and unsubscribe user is "eth_unsubscribe"
    if call["method"].eq(&EthRpcMethod::Unsubscribe) {
        // subscription_id is ["params"][0]
        let subscription_id = match call["params"][0].as_str() {
            Some(subscription_id) => subscription_id.to_string(),
            None => {
                return Ok(format!(
                    "{{\"jsonrpc\":\"2.0\", \"id\":{}, \"error\": \"Bad Subscription ID!\"}}",
                    id
                ));
            }
        };
        // we have to get the id of the subsctiption and what node is subscribed and send the message
        let index = match sub_data.get_node_from_id(&subscription_id) {
            Some(rax) => Some(rax),
            None => {
                return Ok(format!(
                    "{{\"jsonrpc\":\"2.0\", \"id\":{}, \"error\": \"false\"}}",
                    id
                ));
            }
        };
        tracing::info!("execute_ws_call: index: {index:?}");

        sub_data.unsubscribe_user(user_id, subscription_id);

        return Ok(format!(
            "{{\"jsonrpc\":\"2.0\",\"id\":{},\"result\":true}}",
            id
        ));
    }

    if is_subscription {
        // Check if we're already subscribed to this
        // if so return the subscription id and add this user to the dispatch
        // if not continue
        if let Ok(rax) = sub_data.subscribe_user(user_id, call.clone()) {
            tracing::debug!("has subscription already");
            return Ok(format!(
                "{{\"jsonrpc\":\"2.0\",\"id\":{},\"result\":\"{}\"}}",
                id, rax
            ));
        }
    }

    static NEXT_REQUEST_ID: std::sync::atomic::AtomicU64 =
        std::sync::atomic::AtomicU64::new(u32::MAX as u64 + 1);
    let request_id = NEXT_REQUEST_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    call["id"] = request_id.into();
    incoming_tx.send(WsconnMessage::Message(call.clone(), None))?;
    let mut response = listen_for_response(request_id, broadcast_rx).await?;

    if is_subscription {
        tracing::debug!("is subscription!");
        tracing::debug!(?response.content, "response content");
        // add the subscription id and add this user to the dispatch
        let sub_id = match response.content["result"].as_str() {
            Some(sub_id) => sub_id.to_string(),
            None => {
                return Ok(format!(
                    "\"jsonrpc\":\"2.0\", \"id\":{}, \"error\": \"Bad Subscription ID!\"",
                    id
                ));
            }
        };

        tracing::info!(sub_id, "sub_id");
        sub_data.register_subscription(call.clone(), sub_id.clone(), response.node_id);
        sub_data.subscribe_user(user_id, call)?;
    } else {
        cache_query(&response.content.to_string(), call, tx_hash, cache_args).await;
    }

    response.content["id"] = id;
    Ok(response.content.to_string())
}

/// Wait for the response to this upstream request.
async fn listen_for_response(
    request_id: u64,
    mut broadcast_rx: broadcast::Receiver<IncomingResponse>,
) -> Result<IncomingResponse, WsError> {
    while let Ok(response) = broadcast_rx.recv().await {
        if response.content["id"].as_u64() == Some(request_id) {
            return Ok(response);
        }
    }
    Err(WsError::NoWsResponse)
}

#[cfg(test)]
mod tests {
    use crate::rpc::method::EthRpcMethod;

    use super::*;
    use serde_json::json;
    use std::time::Duration;

    // Helper function to create a mock Rpc object
    fn mock_rpc(url: &str) -> Rpc {
        Rpc::new(
            format!("http://{}", url).parse().unwrap(),
            Some(format!("ws://{}", url).parse().unwrap()),
            10000,
            1,
            10.0,
        )
    }

    async fn create_mock_rpc_list() -> Arc<RwLock<Vec<Rpc>>> {
        Arc::new(RwLock::new(vec![
            Rpc::new(
                "http://test1".parse().unwrap(),
                Some("ws://test1".parse().unwrap()),
                0,
                0,
                0.0,
            ),
            Rpc::new(
                "http://test2".parse().unwrap(),
                Some("ws://test2".parse().unwrap()),
                0,
                0,
                0.0,
            ),
        ]))
    }

    // Helper function to setup the environment for ws_conn_manager tests
    #[allow(clippy::type_complexity)]
    fn setup_ws_conn_manager_test() -> (
        Arc<RwLock<Vec<Rpc>>>,
        mpsc::UnboundedSender<WsconnMessage>,
        mpsc::UnboundedReceiver<WsconnMessage>,
        broadcast::Sender<IncomingResponse>,
        mpsc::UnboundedSender<WsChannelErr>,
    ) {
        let rpc_list = Arc::new(RwLock::new(vec![
            mock_rpc("node1.example.com"),
            mock_rpc("node2.example.com"),
        ]));
        let (incoming_tx, incoming_rx) = mpsc::unbounded_channel();
        let (broadcast_tx, _) = broadcast::channel(10);
        let (ws_error_tx, _) = mpsc::unbounded_channel();

        (
            rpc_list,
            incoming_tx,
            incoming_rx,
            broadcast_tx,
            ws_error_tx,
        )
    }

    #[tokio::test]
    async fn test_handle_incoming_message() {
        let rpc_list = create_mock_rpc_list().await;
        let (tx, mut rx) = mpsc::unbounded_channel();
        let ws_handles = Arc::new(RwLock::new(vec![WsConnection {
            rpc: rpc_list.read().unwrap()[0].clone(),
            sender: Some(tx),
        }]));
        let incoming = json!({"type": "test"});
        let (broadcast_tx, _) = broadcast::channel(1);

        handle_incoming_message(
            &ws_handles,
            &rpc_list,
            incoming.clone(),
            Some(0),
            &broadcast_tx,
        );

        // Check if the message was sent through the channel
        let received = rx.recv().await;
        assert_eq!(received, Some(incoming));
    }

    #[tokio::test]
    async fn test_ws_conn_handling_error() {
        let (_rpc_list, incoming_tx, mut incoming_rx, _broadcast_tx, _ws_error_tx) =
            setup_ws_conn_manager_test();

        // Sending a message that should cause an error
        let invalid_message = json!({"invalid": "message"});
        incoming_tx
            .send(WsconnMessage::Message(invalid_message, None))
            .unwrap();

        // Expecting an error response
        if let Some(WsconnMessage::Message(_, _)) = incoming_rx.recv().await {
            // Handling error cases here
        }
    }

    #[tokio::test]
    async fn test_execute_ws_subscription_and_call() {
        let (incoming_tx, mut incoming_rx) = mpsc::unbounded_channel();
        let (broadcast_tx, broadcast_rx) = broadcast::channel(10);
        let sub_data = Arc::new(SubscriptionData::new());
        let cache_args = CacheArgs::default();
        let upstream =
            tokio::spawn(async move {
                while let Some(WsconnMessage::Message(request, _)) = incoming_rx.recv().await {
                    broadcast_tx.send(IncomingResponse {
                    content: json!({"jsonrpc":"2.0", "id":request["id"], "result":"0x1a2b3c"}),
                    node_id: 0,
                }).unwrap();
                }
            });
        for method in [EthRpcMethod::Subscribe, EthRpcMethod::BlockNumber] {
            let result = execute_ws_call(
                json!({"jsonrpc":"2.0", "id":1, "method":method, "params":["newHeads"]}),
                1,
                &incoming_tx,
                broadcast_rx.resubscribe(),
                &sub_data,
                &cache_args,
            )
            .await
            .unwrap();
            assert_eq!(
                result,
                "{\"id\":1,\"jsonrpc\":\"2.0\",\"result\":\"0x1a2b3c\"}"
            );
        }
        upstream.abort();
    }

    #[tokio::test]
    async fn batch_timeout_does_not_misroute_a_late_response() {
        let (incoming_tx, mut incoming_rx) = mpsc::unbounded_channel();
        let (broadcast_tx, broadcast_rx) = broadcast::channel(10);
        let upstream = tokio::spawn(async move {
            let Some(WsconnMessage::Message(first, _)) = incoming_rx.recv().await else {
                panic!("first request missing")
            };
            let Some(WsconnMessage::Message(second, _)) = incoming_rx.recv().await else {
                panic!("second request missing")
            };
            assert_ne!(first["id"], second["id"]);
            for (request, result) in [(first, "late"), (second, "current")] {
                broadcast_tx
                    .send(IncomingResponse {
                        node_id: 0,
                        content: json!({"jsonrpc":"2.0", "id":request["id"], "result":result}),
                    })
                    .unwrap();
            }
        });
        let batch = json!([
            {"jsonrpc":"2.0", "id":"first", "method":"eth_chainId"},
            {"jsonrpc":"2.0", "id":"second", "method":"eth_chainId"}
        ]);
        let response = execute_ws_request(
            batch,
            1,
            &incoming_tx,
            broadcast_rx,
            &Arc::new(SubscriptionData::new()),
            &CacheArgs::default(),
            Duration::from_millis(50),
        )
        .await
        .unwrap();
        let response: Value = serde_json::from_str(&response).unwrap();
        assert_eq!(response[0]["id"], "first");
        assert_eq!(response[0]["error"]["code"], -32001);
        assert_eq!(response[1]["id"], "second");
        assert_eq!(response[1]["result"], "current");
        upstream.await.unwrap();
    }

    #[tokio::test]
    async fn invalid_ws_requests_return_errors_without_dispatch() {
        let (incoming_tx, mut incoming_rx) = mpsc::unbounded_channel();
        let (_broadcast_tx, broadcast_rx) = broadcast::channel(1);
        let sub_data = Arc::new(SubscriptionData::new());
        let cache_args = CacheArgs::default();
        for call in [
            json!([]),
            json!(null),
            json!(false),
            json!({}),
            json!({"jsonrpc":"2.0","id":"original","method":false}),
        ] {
            let response = execute_ws_call(
                call.clone(),
                1,
                &incoming_tx,
                broadcast_rx.resubscribe(),
                &sub_data,
                &cache_args,
            )
            .await
            .unwrap();
            let response: Value = serde_json::from_str(&response).unwrap();
            assert_eq!(response["error"]["code"], -32600);
            assert_eq!(response["id"], call["id"]);
            assert!(incoming_rx.try_recv().is_err());
        }
    }

    #[cfg(not(feature = "no-cache"))]
    #[tokio::test]
    async fn cached_ws_calls_follow_head_and_preserve_ids() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let (incoming_tx, mut incoming_rx) = mpsc::unbounded_channel();
        let (broadcast_tx, broadcast_rx) = broadcast::channel(10);
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_clone = calls.clone();
        let upstream = tokio::spawn(async move {
            while let Some(WsconnMessage::Message(request, _)) = incoming_rx.recv().await {
                calls_clone.fetch_add(1, Ordering::SeqCst);
                broadcast_tx.send(IncomingResponse {
                    node_id: 0,
                    content: json!({"jsonrpc": "2.0", "id": request["id"], "result": request["params"][1]}),
                }).unwrap();
            }
        });
        let cache_args = CacheArgs::default();
        let sub_data = Arc::new(SubscriptionData::new());
        for (id, head, expected_calls) in [
            (json!("first"), 16, 1),
            (json!(-7), 16, 1),
            (json!("third"), 17, 2),
        ] {
            cache_args.named_numbers.write().unwrap().latest = head;
            let call = json!({"jsonrpc": "2.0", "id": id, "method": "eth_getBalance", "params": ["0x1", "latest"]});
            let response = tokio::time::timeout(
                Duration::from_secs(5),
                execute_ws_call(
                    call,
                    1,
                    &incoming_tx,
                    broadcast_rx.resubscribe(),
                    &sub_data,
                    &cache_args,
                ),
            )
            .await
            .unwrap()
            .unwrap();
            let response: Value = serde_json::from_str(&response).unwrap();
            assert_eq!(response["id"], id);
            assert_eq!(response["result"], format!("0x{head:x}"));
            assert_eq!(calls.load(Ordering::SeqCst), expected_calls);
        }
        upstream.abort();
    }

    #[tokio::test]
    async fn test_listen_for_response() {
        let (broadcast_tx, broadcast_rx) = broadcast::channel(10);

        // Simulate a response
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            let response = IncomingResponse {
                content: json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "result": "0x1a2b3c"
                }),
                node_id: 0,
            };
            broadcast_tx.send(response).unwrap();
        });

        let result = listen_for_response(1, broadcast_rx).await;
        assert!(result.is_ok());
        assert_eq!(
            result.unwrap().content,
            json!({
                "jsonrpc": "2.0",
                "id": 1,
                "result": "0x1a2b3c"
            })
        );
    }
}

#[cfg(test)]
mod connection_tests {
    use super::*;
    use serde_json::json;
    use std::time::Duration;
    use tokio::{io::AsyncReadExt, net::TcpListener, time::timeout};

    #[tokio::test]
    async fn stalled_handshake_is_cancelled_and_socket_is_closed() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buffer = [0; 4096];
            assert!(socket.read(&mut buffer).await.unwrap() > 0);
            while socket.read(&mut buffer).await.unwrap() > 0 {}
        });
        let rpc = Rpc::new(
            format!("http://{address}").parse().unwrap(),
            Some(format!("ws://{address}").parse().unwrap()),
            1,
            0,
            1.0,
        );
        let rpcs = Arc::new(RwLock::new(vec![rpc]));
        let (responses, _) = broadcast::channel(1);
        let (errors, _) = mpsc::unbounded_channel();
        let handles = Arc::new(RwLock::new(Vec::new()));
        update_ws_connections(
            &rpcs,
            &handles,
            &responses,
            &errors,
            Duration::from_millis(100),
        )
        .await;
        assert!(handles.read().unwrap()[0].sender.is_none());
        timeout(Duration::from_secs(2), server)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn replacing_sender_closes_socket_after_ping_and_response() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut stream = tokio_tungstenite::accept_async(socket).await.unwrap();
            stream.send(Message::Ping(vec![1].into())).await.unwrap();
            assert_eq!(
                stream.next().await.unwrap().unwrap(),
                Message::Pong(vec![1].into())
            );
            stream
                .send(Message::Text(
                    json!({"jsonrpc": "2.0", "id": 7, "result": true})
                        .to_string()
                        .into(),
                ))
                .await
                .unwrap();
            loop {
                match stream.next().await {
                    None | Some(Err(_)) | Some(Ok(Message::Close(_))) => break,
                    _ => {}
                }
            }
        });
        let rpc = Rpc::new(
            format!("http://{address}").parse().unwrap(),
            Some(format!("ws://{address}").parse().unwrap()),
            1,
            0,
            1.0,
        );
        let rpcs = Arc::new(RwLock::new(vec![rpc]));
        let (responses, mut received) = broadcast::channel(1);
        let (errors, _errors_rx) = mpsc::unbounded_channel();
        let handles = Arc::new(RwLock::new(Vec::new()));
        update_ws_connections(&rpcs, &handles, &responses, &errors, Duration::from_secs(1)).await;
        assert!(handles.read().unwrap()[0].sender.is_some());
        let response = timeout(Duration::from_secs(2), received.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(response.content["id"], 7);
        drop(handles);
        timeout(Duration::from_secs(2), server)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn reconnect_preserves_healthy_subscription_and_stable_slot() {
        let failed_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let failed_address = failed_listener.local_addr().unwrap();
        let failed_server = tokio::spawn(async move {
            let (socket, _) = failed_listener.accept().await.unwrap();
            let mut stream = tokio_tungstenite::accept_async(socket).await.unwrap();
            assert!(!matches!(stream.next().await, Some(Ok(Message::Text(_)))));
        });
        let healthy_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let healthy_address = healthy_listener.local_addr().unwrap();
        let healthy_server = tokio::spawn(async move {
            let (socket, _) = healthy_listener.accept().await.unwrap();
            let mut stream = tokio_tungstenite::accept_async(socket).await.unwrap();
            let subscribe = stream.next().await.unwrap().unwrap().into_text().unwrap();
            let subscribe: Value = serde_json::from_str(&subscribe).unwrap();
            assert_eq!(subscribe["method"], "eth_subscribe");
            stream
                .send(Message::Text(
                    json!({"jsonrpc":"2.0", "id":subscribe["id"], "result":"healthy-sub"})
                        .to_string()
                        .into(),
                ))
                .await
                .unwrap();
            let next_request = stream.next().await.unwrap().unwrap().into_text().unwrap();
            let next_request: Value = serde_json::from_str(&next_request).unwrap();
            assert_eq!(next_request["id"], 8);
            stream.send(Message::Text(json!({"jsonrpc":"2.0", "method":"eth_subscription", "params":{"subscription":"healthy-sub", "result":"after-reconnect"}}).to_string().into())).await.unwrap();
            assert!(!matches!(stream.next().await, Some(Ok(Message::Text(_)))));
        });
        let rpcs = Arc::new(RwLock::new(
            [failed_address, healthy_address]
                .map(|address| {
                    Rpc::new(
                        format!("http://{address}").parse().unwrap(),
                        Some(format!("ws://{address}").parse().unwrap()),
                        1,
                        0,
                        1.0,
                    )
                })
                .to_vec(),
        ));
        let (responses, mut received) = broadcast::channel(4);
        let (errors, _errors_rx) = mpsc::unbounded_channel();
        let handles = Arc::new(RwLock::new(Vec::new()));
        update_ws_connections(&rpcs, &handles, &responses, &errors, Duration::from_secs(1)).await;
        let request =
            json!({"jsonrpc":"2.0", "id":7, "method":"eth_subscribe", "params":["newHeads"]});
        handle_incoming_message(&handles, &rpcs, request.clone(), Some(1), &responses);
        let subscribed = timeout(Duration::from_secs(2), received.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(subscribed.node_id, 1);
        assert_eq!(subscribed.content["result"], "healthy-sub");
        let subscriptions = SubscriptionData::new();
        let (user_tx, mut user_rx) = mpsc::unbounded_channel();
        subscriptions.add_user(1, user_tx);
        subscriptions.register_subscription(request.clone(), "healthy-sub".to_owned(), 1);
        subscriptions.subscribe_user(1, request).unwrap();
        let healthy_sender = handles.read().unwrap()[1].sender.clone().unwrap();

        rpcs.write().unwrap().remove(0);
        update_ws_connections(&rpcs, &handles, &responses, &errors, Duration::from_secs(1)).await;
        assert!(handles.read().unwrap()[0].sender.is_none());
        assert!(
            handles.read().unwrap()[1]
                .sender
                .as_ref()
                .unwrap()
                .same_channel(&healthy_sender)
        );
        timeout(Duration::from_secs(2), failed_server)
            .await
            .unwrap()
            .unwrap();
        handle_incoming_message(
            &handles,
            &rpcs,
            json!({"jsonrpc":"2.0", "id":8, "method":"eth_blockNumber"}),
            None,
            &responses,
        );
        let notification = timeout(Duration::from_secs(2), received.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(notification.node_id, 1);
        subscriptions
            .dispatch_to_subscribers(
                "healthy-sub",
                notification.node_id,
                &crate::websocket::types::RequestResult::Subscription(notification.content),
            )
            .await
            .unwrap();
        let notification: Value = timeout(Duration::from_secs(2), user_rx.recv())
            .await
            .unwrap()
            .unwrap()
            .into();
        assert_eq!(notification["params"]["subscription"], "healthy-sub");
        assert_eq!(notification["params"]["result"], "after-reconnect");
        drop(healthy_sender);
        drop(handles);
        timeout(Duration::from_secs(2), healthy_server)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn unavailable_upstream_returns_error_without_buffering_subscriptions() {
        let rpcs = Arc::new(RwLock::new(Vec::new()));
        let handles = Arc::new(RwLock::new(Vec::new()));
        let (responses, mut received) = broadcast::channel(1);
        handle_incoming_message(
            &handles,
            &rpcs,
            json!({"jsonrpc": "2.0", "id": 7, "method": "eth_subscribe", "params": ["newHeads"]}),
            None,
            &responses,
        );
        let response = received.recv().await.unwrap();
        assert_eq!(response.content["id"], 7);
        assert_eq!(response.content["error"]["code"], -32000);
    }

    #[tokio::test]
    async fn requests_skip_endpoints_without_live_websockets() {
        let failed = Rpc::new("http://localhost/failed".parse().unwrap(), None, 1, 0, 1.0);
        let healthy = Rpc::new("http://localhost/healthy".parse().unwrap(), None, 1, 0, 1.0);
        let rpcs = Arc::new(RwLock::new(vec![failed.clone(), healthy.clone()]));
        let (sender, mut receiver) = mpsc::unbounded_channel();
        let handles = Arc::new(RwLock::new(vec![
            WsConnection {
                rpc: failed,
                sender: None,
            },
            WsConnection {
                rpc: healthy,
                sender: Some(sender),
            },
        ]));
        let (responses, mut errors) = broadcast::channel(1);
        let request = json!({"jsonrpc":"2.0", "id":7, "method":"eth_blockNumber"});
        handle_incoming_message(&handles, &rpcs, request.clone(), None, &responses);
        assert_eq!(receiver.try_recv().unwrap(), request);
        assert!(errors.try_recv().is_err());
        assert_eq!(rpcs.read().unwrap().len(), 2);
        drop(receiver);
        handle_incoming_message(&handles, &rpcs, request, None, &responses);
        assert_eq!(errors.try_recv().unwrap().content["error"]["code"], -32000);
    }
}
