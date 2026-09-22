use std::sync::Arc;

use crate::{
    balancer::processing::CacheArgs,
    database::types::GenericBytes,
    websocket::{
        client::execute_ws_request,
        error::WsError,
        types::{IncomingResponse, RequestResult, SubscriptionData, WsconnMessage},
    },
};

use rand::random;

use tokio::sync::{broadcast, mpsc};

use simd_json::from_slice;

use futures::{sink::SinkExt, stream::StreamExt};

use hyper_tungstenite::HyperWebsocket;
use tungstenite::Message;

/// Handle a WebSocket connection request.
///
/// Opens a WebSocket connection between Rpsee and a client,
/// sending their requests to be processed.
pub async fn serve_websocket<K, V>(
    websocket: HyperWebsocket,
    incoming_tx: mpsc::UnboundedSender<WsconnMessage>,
    outgoing_rx: broadcast::Receiver<IncomingResponse>,
    sub_data: Arc<SubscriptionData>,
    cache_args: CacheArgs<K, V>,
    ttl: std::time::Duration,
) -> Result<(), WsError>
where
    K: GenericBytes + From<[u8; 32]> + 'static,
    V: GenericBytes + From<Vec<u8>> + 'static,
{
    let websocket = websocket.await?;

    // Split the Sink so we can do async send/recv
    let (mut websocket_sink, mut websocket_stream) = websocket.split();

    // Create channels for message send/receiving
    let (tx, mut rx) = mpsc::unbounded_channel::<RequestResult>();

    // Generate an id for our user
    //
    // We use this to identify which requests are for us
    let user_id = random::<u32>();

    // Add the user to the sink map
    tracing::info!("Adding user {} to sink map", user_id);
    let user_data = tx.clone();
    sub_data.add_user(user_id, user_data);

    let sub_data_clone = sub_data.clone();

    // Spawn taks for sending messages to the client
    let writer = tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            // Forward the message to the best available RPC
            //
            // If we received a subscription, just send it to the client
            match msg {
                RequestResult::Call(call) => {
                    let Some(resp) = execute_ws_request(
                        call,
                        user_id,
                        &incoming_tx,
                        outgoing_rx.resubscribe(),
                        &sub_data_clone,
                        &cache_args,
                        ttl,
                    )
                    .await
                    else {
                        continue;
                    };

                    match websocket_sink.send(Message::text::<String>(resp)).await {
                        Ok(_) => {}
                        Err(e) => {
                            // Remove the user from the sink map
                            sub_data_clone.remove_user(user_id);
                            tracing::error!(?e, "Error sending call");
                            break;
                        }
                    }
                }
                RequestResult::Subscription(sub) => {
                    match websocket_sink
                        .send(Message::text::<String>(sub.to_string()))
                        .await
                    {
                        Ok(_) => {}
                        Err(e) => {
                            // Remove the user from the sink map
                            sub_data_clone.remove_user(user_id);
                            return Err(WsError::MessageSendFailed((e).to_string()));
                        }
                    }
                }
            }
        }
        Ok(())
    });

    let mut result = Ok(());
    while let Some(message) = websocket_stream.next().await {
        match message {
            Ok(Message::Text(msg)) => {
                tracing::info!(%msg, "Received WS text message");
                // Send message to the channel
                let rax = match from_slice(&mut msg.as_bytes().to_vec()) {
                    Ok(rax) => rax,
                    Err(_) => {
                        let error = serde_json::json!({"jsonrpc":"2.0", "id":null, "error":{"code":-32700, "message":"Parse error"}});
                        tx.send(RequestResult::Subscription(error)).unwrap_or(());
                        continue;
                    }
                };

                tx.send(RequestResult::Call(rax)).unwrap_or(());
            }
            Ok(Message::Close(msg)) => {
                if let Some(msg) = &msg {
                    tracing::info!(
                        "Received close message with code {} and message: {}",
                        msg.code,
                        msg.reason
                    );
                } else {
                    tracing::info!("Received close message");
                }
                break;
            }
            Err(e) => {
                result = Err(WsError::MessageReceptionFailed(e.to_string()));
                break;
            }
            _ => {}
        }
    }

    sub_data.remove_user(user_id);
    writer.abort();
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use hyper::{server::conn::http1, service::service_fn};
    use hyper_util::rt::TokioIo;
    use serde_json::{Value, json};
    use std::{
        convert::Infallible,
        sync::atomic::{AtomicUsize, Ordering},
        time::Duration,
    };
    use tokio::net::TcpListener;

    #[tokio::test]
    async fn websocket_batches_return_one_frame_and_suppress_notifications() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = format!("ws://{}", listener.local_addr().unwrap());
        let (incoming_tx, mut incoming_rx) = mpsc::unbounded_channel();
        let (broadcast_tx, broadcast_rx) = broadcast::channel(32);
        let notifications = Arc::new(AtomicUsize::new(0));
        let observed = notifications.clone();
        let upstream = tokio::spawn(async move {
            while let Some(WsconnMessage::Message(request, _)) = incoming_rx.recv().await {
                if request.get("id").is_none() {
                    observed.fetch_add(1, Ordering::SeqCst);
                } else {
                    broadcast_tx.send(IncomingResponse { node_id: 0, content: json!({"jsonrpc":"2.0", "id":request["id"], "result":request["params"]}) }).unwrap();
                }
            }
        });
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            http1::Builder::new()
                .serve_connection(
                    TokioIo::new(stream),
                    service_fn(move |mut request| {
                        let (response, websocket) =
                            hyper_tungstenite::upgrade(&mut request, None).unwrap();
                        let incoming_tx = incoming_tx.clone();
                        let broadcast_rx = broadcast_rx.resubscribe();
                        tokio::spawn(async move {
                            serve_websocket(
                                websocket,
                                incoming_tx,
                                broadcast_rx,
                                Arc::new(SubscriptionData::new()),
                                CacheArgs::default(),
                                Duration::from_secs(1),
                            )
                            .await
                            .unwrap();
                        });
                        async { Ok::<_, Infallible>(response) }
                    }),
                )
                .with_upgrades()
                .await
                .unwrap();
        });
        let (mut websocket, _) = tokio_tungstenite::connect_async(address).await.unwrap();
        let batch = json!([
            {"jsonrpc":"2.0", "id":"one", "method":"eth_chainId", "params":[1]},
            {"jsonrpc":"2.0", "method":"notify", "params":[]},
            false,
            {"jsonrpc":"2.0", "id":null, "method":"eth_chainId", "params":[2]},
            {"jsonrpc":"2.0", "id":-2, "method":"eth_chainId", "params":[3]}
        ]);
        websocket
            .send(Message::text(batch.to_string()))
            .await
            .unwrap();
        let frame = tokio::time::timeout(Duration::from_secs(5), websocket.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let responses: Value = serde_json::from_str(frame.to_text().unwrap()).unwrap();
        assert_eq!(responses.as_array().unwrap().len(), 4);
        assert_eq!(responses[0]["id"], "one");
        assert_eq!(responses[0]["result"], json!([1]));
        assert_eq!(responses[1]["error"]["code"], -32600);
        assert!(responses[2]["id"].is_null());
        assert_eq!(responses[2]["result"], json!([2]));
        assert_eq!(responses[3]["id"], -2);
        for (payload, code) in [("[]", -32600), ("{", -32700)] {
            websocket.send(Message::text(payload)).await.unwrap();
            let frame = tokio::time::timeout(Duration::from_secs(1), websocket.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            let error: Value = serde_json::from_str(frame.to_text().unwrap()).unwrap();
            assert_eq!(error["error"]["code"], code);
        }
        websocket
            .send(Message::text(
                r#"[{"jsonrpc":"2.0","method":"notify"},{"jsonrpc":"2.0","method":"notify"}]"#,
            ))
            .await
            .unwrap();
        websocket
            .send(Message::text(r#"{"jsonrpc":"2.0","method":"notify"}"#))
            .await
            .unwrap();
        websocket
            .send(Message::text(
                r#"{"jsonrpc":"2.0","id":"barrier","method":"eth_chainId","params":[]}"#,
            ))
            .await
            .unwrap();
        let frame = tokio::time::timeout(Duration::from_secs(1), websocket.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let response: Value = serde_json::from_str(frame.to_text().unwrap()).unwrap();
        assert_eq!(response["id"], "barrier");
        assert_eq!(notifications.load(Ordering::SeqCst), 4);
        websocket.close(None).await.unwrap();
        upstream.abort();
        server.abort();
    }
}
