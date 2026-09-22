use crate::{
    config::system::WS_SUB_MANAGER_ID,
    rpc::method::EthRpcMethod,
    websocket::{
        error::WsError,
        types::{IncomingResponse, RequestResult, SubscriptionData, WsconnMessage},
    },
};

use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use tokio::sync::{
    broadcast::{self, error::RecvError},
    mpsc,
};

use serde_json::json;

/// Sends all subscriptions to their relevant nodes
pub async fn subscription_dispatcher(
    mut rx: broadcast::Receiver<IncomingResponse>,
    incoming_tx: mpsc::UnboundedSender<WsconnMessage>,
    sub_data: Arc<SubscriptionData>,
) -> Result<(), WsError> {
    loop {
        // Receive the WS response
        let response = match rx.recv().await {
            Ok(rax) => rax,
            Err(RecvError::Closed) => return Err(WsError::ChannelClosed()),
            Err(RecvError::Lagged(_)) => return Err(WsError::ReceiverLagged()),
        };

        // Check if its a subscription
        if response.content["method"].ne(&EthRpcMethod::Subscription) {
            continue;
        }

        tracing::debug!(
            ?response.content,
            "subscription_dispatcher: received subscription"
        );

        // Get the subscription id
        // this is retarded???
        let resp_clone = response.clone();
        let id = match response.content["params"]["subscription"].as_str() {
            Some(rax) => rax,
            None => continue, // if this doesnt exist something in the pipeline is wrong and should be ignored
        };

        // Send the response to all the users
        match sub_data
            .dispatch_to_subscribers(
                id,
                response.node_id,
                &RequestResult::Subscription(resp_clone.content),
            )
            .await
        {
            // Getting true means that we should unsubscribe from the subscription
            // as thre are no more users needing it.
            Ok(true) => {
                let unsub = json!({"jsonrpc": "2.0","id": WS_SUB_MANAGER_ID, "method": EthRpcMethod::Unsubscribe, "params": [id]});
                let message = WsconnMessage::Message(unsub, Some(response.node_id));
                let _ = incoming_tx.send(message);
            }
            // False means tht we do not need to do anything
            Ok(false) => {}
            Err(e) => {
                tracing::error!(?e, "Fatal error while trying to send subscriptions");
            }
        };
    }
}

/// Moves all subscriptions from one node to another one.
/// Used during node failiure. *Do not* use this liberally as it is very heavy.
pub async fn move_subscriptions(
    incoming_tx: &mpsc::UnboundedSender<WsconnMessage>,
    mut rx: broadcast::Receiver<IncomingResponse>,
    sub_data: &Arc<SubscriptionData>,
    node_id: usize,
    ttl: Duration,
) -> Result<(), WsError> {
    // Collect all subscriptions/ids we have assigned to `node_id` and put them in a vec
    let subs = sub_data.get_subscription_by_node(node_id);
    let ids = sub_data.get_sub_id_by_node(node_id);

    // We want to send unsubscribe messages (for postoriety) to node_id
    for id in ids {
        let unsub = json!({"jsonrpc": "2.0", "id": WS_SUB_MANAGER_ID, "method": EthRpcMethod::Unsubscribe, "params": [id]});
        let message = WsconnMessage::Message(unsub, Some(node_id));
        let _ = incoming_tx.send(message);
    }

    static NEXT_REQUEST_ID: AtomicU64 = AtomicU64::new(1 << 63);
    let mut pairs = HashMap::new();
    for params in subs {
        let id = NEXT_REQUEST_ID.fetch_add(1, Ordering::Relaxed);
        let parsed: serde_json::Value =
            serde_json::from_str(&params).map_err(|_| WsError::FailedParsing())?;
        let sub = json!({"jsonrpc": "2.0", "id": id, "method": EthRpcMethod::Subscribe, "params": parsed});
        pairs.insert(id, params);
        incoming_tx.send(WsconnMessage::Message(sub, None))?;
    }

    let deadline = tokio::time::Instant::now() + ttl;
    while !pairs.is_empty() {
        let response = tokio::time::timeout_at(deadline, rx.recv())
            .await
            .map_err(|_| WsError::NoWsResponse)??;
        let Some(pair_id) = response.content["id"].as_u64() else {
            continue;
        };
        let Some(params) = pairs.remove(&pair_id) else {
            continue;
        };
        let sub_id = response.content["result"]
            .as_str()
            .ok_or_else(|| WsError::InvalidData("Upstream rejected subscription".to_string()))?;
        sub_data.move_subscriptions(response.node_id, params, sub_id.to_string())?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::rpc::method::EthRpcMethod;

    use super::*;
    use serde_json::json;
    use std::time::Duration;

    #[tokio::test]
    async fn test_subscription_dispatcher() {
        let (tx, rx) = broadcast::channel(10);
        let (incoming_tx, _incoming_rx) = mpsc::unbounded_channel();
        let sub_data = Arc::new(SubscriptionData::new());
        let user_id = 1;
        let subscription_id = "sub123";

        // Mock user and subscription setup
        let (user_tx, mut user_rx) = mpsc::unbounded_channel();
        sub_data.add_user(user_id, user_tx);

        let subscription_request = json!({"jsonrpc":"2.0", "id": 1, "method": EthRpcMethod::Subscribe, "params": ["newHeads"]});
        sub_data.register_subscription(
            subscription_request.clone(),
            subscription_id.to_string(),
            0,
        );
        sub_data
            .subscribe_user(user_id, subscription_request)
            .unwrap();

        tokio::spawn(async move {
            let _ = subscription_dispatcher(rx, incoming_tx, Arc::clone(&sub_data)).await;
        });

        let subscription_content = json!({"method": EthRpcMethod::Subscription, "params": {"subscription": subscription_id}});
        let incoming_response = IncomingResponse {
            content: subscription_content,
            node_id: 0,
        };
        tx.send(incoming_response).unwrap();

        // Check if the user receives the message
        if let Some(RequestResult::Subscription(msg)) = user_rx.recv().await {
            assert_eq!(
                msg,
                json!({"method": EthRpcMethod::Subscription, "params": {"subscription": subscription_id}})
            );
        } else {
            panic!("User did not receive the expected message.");
        }
    }

    #[tokio::test]
    async fn test_move_subscriptions() {
        let (incoming_tx, mut incoming_rx) = mpsc::unbounded_channel();
        let (tx, rx) = broadcast::channel(10);
        let sub_data = Arc::new(SubscriptionData::new());
        let node_id = 1;
        let user_id = 2;

        // Setup for subscriptions
        let subscription_request = json!({"jsonrpc":"2.0","id": 2, "method": EthRpcMethod::Subscribe, "params": ["newHeads"]});
        sub_data.register_subscription(subscription_request.clone(), "sub789".to_string(), node_id);
        sub_data
            .subscribe_user(user_id, subscription_request)
            .unwrap();

        // Spawn a thread to handle incoming subscription requests
        let tx_clone = tx.clone();
        tokio::spawn(async move {
            while let Some(WsconnMessage::Message(message, _)) = incoming_rx.recv().await {
                if message["method"].eq(&EthRpcMethod::Subscribe) {
                    let id = message["id"].as_u64().unwrap();
                    let random_result = rand::random::<u64>().to_string();
                    let mock_response = IncomingResponse {
                        content: json!({"jsonrpc": "2.0", "id": id, "result": random_result}),
                        node_id: 2, // new node ID
                    };
                    tokio::time::sleep(Duration::from_millis(50)).await; // Simulate network delay
                    tx_clone.send(mock_response).unwrap();
                }
            }
        });

        // Execute move_subscriptions
        let move_result = move_subscriptions(
            &incoming_tx,
            rx,
            &Arc::clone(&sub_data),
            node_id,
            Duration::from_secs(1),
        )
        .await;
        assert!(move_result.is_ok(), "move_subscriptions should succeed");

        // Verify the mock responses have been processed and subscriptions moved
        let og_subs = sub_data.get_subscription_by_node(1);
        assert!(
            og_subs.is_empty(),
            "Subscriptions should have been moved to the new node"
        );

        let moved_subs = sub_data.get_subscription_by_node(2); // new node ID
        assert!(
            !moved_subs.is_empty(),
            "Subscriptions should have been moved to the new node"
        );
    }
    #[tokio::test]
    async fn migration_preserves_filter_and_uses_new_upstream_subscription_id() {
        let (incoming_tx, mut incoming_rx) = mpsc::unbounded_channel();
        let (responses, response_rx) = broadcast::channel(4);
        let subscriptions = Arc::new(SubscriptionData::new());
        let (user_tx, mut user_rx) = mpsc::unbounded_channel();
        subscriptions.add_user(1, user_tx);
        let params = json!(["logs", {"address": "0x1234", "topics": []}]);
        let request = json!({"params": params});
        subscriptions.register_subscription(request.clone(), "old".to_string(), 1);
        subscriptions.subscribe_user(1, request).unwrap();
        let upstream = tokio::spawn(async move {
            while let Some(WsconnMessage::Message(message, _)) = incoming_rx.recv().await {
                if message["method"] != "eth_subscribe" {
                    continue;
                }
                assert_eq!(message["params"], params);
                responses
                    .send(IncomingResponse {
                        node_id: 3,
                        content: json!({"method": "eth_subscription", "params": {}}),
                    })
                    .unwrap();
                responses
                    .send(IncomingResponse {
                        node_id: 2,
                        content: json!({"id": message["id"], "result": "new"}),
                    })
                    .unwrap();
                return;
            }
        });
        move_subscriptions(
            &incoming_tx,
            response_rx,
            &subscriptions,
            1,
            Duration::from_secs(1),
        )
        .await
        .unwrap();
        upstream.await.unwrap();
        assert!(subscriptions.get_sub_id_by_node(1).is_empty());
        assert_eq!(subscriptions.get_sub_id_by_node(2), vec!["new"]);
        assert!(subscriptions.get_users_for_subscription("old").is_empty());
        let notification = json!({"params": {"subscription": "new", "result": "event"}});
        subscriptions
            .dispatch_to_subscribers("new", 2, &RequestResult::Subscription(notification.clone()))
            .await
            .unwrap();
        match user_rx.recv().await.unwrap() {
            RequestResult::Subscription(message) => {
                assert_eq!(message["params"]["subscription"], "old");
                assert_eq!(message["params"]["result"], "event");
            }
            _ => panic!("expected migrated notification"),
        }
        assert_eq!(subscriptions.get_node_from_id("old"), Some(2));
        assert_eq!(
            subscriptions
                .subscribe_user(
                    2,
                    json!({"params":["logs", {"address":"0x1234", "topics":[]}]})
                )
                .unwrap(),
            "old"
        );
        let (_responses, response_rx) = broadcast::channel(1);
        let unsubscribe = crate::websocket::client::execute_ws_call(
            json!({"jsonrpc":"2.0", "id":17, "method":"eth_unsubscribe", "params":["old"]}),
            1,
            &incoming_tx,
            response_rx,
            &subscriptions,
            &crate::balancer::processing::CacheArgs::default(),
        )
        .await
        .unwrap();
        let unsubscribe: serde_json::Value = serde_json::from_str(&unsubscribe).unwrap();
        assert_eq!(
            unsubscribe,
            json!({"jsonrpc":"2.0", "id":17, "result":true})
        );
        subscriptions.unsubscribe_user(2, "old".to_owned());
        assert!(
            subscriptions
                .dispatch_to_subscribers("new", 2, &RequestResult::Subscription(notification))
                .await
                .unwrap()
        );
        assert!(user_rx.try_recv().is_err());
        assert_eq!(subscriptions.get_node_from_id("old"), None);
    }

    #[tokio::test]
    async fn unanswered_migration_times_out_without_losing_subscribers() {
        let (incoming_tx, _incoming_rx) = mpsc::unbounded_channel();
        let (_responses, response_rx) = broadcast::channel(1);
        let subscriptions = Arc::new(SubscriptionData::new());
        let request = json!({"params": ["newHeads"]});
        subscriptions.register_subscription(request.clone(), "old".to_string(), 1);
        subscriptions.subscribe_user(7, request).unwrap();
        let result = tokio::time::timeout(
            Duration::from_secs(1),
            move_subscriptions(
                &incoming_tx,
                response_rx,
                &subscriptions,
                1,
                Duration::from_millis(20),
            ),
        )
        .await
        .unwrap();
        assert!(matches!(result, Err(WsError::NoWsResponse)));
        assert_eq!(subscriptions.get_users_for_subscription("old"), vec![7]);
    }
}
