use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, RwLock},
};

use crate::websocket::error::WsError;
use rust_tracing::deps::metrics;
use serde_json::Value;
use tokio::sync::mpsc;

/// RequestResult enum
#[derive(Debug, Clone)]
pub enum RequestResult {
    Call(Value),
    Subscription(Value),
}

impl From<RequestResult> for Value {
    fn from(req: RequestResult) -> Self {
        match req {
            RequestResult::Call(call) => call,
            RequestResult::Subscription(sub) => sub,
        }
    }
}

/// WsconnMessage enum
#[derive(Debug)]
pub enum WsconnMessage {
    // call received from user and optional node index
    Message(Value, Option<usize>),
    Reconnect(),
}

impl From<WsconnMessage> for Value {
    fn from(msg: WsconnMessage) -> Self {
        match msg {
            WsconnMessage::Message(msg, _) => msg,
            WsconnMessage::Reconnect() => Value::Null,
        }
    }
}

/// WsChannelErr enum
#[derive(Debug, Clone)]
pub enum WsChannelErr {
    Closed(usize, crate::Rpc),
}

pub type UserData = mpsc::UnboundedSender<RequestResult>;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct NodeSubInfo {
    pub node_id: usize,
    pub subscription_id: String,
}

#[derive(Debug, Clone)]
pub struct IncomingResponse {
    pub content: Value,
    pub node_id: usize,
}

#[derive(Debug, Clone)]
struct Subscription {
    // The client keeps this ID when the upstream subscription moves.
    client_id: String,
    users: HashSet<u32>,
}

/// Main struct for storing data related to subscriptions and the associated users
/// TODO: we should probably store more data for the sake of compute performance
#[derive(Debug, Clone)]
pub struct SubscriptionData {
    users: Arc<RwLock<HashMap<u32, UserData>>>,
    subscriptions: Arc<RwLock<HashMap<NodeSubInfo, Subscription>>>,
    incoming_subscriptions: Arc<RwLock<HashMap<String, NodeSubInfo>>>,
}

impl SubscriptionData {
    pub fn new() -> Self {
        SubscriptionData {
            users: Arc::new(RwLock::new(HashMap::new())),
            subscriptions: Arc::new(RwLock::new(HashMap::new())),
            incoming_subscriptions: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn add_user(&self, user_id: u32, user_data: UserData) {
        let mut users = self.users.write().unwrap_or_else(|e| e.into_inner());

        if users.insert(user_id, user_data).is_none() {
            metrics::gauge!("ws_users_total").increment(1);
        }
    }

    pub fn remove_user(&self, user_id: u32) {
        // Remove the user from all subscriptions before doing anything
        self.unsubscribe_user_from_all(user_id);

        let mut users = self.users.write().unwrap_or_else(|e| e.into_inner());

        if users.remove(&user_id).is_some() {
            metrics::gauge!("ws_users_total").decrement(1);
        }
    }

    // Used to add a new subscription to the active subscription list
    pub fn register_subscription(
        &self,
        subscription: Value,
        subscription_id: String,
        node_id: usize,
    ) {
        // TODO: pepega
        let subscription = format!("{}", subscription["params"]);

        // Detect duplicates before inserting
        {
            let incoming_subscriptions = self
                .incoming_subscriptions
                .read()
                .unwrap_or_else(|e| e.into_inner());
            if incoming_subscriptions.contains_key(&subscription) {
                metrics::counter!("ws_duplicate_node_subs", "node_id" => node_id.to_string())
                    .increment(1);
            }
        }

        self.raw_register(&subscription, subscription_id, node_id);
    }

    fn raw_register(&self, subscription: &str, subscription_id: String, node_id: usize) {
        let mut incoming_subscriptions = self
            .incoming_subscriptions
            .write()
            .unwrap_or_else(|e| e.into_inner());

        tracing::info!(subscription, "Register_subscription inserting");
        if incoming_subscriptions
            .insert(
                subscription.to_owned(),
                NodeSubInfo {
                    node_id,
                    subscription_id,
                },
            )
            .is_none()
        {
            metrics::gauge!("ws_node_subs_total").increment(1);
        }
    }

    // Subscribe user to existing subscription and return the subscription id
    //
    // If the subscription does not exist, return error
    pub fn subscribe_user(&self, user_id: u32, subscription: Value) -> Result<String, WsError> {
        if subscription["params"].as_array().is_none()
            || subscription["params"].as_array().unwrap().is_empty()
        {
            return Err(WsError::FailedParsing());
        }

        // TODO: pepega
        let subscription = format!("{}", subscription["params"]);
        tracing::info!(subscription, "Subscribe_user finding");

        self.raw_subscribe(user_id, &subscription)
    }

    fn raw_subscribe(&self, user_id: u32, subscription: &String) -> Result<String, WsError> {
        let incoming_subscriptions = self
            .incoming_subscriptions
            .read()
            .unwrap_or_else(|e| e.into_inner());

        let node_sub_info = match incoming_subscriptions.get(subscription) {
            Some(rax) => rax,
            None => return Err(WsError::FailedParsing()),
        };

        let mut subscriptions = self.subscriptions.write().unwrap();
        let subscription = subscriptions
            .entry(node_sub_info.clone())
            .or_insert_with(|| Subscription {
                client_id: node_sub_info.subscription_id.clone(),
                users: HashSet::new(),
            });
        if subscription.users.insert(user_id) {
            metrics::gauge!("ws_user_subs_total").increment(1);
        }
        Ok(subscription.client_id.clone())
    }

    // Unsubscribe a user from a subscription
    pub fn unsubscribe_user(&self, user_id: u32, subscription_id: String) {
        let mut subscriptions = self
            .subscriptions
            .write()
            .unwrap_or_else(|e| e.into_inner());

        for (node, subscription) in subscriptions.iter_mut() {
            if (subscription.client_id == subscription_id
                || node.subscription_id == subscription_id)
                && subscription.users.remove(&user_id)
            {
                metrics::gauge!("ws_user_subs_total").decrement(1);
            }
        }
    }

    // Unsubscribe a user from all of their subscriptions
    pub fn unsubscribe_user_from_all(&self, user_id: u32) {
        let mut subscriptions = self
            .subscriptions
            .write()
            .unwrap_or_else(|e| e.into_inner());

        // Directly unsubscribing the user within the loop
        for subscribers in subscriptions.values_mut() {
            if subscribers.users.remove(&user_id) {
                metrics::gauge!("ws_user_subs_total").decrement(1);
            }
        }
    }

    // Return the node_id for a given subscription_id
    pub fn get_node_from_id(&self, subscription_id: &str) -> Option<usize> {
        if let Some(node_id) =
            self.subscriptions
                .read()
                .unwrap()
                .iter()
                .find_map(|(node, subscription)| {
                    (subscription.client_id == subscription_id).then_some(node.node_id)
                })
        {
            return Some(node_id);
        }
        let incoming_subscriptions = self
            .incoming_subscriptions
            .read()
            .unwrap_or_else(|e| e.into_inner());

        incoming_subscriptions
            .iter()
            .find_map(|(_, node_sub_info)| {
                if node_sub_info.subscription_id == subscription_id {
                    Some(node_sub_info.node_id)
                } else {
                    None
                }
            })
    }

    // Return all sub ids for a given node_id
    pub fn get_sub_id_by_node(&self, node_id: usize) -> Vec<String> {
        let incoming_subscriptions = self
            .incoming_subscriptions
            .read()
            .unwrap_or_else(|e| e.into_inner());

        incoming_subscriptions
            .values()
            .filter_map(|node_sub_info| {
                if node_sub_info.node_id == node_id {
                    Some(node_sub_info.subscription_id.to_owned())
                } else {
                    None
                }
            })
            .collect()
    }

    // Return all subscriptions for a given node_id
    pub fn get_subscription_by_node(&self, node_id: usize) -> Vec<String> {
        let incoming_subscriptions = self
            .incoming_subscriptions
            .read()
            .unwrap_or_else(|e| e.into_inner());

        incoming_subscriptions
            .iter()
            .filter_map(|(subscription, node_sub_info)| {
                if node_sub_info.node_id == node_id {
                    Some(subscription.clone())
                } else {
                    None
                }
            })
            .collect()
    }

    pub fn get_sub_id_by_params(&self, params: &str) -> Option<String> {
        let incoming_subscriptions = self
            .incoming_subscriptions
            .read()
            .unwrap_or_else(|e| e.into_inner());

        incoming_subscriptions
            .iter()
            .find_map(|(subscription, node_sub_info)| {
                if subscription == params {
                    Some(node_sub_info.subscription_id.to_owned())
                } else {
                    None
                }
            })
    }

    // Return a Vec of all users subscribed to a subscription
    #[cfg(test)]
    pub fn get_users_for_subscription(&self, subscription_id: &str) -> Vec<u32> {
        let subscriptions = self.subscriptions.read().unwrap_or_else(|e| e.into_inner());

        let mut users = Vec::new();

        for (node_sub_info, subscribers) in subscriptions.iter() {
            if node_sub_info.subscription_id == subscription_id {
                users.extend(subscribers.users.iter().copied());
                break;
            }
        }

        users
    }

    // Moves all subscription from one node to another
    pub fn move_subscriptions(
        &self,
        target: usize,
        request: String,
        subscription_id: String,
    ) -> Result<(), WsError> {
        let mut incoming = self.incoming_subscriptions.write().unwrap();
        let previous = incoming
            .get_mut(&request)
            .ok_or(WsError::MissingSubscription())?;
        let mut subscriptions = self.subscriptions.write().unwrap();
        let subscription = subscriptions
            .remove(previous)
            .filter(|subscription| !subscription.users.is_empty())
            .ok_or_else(|| WsError::EmptyList("User list empty!".to_string()))?;
        *previous = NodeSubInfo {
            node_id: target,
            subscription_id,
        };
        subscriptions.insert(previous.clone(), subscription);

        Ok(())
    }

    pub async fn dispatch_to_subscribers(
        &self,
        subscription_id: &str,
        node_id: usize,
        message: &RequestResult,
    ) -> Result<bool, WsError> {
        if let RequestResult::Call(_) = message {
            return Err(WsError::InvalidData(
                "Trying to send a call as a subscription!".to_string(),
            ));
        }

        let node_sub_info = NodeSubInfo {
            node_id,
            subscription_id: subscription_id.to_string(),
        };

        let subscription = self
            .subscriptions
            .read()
            .unwrap()
            .get(&node_sub_info)
            .cloned();
        if let Some(subscription) = subscription {
            if subscription.users.is_empty() {
                let mut incoming = self.incoming_subscriptions.write().unwrap();
                let mut subscriptions = self.subscriptions.write().unwrap();
                if !subscriptions
                    .get(&node_sub_info)
                    .is_some_and(|subscription| subscription.users.is_empty())
                {
                    return Ok(false);
                }
                let previous_count = incoming.len();
                incoming.retain(|_, node| node != &node_sub_info);
                metrics::gauge!("ws_node_subs_total")
                    .decrement((previous_count - incoming.len()) as f64);
                subscriptions.remove(&node_sub_info);
                return Ok(true);
            }
            let mut message = message.clone();
            if let RequestResult::Subscription(message) = &mut message
                && let Some(id) = message
                    .get_mut("params")
                    .and_then(|params| params.get_mut("subscription"))
            {
                *id = subscription.client_id.clone().into();
            }
            for user_id in subscription.users {
                let user = self.users.read().unwrap().get(&user_id).cloned();
                if user.is_some_and(|user| user.send(message.clone()).is_err()) {
                    self.unsubscribe_user(user_id, subscription.client_id.clone());
                }
            }
        }

        Ok(false)
    }
}

#[cfg(test)]
mod tests {
    use crate::rpc::method::EthRpcMethod;

    use super::*;
    use serde_json::json;

    fn setup_user_and_subscription_data() -> (
        SubscriptionData,
        u32,
        mpsc::UnboundedReceiver<RequestResult>,
    ) {
        let (tx, rx) = mpsc::unbounded_channel();
        let user_data = tx;
        let user_id = 100;
        let subscription_data = SubscriptionData::new();
        subscription_data.add_user(user_id, user_data);
        (subscription_data, user_id, rx)
    }

    #[tokio::test]
    async fn test_add_and_remove_user() {
        let (subscription_data, user_id, _) = setup_user_and_subscription_data();

        assert!(
            subscription_data
                .users
                .read()
                .unwrap()
                .contains_key(&user_id)
        );
        subscription_data.remove_user(user_id);
        assert!(
            !subscription_data
                .users
                .read()
                .unwrap()
                .contains_key(&user_id)
        );
    }

    #[tokio::test]
    async fn test_get_node_from_id() {
        let subscription_data = SubscriptionData::new();

        // Setup test data
        let node_id = 42;
        let subscription_id = "sub123".to_string();
        let subscription_request = json!({"jsonrpc":"2.0","id": 2, "method": EthRpcMethod::Subscribe, "params": ["newHeads"]});

        // Register a subscription
        subscription_data.register_subscription(
            subscription_request,
            subscription_id.clone(),
            node_id,
        );

        // Verify that get_node_from_id returns the correct node_id
        assert_eq!(
            subscription_data.get_node_from_id(&subscription_id),
            Some(node_id),
            "get_node_from_id should return the correct node_id"
        );

        // Verify for a non-existent subscription_id
        assert_eq!(
            subscription_data.get_node_from_id("nonexistent"),
            None,
            "get_node_from_id should return None for a non-existent subscription_id"
        );
    }

    #[tokio::test]
    async fn test_subscribe_and_unsubscribe_user() {
        let (subscription_data, user_id, _) = setup_user_and_subscription_data();
        let subscription_request = json!({"jsonrpc":"2.0","id": 2, "method": EthRpcMethod::Subscribe, "params": ["newHeads"]});
        let subscription_id = "200".to_string();
        let node_id = 1;

        subscription_data.register_subscription(
            subscription_request.clone(),
            subscription_id.clone(),
            node_id,
        );
        subscription_data
            .subscribe_user(user_id, subscription_request.clone())
            .unwrap();
        assert!(
            subscription_data
                .subscriptions
                .read()
                .unwrap()
                .iter()
                .any(|(k, v)| {
                    k.node_id == node_id
                        && k.subscription_id == subscription_id
                        && v.users.contains(&user_id)
                })
        );

        subscription_data.unsubscribe_user(user_id, subscription_id.clone());
        assert!(
            !subscription_data
                .subscriptions
                .read()
                .unwrap()
                .iter()
                .any(|(k, v)| {
                    k.node_id == node_id
                        && k.subscription_id == subscription_id
                        && v.users.contains(&user_id)
                })
        );
    }

    #[tokio::test]
    async fn test_unsubscribe_user_from_all() {
        let (subscription_data, user_id, _) = setup_user_and_subscription_data();
        let subscription_request = json!({"jsonrpc":"2.0","id": 2, "method": EthRpcMethod::Subscribe, "params": ["newHeads"]});
        let subscription_id = "200".to_string();
        let node_id = 1;

        subscription_data.register_subscription(
            subscription_request.clone(),
            subscription_id.clone(),
            node_id,
        );
        subscription_data
            .subscribe_user(user_id, subscription_request.clone())
            .unwrap();
        assert!(
            subscription_data
                .subscriptions
                .read()
                .unwrap()
                .iter()
                .any(|(k, v)| {
                    k.node_id == node_id
                        && k.subscription_id == subscription_id
                        && v.users.contains(&user_id)
                })
        );

        subscription_data.unsubscribe_user_from_all(user_id);
        assert!(
            !subscription_data
                .subscriptions
                .read()
                .unwrap()
                .iter()
                .any(|(k, v)| {
                    k.node_id == node_id
                        && k.subscription_id == subscription_id
                        && v.users.contains(&user_id)
                })
        );
    }

    #[tokio::test]
    async fn test_dispatch_to_subscribers() {
        let (subscription_data, user_id, mut rx) = setup_user_and_subscription_data();
        let subscription_request = json!({"jsonrpc":"2.0","id": 2, "method": EthRpcMethod::Subscribe, "params": ["newHeads"]});
        let subscription_id = "300".to_string();
        let node_id = 1;
        let message =
            RequestResult::Subscription(serde_json::Value::String("test message".to_string()));

        subscription_data.register_subscription(
            subscription_request.clone(),
            subscription_id.clone(),
            node_id,
        );
        subscription_data
            .subscribe_user(user_id, subscription_request)
            .unwrap();
        subscription_data
            .dispatch_to_subscribers(&subscription_id, node_id, &message)
            .await
            .unwrap();

        match rx.recv().await {
            Some(RequestResult::Subscription(msg)) => assert_eq!(msg, "test message"),
            _ => panic!("Expected to receive a subscription message"),
        }
    }

    #[tokio::test]
    async fn test_remove_nonexistent_user() {
        let (subscription_data, _, _) = setup_user_and_subscription_data();
        let non_existent_user_id = 999;

        assert!(
            !subscription_data
                .users
                .read()
                .unwrap()
                .contains_key(&non_existent_user_id)
        );
        subscription_data.remove_user(non_existent_user_id);
        assert!(
            !subscription_data
                .users
                .read()
                .unwrap()
                .contains_key(&non_existent_user_id)
        );
    }

    #[tokio::test]
    async fn test_unsubscribe_nonexistent_subscription() {
        let (subscription_data, user_id, _) = setup_user_and_subscription_data();
        let nonexistent_subscription_id = "sub400".to_string();
        let nonexistent_node_id = 10000;

        let nonexistent_node_sub_info = NodeSubInfo {
            node_id: nonexistent_node_id,
            subscription_id: nonexistent_subscription_id.clone(),
        };

        subscription_data.unsubscribe_user(user_id, nonexistent_subscription_id.clone());
        assert!(
            subscription_data
                .subscriptions
                .read()
                .unwrap()
                .get(&nonexistent_node_sub_info)
                .is_none()
        );
    }

    #[tokio::test]
    async fn test_get_sub_id_by_node() {
        let subscription_data = SubscriptionData::new();
        let node_id = 10;
        let subscription_id = "sub123".to_string();

        let subscription_request = json!({"params": ["newHeads"]});
        subscription_data.register_subscription(
            subscription_request,
            subscription_id.clone(),
            node_id,
        );

        let sub_ids = subscription_data.get_sub_id_by_node(node_id);
        assert_eq!(sub_ids, vec![subscription_id]);

        let sub_ids_for_nonexistent_node = subscription_data.get_sub_id_by_node(999);
        assert!(sub_ids_for_nonexistent_node.is_empty());
    }

    #[tokio::test]
    async fn test_get_sub_id_by_node_with_multiple_subscriptions() {
        let subscription_data = SubscriptionData::new();
        let node_id = 10;

        let subscription_request_1 = json!({"params": ["newHeads"]});
        let subscription_request_2 = json!({"params": ["logs"]});

        subscription_data.register_subscription(
            subscription_request_1,
            "sub123".to_string(),
            node_id,
        );
        subscription_data.register_subscription(
            subscription_request_2,
            "sub456".to_string(),
            node_id,
        );

        let sub_ids = subscription_data.get_sub_id_by_node(node_id);
        assert_eq!(sub_ids.len(), 2);
        assert!(sub_ids.contains(&"sub123".to_string()));
        assert!(sub_ids.contains(&"sub456".to_string()));
    }

    #[tokio::test]
    async fn test_get_subscription_by_node() {
        let subscription_data = SubscriptionData::new();
        let node_id = 20;
        let subscription_params = vec!["newHeads"];
        let subscription_request_str = serde_json::to_string(&subscription_params).unwrap();

        let subscription_request = json!({"params": subscription_params});
        subscription_data.register_subscription(
            subscription_request,
            "sub456".to_string(),
            node_id,
        );

        let subscriptions = subscription_data.get_subscription_by_node(node_id);
        assert_eq!(subscriptions, vec![subscription_request_str]);

        let subscriptions_for_nonexistent_node = subscription_data.get_subscription_by_node(999);
        assert!(subscriptions_for_nonexistent_node.is_empty());
    }

    #[tokio::test]
    async fn test_get_sub_id_by_params() {
        // Create a mock SubscriptionData
        let sub_data = SubscriptionData {
            users: Arc::new(RwLock::new(HashMap::new())),
            subscriptions: Arc::new(RwLock::new(HashMap::new())),
            incoming_subscriptions: Arc::new(RwLock::new(HashMap::new())),
        };

        // Mock subscription data
        let params = "newHeads";
        let subscription_id = "sub123";
        let node_id = 1;
        sub_data.incoming_subscriptions.write().unwrap().insert(
            params.to_string(),
            NodeSubInfo {
                node_id,
                subscription_id: subscription_id.to_string(),
            },
        );

        // Test for existing params
        let result = sub_data.get_sub_id_by_params(params);
        assert_eq!(
            result,
            Some(subscription_id.to_string()),
            "Should return the correct subscription ID for existing params"
        );

        // Test for non-existing params
        let non_existing_params = "logs";
        let result = sub_data.get_sub_id_by_params(non_existing_params);
        assert_eq!(result, None, "Should return None for non-existing params");
    }

    #[tokio::test]
    async fn test_get_subscription_by_node_with_multiple_subscriptions() {
        let subscription_data = SubscriptionData::new();
        let node_id = 20;

        let subscription_params_1 = vec!["newHeads"];
        let subscription_request_str_1 = serde_json::to_string(&subscription_params_1).unwrap();

        let subscription_params_2 = vec!["logs", "adasdas"];
        let subscription_request_str_2 = serde_json::to_string(&subscription_params_2).unwrap();

        let subscription_request_1 = json!({"params": subscription_params_1});
        let subscription_request_2 = json!({"params": subscription_params_2});

        subscription_data.register_subscription(
            subscription_request_1,
            "sub789".to_string(),
            node_id,
        );
        subscription_data.register_subscription(
            subscription_request_2,
            "sub101112".to_string(),
            node_id,
        );

        let subscriptions = subscription_data.get_subscription_by_node(node_id);
        assert_eq!(subscriptions.len(), 2);
        assert!(subscriptions.contains(&subscription_request_str_1));
        assert!(subscriptions.contains(&subscription_request_str_2));
    }

    #[tokio::test]
    async fn test_move_subscriptions() {
        let subscription_data = SubscriptionData::new();
        let source_node_id = 30;
        let target_node_id = 31;
        let subscription_request = json!({"params": ["oldHeads"]});
        let subscription_id = "sub789".to_string();

        let (tx, _rx) = mpsc::unbounded_channel();
        let user_id = 123;
        subscription_data.register_subscription(
            subscription_request.clone(),
            subscription_id.clone(),
            source_node_id,
        );

        subscription_data.add_user(user_id, tx);

        subscription_data
            .subscribe_user(user_id, subscription_request)
            .unwrap();

        assert!(
            subscription_data
                .move_subscriptions(
                    target_node_id,
                    r#"["oldHeads"]"#.to_string(),
                    subscription_id.clone()
                )
                .is_ok()
        );

        // Check if user is subscribed to the new node
        let subscriptions = subscription_data.subscriptions.read().unwrap();
        let node_sub_info = NodeSubInfo {
            node_id: target_node_id,
            subscription_id,
        };
        assert!(
            subscriptions
                .get(&node_sub_info)
                .unwrap()
                .users
                .contains(&user_id)
        );

        // Check if subscription has been moved from the old node
        let old_node_sub_info = NodeSubInfo {
            node_id: source_node_id,
            subscription_id: r#"["oldHeads"]"#.to_string(),
        };
        assert!(subscriptions.get(&old_node_sub_info).is_none());
    }

    #[tokio::test]
    async fn test_move_subscriptions_with_no_subscribers() {
        let subscription_data = SubscriptionData::new();
        let source_node_id = 30;
        let target_node_id = 31;
        let subscription_request = "oldHeads".to_string();
        let subscription_id = "sub789".to_string();

        subscription_data.raw_register(
            &subscription_request,
            subscription_id.clone(),
            source_node_id,
        );

        assert!(
            subscription_data
                .move_subscriptions(
                    target_node_id,
                    subscription_request.clone(),
                    subscription_id.clone()
                )
                .is_err()
        );

        // Check if the subscription exists for the target node
        let incoming_subscriptions = subscription_data.incoming_subscriptions.read().unwrap();
        let node_sub_info = incoming_subscriptions.get(&subscription_request).unwrap();

        // Ensure there are no subscribers to the moved subscription
        let subscriptions = subscription_data.subscriptions.read().unwrap();
        assert!(
            subscriptions.get(node_sub_info).is_none()
                || subscriptions.get(node_sub_info).unwrap().users.is_empty()
        );
    }

    #[tokio::test]
    async fn test_dispatch_to_empty_subscription_list() {
        let subscription_data = SubscriptionData::new();
        let empty_subscription_request = json!({"jsonrpc":"2.0","id": 2, "method": EthRpcMethod::Subscribe, "params": ["newHeads"]});
        let empty_subscription_id = "500".to_string();
        let empty_node_id = 10000;
        let message = RequestResult::Subscription(serde_json::Value::String(
            "empty test message".to_string(),
        ));

        // No users are subscribed to this subscription
        subscription_data.register_subscription(
            empty_subscription_request,
            empty_subscription_id.clone(),
            empty_node_id,
        );
        let dispatch_result = subscription_data
            .dispatch_to_subscribers(&empty_subscription_id, empty_node_id, &message)
            .await;
        assert!(dispatch_result.is_ok()); // Should succeed even though there are no subscribers
    }

    #[tokio::test]
    async fn test_get_users_for_subscription() {
        let (subscription_data, user_id, _) = setup_user_and_subscription_data();
        let subscription_request = json!({"jsonrpc":"2.0","id": 2, "method": EthRpcMethod::Subscribe, "params": ["newHeads"]});
        let subscription_id = "200".to_string();
        let node_id = 1;

        // Register and subscribe a user to the subscription
        subscription_data.register_subscription(
            subscription_request.clone(),
            subscription_id.clone(),
            node_id,
        );
        subscription_data
            .subscribe_user(user_id, subscription_request.clone())
            .unwrap();

        // Test get_users_for_subscription function
        let users = subscription_data.get_users_for_subscription(&subscription_id);
        assert_eq!(users.len(), 1);
        assert!(users.contains(&user_id));

        // Test with a non-existent subscription_id
        let non_existent_subscription_id = "nonexistent".to_string();
        let empty_users =
            subscription_data.get_users_for_subscription(&non_existent_subscription_id);
        assert!(empty_users.is_empty());
    }

    #[tokio::test]
    async fn test_dispatch_to_nonexistent_subscription() {
        let subscription_data = SubscriptionData::new();
        let _nonexistent_subscription_request = "sub600".to_string();
        let nonexistent_subscription_id = "600".to_string();
        let nonexistent_node_id = 10000;

        let message = RequestResult::Subscription(serde_json::Value::String(
            "nonexistent subscription message".to_string(),
        ));

        let dispatch_result = subscription_data
            .dispatch_to_subscribers(&nonexistent_subscription_id, nonexistent_node_id, &message)
            .await;
        assert!(dispatch_result.is_ok()); // Should succeed as it should handle subscriptions with no users gracefully
    }
}
