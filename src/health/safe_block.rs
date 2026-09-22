use crate::{
    balancer::processing::CacheArgs,
    config::system::WS_HEALTH_CHECK_USER_ID,
    database::types::GenericBytes,
    rpc::{
        error::RpcError,
        method::EthRpcMethod,
        types::{Rpc, hex_to_decimal},
    },
    websocket::{
        client::execute_ws_call,
        subscription_manager::move_subscriptions,
        types::{IncomingResponse, RequestResult, SubscriptionData, WsconnMessage},
    },
};

use futures_util::future::join_all;
use std::sync::{Arc, RwLock};

use tokio::{
    sync::{broadcast, mpsc, watch},
    time::{Duration, timeout},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct NamedBlocknumbers {
    pub latest: u64,
    pub earliest: u64,
    pub safe: u64,
    pub finalized: u64,
    pub pending: u64,
}

impl NamedBlocknumbers {
    #[allow(dead_code)] // allowed for tests
    pub fn defualt() -> NamedBlocknumbers {
        NamedBlocknumbers {
            latest: 0,
            earliest: 0,
            safe: 0,
            finalized: 0,
            pending: 0,
        }
    }
}

/// Get the latest finalized block
pub async fn get_safe_block(
    rpc_list: &Arc<RwLock<Vec<Rpc>>>,
    finalized_tx: &tokio::sync::watch::Sender<u64>,
    named_numbers_rwlock: &Arc<RwLock<NamedBlocknumbers>>,
    ttl: u64,
) -> Result<u64, RpcError> {
    let rpcs = rpc_list.read().unwrap_or_else(|e| e.into_inner()).clone();
    let checks = rpcs.into_iter().map(|rpc| async move {
        timeout(Duration::from_millis(ttl), rpc.get_finalized_block())
            .await
            .ok()
            .and_then(Result::ok)
    });
    let safe = join_all(checks).await.into_iter().flatten().max();
    let Some(safe) = safe else {
        return Ok(named_numbers_rwlock.read().unwrap().finalized);
    };

    // Send new blocknumber if modified
    let send_if_changed = |number: &mut u64| {
        if number != &safe {
            *number = safe;
            return true;
        }
        false
    };

    finalized_tx.send_if_modified(send_if_changed);

    tracing::debug!("Safe block: {}", safe);

    // Return as NamedBlocknumbers
    let mut nn_rwlock = named_numbers_rwlock.write().unwrap();
    nn_rwlock.finalized = safe;

    Ok(safe)
}

/// Send a message subscribing to newHeads
async fn send_newheads_sub_message<K, V>(
    user_id: u32,
    incoming_tx: &mpsc::UnboundedSender<WsconnMessage>,
    outgoing_rx: &broadcast::Receiver<IncomingResponse>,
    sub_data: &Arc<SubscriptionData>,
    cache_args: &CacheArgs<K, V>,
    ttl: Duration,
) -> bool
where
    K: GenericBytes + From<[u8; 32]>,
    V: GenericBytes + From<Vec<u8>>,
{
    let call = serde_json::json!({
        "jsonrpc": "2.0",
        "method": EthRpcMethod::Subscribe,
        "params": ["newHeads"],
        "id": user_id.to_string(),
    });

    matches!(
        timeout(
            ttl,
            execute_ws_call(
                call.clone(),
                user_id,
                incoming_tx,
                outgoing_rx.resubscribe(),
                sub_data,
                cache_args,
            )
        )
        .await,
        Ok(Ok(_))
    ) && sub_data.subscribe_user(user_id, call).is_ok()
}

/// Subscribe to eth_subscribe("newHeads") and write to NamedBlocknumbers
pub async fn subscribe_to_new_heads<K, V>(
    incoming_tx: mpsc::UnboundedSender<WsconnMessage>,
    outgoing_rx: broadcast::Receiver<IncomingResponse>,
    blocknum_tx: watch::Sender<u64>,
    sub_data: Arc<SubscriptionData>,
    cache_args: CacheArgs<K, V>,
    expected_block_time: u64,
) where
    K: GenericBytes + From<[u8; 32]>,
    V: GenericBytes + From<Vec<u8>>,
{
    // We basically have to create a new system-only user for subscribing to newHeads

    // Create channels for message send/receiving
    let (tx, mut rx) = mpsc::unbounded_channel::<RequestResult>();

    // Generate an id for our user
    //
    // We use this to identify which requests are for us
    let user_id = WS_HEALTH_CHECK_USER_ID;

    // Add the user to the sink map
    tracing::info!("Adding user {} to sink map", user_id);
    let user_data = tx.clone();
    sub_data.add_user(user_id, user_data);

    let ttl = Duration::from_millis(expected_block_time);
    while !send_newheads_sub_message(
        user_id,
        &incoming_tx,
        &outgoing_rx,
        &sub_data,
        &cache_args,
        ttl,
    )
    .await
    {
        if incoming_tx.send(WsconnMessage::Reconnect()).is_err() {
            return;
        }
        tracing::warn!("Unable to subscribe to newHeads; retrying");
        tokio::time::sleep(ttl).await;
    }

    // New message == new head received. We can then update and process
    // everything associated with a new head block.
    loop {
        match timeout(Duration::from_millis(expected_block_time), rx.recv()).await {
            Ok(Some(msg)) => {
                if let RequestResult::Subscription(sub) = msg {
                    let Some(a) = sub["params"]["result"]["number"]
                        .as_str()
                        .and_then(|number| hex_to_decimal(number).ok())
                    else {
                        tracing::warn!("Invalid block number in newHeads notification");
                        continue;
                    };
                    let mut nn_rwlock = cache_args.named_numbers.write().unwrap();
                    tracing::info!(a, "New chain head");
                    let _ = blocknum_tx.send(a);
                    nn_rwlock.latest = a;
                }
            }
            Ok(None) => {
                // Handle the case where the channel is closed
                tracing::error!("newHeads channel closed.");
                return;
            }
            Err(_) => {
                // Handle the timeout case
                {
                    let mut nn_rwlock = cache_args.named_numbers.write().unwrap_or_else(|e| {
                        // Handle the case where the named_numbers RwLock is poisoned
                        tracing::error!(?e);
                        e.into_inner()
                    });
                    nn_rwlock.latest = 0;
                    match incoming_tx.send(WsconnMessage::Reconnect()) {
                        Ok(_) => {}
                        Err(_) => {
                            tracing::error!("WS incoming channel closed.");
                            return;
                        }
                    }
                    drop(nn_rwlock);
                }
                tracing::warn!(
                    "Timeout in newHeads subscription, possible connection failiure or missed block."
                );
                let node_id = match sub_data
                    .get_sub_id_by_params(r#"["newHeads"]"#)
                    .and_then(|id| sub_data.get_node_from_id(&id))
                {
                    Some(node_id) => node_id,
                    None => {
                        tracing::error!(
                            "Failed to get some failed node subscription IDs! Subscriptions might be silently dropped!"
                        );
                        continue;
                    }
                };
                match move_subscriptions(
                    &incoming_tx,
                    outgoing_rx.resubscribe(),
                    &sub_data,
                    node_id,
                    Duration::from_millis(expected_block_time),
                )
                .await
                {
                    Ok(_) => {}
                    Err(err) => {
                        tracing::error!(?err);
                    }
                };
            }
        }
    }
}
