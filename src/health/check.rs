use crate::{
    IncomingResponse, Rpc, Settings, SubscriptionData,
    admin::liveready::{HealthState, LiveReadyUpdate, LiveReadyUpdateSnd},
    health::{
        error::HealthError,
        safe_block::{NamedBlocknumbers, get_safe_block},
    },
    websocket::{
        subscription_manager::move_subscriptions,
        types::{WsChannelErr, WsconnMessage},
    },
};

use std::{
    sync::{Arc, RwLock},
    time::Duration,
};

use futures_util::future::join_all;
use rust_tracing::deps::metrics;
use tokio::{
    sync::{broadcast, mpsc},
    time::{sleep, timeout},
};

#[derive(Debug, Clone)]
struct HeadResult {
    rpc: Rpc,
    is_syncing: bool,
    reported_head: u64,
}

/// Call check and safe_block in a loop
pub async fn health_check(
    rpc_list: Arc<RwLock<Vec<Rpc>>>,
    poverty_list: Arc<RwLock<Vec<Rpc>>>,
    finalized_tx: tokio::sync::watch::Sender<u64>,
    liveness_tx: LiveReadyUpdateSnd,
    named_numbers_rwlock: &Arc<RwLock<NamedBlocknumbers>>,
    config: &Arc<RwLock<Settings>>,
) -> Result<(), HealthError> {
    loop {
        let health_check_ttl = config.read().unwrap().health_check_ttl;
        let ttl = config.read().unwrap().ttl;
        let supress_rpc_check = config.read().unwrap().supress_rpc_check;

        sleep(Duration::from_millis(health_check_ttl)).await;

        check(
            &rpc_list,
            &poverty_list,
            &ttl,
            &liveness_tx,
            supress_rpc_check,
        )
        .await?;

        get_safe_block(
            &rpc_list,
            &finalized_tx,
            named_numbers_rwlock,
            u64::try_from(ttl).unwrap_or(u64::MAX),
        )
        .await?;
    }
}

/// Track the head of each RPC and process them accordingly
async fn check(
    rpc_list: &Arc<RwLock<Vec<Rpc>>>,
    poverty_list: &Arc<RwLock<Vec<Rpc>>>,
    ttl: &u128,
    liveness_tx: &LiveReadyUpdateSnd,
    supress_rpc_check: bool,
) -> Result<(), HealthError> {
    if !supress_rpc_check {
        tracing::info!("Checking RPC health... ");
    }
    // Head blocks reported by each RPC, we also use it to mark delinquents
    //
    // If a head is marked at `0` that means that the rpc is delinquent
    let heads = head_check(rpc_list, *ttl).await?;

    // Remove RPCs that are falling behind
    let agreed_head = make_poverty(rpc_list, poverty_list, heads)?;
    metrics::gauge!("rpc_head_height").set(agreed_head as f64);

    // Check if any rpc nodes made it out
    // Its ok if we call them twice because some might have been accidentally put here

    // Do a head check over the current poverty list to see if any nodes are back to normal
    let poverty_heads = head_check(poverty_list, *ttl).await?;

    let to_send = escape_poverty(rpc_list, poverty_list, poverty_heads, agreed_head)?;

    // Send the current status of nodes to the liveness monitor
    let _ = liveness_tx.send(to_send).await;

    if !supress_rpc_check {
        tracing::info!("OK!");
    }

    Ok(())
}

/// Check what heads are reported by each RPC
async fn head_check(
    rpc_list: &Arc<RwLock<Vec<Rpc>>>,
    ttl: u128,
) -> Result<Vec<HeadResult>, HealthError> {
    let rpcs = rpc_list.read().unwrap_or_else(|e| e.into_inner()).clone();
    let deadline = Duration::from_millis(u64::try_from(ttl).unwrap_or(u64::MAX));
    let checks = rpcs.into_iter().map(|rpc| async move {
        let probe = async {
            let reported_head = rpc.block_number().await.unwrap_or(0);
            let is_syncing = rpc.syncing().await.unwrap_or(true);
            (reported_head, is_syncing)
        };
        let (reported_head, is_syncing) = timeout(deadline, probe).await.unwrap_or((0, true));
        HeadResult {
            rpc,
            is_syncing,
            reported_head,
        }
    });
    Ok(join_all(checks).await)
}

/// Add unresponsive/erroring RPCs to the poverty list
fn make_poverty(
    rpc_list: &Arc<RwLock<Vec<Rpc>>>,
    poverty_list: &Arc<RwLock<Vec<Rpc>>>,
    heads: Vec<HeadResult>,
) -> Result<u64, HealthError> {
    let mut rpc_list_guard = rpc_list.write().unwrap();
    let mut poverty_list_guard = poverty_list.write().unwrap();
    let highest_head = heads
        .iter()
        .filter(|head| {
            rpc_list_guard
                .iter()
                .any(|rpc| rpc.same_endpoint(&head.rpc))
        })
        .map(|head| head.reported_head)
        .max()
        .unwrap_or(0);

    for head in heads {
        if head.reported_head < highest_head || head.is_syncing {
            let Some(rpc) = rpc_list_guard
                .iter_mut()
                .find(|rpc| rpc.same_endpoint(&head.rpc))
            else {
                continue;
            };
            rpc.status.is_erroring = true;
            let rpc_name = &rpc.name;
            tracing::warn!("{rpc_name} is falling behind! Removing from active RPC pool.");
            metrics::gauge!(
                "rpc_health_by_name",
                "rpc_name" => rpc_name.to_owned()
            )
            .set(0.0);

            // Add the RPC to the poverty list
            poverty_list_guard.push(rpc.clone());
        }
    }

    // Go over rpc_list_guard and remove all erroring rpcs
    rpc_list_guard.retain(|rpc| !rpc.status.is_erroring);

    Ok(highest_head)
}

/// Go over the `poverty_list` to see if any nodes are back to normal.
///
/// Update liveness statuses when done.
fn escape_poverty(
    rpc_list: &Arc<RwLock<Vec<Rpc>>>,
    poverty_list: &Arc<RwLock<Vec<Rpc>>>,
    poverty_heads: Vec<HeadResult>,
    agreed_head: u64,
) -> Result<crate::LiveReadyUpdate, HealthError> {
    // Check if any nodes made it 🗣️🔥🔥🔥
    let mut rpc_list_guard = rpc_list.write().unwrap_or_else(|e| {
        // Handle the case where the RwLock is poisoned
        e.into_inner()
    });
    let mut poverty_list_guard = poverty_list.write().unwrap_or_else(|e| {
        // Handle the case where the RwLock is poisoned
        e.into_inner()
    });

    for head in poverty_heads {
        if head.reported_head >= agreed_head && !head.is_syncing {
            let Some(index) = poverty_list_guard
                .iter()
                .position(|rpc| rpc.same_endpoint(&head.rpc))
            else {
                continue;
            };
            let mut rpc = poverty_list_guard.remove(index);
            rpc.status.is_erroring = false;
            let rpc_name = &rpc.name;
            tracing::info!("{rpc_name} is following the head again! Added to active RPC pool.");
            metrics::gauge!(
                "rpc_health_by_name",
                "rpc_name" => rpc_name.to_owned()
            )
            .set(1.0);

            // Move the RPC from the poverty list to the rpc list
            rpc_list_guard.push(rpc);
        }
    }

    let healthy = rpc_list_guard.len() as f64;
    let unhealthy = poverty_list_guard.len() as f64;
    let total = healthy + unhealthy;
    metrics::gauge!("rpc_total").set(total);
    metrics::gauge!("rpc_healthy_total").set(healthy);
    metrics::gauge!("rpc_unhealthy_total").set(unhealthy);
    metrics::gauge!("rpc_health_ratio").set(healthy / total);

    //todo: i dont like this but its whatever

    let is_pov_empty = poverty_list_guard.is_empty();
    let is_rpc_empty = rpc_list_guard.is_empty();
    let to_send = if !is_rpc_empty && is_pov_empty {
        LiveReadyUpdate::Health(HealthState::Healthy)
    } else if !is_pov_empty && !is_rpc_empty {
        LiveReadyUpdate::Health(HealthState::MissingRpcs)
    } else {
        LiveReadyUpdate::Health(HealthState::Unhealthy)
    };

    Ok(to_send)
}

/// Remove the RPC that dropped out ws_conn and add it to the poverty list.
pub async fn send_dropped_to_poverty(
    rpc_list: &Arc<RwLock<Vec<Rpc>>>,
    poverty_list: &Arc<RwLock<Vec<Rpc>>>,
    incoming_tx: &mpsc::UnboundedSender<WsconnMessage>,
    rx: broadcast::Receiver<IncomingResponse>,
    sub_data: &Arc<SubscriptionData>,
    dropped: WsChannelErr,
    ttl: u128,
) -> Result<(), HealthError> {
    let WsChannelErr::Closed(ws_conn_index, dropped_rpc) = dropped;
    {
        let mut rpc_list_guard = rpc_list.write().unwrap();
        let mut poverty_list_guard = poverty_list.write().unwrap();

        // Check if the RPC is in the rpc_list
        if let Some(index) = rpc_list_guard
            .iter()
            .position(|rpc| rpc.same_endpoint(&dropped_rpc))
        {
            let mut rpc = rpc_list_guard.remove(index);
            rpc.status.is_erroring = true;
            poverty_list_guard.push(rpc);
        }
    }

    incoming_tx
        .send(WsconnMessage::Reconnect())
        .map_err(|_| HealthError::Unresponsive)?;
    // Move subscriptions away from that node
    move_subscriptions(
        incoming_tx,
        rx,
        sub_data,
        ws_conn_index,
        Duration::from_millis(u64::try_from(ttl).unwrap_or(u64::MAX)),
    )
    .await?;

    Ok(())
}

/// Listen for dropped ws connections and handle them.
pub async fn dropped_listener(
    rpc_list: Arc<RwLock<Vec<Rpc>>>,
    poverty_list: Arc<RwLock<Vec<Rpc>>>,
    mut ws_err_rx: mpsc::UnboundedReceiver<WsChannelErr>,
    incoming_tx: mpsc::UnboundedSender<WsconnMessage>,
    rx: broadcast::Receiver<IncomingResponse>,
    sub_data: Arc<SubscriptionData>,
    ttl: u128,
) -> Result<(), HealthError> {
    loop {
        let ws_err = ws_err_rx.recv().await;

        match ws_err {
            Some(error) => {
                send_dropped_to_poverty(
                    &rpc_list,
                    &poverty_list,
                    &incoming_tx,
                    rx.resubscribe(),
                    &sub_data,
                    error,
                    ttl,
                )
                .await
                .unwrap_or(());
            }
            None => {
                return Err(HealthError::InvalidResponse(
                    "Expected WsChannelErr!".to_string(),
                ));
            }
        };
    }
}

/*
 * Tests
 */
#[cfg(test)]
mod tests {
    use super::*;

    fn mock_rpc(id: u8) -> Rpc {
        Rpc::new(
            format!("https://example.com/{id}").parse().unwrap(),
            None,
            1,
            0,
            1.0,
        )
    }

    fn dummy_head_check() -> Vec<HeadResult> {
        vec![
            HeadResult {
                rpc: mock_rpc(1),
                is_syncing: false,
                reported_head: 18177557,
            },
            HeadResult {
                rpc: mock_rpc(2),
                is_syncing: false,
                reported_head: 18193012,
            },
            HeadResult {
                rpc: mock_rpc(3),
                is_syncing: false,
                reported_head: 0,
            },
        ]
    }

    #[test]
    fn test_poverty() {
        // Create a mock RPC list and poverty list
        let rpc1 = mock_rpc(1);
        let rpc2 = mock_rpc(2);
        let rpc3 = mock_rpc(3);

        let rpc_list = Arc::new(RwLock::new(vec![rpc1.clone(), rpc2.clone(), rpc3.clone()]));
        let poverty_list = Arc::new(RwLock::new(vec![]));

        // Test with dummy head results
        let heads = dummy_head_check();

        // Call the make_poverty function
        let result = make_poverty(&rpc_list, &poverty_list, heads);
        assert!(result.is_ok());

        // Check the state of RPCs after the test
        let rpc_list_guard = rpc_list.read().unwrap();
        let poverty_list_guard = poverty_list.read().unwrap();

        // Only 1 RPC should be in the rpc list
        assert_eq!(rpc_list_guard.len(), 1);

        // The poverty list should now contain 2 RPCs
        assert_eq!(poverty_list_guard.len(), 2);
    }

    #[test]
    fn test_escape() {
        // Create a mock RPC list and poverty list
        let mut rpc1 = mock_rpc(1);
        rpc1.status.is_erroring = true;

        let rpc2 = mock_rpc(2);
        let mut rpc3 = mock_rpc(3);
        rpc3.status.is_erroring = true;

        let rpc_list = Arc::new(RwLock::new(vec![rpc2.clone()]));
        let poverty_list = Arc::new(RwLock::new(vec![rpc1.clone(), rpc3.clone()]));

        // Test with dummy head results
        let heads = vec![
            HeadResult {
                rpc: rpc1.clone(),
                is_syncing: false,
                reported_head: 18177557,
            },
            HeadResult {
                rpc: rpc3.clone(),
                is_syncing: false,
                reported_head: 18193012,
            },
        ];

        poverty_list.write().unwrap().swap(0, 1);
        // Call the escape_poverty function
        let result = escape_poverty(&rpc_list, &poverty_list, heads, 18193012);
        assert!(result.is_ok());

        // Check the state of RPCs after the test
        let rpc_list_guard = rpc_list.read().unwrap();
        let poverty_list_guard = poverty_list.read().unwrap();
        // RPC3 should have escaped poverty
        assert_eq!(rpc_list_guard.len(), 2);

        // The poverty list should have 1 RPC
        assert_eq!(poverty_list_guard.len(), 1);
        assert!(poverty_list_guard[0].same_endpoint(&rpc1));
        assert!(rpc_list_guard.iter().any(|rpc| rpc.same_endpoint(&rpc3)));
    }

    #[test]
    fn test_escape_sync() {
        // Create a mock RPC list and poverty list
        let mut rpc1 = mock_rpc(1);
        rpc1.status.is_erroring = true;

        let rpc2 = mock_rpc(2);
        let mut rpc3 = mock_rpc(3);
        rpc3.status.is_erroring = true;

        let rpc_list = Arc::new(RwLock::new(vec![rpc2.clone()]));
        let poverty_list = Arc::new(RwLock::new(vec![rpc1.clone(), rpc3.clone()]));

        // Test with dummy head results
        let heads = vec![
            HeadResult {
                rpc: rpc1.clone(),
                is_syncing: false,
                reported_head: 18193012,
            },
            HeadResult {
                rpc: rpc3.clone(),
                is_syncing: true,
                reported_head: 18193012,
            },
        ];

        // Call the escape_poverty function
        let result = escape_poverty(&rpc_list, &poverty_list, heads, 18193012);
        assert!(result.is_ok());

        // Check the state of RPCs after the test
        let rpc_list_guard = rpc_list.read().unwrap();
        let poverty_list_guard = poverty_list.read().unwrap();
        // RPC3 should have escaped poverty
        assert_eq!(rpc_list_guard.len(), 2);

        // The poverty list should have 1 RPC
        assert_eq!(poverty_list_guard.len(), 1);
    }
    async fn stalled_rpc() -> (
        Rpc,
        tokio::sync::oneshot::Receiver<()>,
        tokio::task::JoinHandle<()>,
    ) {
        use tokio::{io::AsyncReadExt, net::TcpListener};
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap())
            .parse()
            .unwrap();
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut data = [0; 4096];
            assert!(stream.read(&mut data).await.unwrap() > 0);
            started_tx.send(()).unwrap();
            while stream.read(&mut data).await.unwrap() != 0 {}
        });
        (Rpc::new(url, None, 1, 0, 1.0), started_rx, server)
    }

    #[tokio::test]
    async fn timed_out_health_probe_closes_connection_and_evicts_endpoint() {
        let (rpc, started, closed) = stalled_rpc().await;
        let rpc_list = Arc::new(RwLock::new(vec![rpc]));
        let probing_list = rpc_list.clone();
        let probe = tokio::spawn(async move { head_check(&probing_list, 100).await.unwrap() });
        timeout(Duration::from_secs(2), started)
            .await
            .unwrap()
            .unwrap();
        let replacement = mock_rpc(2);
        rpc_list.write().unwrap().insert(0, replacement.clone());
        let heads = probe.await.unwrap();
        let stale_heads = heads.clone();
        assert!(heads[0].is_syncing);
        timeout(Duration::from_secs(2), closed)
            .await
            .unwrap()
            .unwrap();

        let poverty_list = Arc::new(RwLock::new(Vec::new()));
        make_poverty(&rpc_list, &poverty_list, heads).unwrap();
        assert_eq!(rpc_list.read().unwrap().len(), 1);
        assert!(rpc_list.read().unwrap()[0].same_endpoint(&replacement));
        assert!(poverty_list.read().unwrap()[0].status.is_erroring);
        make_poverty(&rpc_list, &poverty_list, stale_heads).unwrap();
        assert_eq!(rpc_list.read().unwrap().len(), 1);
        assert_eq!(poverty_list.read().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn failed_finality_probe_preserves_last_finalized_block() {
        let (rpc, started, closed) = stalled_rpc().await;
        let rpc_list = Arc::new(RwLock::new(vec![rpc]));
        let (finalized_tx, finalized_rx) = tokio::sync::watch::channel(42);
        let numbers = Arc::new(RwLock::new(NamedBlocknumbers {
            finalized: 42,
            ..NamedBlocknumbers::default()
        }));
        let probing_numbers = numbers.clone();
        let probe = tokio::spawn(async move {
            get_safe_block(&rpc_list, &finalized_tx, &probing_numbers, 100)
                .await
                .unwrap()
        });
        timeout(Duration::from_secs(2), started)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(probe.await.unwrap(), 42);
        assert_eq!(*finalized_rx.borrow(), 42);
        assert_eq!(numbers.read().unwrap().finalized, 42);
        timeout(Duration::from_secs(2), closed)
            .await
            .unwrap()
            .unwrap();
    }
}
