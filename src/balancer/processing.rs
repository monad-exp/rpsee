use crate::{
    Rpc,
    balancer::{
        format::get_block_number_from_request,
        selection::cache_rules::{cache_method, cache_result},
    },
    database::{
        accept::db_insert,
        types::{GenericBytes, RequestBus},
    },
    health::safe_block::NamedBlocknumbers,
};

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, RwLock},
    time::Duration,
};

use tokio::sync::watch;

use blake3::Hash;
use serde_json::Value;

#[derive(Clone)]
pub struct CacheArgs<K, V>
where
    K: GenericBytes,
    V: GenericBytes,
{
    pub finalized_rx: watch::Receiver<u64>,
    pub named_numbers: Arc<RwLock<NamedBlocknumbers>>,
    pub head_cache: Arc<RwLock<BTreeMap<u64, BTreeSet<K>>>>,
    pub cache: RequestBus<K, V>,
}

impl CacheArgs<[u8; 32], Vec<u8>> {
    #[cfg(test)]
    /// **Note:** This should only be used for testing!
    pub fn default() -> Self {
        use crate::database_processing;

        use sled::{Config, Db};

        use tokio::sync::mpsc;

        let cache = Config::tmp().unwrap();
        let cache = Db::open_with_config(&cache).unwrap();

        let (db_tx, db_rx) = mpsc::unbounded_channel();
        tokio::task::spawn(database_processing(db_rx, cache));

        CacheArgs {
            finalized_rx: watch::channel(0).1,
            named_numbers: Arc::new(RwLock::new(NamedBlocknumbers::default())),
            head_cache: Arc::new(RwLock::new(BTreeMap::new())),
            cache: db_tx,
        }
    }
}

// TODO: we should find a way to check values directly and not convert Value to str
//
// @makemake -- Here's an intermediate solution to step towards the above todo which
// uses a loose trait constraint `AsRef<str>` which is implemented for the method types.
pub fn can_cache<M: AsRef<str>>(method: M, result: &str) -> bool {
    cache_method(method) && cache_result(result)
}

pub fn hash_request(request: &Value) -> Hash {
    let request = request.to_string();
    #[cfg(not(feature = "xxhash"))]
    {
        blake3::hash(request.as_bytes())
    }
    #[cfg(feature = "xxhash")]
    {
        let mut bytes = [0; 32];
        bytes[..16].copy_from_slice(&xxhash_rust::xxh3::xxh3_128(request.as_bytes()).to_le_bytes());
        Hash::from(bytes)
    }
}

/// Check if we should cache the query, and if so cache it in the DB.
pub async fn cache_query<K, V>(
    response: &str,
    request: Value,
    request_hash: Hash,
    cache_args: &CacheArgs<K, V>,
) where
    K: GenericBytes + From<[u8; 32]>,
    V: GenericBytes + From<Vec<u8>>,
{
    if !can_cache(request.to_string(), response) {
        return;
    }
    let Some(number) = get_block_number_from_request(request, &cache_args.named_numbers) else {
        return;
    };
    let Ok(mut response) = serde_json::from_str::<Value>(response) else {
        return;
    };
    let Some(id) = response.get_mut("id") else {
        return;
    };
    *id = Value::Null;

    {
        let mut head_cache = cache_args.head_cache.write().unwrap();
        if number > *cache_args.finalized_rx.borrow() {
            head_cache
                .entry(number)
                .or_default()
                .insert(request_hash.as_bytes().to_owned().into());
        }
    }

    drop(
        db_insert(
            &cache_args.cache,
            request_hash.as_bytes().to_owned().into(),
            response.to_string().into_bytes().into(),
        )
        .await,
    );
}

/// Updates the latency of an RPC node given an rpc list, its position, and the time it took for
/// a request to complete.
pub fn update_rpc_latency(rpc_list: &Arc<RwLock<Vec<Rpc>>>, rpc_position: usize, time: Duration) {
    let mut rpc_list_guard = rpc_list.write().unwrap_or_else(|e| {
        // Handle the case where the RwLock is poisoned
        e.into_inner()
    });

    // Handle weird edge cases ¯\_(ツ)_/¯
    if !rpc_list_guard.is_empty() {
        let index = if rpc_position >= rpc_list_guard.len() {
            rpc_list_guard.len() - 1
        } else {
            rpc_position
        };
        rpc_list_guard[index].update_latency(time.as_nanos() as f64);
        tracing::info!("LA {}", rpc_list_guard[index].status.latency);
    }
}

#[cfg(test)]
mod tests {
    use crate::{db_get, rpc::method::EthRpcMethod};
    use serde_json::json;

    use super::*;

    #[test]
    fn test_can_cache() {
        assert_eq!(
            can_cache(EthRpcMethod::GetBlockByNumber, r#"{"result": "0x1"}"#),
            cfg!(not(feature = "no-cache"))
        );
        assert!(!can_cache(EthRpcMethod::Subscribe, r#"{"result": "0x1"}"#));
    }

    #[test]
    fn test_dont_cache_infura_err() {
        assert!(!can_cache(
            r#"{"method": "eth_getBlockByNumber", "params": ["0x10", false]}"#,
            r#"{ "code": -32005, "data": { "see": "https://infura.io/dashboard" }, "message": "daily request count exceeded, request rate limited" }, payload={ "id": 12449, "jsonrpc": "2.0", "method": "eth_blockNumber", "params": [  ] }"#
        ));
    }

    #[cfg(not(feature = "no-cache"))]
    #[tokio::test]
    #[serial_test::serial]
    async fn test_cache_query() {
        let cache_args = CacheArgs::default();
        let rx = r#"{"jsonrpc":"2.0","result":"line\n\"quoted\"","id":1}"#.to_string();
        let method = json!({"method": EthRpcMethod::GetBlockByNumber, "params": ["0x10", false]});
        let tx_hash = blake3::hash(method.to_string().as_bytes());

        cache_query(&rx, method.clone(), tx_hash, &cache_args).await;

        let cached_value = db_get!(cache_args.cache, tx_hash.as_bytes().to_owned())
            .unwrap()
            .unwrap();
        let cached_str = std::str::from_utf8(&cached_value).unwrap();
        assert_eq!(
            cached_str,
            r#"{"id":null,"jsonrpc":"2.0","result":"line\n\"quoted\""}"#
        );
        assert_eq!(
            rx,
            r#"{"jsonrpc":"2.0","result":"line\n\"quoted\"","id":1}"#
        );
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn test_cache_infura_error_query() {
        let cache_args = CacheArgs::default();
        let rx = r#"{ "code": -32005, "data": { "see": "https://infura.io/dashboard" }, "message": "daily request count exceeded, request rate limited" }, payload={ "id": 12449, "jsonrpc": "2.0", "method": "eth_blockNumber", "params": [  ] }"#.to_string();
        let method = json!({"method": EthRpcMethod::GetBlockByNumber, "params": ["0x10", false]});
        let tx_hash = blake3::hash(method.to_string().as_bytes());

        cache_query(&rx, method.clone(), tx_hash, &cache_args).await;

        let cached_value = db_get!(cache_args.cache, tx_hash.as_bytes().to_owned()).unwrap();
        assert!(
            cached_value.is_none(),
            "got cached value for transaction that should have failed"
        );
    }

    #[tokio::test]
    async fn malformed_responses_are_not_cached() {
        let cache_args = CacheArgs::default();
        let request = json!({"method": "eth_getBalance", "params": ["0x1", "0x10"]});
        let hash = hash_request(&request);
        for response in ["not JSON", r#"{"result":"0x1"}"#] {
            cache_query(response, request.clone(), hash, &cache_args).await;
            assert!(
                db_get!(cache_args.cache, *hash.as_bytes())
                    .unwrap()
                    .is_none()
            );
        }
        assert!(cache_args.head_cache.read().unwrap().is_empty());
    }

    #[cfg(not(feature = "no-cache"))]
    #[tokio::test]
    async fn repeated_cache_writes_track_each_key_once() {
        let cache_args = CacheArgs::default();
        let request = json!({"method": "eth_getBlockByNumber", "params": ["0x10", false]});
        let hash = hash_request(&request);
        for _ in 0..3 {
            cache_query(
                r#"{"jsonrpc":"2.0","id":1,"result":{"number":"0x10"}}"#,
                request.clone(),
                hash,
                &cache_args,
            )
            .await;
        }
        assert_eq!(cache_args.head_cache.read().unwrap()[&16].len(), 1);
    }

    #[tokio::test]
    async fn test_update_rpc_latency() {
        let rpc_list = Arc::new(RwLock::new(vec![Rpc::new(
            "http://test_rpc".parse().unwrap(),
            Some("ws://test_rpc".parse().unwrap()),
            0,
            0,
            1.0,
        )]));
        rpc_list.write().unwrap()[0].last_used = 12345;
        update_rpc_latency(&rpc_list, 0, Duration::from_nanos(100));

        let rpcs = rpc_list.read().unwrap();
        assert_eq!(rpcs[0].status.latency, 100.0);
        assert_eq!(rpcs[0].last_used, 12345);
    }

    #[tokio::test]
    async fn test_update_rpc_latency_with_multiple_rpcs() {
        let rpc_list = Arc::new(RwLock::new(vec![
            Rpc::new(
                "http://test_rpc1".parse().unwrap(),
                Some("ws://test_rpc1".parse().unwrap()),
                0,
                0,
                1.0,
            ),
            Rpc::new(
                "http://test_rpc2".parse().unwrap(),
                Some("ws://test_rpc2".parse().unwrap()),
                0,
                0,
                1.0,
            ),
        ]));
        update_rpc_latency(&rpc_list, 1, Duration::from_nanos(200));

        let rpcs = rpc_list.read().unwrap();
        assert_eq!(rpcs[1].status.latency, 200.0);
    }

    #[tokio::test]
    async fn test_update_rpc_latency_with_invalid_position() {
        let rpc_list = Arc::new(RwLock::new(vec![Rpc::new(
            "http://test_rpc".parse().unwrap(),
            Some("ws://test_rpc".parse().unwrap()),
            0,
            0,
            1.0,
        )]));
        update_rpc_latency(&rpc_list, 10, Duration::from_nanos(300));

        // Since the position is invalid, it should update the last available RPC
        let rpcs = rpc_list.read().unwrap();
        assert_eq!(rpcs[0].status.latency, 300.0);
    }

    #[tokio::test]
    async fn test_update_rpc_latency_with_empty_rpc_list() {
        let rpc_list = Arc::new(RwLock::new(Vec::new()));
        update_rpc_latency(&rpc_list, 0, Duration::from_nanos(400));

        // With an empty RPC list, there should be no panic and no update
        let rpcs = rpc_list.read().unwrap();
        assert!(rpcs.is_empty());
    }

    #[tokio::test]
    async fn test_update_rpc_latency_edge_cases() {
        let rpc_list = Arc::new(RwLock::new(vec![
            Rpc::new(
                "http://test_rpc1".parse().unwrap(),
                Some("ws://test_rpc1".parse().unwrap()),
                0,
                0,
                1.0,
            ),
            Rpc::new(
                "http://test_rpc2".parse().unwrap(),
                Some("ws://test_rpc2".parse().unwrap()),
                0,
                0,
                1.0,
            ),
        ]));

        // Test edge case where rpc_position is equal to rpc_list length
        update_rpc_latency(&rpc_list, 2, Duration::from_nanos(500));
        let rpcs = rpc_list.read().unwrap();
        assert_eq!(
            rpcs[1].status.latency, 500.0,
            "Should update the last RPC in the list"
        );
    }
}
