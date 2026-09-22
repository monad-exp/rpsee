use crate::{
    balancer::processing::hash_request, config::system::VERSION_STR,
    database::types::GenericDatabase,
};

/// Sets up the cache with various basic data about our current rpsee instance.
pub fn setup_data<DB: GenericDatabase>(cache: &DB, do_clear: bool) {
    // Clear database if specified
    if do_clear {
        cache.clear().unwrap();
        tracing::warn!("All data cleared from the database.");
    }

    let version_json = format!(
        "{{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":\"rpsee {}\"}}",
        VERSION_STR
    );

    tracing::info!("Starting Rpsee {}", VERSION_STR);

    for method in ["rpsee_is_lb", "web3_clientVersion"] {
        let request = serde_json::json!({
            "id": null,
            "jsonrpc": "2.0",
            "method": method,
            "params": [],
        });
        let key = hash_request(&request);
        cache.write(key, version_json.as_bytes()).unwrap();
    }

    let (algorithm, other) = if cfg!(feature = "blake3") {
        ("blake3", "xxhash")
    } else {
        ("xxhash", "blake3")
    };
    cache.write(algorithm.as_bytes(), b"true").unwrap();
    if cache.read(other.as_bytes()).unwrap().is_some() {
        tracing::warn!(
            "Cache contains {other} keys; this build uses {algorithm}. \
             Use --clear-cache to reclaim entries from the other hash algorithm."
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn startup_identity_uses_request_cache_hash() {
        let config = sled::Config::tmp().unwrap();
        let cache = <sled::Db<{ crate::FANOUT }> as GenericDatabase>::open(&config).unwrap();
        setup_data(&cache, false);

        let (algorithm, other) = if cfg!(feature = "blake3") {
            (b"blake3", b"xxhash")
        } else {
            (b"xxhash", b"blake3")
        };
        assert_eq!(cache.read(algorithm).unwrap(), Some(b"true".to_vec()));
        assert!(cache.read(other).unwrap().is_none());

        for method in ["rpsee_is_lb", "web3_clientVersion"] {
            let request = serde_json::json!({
                "id": null, "jsonrpc": "2.0", "method": method, "params": [],
            });
            let stored = cache.read(hash_request(&request)).unwrap().unwrap();
            let response: serde_json::Value = serde_json::from_slice(&stored).unwrap();
            assert_eq!(response["result"], format!("rpsee {VERSION_STR}"));
        }
    }
}
