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
        cache
            .write(*key.as_bytes(), version_json.as_bytes())
            .unwrap();
    }

    // Insert which hashing algo we're using based on the selected features.
    // If `xxhash` is enabled we're using xxhash3, otherwise blake3.
    //
    // Print a warning if we see an keys are in an unexpectd hash format.
    if cfg!(feature = "xxhash") {
        let _ = cache.write(b"xxhash", b"true");
        if cache.read(b"blake3").unwrap().is_some() {
            tracing::error!(
                "Rpsee has detected that your DB is using blake3 while we're currently using xxhash! \
                Please remove all cache entries and try again."
            );
            tracing::info!("If you believe this is an error, please open a pull request!");
        }
    } else {
        let _ = cache.write(b"blake3", b"true");
        if cache.read(b"xxhash").unwrap().is_some() {
            tracing::error!(
                "Rpsee has detected that your DB is using xxhash while we're currently using blake3! \
                Please remove all cache entries and try again."
            );
            tracing::info!("If you believe this is an error, please open a pull request!");
        }
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

        for method in ["rpsee_is_lb", "web3_clientVersion"] {
            let request = serde_json::json!({
                "id": null, "jsonrpc": "2.0", "method": method, "params": [],
            });
            let stored = cache
                .read(*hash_request(&request).as_bytes())
                .unwrap()
                .unwrap();
            let response: serde_json::Value = serde_json::from_slice(&stored).unwrap();
            assert_eq!(response["result"], format!("rpsee {VERSION_STR}"));
        }
    }
}
