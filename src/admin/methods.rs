use crate::{
    Rpc, Settings,
    admin::error::AdminError,
    database::types::{GenericBytes, RequestBus},
    db_flush,
};

use std::{
    fmt,
    sync::{Arc, RwLock},
    time::Instant,
};

use serde_json::{Value, Value::Null, json};

#[derive(Debug, thiserror::Error)]
#[error("failed to convert method to `RpseeRpcMethod`:\n\ngot: {0:?}\nexpected:\n{1:#?}")]
pub struct Error<Method>(Method, &'static [&'static str])
where
    Method: fmt::Debug;
impl<Method> Error<Method>
where
    Method: fmt::Debug,
{
    pub fn new(method: Method) -> Self {
        Self(method, RpseeRpcMethod::RPSEE_ALL)
    }
}

// @makemake -- This could make it easier to port code into a library for interacting with rpsee.
/// Available internal RPC method for Rpsee.
#[derive(Debug)]
pub enum RpseeRpcMethod {
    Quit,
    RpcList,
    FlushCache,
    Config,
    PovertyList,
    Ttl,
    HealthCheckTtl,
    SetTtl,
    SetHealthCheckTtl,
    AddToRpcList,
    AddToPovertyList,
    RemoveFromRpcList,
    RemoveFromPovertyList,
}
impl RpseeRpcMethod {
    const RPSEE_QUIT: &str = "rpsee_quit";
    const RPSEE_RPC_LIST: &str = "rpsee_rpc_list";
    const RPSEE_FLUSH_CACHE: &str = "rpsee_flush_cache";
    const RPSEE_CONFIG: &str = "rpsee_config";
    const RPSEE_POVERTY_LIST: &str = "rpsee_poverty_list";
    const RPSEE_TTL: &str = "rpsee_ttl";
    const RPSEE_HEALTH_CHECK_TTL: &str = "rpsee_health_check_ttl";
    const RPSEE_SET_TTL: &str = "rpsee_set_ttl";
    const RPSEE_SET_HEALTH_CHECK_TTL: &str = "rpsee_set_health_check_ttl";
    const RPSEE_ADD_TO_RPC_LIST: &str = "rpsee_add_to_rpc_list";
    const RPSEE_ADD_TO_POVERTY_LIST: &str = "rpsee_add_to_poverty_list";
    const RPSEE_REMOVE_FROM_RPC_LIST: &str = "rpsee_remove_from_rpc_list";
    const RPSEE_REMOVE_FROM_POVERTY_LIST: &str = "rpsee_remove_from_poverty_list";

    const RPSEE_ALL: &[&str; 13] = &[
        Self::RPSEE_QUIT,
        Self::RPSEE_RPC_LIST,
        Self::RPSEE_FLUSH_CACHE,
        Self::RPSEE_CONFIG,
        Self::RPSEE_POVERTY_LIST,
        Self::RPSEE_TTL,
        Self::RPSEE_HEALTH_CHECK_TTL,
        Self::RPSEE_SET_TTL,
        Self::RPSEE_SET_HEALTH_CHECK_TTL,
        Self::RPSEE_ADD_TO_RPC_LIST,
        Self::RPSEE_ADD_TO_POVERTY_LIST,
        Self::RPSEE_REMOVE_FROM_RPC_LIST,
        Self::RPSEE_REMOVE_FROM_POVERTY_LIST,
    ];

    /// Useful for circumventing lifetimes associated with `let` bindings.
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Quit => Self::RPSEE_QUIT,
            Self::RpcList => Self::RPSEE_RPC_LIST,
            Self::FlushCache => Self::RPSEE_FLUSH_CACHE,
            Self::Config => Self::RPSEE_CONFIG,
            Self::PovertyList => Self::RPSEE_POVERTY_LIST,
            Self::Ttl => Self::RPSEE_TTL,
            Self::HealthCheckTtl => Self::RPSEE_HEALTH_CHECK_TTL,
            Self::SetTtl => Self::RPSEE_SET_TTL,
            Self::SetHealthCheckTtl => Self::RPSEE_SET_HEALTH_CHECK_TTL,
            Self::AddToRpcList => Self::RPSEE_ADD_TO_RPC_LIST,
            Self::AddToPovertyList => Self::RPSEE_ADD_TO_POVERTY_LIST,
            Self::RemoveFromRpcList => Self::RPSEE_REMOVE_FROM_RPC_LIST,
            Self::RemoveFromPovertyList => Self::RPSEE_REMOVE_FROM_POVERTY_LIST,
        }
    }
}
impl TryFrom<Option<&str>> for RpseeRpcMethod {
    type Error = Error<Option<String>>;
    fn try_from(value: Option<&str>) -> Result<Self, Self::Error> {
        match value {
            Some(Self::RPSEE_QUIT) => Ok(Self::Quit),
            Some(Self::RPSEE_RPC_LIST) => Ok(Self::RpcList),
            Some(Self::RPSEE_FLUSH_CACHE) => Ok(Self::FlushCache),
            Some(Self::RPSEE_CONFIG) => Ok(Self::Config),
            Some(Self::RPSEE_POVERTY_LIST) => Ok(Self::PovertyList),
            Some(Self::RPSEE_TTL) => Ok(Self::Ttl),
            Some(Self::RPSEE_HEALTH_CHECK_TTL) => Ok(Self::HealthCheckTtl),
            Some(Self::RPSEE_SET_TTL) => Ok(Self::SetTtl),
            Some(Self::RPSEE_SET_HEALTH_CHECK_TTL) => Ok(Self::SetHealthCheckTtl),
            Some(Self::RPSEE_ADD_TO_RPC_LIST) => Ok(Self::AddToRpcList),
            Some(Self::RPSEE_ADD_TO_POVERTY_LIST) => Ok(Self::AddToPovertyList),
            Some(Self::RPSEE_REMOVE_FROM_RPC_LIST) => Ok(Self::RemoveFromRpcList),
            Some(Self::RPSEE_REMOVE_FROM_POVERTY_LIST) => Ok(Self::RemoveFromPovertyList),
            _ => Err(Error::new(value.map(ToString::to_string))),
        }
    }
}
impl serde::Serialize for RpseeRpcMethod {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}
impl<'de> serde::Deserialize<'de> for RpseeRpcMethod {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = <&str>::deserialize(deserializer)?;
        match s {
            Self::RPSEE_QUIT => Ok(Self::Quit),
            Self::RPSEE_RPC_LIST => Ok(Self::RpcList),
            Self::RPSEE_FLUSH_CACHE => Ok(Self::FlushCache),
            Self::RPSEE_CONFIG => Ok(Self::Config),
            Self::RPSEE_POVERTY_LIST => Ok(Self::PovertyList),
            Self::RPSEE_TTL => Ok(Self::Ttl),
            Self::RPSEE_HEALTH_CHECK_TTL => Ok(Self::HealthCheckTtl),
            Self::RPSEE_SET_TTL => Ok(Self::SetTtl),
            Self::RPSEE_SET_HEALTH_CHECK_TTL => Ok(Self::SetHealthCheckTtl),
            Self::RPSEE_ADD_TO_RPC_LIST => Ok(Self::AddToRpcList),
            Self::RPSEE_ADD_TO_POVERTY_LIST => Ok(Self::AddToPovertyList),
            Self::RPSEE_REMOVE_FROM_RPC_LIST => Ok(Self::RemoveFromRpcList),
            Self::RPSEE_REMOVE_FROM_POVERTY_LIST => Ok(Self::RemoveFromPovertyList),
            _ => Err(serde::de::Error::unknown_variant(s, Self::RPSEE_ALL)),
        }
    }
}

/// Extract the method, call the appropriate function and return the response
pub async fn execute_method<K, V>(
    tx: Value,
    rpc_list: &Arc<RwLock<Vec<Rpc>>>,
    poverty_list: &Arc<RwLock<Vec<Rpc>>>,
    config: Arc<RwLock<Settings>>,
    cache: RequestBus<K, V>,
) -> Result<Value, AdminError>
where
    K: GenericBytes,
    V: GenericBytes,
{
    let method = tx["method"].as_str().try_into();
    tracing::debug!("Method: {:?}", method);

    // Check if write protection is enabled
    let write_protection_enabled = config.read().unwrap().admin.readonly;

    match method {
        Ok(RpseeRpcMethod::Quit) => {
            if write_protection_enabled {
                Err(AdminError::WriteProtectionEnabled)
            } else {
                admin_rpsee_quit(cache).await
            }
        }
        Ok(RpseeRpcMethod::RpcList) => admin_list_rpc(rpc_list),
        Ok(RpseeRpcMethod::FlushCache) => {
            if write_protection_enabled {
                Err(AdminError::WriteProtectionEnabled)
            } else {
                admin_flush_cache(cache).await
            }
        }
        Ok(RpseeRpcMethod::Config) => admin_config(config),
        Ok(RpseeRpcMethod::PovertyList) => admin_list_rpc(poverty_list),
        Ok(RpseeRpcMethod::Ttl) => admin_rpsee_ttl(config),
        Ok(RpseeRpcMethod::HealthCheckTtl) => admin_rpsee_health_check_ttl(config),
        Ok(RpseeRpcMethod::SetTtl) => {
            if write_protection_enabled {
                Err(AdminError::WriteProtectionEnabled)
            } else {
                admin_rpsee_set_ttl(config, tx["params"].as_array())
            }
        }
        Ok(RpseeRpcMethod::SetHealthCheckTtl) => {
            if write_protection_enabled {
                Err(AdminError::WriteProtectionEnabled)
            } else {
                admin_rpsee_set_health_check_ttl(config, tx["params"].as_array())
            }
        }
        Ok(RpseeRpcMethod::AddToRpcList) => {
            if write_protection_enabled {
                Err(AdminError::WriteProtectionEnabled)
            } else {
                admin_add_rpc(rpc_list, tx["params"].as_array())
            }
        }
        Ok(RpseeRpcMethod::AddToPovertyList) => {
            if write_protection_enabled {
                Err(AdminError::WriteProtectionEnabled)
            } else {
                admin_add_rpc(poverty_list, tx["params"].as_array())
            }
        }
        Ok(RpseeRpcMethod::RemoveFromRpcList) => {
            if write_protection_enabled {
                Err(AdminError::WriteProtectionEnabled)
            } else {
                admin_remove_rpc(rpc_list, tx["params"].as_array())
            }
        }
        Ok(RpseeRpcMethod::RemoveFromPovertyList) => {
            if write_protection_enabled {
                Err(AdminError::WriteProtectionEnabled)
            } else {
                admin_remove_rpc(poverty_list, tx["params"].as_array())
            }
        }
        Err(err) => Err(AdminError::InvalidMethod(err)),
    }
}

/// Quit Rpsee upon receiving this method
/// We're returning a Null and allowing unreachable code so rustc doesnt cry
#[allow(unreachable_code)]
async fn admin_rpsee_quit<K, V>(cache: RequestBus<K, V>) -> Result<Value, AdminError>
where
    K: GenericBytes,
    V: GenericBytes,
{
    // We're doing something not-good so flush everything to disk
    drop(db_flush!(cache));

    std::process::exit(0);
    Ok(Value::Null)
}

/// Flushes sled cache to disk
async fn admin_flush_cache<K, V>(cache: RequestBus<K, V>) -> Result<Value, AdminError>
where
    K: GenericBytes,
    V: GenericBytes,
{
    let time = Instant::now();
    drop(db_flush!(cache));
    let time = time.elapsed();

    let rx = json!({
        "id": Null,
        "jsonrpc": "2.0",
        "result": format!("Cache flushed in {:?}", time),
    });

    Ok(rx)
}

/// Respond with the config we started rpsee with
fn admin_config(config: Arc<RwLock<Settings>>) -> Result<Value, AdminError> {
    let guard = config.read().unwrap();
    let rx = json!({
        "id": Null,
        "jsonrpc": "2.0",
        "result": {
            "address": guard.address,
            "do_clear": guard.do_clear,
            "health_check": guard.health_check,
            "admin": {
                "enabled": guard.admin.enabled,
                "readonly": guard.admin.readonly,
            },
            "ttl": guard.ttl,
            "health_check_ttl": guard.health_check_ttl,
        },
    });

    Ok(rx)
}

/// Lists the RPC names.
/// Used for `rpsee_rpc_list` and `rpsee_poverty_list`
fn admin_list_rpc(rpc_list: &Arc<RwLock<Vec<Rpc>>>) -> Result<Value, AdminError> {
    // Read the RPC list, handling errors
    let rpc_list = rpc_list.read().map_err(|_| AdminError::Inaccessible)?;

    let entries: Vec<Value> = rpc_list
        .iter()
        .map(|rpc| {
            json!({
                "name": rpc.name,
                "max_consecutive": rpc.max_consecutive,
                "last_error": rpc.status.last_error,
            })
        })
        .collect();
    let rx = json!({
        "id": Null,
        "jsonrpc": "2.0",
        "result": entries,
    });

    Ok(rx)
}

/// Params: HTTP URL, optional WS URL, max consecutive requests, requests per second, latency window.
fn admin_add_rpc(
    rpc_list: &Arc<RwLock<Vec<Rpc>>>,
    params: Option<&Vec<Value>>,
) -> Result<Value, AdminError> {
    let params = match params {
        Some(params) => params,
        None => return Err(AdminError::InvalidParams),
    };

    if params.len() != 5 {
        return Err(AdminError::InvalidLen);
    }

    let rpc: url::Url = params[0]
        .as_str()
        .ok_or(AdminError::ParseError)?
        .parse()
        .map_err(|_| AdminError::ParseError)?;
    if !matches!(rpc.scheme(), "http" | "https") || rpc.host_str().is_none() {
        return Err(AdminError::ParseError);
    }
    let ws_url = if params[1].is_null() {
        None
    } else {
        let url: url::Url = params[1]
            .as_str()
            .ok_or(AdminError::ParseError)?
            .parse()
            .map_err(|_| AdminError::ParseError)?;
        if !matches!(url.scheme(), "ws" | "wss") || url.host_str().is_none() {
            return Err(AdminError::ParseError);
        }
        Some(url)
    };
    let max_consecutive = params[2]
        .as_u64()
        .and_then(|n| u32::try_from(n).ok())
        .ok_or(AdminError::ParseError)?;
    let rate = params[3].as_u64().ok_or(AdminError::ParseError)?;
    let ma_len = params[4].as_f64().ok_or(AdminError::ParseError)?;
    if !ma_len.is_finite() || !(1.0..=f64::from(u32::MAX)).contains(&ma_len) {
        return Err(AdminError::ParseError);
    }
    let delta = 1_000_000_u64.checked_div(rate).unwrap_or(0);
    let rpc = Rpc::new(rpc, ws_url, max_consecutive, delta.into(), ma_len);
    let name = rpc.name.clone();

    let mut rpc_list = rpc_list.write().map_err(|_| AdminError::Inaccessible)?;

    rpc_list.push(rpc);

    let rx = json!({
        "id": Null,
        "jsonrpc": "2.0",
        "result": format!("RPC: {}, max_consecutive: {}, ma: {}", name, max_consecutive, ma_len),
    });

    Ok(rx)
}

/// Remove RPC at a specified index, return the url of the removed RPC:
/// - `param[0]`: RPC index
fn admin_remove_rpc(
    rpc_list: &Arc<RwLock<Vec<Rpc>>>,
    params: Option<&Vec<Value>>,
) -> Result<Value, AdminError> {
    let params = match params {
        Some(params) => params,
        None => return Err(AdminError::InvalidParams),
    };

    if params.len() != 1 {
        return Err(AdminError::InvalidLen);
    }

    let index = params[0]
        .as_u64()
        .and_then(|index| usize::try_from(index).ok())
        .ok_or(AdminError::ParseError)?;

    let mut rpc_list = rpc_list.write().map_err(|_| AdminError::Inaccessible)?;

    // Check if index exists before removing
    if index >= rpc_list.len() {
        return Err(AdminError::OutOfBounds);
    }

    // Finally, remove the index
    let removed: Rpc = rpc_list.remove(index);

    let rx = json!({
        "id": Null,
        "jsonrpc": "2.0",
        "result": removed.name,
    });

    Ok(rx)
}

// TODO: change the following 4 fn so theyre generic

/// Responds with health_check_ttl
fn admin_rpsee_health_check_ttl(config: Arc<RwLock<Settings>>) -> Result<Value, AdminError> {
    let guard = config.read().unwrap();
    let rx = json!({
        "id": Null,
        "jsonrpc": "2.0",
        "result": guard.health_check_ttl,
    });

    Ok(rx)
}

/// Responds with ttl
fn admin_rpsee_ttl(config: Arc<RwLock<Settings>>) -> Result<Value, AdminError> {
    let guard = config.read().unwrap();
    let rx = json!({
        "id": Null,
        "jsonrpc": "2.0",
        "result": guard.ttl,
    });

    Ok(rx)
}

/// Sets health_check_ttl:
/// - `param[0]`: health check interval
fn admin_rpsee_set_health_check_ttl(
    config: Arc<RwLock<Settings>>,
    params: Option<&Vec<Value>>,
) -> Result<Value, AdminError> {
    let params = match params {
        Some(params) => params,
        None => return Err(AdminError::InvalidParams),
    };

    if params.len() != 1 {
        return Err(AdminError::InvalidLen);
    }

    let health_check_ttl = params[0].as_u64().ok_or(AdminError::ParseError)?;
    if health_check_ttl == 0 {
        return Err(AdminError::InvalidParams);
    }

    let mut guard = config.write().unwrap();
    guard.health_check_ttl = health_check_ttl;

    let rx = json!({
        "id": Null,
        "jsonrpc": "2.0",
        "result": guard.health_check_ttl,
    });

    Ok(rx)
}

/// Sets ttl:
/// `param[0]`: request timeout
fn admin_rpsee_set_ttl(
    config: Arc<RwLock<Settings>>,
    params: Option<&Vec<Value>>,
) -> Result<Value, AdminError> {
    let params = match params {
        Some(params) => params,
        None => return Err(AdminError::InvalidParams),
    };

    if params.len() != 1 {
        return Err(AdminError::InvalidLen);
    }

    let ttl = params[0].as_u64().ok_or(AdminError::ParseError)?;

    let mut guard = config.write().unwrap();
    guard.ttl = ttl as u128;

    let rx = json!({
        "id": Null,
        "jsonrpc": "2.0",
        "result": guard.ttl,
    });

    Ok(rx)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::database_processing;
    use jsonwebtoken::DecodingKey;
    use sled::Config;
    use sled::Db;
    use tokio::sync::mpsc;

    // Helper function to create a test RPC list
    fn create_test_rpc_list() -> Arc<RwLock<Vec<Rpc>>> {
        Arc::new(RwLock::new(vec![Rpc::new(
            "http://example.com".parse().unwrap(),
            None,
            5,
            1000,
            5.0,
        )]))
    }

    // Helper function to create a test poverty list
    fn create_test_poverty_list() -> Arc<RwLock<Vec<Rpc>>> {
        Arc::new(RwLock::new(vec![Rpc::new(
            "http://poverty.com".parse().unwrap(),
            None,
            2,
            1000,
            1.0,
        )]))
    }

    // Helper function to create a test Settings config
    fn create_test_settings_config() -> Arc<RwLock<Settings>> {
        let mut config = Settings {
            do_clear: true,
            ..Settings::default()
        };
        config.admin.key = DecodingKey::from_secret(b"some-key");
        Arc::new(RwLock::new(config))
    }

    // Helper function to create a test cache
    fn create_test_cache() -> RequestBus<Vec<u8>, Vec<u8>> {
        let cache = Config::tmp().unwrap();
        let cache = Db::open_with_config(&cache).unwrap();
        let (db_tx, db_rx) = mpsc::unbounded_channel();
        tokio::task::spawn(database_processing(db_rx, cache));

        db_tx
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn test_execute_method_rpsee_rpc_list() {
        // Arrange
        let cache = create_test_cache();
        let tx = json!({ "id":1,"method": RpseeRpcMethod::RpcList });

        // Act
        let result = execute_method(
            tx,
            &create_test_rpc_list(),
            &create_test_poverty_list(),
            create_test_settings_config(),
            cache,
        )
        .await;

        // Assert
        assert!(result.is_ok());
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn test_execute_method_rpsee_flush_cache() {
        // Arrange
        let cache = create_test_cache();
        let tx = json!({ "id":1,"method": RpseeRpcMethod::FlushCache });

        // Act
        let result = execute_method(
            tx,
            &create_test_rpc_list(),
            &create_test_poverty_list(),
            create_test_settings_config(),
            cache,
        )
        .await;

        // Assert
        assert!(result.is_ok()); // Verify that flushing the cache doesn't produce an error
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn test_execute_method_rpsee_config() {
        // Arrange
        let cache = create_test_cache();
        let tx = json!({ "id":1,"method": RpseeRpcMethod::Config });

        // Act
        let result = execute_method(
            tx,
            &create_test_rpc_list(),
            &create_test_poverty_list(),
            create_test_settings_config(),
            cache,
        )
        .await;

        // Assert
        assert!(result.is_ok());
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn test_execute_method_rpsee_poverty_list() {
        // Arrange
        let cache = create_test_cache();
        let tx = json!({ "id":1,"method": RpseeRpcMethod::PovertyList });

        // Act
        let result = execute_method(
            tx,
            &create_test_rpc_list(),
            &create_test_poverty_list(),
            create_test_settings_config(),
            cache,
        )
        .await;

        // Assert
        assert!(result.is_ok());
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn test_execute_method_rpsee_ttl() {
        // Arrange
        let cache = create_test_cache();
        let tx = json!({ "id":1,"method": RpseeRpcMethod::Ttl });

        // Act
        let result = execute_method(
            tx,
            &create_test_rpc_list(),
            &create_test_poverty_list(),
            create_test_settings_config(),
            cache,
        )
        .await;

        // Assert
        assert!(result.is_ok());
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn test_execute_method_rpsee_health_check_ttl() {
        // Arrange
        let cache = create_test_cache();
        let tx = json!({ "id":1,"method": RpseeRpcMethod::HealthCheckTtl });

        // Act
        let result = execute_method(
            tx,
            &create_test_rpc_list(),
            &create_test_poverty_list(),
            create_test_settings_config(),
            cache,
        )
        .await;

        // Assert
        assert!(result.is_ok());
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn test_execute_method_invalid_method() {
        // Arrange
        let cache = create_test_cache();
        let tx = json!({ "id":1,"method": "invalid_method" });

        // Act
        let result = execute_method(
            tx,
            &create_test_rpc_list(),
            &create_test_poverty_list(),
            create_test_settings_config(),
            cache,
        )
        .await;

        // Assert
        assert!(result.is_err());
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn test_execute_method_add_to_rpc_list() {
        // Arrange
        let cache = create_test_cache();
        let tx = json!({ "id":1,"method": RpseeRpcMethod::AddToRpcList, "params": ["http://example.com", "ws://example.com", 5, 10, 5.0] });

        let rpc_list = create_test_rpc_list();
        let len = rpc_list.read().unwrap().len();

        // Act
        let result = execute_method(
            tx,
            &rpc_list,
            &create_test_poverty_list(),
            create_test_settings_config(),
            cache,
        )
        .await;

        // Assert
        assert!(result.is_ok());
        assert!(rpc_list.read().unwrap().len() == len + 1);
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn test_execute_method_add_to_rpc_list_no_ws() {
        // Arrange
        let cache = create_test_cache();
        let tx = json!({ "id":1,"method": RpseeRpcMethod::AddToRpcList, "params": ["http://example.com", Null, 5, 10, 5.0] });

        let rpc_list = create_test_rpc_list();
        let len = rpc_list.read().unwrap().len();

        // Act
        let result = execute_method(
            tx,
            &rpc_list,
            &create_test_poverty_list(),
            create_test_settings_config(),
            cache,
        )
        .await;

        // Assert
        assert!(result.is_ok());
        assert!(rpc_list.read().unwrap().len() == len + 1);
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn test_execute_method_remove_from_rpc_list() {
        // Arrange
        let cache = create_test_cache();
        // purpusefully OOB
        let tx = json!({ "id":1,"method": RpseeRpcMethod::RemoveFromRpcList, "params": [10] });

        let rpc_list = create_test_rpc_list();
        // rpc_list has only 1 so add another one to keep the 1st one some company
        rpc_list.write().unwrap().push(Rpc::new(
            "http://example.com".parse().unwrap(),
            None,
            5,
            1000,
            5.0,
        ));
        let len = rpc_list.read().unwrap().len();

        // Act
        let result = execute_method(
            tx,
            &rpc_list,
            &create_test_poverty_list(),
            create_test_settings_config(),
            cache.clone(),
        )
        .await;

        // Assert
        assert!(result.is_err());

        // Arrange
        let tx = json!({ "id":1,"method": RpseeRpcMethod::RemoveFromRpcList, "params": [0] });

        // Act
        let binding = create_test_poverty_list();
        let result = execute_method(
            tx,
            &rpc_list,
            &binding,
            create_test_settings_config(),
            cache,
        )
        .await;

        // Assert
        assert!(result.is_ok());
        assert!(rpc_list.read().unwrap().len() == len - 1);
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn test_execute_method_rpsee_set_ttl() {
        // Arrange
        let cache = create_test_cache();
        let tx = json!({ "id":1,"method": RpseeRpcMethod::SetTtl, "params": [9001] });

        let config = create_test_settings_config();
        let ttl = config.read().unwrap().ttl;

        // Act
        let result = execute_method(
            tx,
            &create_test_rpc_list(),
            &create_test_poverty_list(),
            Arc::clone(&config),
            cache,
        )
        .await;

        // Assert
        assert!(result.is_ok());
        assert!(config.read().unwrap().ttl != ttl);
        assert!(config.read().unwrap().ttl == 9001)
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn test_execute_method_rpsee_set_health_check_ttl() {
        // Arrange
        let cache = create_test_cache();
        let tx = json!({ "id":1,"method": RpseeRpcMethod::SetHealthCheckTtl, "params": [9001] });

        let config = create_test_settings_config();
        let health_check_ttl = config.read().unwrap().health_check_ttl;

        // Act
        let result = execute_method(
            tx,
            &create_test_rpc_list(),
            &create_test_poverty_list(),
            Arc::clone(&config),
            cache,
        )
        .await;

        // Assert
        assert!(result.is_ok());
        assert!(config.read().unwrap().health_check_ttl != health_check_ttl);
        assert!(config.read().unwrap().health_check_ttl == 9001)
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn test_rw_protection() {
        // Arrange
        let cache = create_test_cache();
        let tx = json!({ "id":1,"method": RpseeRpcMethod::SetHealthCheckTtl, "params": [9001] });

        let config = create_test_settings_config();
        config.write().unwrap().admin.readonly = true;

        // Act
        let result = execute_method(
            tx,
            &create_test_rpc_list(),
            &create_test_poverty_list(),
            Arc::clone(&config),
            cache.clone(),
        )
        .await;

        // Assert
        assert!(result.is_err());

        // Also check that we can read
        let tx = json!({ "id":1,"method": RpseeRpcMethod::HealthCheckTtl });
        let result = execute_method(
            tx,
            &create_test_rpc_list(),
            &create_test_poverty_list(),
            Arc::clone(&config),
            cache,
        )
        .await;

        // Assert
        assert!(result.is_ok());
    }
    #[test]
    fn rpc_list_returns_an_array_and_escapes_names() {
        let rpc_list = create_test_rpc_list();
        rpc_list.write().unwrap().push(Rpc::default());
        rpc_list.write().unwrap()[1].name = "a\"b".to_string();
        let response = admin_list_rpc(&rpc_list).unwrap();
        assert_eq!(response["result"].as_array().unwrap().len(), 2);
        assert_eq!(response["result"][1]["name"], "a\"b");
        assert_eq!(
            serde_json::from_str::<Value>(&response.to_string()).unwrap(),
            response
        );
    }

    #[test]
    fn malformed_rpc_parameters_do_not_mutate_or_poison_the_list() {
        let rpc_list = create_test_rpc_list();
        for params in [
            json!(["invalid", null, 5, 10, 5]),
            json!(["http://example.com", "invalid", 5, 10, 5]),
            json!(["file:///tmp/rpc", null, 5, 10, 5]),
            json!(["http://example.com", "http://example.com", 5, 10, 5]),
            json!(["http://example.com", null, -1, 10, 5]),
            json!(["http://example.com", null, 5, "invalid", 5]),
            json!(["http://example.com", null, 5, 10, 0]),
        ] {
            assert!(admin_add_rpc(&rpc_list, params.as_array()).is_err());
            assert_eq!(rpc_list.read().unwrap().len(), 1);
        }
    }
}
