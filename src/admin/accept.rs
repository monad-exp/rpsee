use crate::{
    admin::liveready::{LiveReadyRequestSnd, accept_health_request, accept_readiness_request},
    database::types::{GenericBytes, RequestBus},
};
use http_body_util::Full;
use hyper::{Request, body::Bytes};

use jsonwebtoken::{Validation, decode};

use serde::{Deserialize, Serialize};

use serde_json::{Value, Value::Null, json};

use std::{
    convert::Infallible,
    sync::{Arc, RwLock},
    time::Instant,
};

use crate::{Rpc, Settings, admin::methods::execute_method, balancer::format::incoming_to_value};

/// For decoding JWT
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Claims {
    id: Value,
    jsonrpc: Value,
    method: Value,
    params: Value,
    exp: usize,
}

/// Execute request and construct a HTTP response
async fn forward_body<K, V>(
    tx: Value,
    rpc_list_rwlock: &Arc<RwLock<Vec<Rpc>>>,
    poverty_list_rwlock: &Arc<RwLock<Vec<Rpc>>>,
    cache: RequestBus<K, V>,
    config: Arc<RwLock<Settings>>,
) -> Result<hyper::Response<Full<Bytes>>, Infallible>
where
    K: GenericBytes,
    V: GenericBytes,
{
    let id = tx.get("id").cloned().unwrap_or(Null);
    let mut response = if !tx.is_object() || tx["jsonrpc"] != "2.0" || !tx["method"].is_string() {
        json!({"jsonrpc": "2.0", "error": {"code": -32600, "message": "Invalid Request"}})
    } else {
        match execute_method(tx, rpc_list_rwlock, poverty_list_rwlock, config, cache).await {
            Ok(response) => response,
            Err(err) => {
                use crate::admin::error::AdminError;
                let code = match &err {
                    AdminError::InvalidMethod(_) => -32601,
                    AdminError::InvalidParams
                    | AdminError::InvalidLen
                    | AdminError::ParseError
                    | AdminError::OutOfBounds => -32602,
                    AdminError::WriteProtectionEnabled => -32000,
                    AdminError::Inaccessible => -32603,
                };
                json!({"jsonrpc": "2.0", "error": {"code": code, "message": err.to_string()}})
            }
        }
    };
    response["id"] = id;
    let res = hyper::Response::builder()
        .status(200)
        .header(hyper::header::CONTENT_TYPE, "application/json")
        .body(Full::new(Bytes::from(response.to_string())))
        .unwrap();

    Ok(res)
}

/// Accept admin request, self explanatory
pub async fn accept_admin_request<K, V>(
    tx: Request<hyper::body::Incoming>,
    rpc_list_rwlock: Arc<RwLock<Vec<Rpc>>>,
    poverty_list_rwlock: Arc<RwLock<Vec<Rpc>>>,
    cache: RequestBus<K, V>,
    config: Arc<RwLock<Settings>>,
    liveness_request_tx: LiveReadyRequestSnd,
) -> Result<hyper::Response<Full<Bytes>>, Infallible>
where
    K: GenericBytes,
    V: GenericBytes,
{
    if tx.uri().path() == "/ready" {
        return accept_readiness_request(liveness_request_tx).await;
    } else if tx.uri().path() == "/health" {
        return accept_health_request(liveness_request_tx).await;
    }

    let mut tx = match incoming_to_value(tx).await {
        Ok(res) => res,
        Err(err) => {
            tracing::error!(?err, "Admin request malformed");
            return Ok(hyper::Response::builder()
                .status(401)
                .body(Full::new(Bytes::from("Invalid admin request format")))
                .unwrap());
        }
    };

    // If we have JWT enabled check that tx is valid
    if config.read().unwrap().admin.jwt {
        let mut token_str = tx["token"].to_string();
        token_str = token_str.trim_matches('"').to_string();

        let token = match decode::<Claims>(
            &token_str,
            &config.read().unwrap().admin.key,
            &Validation::default(),
        ) {
            Ok(token) => token,
            Err(err) => {
                tracing::error!(?err, "JWT Auth error");
                return Ok(hyper::Response::builder()
                    .status(401)
                    .body(Full::new(Bytes::from("Unauthorized or invalid token")))
                    .unwrap());
            }
        };

        tx = json!({
            "id": token.claims.id,
            "jsonrpc": "2.0",
            "method": token.claims.method,
            "params": token.claims.params,
        });
    }

    // Send the request off to be processed
    let time = Instant::now();
    let response = forward_body(tx, &rpc_list_rwlock, &poverty_list_rwlock, cache, config).await;
    let time = time.elapsed();
    tracing::info!(?time, "Request time");

    response
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::admin::methods::RpseeRpcMethod;
    use crate::database_processing;
    use jsonwebtoken::DecodingKey;
    use sled::Config;
    use sled::Db;
    use tokio::sync::mpsc;

    // Helper function to create a test Settings config
    fn create_test_settings() -> Arc<RwLock<Settings>> {
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
    async fn forward_body_preserves_ids_and_reports_rpc_errors() {
        use http_body_util::BodyExt;

        let settings = create_test_settings();
        let cache = create_test_cache();
        let rpc_list = Arc::new(RwLock::new(vec![]));
        let poverty_list = Arc::new(RwLock::new(vec![]));
        for id in [json!("request-1"), json!(7), Null] {
            let request = json!({
                "id": id, "jsonrpc": "2.0", "method": RpseeRpcMethod::Ttl, "params": [],
            });
            let response = forward_body(
                request,
                &rpc_list,
                &poverty_list,
                cache.clone(),
                settings.clone(),
            )
            .await
            .unwrap();
            assert_eq!(
                response.headers()[hyper::header::CONTENT_TYPE],
                "application/json"
            );
            let body = response.into_body().collect().await.unwrap().to_bytes();
            let response: Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(response["id"], id);
            assert_eq!(response["result"], json!(settings.read().unwrap().ttl));
        }
        for (request, code) in [
            (
                json!({"id": "unknown", "jsonrpc": "2.0", "method": "missing"}),
                -32601,
            ),
            (
                json!({"id": "invalid", "jsonrpc": "2.0", "method": RpseeRpcMethod::SetTtl, "params": []}),
                -32602,
            ),
            (Null, -32600),
        ] {
            let id = request.get("id").cloned().unwrap_or(Null);
            let response = forward_body(
                request,
                &rpc_list,
                &poverty_list,
                cache.clone(),
                settings.clone(),
            )
            .await
            .unwrap();
            let body = response.into_body().collect().await.unwrap().to_bytes();
            let response: Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(response["id"], id);
            assert_eq!(response["error"]["code"], code);
            assert!(response.get("result").is_none());
        }
    }
}
