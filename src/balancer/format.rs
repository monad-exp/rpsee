use crate::{NamedBlocknumbers, rpc::method::EthRpcMethod};
use http_body_util::BodyExt;
use hyper::{Request, body::Incoming};
use serde_json::{Value, json};
use simd_json::serde::from_slice;
use std::sync::{Arc, RwLock};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NamedNumber {
    Latest,
    Earliest,
    Safe,
    Finalized,
    Pending,
    Null,
}
impl AsRef<str> for NamedNumber {
    fn as_ref(&self) -> &str {
        match self {
            Self::Latest => "latest",
            Self::Earliest => "earliest",
            Self::Safe => "safe",
            Self::Finalized => "finalized",
            Self::Pending => "pending",
            Self::Null => "null",
        }
    }
}

/// Returns the corresponding NamedNumber enum value for the named number
/// Null if n/a.
fn has_named_number(param: &str) -> NamedNumber {
    match param {
        "latest" => NamedNumber::Latest,
        "earliest" => NamedNumber::Earliest,
        "safe" => NamedNumber::Safe,
        "finalized" => NamedNumber::Finalized,
        "pending" => NamedNumber::Pending,
        _ => NamedNumber::Null,
    }
}

/// Returns the requested block number, if the method has a recognized block parameter.
pub fn get_block_number_from_request(
    tx: Value,
    named_blocknumbers: &Arc<RwLock<NamedBlocknumbers>>,
) -> Option<u64> {
    let position = EthRpcMethod::get_position(tx["method"].as_str())?;
    let block_number = tx.get("params")?.get(position)?.as_str()?;

    // Return the corresponding named parameter from the RwLock is present
    let nn = has_named_number(block_number);
    if nn != NamedNumber::Null {
        let rwlock_guard = named_blocknumbers.read().unwrap();

        match nn {
            NamedNumber::Latest => return Some(rwlock_guard.latest),
            NamedNumber::Earliest => return Some(rwlock_guard.earliest),
            NamedNumber::Safe => return Some(rwlock_guard.safe),
            NamedNumber::Finalized => return Some(rwlock_guard.finalized),
            NamedNumber::Pending => return Some(rwlock_guard.pending),
            NamedNumber::Null => return None,
        }
    }

    u64::from_str_radix(block_number.strip_prefix("0x")?, 16).ok()
}

/// Replaces block tags with a hex number and return the request
pub fn replace_block_tags(
    tx: &mut Value,
    named_blocknumbers: &Arc<RwLock<NamedBlocknumbers>>,
) -> Value {
    // Return if `params` is not a thing
    let params = tx["params"].as_array();
    if params.is_none_or(|p| p.is_empty()) {
        return tx.to_owned();
    }

    // Determine the correct parameter index based on the method
    let Some(position) = EthRpcMethod::get_position(tx["method"].as_str()) else {
        return tx.to_owned();
    };

    // Extract the block number parameter
    let Some(block_number) = tx["params"][position].as_str() else {
        return tx.to_owned();
    };

    // Check if the block number is a named tag
    let nn = has_named_number(block_number);
    if nn != NamedNumber::Null {
        let rwlock_guard = named_blocknumbers.read().unwrap_or_else(|e| {
            // Handle the case where the RwLock is poisoned
            e.into_inner()
        });

        // Replace the named block tag with its corresponding hex value
        match nn {
            NamedNumber::Latest => {
                if rwlock_guard.latest != 0 {
                    tx["params"][position] = json!(format!("0x{:x}", rwlock_guard.latest));
                }
            }
            NamedNumber::Finalized if rwlock_guard.finalized != 0 => {
                tx["params"][position] = json!(format!("0x{:x}", rwlock_guard.finalized));
            }
            _ => (),
        }
    }

    tx.to_owned()
}

#[derive(Debug, thiserror::Error)]
pub enum IncomingRequestError {
    #[error(transparent)]
    Body(#[from] hyper::Error),
    #[error(transparent)]
    Json(#[from] simd_json::Error),
}

pub fn validate_request(request: &Value) -> Result<(), Value> {
    let id = &request["id"];
    let valid_id = id.is_null() || id.is_string() || id.is_number();
    if request.is_object()
        && request["jsonrpc"] == "2.0"
        && request["method"].is_string()
        && valid_id
    {
        return Ok(());
    }
    Err(json!({
        "jsonrpc": "2.0",
        "id": if valid_id { id } else { &Value::Null },
        "error": { "code": -32600, "message": "Invalid Request" },
    }))
}

/// Parses the JSON body without altering string values.
pub async fn incoming_to_value(tx: Request<Incoming>) -> Result<Value, IncomingRequestError> {
    tracing::debug!(?tx, "Incoming request");
    let mut body = tx.collect().await?.to_bytes().to_vec();
    Ok(from_slice(&mut body)?)
}

#[cfg(test)]
mod tests {
    use crate::rpc::method::EthRpcMethod;

    use super::*;
    use serde_json::json;

    // Dummy NamedBlocknumbers for testing
    fn dummy_named_blocknumbers() -> Arc<RwLock<NamedBlocknumbers>> {
        Arc::new(RwLock::new(NamedBlocknumbers {
            latest: 10,
            earliest: 2,
            safe: 3,
            finalized: 4,
            pending: 5,
        }))
    }

    #[test]
    fn has_named_number_test() {
        assert_eq!(has_named_number("latest"), NamedNumber::Latest);
        assert_eq!(has_named_number("earliest"), NamedNumber::Earliest);
        assert_eq!(has_named_number("safe"), NamedNumber::Safe);
        assert_eq!(has_named_number("finalized"), NamedNumber::Finalized);
        assert_eq!(has_named_number("pending"), NamedNumber::Pending);
        assert_eq!(has_named_number("0x1"), NamedNumber::Null);
        assert_eq!(has_named_number("0x"), NamedNumber::Null);
        assert_eq!(has_named_number("0"), NamedNumber::Null);
    }

    #[test]
    fn malformed_block_parameters_are_not_cacheable() {
        let named_numbers = dummy_named_blocknumbers();
        for block in [
            json!(""),
            json!("0"),
            json!("€"),
            json!("latest-invalid"),
            json!(1),
            json!({"blockHash": "0x1"}),
        ] {
            let request = json!({"method": "eth_getBalance", "params": ["0x1", block]});
            assert_eq!(
                get_block_number_from_request(request.clone(), &named_numbers),
                None
            );
            assert_eq!(
                replace_block_tags(&mut request.clone(), &named_numbers),
                request
            );
        }
    }

    #[test]
    fn get_block_number_from_request_test() {
        // Set up a fake NamedBlocknumbers
        let named_blocknumbers = dummy_named_blocknumbers();

        let request = json!({
            "id": 1,
            "jsonrpc": "2.0",
            "method": EthRpcMethod::GetBalance,
            "params": ["0x407d73d8a49eeb85d32cf465507dd71d507100c1", "latest"]
        });

        assert_eq!(
            get_block_number_from_request(request, &named_blocknumbers),
            Some(10)
        );

        let request = json!({
            "id": 1,
            "jsonrpc": "2.0",
            "method": EthRpcMethod::GetBalance,
            "params": ["0x407d73d8a49eeb85d32cf465507dd71d507100c1", "adiuasiudagbdiad"]
        });

        assert_eq!(
            get_block_number_from_request(request, &named_blocknumbers),
            None
        );

        let request = json!({
            "id": 1,
            "jsonrpc": "2.0",
            "method": EthRpcMethod::GetBalance,
            "params": ["0x407d73d8a49eeb85d32cf465507dd71d507100c1", "0x1"]
        });

        assert_eq!(
            get_block_number_from_request(request, &named_blocknumbers),
            Some(1)
        );

        let request = json!({
            "id": 1,
            "jsonrpc": "2.0",
            "method": EthRpcMethod::GetBalance,
            "params": ["0x407d73d8a49eeb85d32cf465507dd71d507100c1"]
        });

        assert_eq!(
            get_block_number_from_request(request, &named_blocknumbers),
            None
        );

        let request = json!({
            "id": 1,
            "jsonrpc": "2.0",
            "method": EthRpcMethod::GetStorageAt,
            "params": ["0x407d73d8a49eeb85d32cf465507dd71d507100c1", "0x0", "latest"]
        });

        assert_eq!(
            get_block_number_from_request(request, &named_blocknumbers),
            Some(10)
        );

        let request = json!({
            "id": 1,
            "jsonrpc": "2.0",
            "method": EthRpcMethod::GetStorageAt,
            "params": ["0x407d73d8a49eeb85d32cf465507dd71d507100c1", "0x0", "0x1"]
        });

        assert_eq!(
            get_block_number_from_request(request, &named_blocknumbers),
            Some(1)
        );

        let request = json!({
            "id": 1,
            "jsonrpc": "2.0",
            "method": EthRpcMethod::GetStorageAt,
            "params": ["0x407d73d8a49eeb85d32cf465507dd71d507100c1", "0x0"]
        });

        assert_eq!(
            get_block_number_from_request(request, &named_blocknumbers),
            None
        );

        let request = json!({
            "id": 1,
            "jsonrpc": "2.0",
            "method": EthRpcMethod::GetTransactionCount,
            "params": ["0x407d73d8a49eeb85d32cf465507dd71d507100c1", "latest"]
        });

        assert_eq!(
            get_block_number_from_request(request, &named_blocknumbers),
            Some(10)
        );

        let request = json!({
            "id": 1,
            "jsonrpc": "2.0",
            "method": EthRpcMethod::GetTransactionCount,
            "params": ["0x407d73d8a49eeb85d32cf465507dd71d507100c1", "safe"]
        });

        assert_eq!(
            get_block_number_from_request(request, &named_blocknumbers),
            Some(3)
        );

        let request = json!({
            "id": 1,
            "jsonrpc": "2.0",
            "method": EthRpcMethod::GetTransactionCount,
            "params": ["0x407d73d8a49eeb85d32cf465507dd71d507100c1", "finalized"]
        });

        assert_eq!(
            get_block_number_from_request(request, &named_blocknumbers),
            Some(4)
        );

        let request = json!({
            "id": 1,
            "jsonrpc": "2.0",
            "method": EthRpcMethod::GetTransactionCount,
            "params": ["0x407d73d8a49eeb85d32cf465507dd71d507100c1", "pending"]
        });

        assert_eq!(
            get_block_number_from_request(request, &named_blocknumbers),
            Some(5)
        );

        let request = json!({
            "id": 1,
            "jsonrpc": "2.0",
            "method": EthRpcMethod::GetTransactionCount,
            "params": ["0x407d73d8a49eeb85d32cf465507dd71d507100c1", "0x1"]
        });

        assert_eq!(
            get_block_number_from_request(request, &named_blocknumbers),
            Some(1)
        );

        let request = json!({
            "id": 1,
            "jsonrpc": "2.0",
            "method": EthRpcMethod::GetTransactionCount,
            "params": ["0x407d73d8a49eeb85d32cf465507dd71d507100c1"]
        });

        assert_eq!(
            get_block_number_from_request(request, &named_blocknumbers),
            None
        );

        let request = json!({
            "id": 1,
            "jsonrpc": "2.0",
            "method": EthRpcMethod::GetBlockTransactionCountByNumber,
            "params": ["latest"]
        });

        assert_eq!(
            get_block_number_from_request(request, &named_blocknumbers),
            Some(10)
        );

        let request = json!({
            "id": 1,
            "jsonrpc": "2.0",
            "method": EthRpcMethod::GetBlockTransactionCountByNumber,
            "params": ["0x1"]
        });

        assert_eq!(
            get_block_number_from_request(request, &named_blocknumbers),
            Some(1)
        );

        let request = json!({
            "id": 1,
            "jsonrpc": "2.0",
            "method": EthRpcMethod::GetBlockTransactionCountByNumber,
            "params": []
        });

        assert_eq!(
            get_block_number_from_request(request, &named_blocknumbers),
            None
        );
    }

    #[test]
    fn replace_named_block_number_test() {
        let named_blocknumbers = dummy_named_blocknumbers();
        let mut tx = json!({
            "method": EthRpcMethod::GetBalance,
            "params": ["0x407d73d8a49eeb85d32cf465507dd71d507100c1", "latest"]
        });

        let expected = json!({
            "method": EthRpcMethod::GetBalance,
            "params": ["0x407d73d8a49eeb85d32cf465507dd71d507100c1", "0xa"]
        });

        assert_eq!(replace_block_tags(&mut tx, &named_blocknumbers), expected);
    }

    #[test]
    fn keep_hex_block_number_test() {
        let named_blocknumbers = dummy_named_blocknumbers();
        let mut tx = json!({
            "method": EthRpcMethod::GetBalance,
            "params": ["0x407d73d8a49eeb85d32cf465507dd71d507100c1", "0x1"]
        });

        assert_eq!(replace_block_tags(&mut tx, &named_blocknumbers), tx);
    }

    #[test]
    fn handle_invalid_block_number_test() {
        let named_blocknumbers = dummy_named_blocknumbers();
        let mut tx = json!({
            "method": EthRpcMethod::GetBalance,
            "params": ["0x407d73d8a49eeb85d32cf465507dd71d507100c1", "invalid"]
        });

        assert_eq!(replace_block_tags(&mut tx, &named_blocknumbers), tx);
    }

    #[test]
    fn replace_block_number_different_methods_test() {
        let named_blocknumbers = dummy_named_blocknumbers();
        let methods = vec![EthRpcMethod::GetBalance, EthRpcMethod::GetTransactionCount];

        for method in methods {
            let mut tx = json!({
                "method": method,
                "params": ["0x407d73d8a49eeb85d32cf465507dd71d507100c1", "latest"]
            });

            let expected = json!({
                "method": method,
                "params": ["0x407d73d8a49eeb85d32cf465507dd71d507100c1", "0xa"]
            });

            let a = replace_block_tags(&mut tx, &named_blocknumbers);

            assert_eq!(a, expected);
        }
    }

    #[test]
    fn handle_missing_or_empty_params_test() {
        let named_blocknumbers = dummy_named_blocknumbers();
        let mut tx_no_params = json!({
            "method": EthRpcMethod::GetBalance,
        });

        let mut tx_empty_params = json!({
            "method": EthRpcMethod::GetBalance,
            "params": []
        });

        assert_eq!(
            replace_block_tags(&mut tx_no_params, &named_blocknumbers),
            tx_no_params
        );
        assert_eq!(
            replace_block_tags(&mut tx_empty_params, &named_blocknumbers),
            tx_empty_params
        );
    }

    #[test]
    fn handle_zero_blocknumber() {
        let named_blocknumbers = dummy_named_blocknumbers();
        named_blocknumbers.write().unwrap().latest = 0;
        let mut tx = json!({
            "method": EthRpcMethod::GetTransactionCount,
            "params": ["0x407d73d8a49eeb85d32cf465507dd71d507100c1", "latest"]
        });

        let expected = json!({
            "method": EthRpcMethod::GetTransactionCount,
            "params": ["0x407d73d8a49eeb85d32cf465507dd71d507100c1", "latest"]
        });

        assert_eq!(replace_block_tags(&mut tx, &named_blocknumbers), expected);
    }

    #[test]
    fn handle_non_string_block_number_test() {
        let named_blocknumbers = dummy_named_blocknumbers();
        let mut tx = json!({
            "method": EthRpcMethod::GetBalance,
            "params": ["0x407d73d8a49eeb85d32cf465507dd71d507100c1", 100]
        });

        assert_eq!(replace_block_tags(&mut tx, &named_blocknumbers), tx);
    }
}
