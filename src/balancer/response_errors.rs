//! HTTP responses for proxy failures.

#[macro_export]
macro_rules! no_rpc_available {
    ($id:expr) => {
        Ok(hyper::Response::builder()
            .status(500)
            .header(hyper::header::CONTENT_TYPE, "application/json")
            .body(Full::new(Bytes::from(
                serde_json::json!({"jsonrpc":"2.0", "id":$id, "error":{"code":-32002, "message":"No working RPC available"}}).to_string(),
            )))
            .unwrap())
    };
}

#[macro_export]
macro_rules! timed_out {
    ($id:expr) => {
        Ok(hyper::Response::builder()
            .status(408)
            .header(hyper::header::CONTENT_TYPE, "application/json")
            .body(Full::new(Bytes::from(
                serde_json::json!({"jsonrpc":"2.0", "id":$id, "error":{"code":-32001, "message":"Request timed out"}}).to_string(),
            )))
            .unwrap())
    };
}

#[macro_export]
macro_rules! print_cache_error {
    () => {
        tracing::error!("!!! Cache error! Check the DB !!!");
        tracing::error!(
            "To recover, please stop rpsee, delete your cache folder, and start rpsee again."
        );
        tracing::error!(
            "If the error perists, please open up an issue: https://github.com/monad-exp/rpsee/issues"
        );
    };
}

#[macro_export]
macro_rules! cache_error {
    ($id:expr) => {
        Ok(hyper::Response::builder()
            .status(500)
            .header(hyper::header::CONTENT_TYPE, "application/json")
            .body(Full::new(Bytes::from(
                serde_json::json!({"jsonrpc":"2.0", "id":$id, "error":{"code":-32003, "message":"Cache error"}}).to_string(),
            )))
            .unwrap())
    };
}

#[macro_export]
macro_rules! rpc_response {
    (
        $status:expr,
        $body:expr
    ) => {
        Ok(hyper::Response::builder()
            .status($status)
            .body($body)
            .unwrap())
    };
}
