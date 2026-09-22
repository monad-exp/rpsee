use crate::{Rpc, config::error::ConfigError, rpc::error::RpcError};
use std::time::{Duration, Instant};
use tokio::time::timeout;

async fn set_starting_latency(
    rpc: &mut Rpc,
    ma_length: f64,
    deadline: Duration,
) -> Result<(), ConfigError> {
    let mut latencies = Vec::new();
    for _ in 0..ma_length as u32 {
        let start = Instant::now();
        let syncing = timeout(deadline, rpc.syncing())
            .await
            .map_err(|_| RpcError::SendError("Startup probe timed out".into()))??;
        if syncing {
            return Err(ConfigError::Syncing);
        }
        latencies.push(start.elapsed().as_nanos() as f64);
    }
    rpc.update_latency(latencies.iter().sum::<f64>() / latencies.len() as f64);
    tracing::debug!("{}: {}ns", rpc.name, rpc.status.latency);
    Ok(())
}

/// Sample `eth_syncing` latency and move failed endpoints to the recovery pool.
pub async fn sort_by_latency(
    rpc_list: Vec<Rpc>,
    mut poverty_list: Vec<Rpc>,
    ma_length: f64,
    ttl: u128,
) -> Result<(Vec<Rpc>, Vec<Rpc>), ConfigError> {
    let deadline = Duration::from_millis(u64::try_from(ttl).unwrap_or(u64::MAX));
    let probes = rpc_list.into_iter().map(|mut rpc| async move {
        let result = set_starting_latency(&mut rpc, ma_length, deadline).await;
        (rpc, result)
    });
    let mut sorted_rpc_list = Vec::new();
    for (mut rpc, result) in futures_util::future::join_all(probes).await {
        match result {
            Ok(()) => sorted_rpc_list.push(rpc),
            Err(error) => {
                tracing::warn!(?error, rpc_name = rpc.name, "Startup probe failed");
                rpc.status.is_erroring = true;
                poverty_list.push(rpc);
            }
        }
    }
    sorted_rpc_list.sort_by(|a, b| a.status.latency.total_cmp(&b.status.latency));
    Ok((sorted_rpc_list, poverty_list))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    #[tokio::test]
    async fn startup_keeps_healthy_rpc_and_cancels_stalled_probe() {
        let healthy = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let stalled = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let rpcs = [&healthy, &stalled]
            .map(|listener| {
                Rpc::new(
                    format!("http://{}", listener.local_addr().unwrap())
                        .parse()
                        .unwrap(),
                    None,
                    1,
                    0,
                    1.0,
                )
            })
            .to_vec();
        let healthy_server = tokio::spawn(async move {
            let (mut stream, _) = healthy.accept().await.unwrap();
            let mut request = [0; 4096];
            assert!(stream.read(&mut request).await.unwrap() > 0);
            let body = r#"{"jsonrpc":"2.0","id":1,"result":false}"#;
            stream
                .write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
        });
        let stalled_server = tokio::spawn(async move {
            let (mut stream, _) = stalled.accept().await.unwrap();
            let mut request = [0; 4096];
            assert!(stream.read(&mut request).await.unwrap() > 0);
            while stream.read(&mut request).await.unwrap() != 0 {}
        });
        let (active, failed) = timeout(
            Duration::from_secs(2),
            sort_by_latency(rpcs, Vec::new(), 1.0, 100),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(active.len(), 1);
        assert_eq!(failed.len(), 1);
        assert!(failed[0].status.is_erroring);
        healthy_server.await.unwrap();
        timeout(Duration::from_secs(2), stalled_server)
            .await
            .unwrap()
            .unwrap();
    }
}
