# rpsee

rpsee is a caching load balancer for Ethereum JSON-RPC over HTTP and WebSocket. It is based on [Blutgang](https://github.com/rainshowerLabs/blutgang) by Rainshower Labs and its contributors.

Both transports support JSON-RPC batches, preserve request IDs, and omit responses for notifications. HTTP requests containing only notifications return status 204.

## Build and run

Install [Rust through rustup](https://rustup.rs/). The repository pins the Rust toolchain used by CI and Docker.

The default build includes Sled and RocksDB. RocksDB needs a C/C++ toolchain and libclang. On Debian or Ubuntu:

```bash
sudo apt-get update
sudo apt-get install build-essential clang libclang-dev libssl-dev pkg-config
```

On macOS, install the Xcode command line tools with `xcode-select --install`.

From a checkout of this repository:

```bash
cp example_config.toml config.toml
# Edit config.toml to configure your RPC endpoints.
cargo run --release --locked -- --config config.toml
```

To install the binary from the checkout:

```bash
cargo install --path . --locked
rpsee --config config.toml
```

Run `rpsee --help` for command line options. Command line values override the configuration file.

Configuration uses `[rpsee]`, `[rpsee.admin]`, `[rpsee.sled]`, and `[rpsee.rocksdb]` tables. When migrating from Blutgang, rename those table prefixes, update cache paths as needed, and use the `rpsee_` prefix for admin RPC methods. Sled options use `cache_capacity_bytes` and `zstd_compression_level`; see [example_config.toml](example_config.toml).

For a smaller build with only one cache backend:

```bash
cargo build --release --locked --no-default-features --features sled,selection-weighed-round-robin
cargo build --release --locked --no-default-features --features rocksdb,selection-weighed-round-robin
```

Set `db` in the configuration to a backend enabled in your build.

Sled's `cache_capacity_bytes` controls its in-memory cache budget. It does not cap total process memory or database disk usage. Cached historical responses persist on disk; rpsee does not impose a disk quota. Size the cache volume separately from the memory limit. The `ttl` setting is the timeout for each upstream request attempt in milliseconds, not a cache lifetime; `health_check_ttl` is the health-check polling interval.

### Metrics and logging

Enable structured application logs and choose a Prometheus port with environment variables:

```bash
RUST_LOG=info TRACING_LOG_JSON=true TRACING_METRICS_PORT=9000 \
  rpsee --config config.toml
curl http://127.0.0.1:9000/metrics
```

Unset `TRACING_LOG_JSON` to use text logs. The metrics listener binds to `0.0.0.0:9000` by default. When using Docker, publish this port separately if needed, for example with `-p 127.0.0.1:9000:9000`.

Metrics cover cache hits and misses, RPC health, WebSocket subscriptions, and upstream HTTP attempts. `rpc_requests_total`, `rpc_requests_active`, `rpc_response_time_secs`, and `rpc_requests_errors_total` carry `rpc_name` and `method` labels. Unrecognized methods share the `other` label to bound metric cardinality. Errors include transport failures, unsuccessful HTTP statuses, and cancelled attempts such as timeouts; JSON-RPC errors inside successful HTTP responses are not counted. Cache hits do not make upstream attempts.

### Optimized build

The `maxperf` profile enables link-time optimization. `target-cpu=native` additionally targets the build machine's CPU, so only use the resulting binary on compatible machines:

```bash
RUSTFLAGS='-C target-cpu=native' cargo build --locked --profile maxperf
```

### Docker

Copy `example_config.toml` to `config.toml`, configure your RPC endpoints, and run:

```bash
docker compose up --build
```

Compose exposes port 3000, mounts the config read-only, and stores cache data in a named volume. The container binds the public RPC listener to `0.0.0.0`; the admin listener remains disabled in the example configuration.

To build and run the image directly:

```bash
docker build -t rpsee .
docker run --rm -p 3000:3000 \
  --mount type=bind,src="$(pwd)/config.toml",dst=/etc/rpsee/config.toml,readonly \
  --mount type=volume,src=rpsee-data,dst=/data \
  rpsee
```

The image workflow builds pull requests and publishes pushes to `master`, `main`, and `rpsee-v*` tags to `ghcr.io/<repository-owner>/<repository-name>` using the repository's GitHub token.

## License

The existing upstream GPL v2 license is preserved in [LICENSE-v1.md](https://github.com/monad-exp/rpsee/blob/HEAD/LICENSE-v1.md). Monad Foundation contributions are licensed under GPL v3 as set out in [LICENSE](https://github.com/monad-exp/rpsee/blob/HEAD/LICENSE). The additional license applies to those contributions and does not relicense upstream code.

## Acknowledgements

- [Blutgang](https://github.com/rainshowerLabs/blutgang)
- [dshackle](https://github.com/emeraldpay/dshackle)
- [proxyd](https://github.com/ethereum-optimism/optimism/tree/develop/proxyd)
- [web3-proxy](https://github.com/llamanodes/web3-proxy)
