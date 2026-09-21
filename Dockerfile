FROM rust:slim AS build

RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential clang libclang-dev libssl-dev pkg-config \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY . .
ARG CARGO_BUILD_JOBS=2
ARG TARGETARCH
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,id=rpsee-target-${TARGETARCH},target=/app/target,sharing=locked \
    cargo build --locked --profile maxperf \
    && cp target/maxperf/rpsee /usr/local/bin/rpsee

FROM debian:stable-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates libssl3t64 libstdc++6 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=build /usr/local/bin/rpsee /usr/local/bin/rpsee

WORKDIR /data
EXPOSE 3000
ENTRYPOINT ["rpsee"]
CMD ["--config", "/etc/rpsee/config.toml", "--address", "0.0.0.0"]
