FROM rust:1.85-slim-bookworm AS builder

WORKDIR /usr/src/kumquat

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    pkg-config \
    libclang-dev \
    cmake \
    git \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY . .

RUN cargo build --locked --release --bin kumquat

FROM debian:bookworm-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /usr/src/kumquat/target/release/kumquat /usr/local/bin/kumquat

RUN mkdir -p /data/kumquat

WORKDIR /data/kumquat

EXPOSE 30333 8545 9100

ENTRYPOINT ["kumquat"]

CMD ["--config", "/data/kumquat/config.toml"]
