FROM rust:1.88-slim AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config libssl-dev protobuf-compiler clang libclang-dev cmake make \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build
COPY . .
ENV RUSTFLAGS="-C link-args=-Wl,--allow-multiple-definition"

# Build with all features so memory, optimization, and evaluation modules
# are included in the binary. Without --features all these are excluded.
RUN cargo build --release -p horizons_rs --features all

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl && rm -rf /var/lib/apt/lists/*
COPY --from=builder /build/target/release/horizons_rs /usr/local/bin/horizons_rs

ENV HORIZONS_DEV_DATA_DIR=/data
VOLUME /data
EXPOSE 8000

ENTRYPOINT ["horizons_rs"]
CMD ["serve", "--addr", "0.0.0.0:8000"]
