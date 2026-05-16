FROM rust:1.95-bookworm AS builder

WORKDIR /app
COPY . .
RUN cargo build --release --bin donkeyspace-api --bin donkeyspace-worker

FROM debian:bookworm-slim AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates git \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY .donkeyspace/policy.yml /app/.donkeyspace/policy.yml
COPY --from=builder /app/target/release/donkeyspace-api /usr/local/bin/donkeyspace-api
COPY --from=builder /app/target/release/donkeyspace-worker /usr/local/bin/donkeyspace-worker

EXPOSE 8080
CMD ["donkeyspace-api"]
