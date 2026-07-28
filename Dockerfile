FROM rust:1.95-bookworm AS builder

WORKDIR /app
COPY . .
RUN cargo build --release --bin donkeyspace-api --bin donkeyspace-worker

FROM node:25-bookworm-slim AS runtime

ENV CARGO_HOME=/usr/local/cargo
ENV RUSTUP_HOME=/usr/local/rustup
ENV PATH=/usr/local/cargo/bin:$PATH

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates docker.io git ripgrep \
    && npm install -g @openai/codex@0.130.0 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY .donkeyspace/policy.yml /app/.donkeyspace/policy.yml
COPY docs/policy.plugin.example.yml /app/docs/policy.plugin.example.yml
COPY schemas/run-result.schema.json /app/schemas/run-result.schema.json
COPY schemas/run-result.codex.schema.json /app/schemas/run-result.codex.schema.json
COPY schemas/run-result.codex-developer.schema.json /app/schemas/run-result.codex-developer.schema.json
COPY schemas/run-result.codex-reviewer.schema.json /app/schemas/run-result.codex-reviewer.schema.json
COPY schemas/run-result.codex-repair.schema.json /app/schemas/run-result.codex-repair.schema.json
COPY scripts/donkeyspace-codex-triage /usr/local/bin/donkeyspace-codex-triage
COPY scripts/donkeyspace-codex-developer /usr/local/bin/donkeyspace-codex-developer
COPY scripts/donkeyspace-codex-reviewer /usr/local/bin/donkeyspace-codex-reviewer
COPY scripts/donkeyspace-codex-repair /usr/local/bin/donkeyspace-codex-repair
COPY --from=builder /usr/local/cargo /usr/local/cargo
COPY --from=builder /usr/local/rustup /usr/local/rustup
COPY --from=builder /app/target/release/donkeyspace-api /usr/local/bin/donkeyspace-api
COPY --from=builder /app/target/release/donkeyspace-worker /usr/local/bin/donkeyspace-worker

RUN chmod +x /usr/local/bin/donkeyspace-codex-triage /usr/local/bin/donkeyspace-codex-developer /usr/local/bin/donkeyspace-codex-reviewer /usr/local/bin/donkeyspace-codex-repair

EXPOSE 8080
CMD ["donkeyspace-api"]
