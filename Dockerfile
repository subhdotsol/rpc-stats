FROM rust:slim-bookworm AS builder

# Install required build dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    pkg-config \
    libssl-dev \
    protobuf-compiler \
    cmake \
    python3 \
    && rm -rf /var/lib/apt/lists/*


WORKDIR /app

# Install sqlx-cli for database migrations (pin to 0.7.x to avoid edition2024 issues on rust 1.77)
RUN cargo install sqlx-cli --version "^0.7" --no-default-features --features postgres

# Copy the entire workspace
COPY . .

# Build all workspace binaries in release mode
RUN cargo build --release --workspace

# ---------------------------------------------------
# Runtime stage
FROM debian:bookworm-slim

# Install runtime dependencies (ca-certificates for TLS, libssl)
RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy the compiled binaries
COPY --from=builder /app/target/release/api /usr/local/bin/api
COPY --from=builder /app/target/release/alert-service /usr/local/bin/alert-service
COPY --from=builder /app/target/release/geyser-consumer /usr/local/bin/geyser-consumer
COPY --from=builder /app/target/release/ingestion-service /usr/local/bin/ingestion-service
COPY --from=builder /app/target/release/scheduler-service /usr/local/bin/scheduler-service
COPY --from=builder /app/target/release/stat-engine /usr/local/bin/stat-engine
COPY --from=builder /app/target/release/worker-service /usr/local/bin/worker-service
COPY --from=builder /app/target/release/incident-service /usr/local/bin/incident-service

# Copy sqlx-cli
COPY --from=builder /usr/local/cargo/bin/sqlx /usr/local/bin/sqlx

# Copy migrations folder for the migrator service
COPY crates/db/migrations ./migrations

# The default command does nothing, override via docker-compose
CMD ["echo", "Please specify a service to run"]
