.PHONY: all build api scheduler worker ingestion incident alert run-api run-scheduler run-worker run-ingestion run-incident run-alert test clean help

# Default target: build everything
all: build

build:
	cargo build

# Build targets for specific services
api:
	cargo build -p api

scheduler:
	cargo build -p scheduler-service

worker:
	cargo build -p worker-service

ingestion:
	cargo build -p ingestion-service

incident:
	cargo build -p incident-service

alert:
	cargo build -p alert-service

# Run targets for specific services
run-api:
	cargo run -p api

run-scheduler:
	cargo run -p scheduler-service

run-worker:
	cargo run -p worker-service

run-ingestion:
	cargo run -p ingestion-service

run-incident:
	cargo run -p incident-service

run-alert:
	cargo run -p alert-service

# Testing and Maintenance
test:
	cargo test

clean:
	cargo clean

help:
	@echo "Available commands:"
	@echo "  make all          - Build the entire workspace"
	@echo "  make api          - Build the api service"
	@echo "  make scheduler    - Build the scheduler-service"
	@echo "  make worker       - Build the worker-service"
	@echo "  make ingestion    - Build the ingestion-service"
	@echo "  make incident     - Build the incident-service"
	@echo "  make alert        - Build the alert-service"
	@echo "  make run-<name>   - Run the specific service (e.g., make run-api)"
	@echo "  make test         - Run all tests"
	@echo "  make clean        - Clean build artifacts"
