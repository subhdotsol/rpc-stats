# RPC Stats API: Endpoints Documentation

This document outlines the current API endpoints implemented by the `api` crate. These endpoints provide the data backbone for the frontend RPC Stats interface, visualizing provider latency, reliability, incidents, and geographical performance.

## Core Operations

### `GET /`
- **Purpose**: A simple root index.
- **Usage**: Used as a minimal ping/status root. Responds with a welcome message.

### `GET /health`
- **Purpose**: System health check.
- **Usage**: Required for container orchestration (Docker/Kubernetes) or reversed proxies (like Nginx/HAProxy) to verify the service is running and ready to accept traffic. Responds with `200 OK`.

---

## Leaderboard (Under `/api/v1`)

### `GET /api/v1/leaderboard`
- **Query Params**: `window` (`1m`, `5m`, `1h`, `24h`)
- **Purpose**: Computes and returns the overall ranking of all RPC providers based on latency, success rates, and slot lag.
- **Usage**: Needed for the main dashboard Leaderboard to rank providers. Changing the `window` updates rankings based on recent performance vs 24-hour stability.

---

## RPCs (Under `/api/v1/rpcs`)

### `GET /api/v1/rpcs`
- **Purpose**: Fetches a lightweight catalog list of all tracked RPC providers along with basic metadata strings.
- **Usage**: Needed to populate global dropdowns, filter menus, or sidebar navigation lists across the frontend apps.

### `GET /api/v1/rpcs/{id}`
- **Purpose**: Retrieves a specific RPC provider's details, including global current success rate, avg latency, and basic health status.
- **Usage**: Feeds data into an individual RPC detail view or popup widget (e.g. clicking on "Helius" in the leaderboard to view their specialized summary box).

### `GET /api/v1/rpcs/{id}/timeseries`
- **Query Params**: `window` (`1h`, `6h`, `24h`)
- **Purpose**: Fetches periodic historical data buckets aggregating historical performance (success rate and latency trends over time).
- **Usage**: Necessary for rendering rich time-series line, area, or bar charts on the provider’s detail page, showing historical reliability.

### `GET /api/v1/rpcs/{id}/fee-breakdown`
- **Purpose**: Correlates different transaction fee tiers (like No Fee vs Turbo) directly against that specific provider's recorded latency and landing rates.
- **Usage**: Helps users determine if paying priority fees objectively impacts latency times on a given provider.

### `GET /api/v1/rpcs/{id}/region-latency`
- **Purpose**: Showcases that specific provider's latency mapped across geographic AWS/GCP regions (e.g. US-East vs EU-West).
- **Usage**: Used by developers to pick an RPC based on geospatial routing advantages. Displayed in map visualizations or regional tables.

### `GET /api/v1/rpcs/{id}/latest-tests`
- **Purpose**: Spits out the last 5 actual on-chain test transaction signatures ran by the scheduler against this provider.
- **Usage**: Creates a raw "Proof of Test" feed, allowing users to verify transparency by validating transaction signatures on solscan/solana-explorer.

---

## Incidents (Under `/api/v1/incidents`)

### `GET /api/v1/incidents`
- **Query Params**: 
  - `interval` (e.g. `24h` / `7d`)
  - `active` (boolean, optional)
  - `rpc_id` (provider slug, optional)
- **Purpose**: Lists historical or current platform outages, lag spikes, or degraded services.
- **Usage**: Renders an "Uptime / Status" page showing past bumps, active system degradations, or filtering out a specific provider to see if they've suffered recent downtime.

---

## System Overview (Under `/api/v1/summary`)

### `GET /api/v1/summary`
- **Purpose**: Exposes global macro-health of the entire platform mapping network-wide latency, the number of current active incidents, and total RPCs monitored.
- **Usage**: Feeds global "stat cards" at the very top of the frontend (e.g., `Platform Average Latency: 420ms | Active Incidents: 0 | Tracked RPCs: 14`).

---

## Benchmarks & Analytics (Under `/api/v1`)

### `GET /api/v1/benchmarks/methods`
- **Query Params**: `rpc_type` (`standard` or `yellowstone`, optional)
- **Purpose**: Breaks down latency by exact Solana RPC methods payload size, tracking response delays per unique method.
- **Usage**: Used for deep-dive technical reports/charts comparing identical method calls (e.g. `getProgramAccounts` speeds across vendors).

### `GET /api/v1/benchmarks/multi-region`
- **Purpose**: Aggregates latency mapped explicitly by geographical region irrespective of specific providers, rolling them into a matrix.
- **Usage**: Global matrix grids. Helping dApp owners decide what data-center to deploy their backend into.

### `GET /api/v1/rank-history`
- **Purpose**: Exposes how an RPC moved rank to rank.
- **Usage**: Powering graphical "bump charts" or trend lines tracking leaderboard shakeups.

### `GET /api/v1/test-runs`
- **Query Params**: `limit` (default: 20)
- **Purpose**: Global live feed of EVERY raw transaction test landing globally across all RPCs.
- **Usage**: Running in real-time on live activity ticker panels.
