
<div align="center">

# RPC Stats 
## Transaction Landing Benchmark Dashboard

</div>

---

## The Problem

Every RPC provider claims superior transaction landing performance, yet there is no neutral, public, real time benchmark to validate these claims. Developers are forced to choose infrastructure based on marketing narratives rather than verifiable data.

---

## The Solution

RPC stats is a continuously running monitoring system that sends controlled test transactions through multiple major Solana RPC providers and measures real world performance. It exposes a live public dashboard backed by on chain proof, allowing developers to make decisions based on objective data instead of assumptions.

---

## All Planned Endpoints Till Now

https://subhisgreat.notion.site/RPC-STATS-endpoints-34517f1447c180fcbf3df227ce112dae?source=copy_link

## Core Idea

A live public scoreboard that silently tests every major Solana RPC provider every thirty seconds and reveals which providers actually land transactions under real network conditions.

---

## Features

### Landing Rate Tracker  
Simultaneously submits identical transactions through multiple providers and records which ones successfully land on chain.

### True Confirmation Time  
Compares provider reported confirmations against actual on chain inclusion to detect discrepancies and misleading confirmations.

### Slot Lag Measurement  
Tracks how far behind each provider is from the latest slot, a key indicator of transaction reliability.

### Public Leaderboard  
A single page live dashboard displaying real time rankings of providers based on landing performance.

### 24 Hour Trend Visualization  
Displays historical performance over the past day, clearly highlighting outages and degradation periods.

### Congestion Awareness  
Labels all metrics with network conditions, distinguishing performance during high congestion versus normal activity.

### Priority Fee Analysis  
Executes transactions with varying priority fees to measure the real impact of tipping across providers.

### Public API  
Provides programmatic access to benchmark data for integration into external tools, bots, and analytics systems.

### Webhook Alerts  
Sends instant notifications to communication platforms when provider performance drops below defined thresholds.

### Custom RPC Monitoring  
Allows users to test their own private RPC endpoints alongside public providers for direct comparison.

### Multi Region Testing  
Runs probes from multiple geographic regions to reflect real user distribution and latency differences.

### Claim Versus Reality Detection  
Publishes a rolling metric showing the delay between provider claimed confirmation and actual on chain finality.

---

## Architecture Note

RPC stats integrates seamlessly with gRPC Geyser streaming to independently verify when a transaction is truly landed on chain, eliminating reliance on RPC reported status alone.

---

## Vision

To establish a transparent and trustless standard for RPC performance, enabling developers to choose infrastructure based on measurable truth rather than unverified claims.

--- 

<div align="center">

Built for data driven developers who demand proof

</div>

