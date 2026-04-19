-- Add down migration script here
-- Drop continuous aggregates first (they depend on the hypertable)
DROP MATERIALIZED VIEW IF EXISTS provider_metrics_1d CASCADE;
DROP MATERIALIZED VIEW IF EXISTS provider_metrics_1h CASCADE;
DROP MATERIALIZED VIEW IF EXISTS provider_metrics_5m CASCADE;
DROP TABLE IF EXISTS provider_metrics_1m CASCADE;