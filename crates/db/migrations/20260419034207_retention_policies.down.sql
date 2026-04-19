-- Add down migration script here

-- Remove compression policies first, then retention policies.
-- Errors are suppressed (IF EXISTS) so partial rollbacks don't fail.

SELECT remove_compression_policy('network_conditions',   if_not_exists => TRUE);
SELECT remove_compression_policy('ws_stream_metrics',    if_not_exists => TRUE);
SELECT remove_compression_policy('rpc_method_metrics',   if_not_exists => TRUE);
SELECT remove_compression_policy('provider_metrics_1m',  if_not_exists => TRUE);

SELECT remove_retention_policy('provider_metrics_1h',    if_not_exists => TRUE);
SELECT remove_retention_policy('provider_metrics_5m',    if_not_exists => TRUE);
SELECT remove_retention_policy('network_conditions',     if_not_exists => TRUE);
SELECT remove_retention_policy('ws_stream_metrics',      if_not_exists => TRUE);
SELECT remove_retention_policy('rpc_method_metrics',     if_not_exists => TRUE);
SELECT remove_retention_policy('provider_metrics_1m',    if_not_exists => TRUE);
SELECT remove_retention_policy('transactions',           if_not_exists => TRUE);