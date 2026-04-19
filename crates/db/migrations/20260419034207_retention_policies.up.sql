SELECT add_retention_policy('provider_metrics_1m', INTERVAL '7 days',  if_not_exists => TRUE);
SELECT add_retention_policy('rpc_method_metrics',  INTERVAL '7 days',  if_not_exists => TRUE);
SELECT add_retention_policy('ws_stream_metrics',   INTERVAL '7 days',  if_not_exists => TRUE);
SELECT add_retention_policy('network_conditions',  INTERVAL '3 days',  if_not_exists => TRUE);
SELECT add_retention_policy('provider_metrics_5m', INTERVAL '30 days', if_not_exists => TRUE);
SELECT add_retention_policy('provider_metrics_1h', INTERVAL '90 days', if_not_exists => TRUE);

ALTER TABLE provider_metrics_1m SET (timescaledb.compress, timescaledb.compress_segmentby = 'provider_id', timescaledb.compress_orderby = 'time DESC');
SELECT add_compression_policy('provider_metrics_1m', INTERVAL '2 days', if_not_exists => TRUE);

ALTER TABLE rpc_method_metrics SET (timescaledb.compress, timescaledb.compress_segmentby = 'provider_id, method_name', timescaledb.compress_orderby = 'time DESC');
SELECT add_compression_policy('rpc_method_metrics', INTERVAL '2 days', if_not_exists => TRUE);

ALTER TABLE ws_stream_metrics SET (timescaledb.compress, timescaledb.compress_segmentby = 'provider_id, stream_type', timescaledb.compress_orderby = 'time DESC');
SELECT add_compression_policy('ws_stream_metrics', INTERVAL '2 days', if_not_exists => TRUE);

ALTER TABLE network_conditions SET (timescaledb.compress, timescaledb.compress_orderby = 'time DESC');
SELECT add_compression_policy('network_conditions', INTERVAL '1 day', if_not_exists => TRUE);
