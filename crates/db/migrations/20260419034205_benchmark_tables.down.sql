-- Add down migration script here
DROP TABLE IF EXISTS provider_pricing;
DROP TABLE IF EXISTS provider_features;
DROP TABLE IF EXISTS providers CASCADE;
DROP TABLE IF EXISTS network_conditions CASCADE;
DROP TABLE IF EXISTS ws_stream_metrics CASCADE;
DROP TABLE IF EXISTS rpc_method_metrics CASCADE;