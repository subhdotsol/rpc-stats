-- Add down migration script here
DROP TRIGGER IF EXISTS trg_incidents_resolved_notify   ON incidents;
DROP TRIGGER IF EXISTS trg_incidents_notify            ON incidents;
DROP TRIGGER IF EXISTS trg_leaderboard_pg_notify       ON leaderboard_current;
DROP TRIGGER IF EXISTS trg_leaderboard_incident_resolve ON leaderboard_current;
DROP TRIGGER IF EXISTS trg_tx_results_claim_vs_reality ON tx_results;
DROP TRIGGER IF EXISTS trg_tx_results_updated_at       ON tx_results;
 
DROP FUNCTION IF EXISTS fn_notify_incident_resolved();
DROP FUNCTION IF EXISTS fn_notify_incident_created();
DROP FUNCTION IF EXISTS fn_notify_leaderboard_update();
DROP FUNCTION IF EXISTS fn_auto_resolve_incidents();
DROP FUNCTION IF EXISTS fn_compute_claim_vs_reality();
DROP FUNCTION IF EXISTS fn_set_updated_at();
 