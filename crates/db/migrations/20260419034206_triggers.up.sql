-- Add up migration script here


-- ── 1. Auto-update updated_at on tx_results ───────────────────────────────────

CREATE OR REPLACE FUNCTION fn_set_updated_at()
RETURNS TRIGGER
LANGUAGE plpgsql AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$;

CREATE TRIGGER trg_tx_results_updated_at
    BEFORE UPDATE ON tx_results
    FOR EACH ROW
    EXECUTE FUNCTION fn_set_updated_at();

-- ── 2. Auto-compute claim_vs_reality_ms ──────────────────────────────────────
-- Fires on every INSERT or UPDATE of tx_results.
-- Computes the delta only when both landing_time_ms and rpc_confirm_time_ms
-- are non-null, so it is safe to call at any stage of the tx lifecycle.
--
-- Positive value → RPC reported confirmation before Geyser saw the tx on-chain.
-- Negative value → Geyser saw it before RPC reported (rare; RPC was slow).

CREATE OR REPLACE FUNCTION fn_compute_claim_vs_reality()
RETURNS TRIGGER
LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.landing_time_ms IS NOT NULL AND NEW.rpc_confirm_time_ms IS NOT NULL THEN
        NEW.claim_vs_reality_ms := NEW.rpc_confirm_time_ms - NEW.landing_time_ms;
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER trg_tx_results_claim_vs_reality
    BEFORE INSERT OR UPDATE ON tx_results
    FOR EACH ROW
    EXECUTE FUNCTION fn_compute_claim_vs_reality();

-- ── 3. Auto-resolve incidents when a provider recovers ────────────────────────
-- Fires after leaderboard_current is updated.
-- If the provider's status flips from 'outage'/'degraded' → 'healthy',
-- all open incidents for that provider are closed automatically.

CREATE OR REPLACE FUNCTION fn_auto_resolve_incidents()
RETURNS TRIGGER
LANGUAGE plpgsql AS $$
BEGIN
    IF (OLD.status IS DISTINCT FROM 'healthy') AND (NEW.status = 'healthy') THEN
        UPDATE incidents
        SET
            is_resolved      = TRUE,
            resolved_at      = NOW(),
            duration_seconds = EXTRACT(EPOCH FROM (NOW() - started_at))::INT
        WHERE
            provider_id = NEW.provider_id
            AND is_resolved = FALSE;
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER trg_leaderboard_incident_resolve
    AFTER UPDATE ON leaderboard_current
    FOR EACH ROW
    EXECUTE FUNCTION fn_auto_resolve_incidents();

-- ── 4. NOTIFY WebSocket server on leaderboard change ─────────────────────────
-- Fires after any INSERT or UPDATE on leaderboard_current.
-- The WebSocket server listens on the 'leaderboard_updated' channel via
-- LISTEN and pushes diffs to connected browser clients without polling.
--
-- Payload JSON fields:
--   provider_id, old_rank (NULL on insert), new_rank, status, updated_at

CREATE OR REPLACE FUNCTION fn_notify_leaderboard_update()
RETURNS TRIGGER
LANGUAGE plpgsql AS $$
DECLARE
    payload TEXT;
BEGIN
    payload := json_build_object(
        'provider_id', NEW.provider_id,
        'old_rank',    OLD.rank,         -- NULL on INSERT
        'new_rank',    NEW.rank,
        'status',      NEW.status,
        'landing_rate',NEW.landing_rate,
        'updated_at',  NEW.updated_at
    )::TEXT;

    PERFORM pg_notify('leaderboard_updated', payload);
    RETURN NEW;
END;
$$;

CREATE TRIGGER trg_leaderboard_pg_notify
    AFTER INSERT OR UPDATE ON leaderboard_current
    FOR EACH ROW
    EXECUTE FUNCTION fn_notify_leaderboard_update();

-- ── 5. NOTIFY WebSocket server on new incident ────────────────────────────────
-- Fires after a new incident row is inserted.
-- Lets the WebSocket server push an alert banner to all connected clients.

CREATE OR REPLACE FUNCTION fn_notify_incident_created()
RETURNS TRIGGER
LANGUAGE plpgsql AS $$
DECLARE
    payload TEXT;
BEGIN
    payload := json_build_object(
        'incident_id',   NEW.id,
        'provider_id',   NEW.provider_id,
        'incident_type', NEW.incident_type,
        'started_at',    NEW.started_at,
        'description',   NEW.description
    )::TEXT;

    PERFORM pg_notify('incident_created', payload);
    RETURN NEW;
END;
$$;

CREATE TRIGGER trg_incidents_notify
    AFTER INSERT ON incidents
    FOR EACH ROW
    EXECUTE FUNCTION fn_notify_incident_created();

-- ── 6. NOTIFY WebSocket server on incident resolution ────────────────────────

CREATE OR REPLACE FUNCTION fn_notify_incident_resolved()
RETURNS TRIGGER
LANGUAGE plpgsql AS $$
DECLARE
    payload TEXT;
BEGIN
    -- Only fire when is_resolved flips from FALSE → TRUE
    IF OLD.is_resolved = FALSE AND NEW.is_resolved = TRUE THEN
        payload := json_build_object(
            'incident_id',    NEW.id,
            'provider_id',    NEW.provider_id,
            'incident_type',  NEW.incident_type,
            'duration_seconds', NEW.duration_seconds,
            'resolved_at',    NEW.resolved_at
        )::TEXT;

        PERFORM pg_notify('incident_resolved', payload);
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER trg_incidents_resolved_notify
    AFTER UPDATE ON incidents
    FOR EACH ROW
    EXECUTE FUNCTION fn_notify_incident_resolved();