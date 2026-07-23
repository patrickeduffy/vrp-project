\set ON_ERROR_STOP on

-- Run as the migration owner while connected to the migrated VRP database.
-- This file creates password-free capability roles only. Create LOGIN roles
-- separately and assign passwords interactively with psql's \password command.

BEGIN;

DO $roles$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'vrp_reference_loader') THEN
        CREATE ROLE vrp_reference_loader
            NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION INHERIT;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'vrp_eod_shadow_writer') THEN
        CREATE ROLE vrp_eod_shadow_writer
            NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION INHERIT;
    END IF;
END
$roles$;

ALTER ROLE vrp_reference_loader
    NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION INHERIT;
ALTER ROLE vrp_eod_shadow_writer
    NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION INHERIT;

GRANT CONNECT ON DATABASE :"DBNAME"
    TO vrp_reference_loader, vrp_eod_shadow_writer;
GRANT USAGE ON SCHEMA vrp
    TO vrp_reference_loader, vrp_eod_shadow_writer;
REVOKE CREATE ON SCHEMA vrp
    FROM vrp_reference_loader, vrp_eod_shadow_writer;

-- Reset direct table/function grants so rerunning this file converges to the
-- reviewed direct privilege set. PUBLIC function execution is revoked
-- separately below.
REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA vrp
    FROM vrp_reference_loader, vrp_eod_shadow_writer;
REVOKE ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA vrp
    FROM vrp_reference_loader, vrp_eod_shadow_writer;
REVOKE EXECUTE ON ALL FUNCTIONS IN SCHEMA vrp FROM PUBLIC;

-- Immutable reference-history loader.
GRANT SELECT ON TABLE
    vrp.model_versions,
    vrp.configuration_versions,
    vrp.pipeline_runs,
    vrp.pipeline_run_stages,
    vrp.data_assets,
    vrp.pipeline_run_data_assets,
    vrp.qa_results,
    vrp.reference_data_releases,
    vrp.daily_market_feature_definitions,
    vrp.reference_rate_observations,
    vrp.daily_market_features,
    vrp.current_reference_rate_observations,
    vrp.current_daily_market_features
TO vrp_reference_loader;

GRANT INSERT ON TABLE
    vrp.model_versions,
    vrp.configuration_versions,
    vrp.pipeline_runs,
    vrp.pipeline_run_stages,
    vrp.data_assets,
    vrp.pipeline_run_data_assets,
    vrp.qa_results,
    vrp.reference_data_releases,
    vrp.daily_market_feature_definitions,
    vrp.reference_rate_observations,
    vrp.daily_market_features
TO vrp_reference_loader;

GRANT UPDATE (
    status,
    qa_status,
    started_at,
    completed_at,
    error_summary,
    updated_at
) ON vrp.pipeline_runs TO vrp_reference_loader;

GRANT UPDATE (
    status,
    attempt_count,
    input_fingerprint,
    output_fingerprint,
    started_at,
    finished_at,
    last_error,
    metrics,
    updated_at
) ON vrp.pipeline_run_stages TO vrp_reference_loader;

GRANT UPDATE (
    outcome,
    severity,
    is_hard_gate,
    message,
    observed_value,
    expected_value,
    evidence,
    checked_at
) ON vrp.qa_results TO vrp_reference_loader;

GRANT EXECUTE ON FUNCTION vrp.force_current_load_transaction()
    TO vrp_reference_loader;
GRANT EXECUTE ON FUNCTION vrp.assert_compatible_reference_data_releases(UUID, UUID)
    TO vrp_reference_loader;
GRANT EXECUTE ON FUNCTION vrp.validate_reference_rate_successor()
    TO vrp_reference_loader;
GRANT EXECUTE ON FUNCTION vrp.validate_daily_market_feature_successor()
    TO vrp_reference_loader;

-- Post-publication EOD shadow writer. The signal-publication table is readable
-- for reconciliation but deliberately not writable.
GRANT SELECT ON TABLE
    vrp.model_versions,
    vrp.configuration_versions,
    vrp.pipeline_runs,
    vrp.pipeline_run_stages,
    vrp.data_assets,
    vrp.pipeline_run_data_assets,
    vrp.market_snapshots,
    vrp.implied_variance_term_structure,
    vrp.forecast_variance_term_structure,
    vrp.signal_features,
    vrp.signal_evaluations,
    vrp.selected_signals,
    vrp.qa_results,
    vrp.signal_publications,
    vrp.reference_data_releases,
    vrp.daily_market_feature_definitions,
    vrp.current_reference_rate_observations,
    vrp.current_daily_market_features
TO vrp_eod_shadow_writer;

GRANT INSERT ON TABLE
    vrp.model_versions,
    vrp.configuration_versions,
    vrp.pipeline_runs,
    vrp.pipeline_run_stages,
    vrp.data_assets,
    vrp.pipeline_run_data_assets,
    vrp.market_snapshots,
    vrp.implied_variance_term_structure,
    vrp.forecast_variance_term_structure,
    vrp.signal_features,
    vrp.signal_evaluations,
    vrp.selected_signals,
    vrp.qa_results
TO vrp_eod_shadow_writer;

GRANT UPDATE (
    status,
    qa_status,
    completed_at,
    updated_at
) ON vrp.pipeline_runs TO vrp_eod_shadow_writer;

GRANT UPDATE (
    status,
    output_fingerprint,
    finished_at,
    metrics,
    updated_at
) ON vrp.pipeline_run_stages TO vrp_eod_shadow_writer;

COMMENT ON ROLE vrp_reference_loader IS
    'Capability role for immutable SOFR and SPY daily reference-history loads.';
COMMENT ON ROLE vrp_eod_shadow_writer IS
    'Capability role for non-authoritative post-publication EOD snapshot recording.';

COMMIT;
