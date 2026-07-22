BEGIN;

CREATE SCHEMA IF NOT EXISTS vrp;

CREATE TABLE vrp.schema_migrations (
    version             TEXT PRIMARY KEY,
    description         TEXT NOT NULL,
    applied_at          TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE vrp.model_versions (
    model_version_id    UUID PRIMARY KEY,
    model_key           TEXT NOT NULL CHECK (length(btrim(model_key)) > 0),
    version_label       TEXT NOT NULL CHECK (length(btrim(version_label)) > 0),
    content_sha256      TEXT NOT NULL CHECK (content_sha256 ~ '^[0-9a-f]{64}$'),
    manifest            JSONB NOT NULL DEFAULT '{}'::JSONB
                        CHECK (jsonb_typeof(manifest) = 'object'),
    is_locked           BOOLEAN NOT NULL DEFAULT TRUE,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    locked_at           TIMESTAMPTZ,
    UNIQUE (model_key, version_label),
    UNIQUE (model_key, content_sha256),
    CHECK (NOT is_locked OR locked_at IS NOT NULL)
);

CREATE TABLE vrp.configuration_versions (
    configuration_version_id UUID PRIMARY KEY,
    configuration_key        TEXT NOT NULL CHECK (length(btrim(configuration_key)) > 0),
    version_label            TEXT NOT NULL CHECK (length(btrim(version_label)) > 0),
    content_sha256           TEXT NOT NULL CHECK (content_sha256 ~ '^[0-9a-f]{64}$'),
    configuration            JSONB NOT NULL CHECK (jsonb_typeof(configuration) = 'object'),
    created_at               TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (configuration_key, version_label),
    UNIQUE (configuration_key, content_sha256)
);

CREATE TABLE vrp.pipeline_runs (
    pipeline_run_id         UUID PRIMARY KEY,
    environment             TEXT NOT NULL CHECK (length(btrim(environment)) > 0),
    idempotency_key         TEXT NOT NULL CHECK (length(btrim(idempotency_key)) > 0),
    run_kind                TEXT NOT NULL CHECK (
                                run_kind IN (
                                    'EOD',
                                    'INTRADAY',
                                    'BACKFILL',
                                    'RECONCILIATION',
                                    'GOLDEN_TEST'
                                )
                            ),
    valuation_date          DATE NOT NULL,
    snapshot_at             TIMESTAMPTZ NOT NULL,
    data_cutoff_at          TIMESTAMPTZ NOT NULL,
    model_version_id        UUID NOT NULL REFERENCES vrp.model_versions (model_version_id),
    configuration_version_id UUID NOT NULL REFERENCES vrp.configuration_versions (
                                configuration_version_id
                            ),
    code_version            TEXT NOT NULL CHECK (length(btrim(code_version)) > 0),
    orchestrator_version    TEXT NOT NULL CHECK (length(btrim(orchestrator_version)) > 0),
    status                  TEXT NOT NULL DEFAULT 'PENDING' CHECK (
                                status IN (
                                    'PENDING',
                                    'RUNNING',
                                    'COMPLETED',
                                    'FAILED',
                                    'DEGRADED',
                                    'CANCELLED'
                                )
                            ),
    qa_status               TEXT NOT NULL DEFAULT 'NOT_RUN' CHECK (
                                qa_status IN ('NOT_RUN', 'PENDING', 'PASS', 'FAIL')
                            ),
    requested_at            TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    started_at              TIMESTAMPTZ,
    completed_at            TIMESTAMPTZ,
    requested_by            TEXT,
    supersedes_run_id       UUID REFERENCES vrp.pipeline_runs (pipeline_run_id),
    error_summary           TEXT,
    invocation              JSONB NOT NULL DEFAULT '{}'::JSONB
                            CHECK (jsonb_typeof(invocation) = 'object'),
    metadata                JSONB NOT NULL DEFAULT '{}'::JSONB
                            CHECK (jsonb_typeof(metadata) = 'object'),
    created_at              TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (environment, idempotency_key),
    UNIQUE (pipeline_run_id, status, qa_status),
    UNIQUE (pipeline_run_id, valuation_date, snapshot_at),
    CHECK (data_cutoff_at <= snapshot_at),
    CHECK (updated_at >= created_at),
    CHECK (started_at IS NULL OR started_at >= requested_at),
    CHECK (completed_at IS NULL OR (started_at IS NOT NULL AND completed_at >= started_at)),
    CHECK (
        (status = 'PENDING' AND started_at IS NULL AND completed_at IS NULL)
        OR (status = 'RUNNING' AND started_at IS NOT NULL AND completed_at IS NULL)
        OR (
            status IN ('COMPLETED', 'FAILED', 'DEGRADED', 'CANCELLED')
            AND started_at IS NOT NULL
            AND completed_at IS NOT NULL
        )
    ),
    CHECK (status <> 'COMPLETED' OR qa_status = 'PASS')
);

CREATE TABLE vrp.pipeline_run_stages (
    pipeline_stage_id   UUID PRIMARY KEY,
    pipeline_run_id     UUID NOT NULL REFERENCES vrp.pipeline_runs (pipeline_run_id),
    stage_name          TEXT NOT NULL CHECK (length(btrim(stage_name)) > 0),
    stage_order         INTEGER NOT NULL CHECK (stage_order >= 0),
    is_required         BOOLEAN NOT NULL DEFAULT TRUE,
    status              TEXT NOT NULL DEFAULT 'PENDING' CHECK (
                            status IN ('PENDING', 'RUNNING', 'COMPLETED', 'FAILED', 'SKIPPED')
                        ),
    attempt_count       INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    input_fingerprint   TEXT CHECK (
                            input_fingerprint IS NULL
                            OR input_fingerprint ~ '^[0-9a-f]{64}$'
                        ),
    output_fingerprint  TEXT CHECK (
                            output_fingerprint IS NULL
                            OR output_fingerprint ~ '^[0-9a-f]{64}$'
                        ),
    started_at          TIMESTAMPTZ,
    finished_at         TIMESTAMPTZ,
    last_error          JSONB NOT NULL DEFAULT '{}'::JSONB
                        CHECK (jsonb_typeof(last_error) = 'object'),
    metrics             JSONB NOT NULL DEFAULT '{}'::JSONB
                        CHECK (jsonb_typeof(metrics) = 'object'),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (pipeline_run_id, stage_name),
    UNIQUE (pipeline_run_id, stage_order),
    CHECK (finished_at IS NULL OR (started_at IS NOT NULL AND finished_at >= started_at)),
    CHECK (
        (status = 'PENDING' AND started_at IS NULL AND finished_at IS NULL)
        OR (status = 'RUNNING' AND started_at IS NOT NULL AND finished_at IS NULL)
        OR (
            status IN ('COMPLETED', 'FAILED')
            AND started_at IS NOT NULL
            AND finished_at IS NOT NULL
        )
        OR (
            status = 'SKIPPED'
            AND started_at IS NOT NULL
            AND finished_at IS NOT NULL
        )
    )
);

CREATE TABLE vrp.data_assets (
    data_asset_id       UUID PRIMARY KEY,
    dataset_name        TEXT NOT NULL CHECK (length(btrim(dataset_name)) > 0),
    asset_class         TEXT NOT NULL CHECK (
                            asset_class IN ('RAW', 'STANDARDIZED', 'DERIVED', 'MANIFEST', 'REPORT')
                        ),
    asset_format        TEXT NOT NULL CHECK (
                            asset_format IN (
                                'PARQUET',
                                'CSV',
                                'JSON',
                                'TEXT',
                                'BINARY',
                                'API_RESPONSE',
                                'OTHER'
                            )
                        ),
    storage_uri         TEXT NOT NULL CHECK (length(btrim(storage_uri)) > 0),
    content_sha256      TEXT NOT NULL CHECK (content_sha256 ~ '^[0-9a-f]{64}$'),
    schema_version      TEXT,
    source_system       TEXT,
    captured_at         TIMESTAMPTZ NOT NULL,
    observation_start_at TIMESTAMPTZ,
    observation_end_at  TIMESTAMPTZ,
    trade_date_start    DATE,
    trade_date_end      DATE,
    row_count           BIGINT CHECK (row_count IS NULL OR row_count >= 0),
    byte_size           BIGINT CHECK (byte_size IS NULL OR byte_size >= 0),
    is_immutable        BOOLEAN NOT NULL DEFAULT TRUE,
    metadata            JSONB NOT NULL DEFAULT '{}'::JSONB
                        CHECK (jsonb_typeof(metadata) = 'object'),
    created_at          TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (dataset_name, storage_uri, content_sha256),
    CHECK (
        observation_end_at IS NULL
        OR observation_start_at IS NULL
        OR observation_end_at >= observation_start_at
    ),
    CHECK (
        trade_date_end IS NULL
        OR trade_date_start IS NULL
        OR trade_date_end >= trade_date_start
    )
);

CREATE TABLE vrp.pipeline_run_data_assets (
    pipeline_run_id     UUID NOT NULL REFERENCES vrp.pipeline_runs (pipeline_run_id),
    data_asset_id       UUID NOT NULL REFERENCES vrp.data_assets (data_asset_id),
    usage_role          TEXT NOT NULL CHECK (
                            usage_role IN ('INPUT', 'OUTPUT', 'INTERMEDIATE', 'MANIFEST', 'QA_EVIDENCE')
                        ),
    logical_name        TEXT NOT NULL CHECK (length(btrim(logical_name)) > 0),
    stage_name          TEXT,
    is_required         BOOLEAN NOT NULL DEFAULT TRUE,
    lineage             JSONB NOT NULL DEFAULT '{}'::JSONB
                        CHECK (jsonb_typeof(lineage) = 'object'),
    PRIMARY KEY (pipeline_run_id, data_asset_id, usage_role, logical_name),
    FOREIGN KEY (pipeline_run_id, stage_name)
        REFERENCES vrp.pipeline_run_stages (pipeline_run_id, stage_name)
);

CREATE TABLE vrp.market_snapshots (
    market_snapshot_id  UUID PRIMARY KEY,
    pipeline_run_id     UUID NOT NULL UNIQUE,
    valuation_date      DATE NOT NULL,
    snapshot_at         TIMESTAMPTZ NOT NULL,
    snapshot_kind       TEXT NOT NULL CHECK (
                            snapshot_kind IN ('EOD_OFFICIAL', 'INTRADAY_PREVIEW')
                        ),
    market_session      TEXT NOT NULL CHECK (
                            market_session IN ('OPEN', 'CLOSED', 'AFTER_HOURS', 'UNKNOWN')
                        ),
    source_latest_at    TIMESTAMPTZ,
    freshness_status    TEXT NOT NULL CHECK (
                            freshness_status IN ('FRESH', 'STALE', 'INCOMPLETE', 'UNKNOWN')
                        ),
    spx_spot             NUMERIC(20, 8) CHECK (spx_spot IS NULL OR spx_spot > 0),
    spy_price            NUMERIC(20, 8) CHECK (spy_price IS NULL OR spy_price > 0),
    sofr_rate            NUMERIC(16, 12),
    sofr_observation_date DATE,
    details              JSONB NOT NULL DEFAULT '{}'::JSONB
                         CHECK (jsonb_typeof(details) = 'object'),
    created_at           TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (market_snapshot_id, pipeline_run_id),
    FOREIGN KEY (pipeline_run_id, valuation_date, snapshot_at)
        REFERENCES vrp.pipeline_runs (pipeline_run_id, valuation_date, snapshot_at)
);

CREATE TABLE vrp.implied_variance_term_structure (
    implied_variance_id      UUID PRIMARY KEY,
    pipeline_run_id          UUID NOT NULL REFERENCES vrp.pipeline_runs (pipeline_run_id),
    market_snapshot_id       UUID NOT NULL,
    tenor_days               SMALLINT NOT NULL CHECK (tenor_days BETWEEN 1 AND 366),
    target_expiration        DATE,
    effective_dte            DOUBLE PRECISION CHECK (effective_dte IS NULL OR effective_dte > 0),
    annualized_variance      DOUBLE PRECISION CHECK (
                                 annualized_variance IS NULL OR annualized_variance > 0
                             ),
    annualized_volatility_pct DOUBLE PRECISION CHECK (
                                 annualized_volatility_pct IS NULL
                                 OR annualized_volatility_pct > 0
                             ),
    calculation_status       TEXT NOT NULL CHECK (
                                 calculation_status IN ('AVAILABLE', 'MISSING', 'FAILED')
                             ),
    quality_status           TEXT NOT NULL CHECK (quality_status IN ('PASS', 'WARN', 'FAIL')),
    source_quote_at          TIMESTAMPTZ,
    computed_at              TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    quality_details          JSONB NOT NULL DEFAULT '{}'::JSONB
                             CHECK (jsonb_typeof(quality_details) = 'object'),
    UNIQUE (market_snapshot_id, tenor_days),
    UNIQUE (implied_variance_id, market_snapshot_id, tenor_days),
    FOREIGN KEY (market_snapshot_id, pipeline_run_id)
        REFERENCES vrp.market_snapshots (market_snapshot_id, pipeline_run_id),
    CHECK (
        calculation_status <> 'AVAILABLE'
        OR (
            target_expiration IS NOT NULL
            AND effective_dte IS NOT NULL
            AND annualized_variance IS NOT NULL
            AND annualized_volatility_pct IS NOT NULL
        )
    )
);

CREATE TABLE vrp.forecast_variance_term_structure (
    forecast_variance_id     UUID PRIMARY KEY,
    pipeline_run_id          UUID NOT NULL REFERENCES vrp.pipeline_runs (pipeline_run_id),
    market_snapshot_id       UUID NOT NULL,
    tenor_days               SMALLINT NOT NULL CHECK (tenor_days BETWEEN 1 AND 366),
    forecast_as_of_date      DATE NOT NULL,
    predicted_log_variance   DOUBLE PRECISION,
    annualized_variance      DOUBLE PRECISION CHECK (
                                 annualized_variance IS NULL OR annualized_variance > 0
                             ),
    annualized_volatility_pct DOUBLE PRECISION CHECK (
                                 annualized_volatility_pct IS NULL
                                 OR annualized_volatility_pct > 0
                             ),
    calculation_status       TEXT NOT NULL CHECK (
                                 calculation_status IN ('AVAILABLE', 'MISSING', 'FAILED')
                             ),
    quality_status           TEXT NOT NULL CHECK (quality_status IN ('PASS', 'WARN', 'FAIL')),
    computed_at              TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    quality_details          JSONB NOT NULL DEFAULT '{}'::JSONB
                             CHECK (jsonb_typeof(quality_details) = 'object'),
    UNIQUE (market_snapshot_id, tenor_days),
    UNIQUE (forecast_variance_id, market_snapshot_id, tenor_days),
    FOREIGN KEY (market_snapshot_id, pipeline_run_id)
        REFERENCES vrp.market_snapshots (market_snapshot_id, pipeline_run_id),
    CHECK (
        calculation_status <> 'AVAILABLE'
        OR (
            predicted_log_variance IS NOT NULL
            AND annualized_variance IS NOT NULL
            AND annualized_volatility_pct IS NOT NULL
        )
    )
);

CREATE TABLE vrp.signal_features (
    signal_feature_id       UUID PRIMARY KEY,
    pipeline_run_id         UUID NOT NULL REFERENCES vrp.pipeline_runs (pipeline_run_id),
    market_snapshot_id      UUID NOT NULL,
    tenor_days              SMALLINT NOT NULL CHECK (tenor_days BETWEEN 1 AND 366),
    tenor_bucket            TEXT NOT NULL CHECK (tenor_bucket IN ('FRONT', 'MIDDLE', 'BACK')),
    implied_variance_id     UUID NOT NULL,
    forecast_variance_id    UUID NOT NULL,
    vrp_log                 DOUBLE PRECISION,
    vrp_3m_prior_mean       DOUBLE PRECISION,
    vrp_3m_prior_sample_std DOUBLE PRECISION CHECK (
                                vrp_3m_prior_sample_std IS NULL
                                OR vrp_3m_prior_sample_std > 0
                            ),
    vrp_1y_prior_mean       DOUBLE PRECISION,
    vrp_1y_prior_sample_std DOUBLE PRECISION CHECK (
                                vrp_1y_prior_sample_std IS NULL
                                OR vrp_1y_prior_sample_std > 0
                            ),
    zscore_3m               DOUBLE PRECISION,
    zscore_1y               DOUBLE PRECISION,
    rsi14                   DOUBLE PRECISION CHECK (rsi14 IS NULL OR rsi14 BETWEEN 0 AND 100),
    rv21d_variance          DOUBLE PRECISION CHECK (
                                rv21d_variance IS NULL OR rv21d_variance >= 0
                            ),
    rv21d_volatility_pct    DOUBLE PRECISION CHECK (
                                rv21d_volatility_pct IS NULL OR rv21d_volatility_pct >= 0
                            ),
    zscore_3m_sample_count  INTEGER CHECK (
                                zscore_3m_sample_count IS NULL OR zscore_3m_sample_count >= 0
                            ),
    zscore_1y_sample_count  INTEGER CHECK (
                                zscore_1y_sample_count IS NULL OR zscore_1y_sample_count >= 0
                            ),
    history_through_date    DATE,
    is_complete             BOOLEAN NOT NULL,
    computed_at             TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    details                 JSONB NOT NULL DEFAULT '{}'::JSONB
                            CHECK (jsonb_typeof(details) = 'object'),
    UNIQUE (market_snapshot_id, tenor_days),
    UNIQUE (signal_feature_id, market_snapshot_id, tenor_days),
    UNIQUE (signal_feature_id, market_snapshot_id, pipeline_run_id),
    UNIQUE (
        signal_feature_id,
        market_snapshot_id,
        pipeline_run_id,
        tenor_days,
        tenor_bucket
    ),
    FOREIGN KEY (market_snapshot_id, pipeline_run_id)
        REFERENCES vrp.market_snapshots (market_snapshot_id, pipeline_run_id),
    FOREIGN KEY (implied_variance_id, market_snapshot_id, tenor_days)
        REFERENCES vrp.implied_variance_term_structure (
            implied_variance_id,
            market_snapshot_id,
            tenor_days
        ),
    FOREIGN KEY (forecast_variance_id, market_snapshot_id, tenor_days)
        REFERENCES vrp.forecast_variance_term_structure (
            forecast_variance_id,
            market_snapshot_id,
            tenor_days
        ),
    CHECK (
        NOT is_complete
        OR (
            vrp_log IS NOT NULL
            AND vrp_3m_prior_mean IS NOT NULL
            AND vrp_3m_prior_sample_std IS NOT NULL
            AND vrp_1y_prior_mean IS NOT NULL
            AND vrp_1y_prior_sample_std IS NOT NULL
            AND zscore_3m IS NOT NULL
            AND zscore_1y IS NOT NULL
            AND rsi14 IS NOT NULL
            AND rv21d_variance IS NOT NULL
            AND rv21d_volatility_pct IS NOT NULL
        )
    )
);

CREATE TABLE vrp.signal_evaluations (
    signal_evaluation_id UUID PRIMARY KEY,
    pipeline_run_id      UUID NOT NULL REFERENCES vrp.pipeline_runs (pipeline_run_id),
    market_snapshot_id   UUID NOT NULL,
    signal_feature_id    UUID NOT NULL,
    tenor_days           SMALLINT NOT NULL CHECK (tenor_days BETWEEN 1 AND 366),
    tenor_bucket         TEXT NOT NULL CHECK (tenor_bucket IN ('FRONT', 'MIDDLE', 'BACK')),
    signal_layer         TEXT NOT NULL CHECK (signal_layer IN ('CORE', 'SECONDARY', 'TERTIARY')),
    evaluation_status    TEXT NOT NULL CHECK (
                             evaluation_status IN (
                                 'QUALIFIED',
                                 'NOT_QUALIFIED',
                                 'INACTIVE',
                                 'INSUFFICIENT_DATA',
                                 'DATA_DEGRADED'
                             )
                         ),
    qualifies            BOOLEAN NOT NULL,
    vrp_pass             BOOLEAN,
    zscore_3m_pass       BOOLEAN,
    zscore_1y_pass       BOOLEAN,
    rsi14_pass           BOOLEAN,
    rv21d_pass           BOOLEAN,
    threshold_values     JSONB NOT NULL DEFAULT '{}'::JSONB
                         CHECK (jsonb_typeof(threshold_values) = 'object'),
    comparison_results   JSONB NOT NULL DEFAULT '{}'::JSONB
                         CHECK (jsonb_typeof(comparison_results) = 'object'),
    failed_checks        TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
    rank_position        INTEGER CHECK (rank_position IS NULL OR rank_position > 0),
    rank_score           DOUBLE PRECISION,
    target_size_pct_nav  NUMERIC(12, 8) CHECK (
                             target_size_pct_nav IS NULL
                             OR target_size_pct_nav BETWEEN 0 AND 1
                         ),
    evaluated_at         TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    details              JSONB NOT NULL DEFAULT '{}'::JSONB
                         CHECK (jsonb_typeof(details) = 'object'),
    UNIQUE (market_snapshot_id, tenor_days, signal_layer),
    UNIQUE (signal_evaluation_id, market_snapshot_id, pipeline_run_id),
    FOREIGN KEY (
        signal_feature_id,
        market_snapshot_id,
        pipeline_run_id,
        tenor_days,
        tenor_bucket
    )
        REFERENCES vrp.signal_features (
            signal_feature_id,
            market_snapshot_id,
            pipeline_run_id,
            tenor_days,
            tenor_bucket
        ),
    CHECK (
        (qualifies AND evaluation_status = 'QUALIFIED')
        OR (NOT qualifies AND evaluation_status <> 'QUALIFIED')
    )
);

CREATE TABLE vrp.selected_signals (
    selected_signal_id       UUID PRIMARY KEY,
    pipeline_run_id          UUID NOT NULL REFERENCES vrp.pipeline_runs (pipeline_run_id),
    market_snapshot_id       UUID NOT NULL UNIQUE,
    selected_evaluation_id   UUID,
    decision                 TEXT NOT NULL CHECK (decision IN ('TRADE', 'NO_TRADE', 'WITHHELD')),
    signal_state             TEXT NOT NULL CHECK (
                                 signal_state IN (
                                     'NO_SIGNAL',
                                     'PREVIEW_SIGNAL',
                                     'PREVIEW_SIGNAL_CHANGED',
                                     'DATA_DEGRADED',
                                     'EOD_OFFICIAL'
                                 )
                             ),
    selection_rule_id        TEXT NOT NULL CHECK (length(btrim(selection_rule_id)) > 0),
    no_trade_reason          TEXT,
    approved_nav_dollars     NUMERIC(20, 2) CHECK (
                                 approved_nav_dollars IS NULL OR approved_nav_dollars > 0
                             ),
    target_max_risk_dollars  NUMERIC(20, 2) CHECK (
                                 target_max_risk_dollars IS NULL OR target_max_risk_dollars >= 0
                             ),
    first_observed_at        TIMESTAMPTZ,
    consecutive_snapshots    INTEGER CHECK (
                                 consecutive_snapshots IS NULL OR consecutive_snapshots >= 1
                             ),
    decided_at               TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    selection_trace          JSONB NOT NULL DEFAULT '{}'::JSONB
                             CHECK (jsonb_typeof(selection_trace) = 'object'),
    UNIQUE (selected_signal_id, market_snapshot_id, pipeline_run_id),
    FOREIGN KEY (market_snapshot_id, pipeline_run_id)
        REFERENCES vrp.market_snapshots (market_snapshot_id, pipeline_run_id),
    FOREIGN KEY (selected_evaluation_id, market_snapshot_id, pipeline_run_id)
        REFERENCES vrp.signal_evaluations (
            signal_evaluation_id,
            market_snapshot_id,
            pipeline_run_id
        ),
    CHECK (
        (decision = 'TRADE' AND selected_evaluation_id IS NOT NULL)
        OR (
            decision IN ('NO_TRADE', 'WITHHELD')
            AND selected_evaluation_id IS NULL
            AND no_trade_reason IS NOT NULL
            AND length(btrim(no_trade_reason)) > 0
        )
    )
);

CREATE TABLE vrp.qa_results (
    qa_result_id         UUID PRIMARY KEY,
    pipeline_run_id      UUID NOT NULL REFERENCES vrp.pipeline_runs (pipeline_run_id),
    stage_name           TEXT,
    check_code           TEXT NOT NULL CHECK (length(btrim(check_code)) > 0),
    scope_key            TEXT NOT NULL DEFAULT 'run' CHECK (length(btrim(scope_key)) > 0),
    severity             TEXT NOT NULL CHECK (severity IN ('INFO', 'WARNING', 'ERROR')),
    outcome              TEXT NOT NULL CHECK (outcome IN ('PASS', 'WARN', 'FAIL', 'SKIP')),
    is_hard_gate         BOOLEAN NOT NULL DEFAULT FALSE,
    message              TEXT NOT NULL CHECK (length(btrim(message)) > 0),
    observed_value       JSONB,
    expected_value       JSONB,
    evidence             JSONB NOT NULL DEFAULT '{}'::JSONB
                         CHECK (jsonb_typeof(evidence) = 'object'),
    checked_at           TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (pipeline_run_id, check_code, scope_key),
    FOREIGN KEY (pipeline_run_id, stage_name)
        REFERENCES vrp.pipeline_run_stages (pipeline_run_id, stage_name),
    CHECK (NOT (is_hard_gate AND outcome = 'WARN'))
);

CREATE TABLE vrp.signal_publications (
    signal_publication_id UUID PRIMARY KEY,
    publication_scope     TEXT NOT NULL CHECK (length(btrim(publication_scope)) > 0),
    pipeline_run_id       UUID NOT NULL,
    market_snapshot_id    UUID NOT NULL,
    selected_signal_id    UUID NOT NULL,
    run_status            TEXT NOT NULL DEFAULT 'COMPLETED' CHECK (run_status = 'COMPLETED'),
    run_qa_status         TEXT NOT NULL DEFAULT 'PASS' CHECK (run_qa_status = 'PASS'),
    published_at          TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    published_by          TEXT,
    publication_metadata  JSONB NOT NULL DEFAULT '{}'::JSONB
                          CHECK (jsonb_typeof(publication_metadata) = 'object'),
    UNIQUE (publication_scope, pipeline_run_id),
    UNIQUE (publication_scope, market_snapshot_id),
    FOREIGN KEY (pipeline_run_id, run_status, run_qa_status)
        REFERENCES vrp.pipeline_runs (pipeline_run_id, status, qa_status),
    FOREIGN KEY (market_snapshot_id, pipeline_run_id)
        REFERENCES vrp.market_snapshots (market_snapshot_id, pipeline_run_id),
    FOREIGN KEY (selected_signal_id, market_snapshot_id, pipeline_run_id)
        REFERENCES vrp.selected_signals (
            selected_signal_id,
            market_snapshot_id,
            pipeline_run_id
        )
);

CREATE INDEX pipeline_runs_snapshot_idx
    ON vrp.pipeline_runs (environment, run_kind, valuation_date DESC, snapshot_at DESC);
CREATE INDEX pipeline_runs_status_idx
    ON vrp.pipeline_runs (environment, status, requested_at DESC);
CREATE INDEX pipeline_run_stages_status_idx
    ON vrp.pipeline_run_stages (pipeline_run_id, status, stage_order);
CREATE INDEX data_assets_dataset_dates_idx
    ON vrp.data_assets (dataset_name, trade_date_end DESC, captured_at DESC);
CREATE INDEX pipeline_run_data_assets_asset_idx
    ON vrp.pipeline_run_data_assets (data_asset_id, usage_role);
CREATE INDEX market_snapshots_observed_idx
    ON vrp.market_snapshots (snapshot_kind, valuation_date DESC, snapshot_at DESC);
CREATE INDEX implied_variance_tenor_history_idx
    ON vrp.implied_variance_term_structure (tenor_days, computed_at DESC);
CREATE INDEX forecast_variance_tenor_history_idx
    ON vrp.forecast_variance_term_structure (tenor_days, forecast_as_of_date DESC);
CREATE INDEX signal_features_tenor_history_idx
    ON vrp.signal_features (tenor_days, computed_at DESC);
CREATE INDEX signal_evaluations_qualified_idx
    ON vrp.signal_evaluations (market_snapshot_id, qualifies, signal_layer, rank_position);
CREATE INDEX selected_signals_decision_idx
    ON vrp.selected_signals (decision, decided_at DESC);
CREATE INDEX qa_results_attention_idx
    ON vrp.qa_results (pipeline_run_id, outcome, is_hard_gate)
    WHERE outcome IN ('WARN', 'FAIL');
CREATE INDEX signal_publications_latest_idx
    ON vrp.signal_publications (publication_scope, published_at DESC);

CREATE VIEW vrp.latest_published_snapshot AS
SELECT DISTINCT ON (publication.publication_scope)
    publication.publication_scope,
    publication.signal_publication_id,
    publication.published_at,
    run.pipeline_run_id,
    run.environment,
    run.run_kind,
    run.valuation_date,
    run.snapshot_at,
    run.data_cutoff_at,
    run.code_version,
    model.model_key,
    model.version_label AS model_version,
    configuration.configuration_key,
    configuration.version_label AS configuration_version,
    snapshot.market_snapshot_id,
    snapshot.snapshot_kind,
    snapshot.source_latest_at,
    snapshot.freshness_status,
    snapshot.spx_spot,
    snapshot.spy_price,
    selected.selected_signal_id,
    selected.decision,
    selected.signal_state,
    selected.no_trade_reason,
    selected.selection_rule_id,
    selected.decided_at,
    selected.first_observed_at,
    selected.consecutive_snapshots,
    evaluation.signal_layer,
    evaluation.tenor_bucket,
    feature.tenor_days,
    implied.annualized_variance AS implied_variance,
    forecast.annualized_variance AS forecast_variance,
    forecast.forecast_as_of_date,
    feature.vrp_log,
    feature.zscore_3m,
    feature.zscore_1y,
    feature.rsi14,
    feature.rv21d_volatility_pct,
    evaluation.target_size_pct_nav,
    selected.approved_nav_dollars,
    selected.target_max_risk_dollars
FROM vrp.signal_publications AS publication
JOIN vrp.pipeline_runs AS run
  ON run.pipeline_run_id = publication.pipeline_run_id
JOIN vrp.model_versions AS model
  ON model.model_version_id = run.model_version_id
JOIN vrp.configuration_versions AS configuration
  ON configuration.configuration_version_id = run.configuration_version_id
JOIN vrp.market_snapshots AS snapshot
  ON snapshot.market_snapshot_id = publication.market_snapshot_id
JOIN vrp.selected_signals AS selected
  ON selected.selected_signal_id = publication.selected_signal_id
LEFT JOIN vrp.signal_evaluations AS evaluation
  ON evaluation.signal_evaluation_id = selected.selected_evaluation_id
LEFT JOIN vrp.signal_features AS feature
  ON feature.signal_feature_id = evaluation.signal_feature_id
LEFT JOIN vrp.implied_variance_term_structure AS implied
  ON implied.implied_variance_id = feature.implied_variance_id
LEFT JOIN vrp.forecast_variance_term_structure AS forecast
  ON forecast.forecast_variance_id = feature.forecast_variance_id
ORDER BY
    publication.publication_scope,
    publication.published_at DESC,
    run.snapshot_at DESC,
    publication.signal_publication_id DESC;

COMMENT ON TABLE vrp.pipeline_runs IS
    'One idempotent, version-pinned attempt to calculate a single EOD or intraday snapshot.';
COMMENT ON TABLE vrp.pipeline_run_stages IS
    'Restart checkpoints. A completed stage may be reused when its input fingerprint is unchanged.';
COMMENT ON TABLE vrp.data_assets IS
    'Immutable manifests for raw, standardized, and derived files; bulk market rows remain in Parquet.';
COMMENT ON TABLE vrp.signal_publications IS
    'Append-only publication boundary. Only COMPLETED runs with PASS QA may become visible.';
COMMENT ON VIEW vrp.latest_published_snapshot IS
    'The newest atomically published decision in each publication scope.';

INSERT INTO vrp.schema_migrations (version, description)
VALUES ('0001', 'Initial VRP operational pipeline and signal publication schema');

COMMIT;
