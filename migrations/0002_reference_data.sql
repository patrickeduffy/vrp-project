BEGIN;

CREATE TABLE vrp.reference_data_releases (
    reference_data_release_id UUID PRIMARY KEY,
    dataset_key               TEXT NOT NULL
                              CHECK (length(btrim(dataset_key)) > 0),
    dataset_kind              TEXT NOT NULL CHECK (
                                  dataset_kind IN (
                                      'REFERENCE_RATE',
                                      'DAILY_MARKET_FEATURES'
                                  )
                              ),
    dataset_schema_version    TEXT NOT NULL
                              CHECK (length(btrim(dataset_schema_version)) > 0),
    normalized_content_sha256 TEXT NOT NULL
                              CHECK (normalized_content_sha256 ~ '^[0-9a-f]{64}$'),
    source_system             TEXT NOT NULL
                              CHECK (length(btrim(source_system)) > 0),
    loader_version            TEXT NOT NULL
                              CHECK (length(btrim(loader_version)) > 0),
    normalized_data_asset_id  UUID NOT NULL
                              REFERENCES vrp.data_assets (data_asset_id),
    qa_manifest_data_asset_id UUID
                              REFERENCES vrp.data_assets (data_asset_id),
    loaded_by_pipeline_run_id UUID
                              REFERENCES vrp.pipeline_runs (pipeline_run_id),
    vintage_kind              TEXT NOT NULL CHECK (
                                  vintage_kind IN (
                                      'POINT_IN_TIME',
                                      'LATEST_REVISED',
                                      'UNKNOWN'
                                  )
                              ),
    source_published_at       TIMESTAMPTZ,
    retrieved_at              TIMESTAMPTZ NOT NULL,
    accepted_at               TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    load_transaction_id       TEXT NOT NULL
                              DEFAULT (pg_current_xact_id()::TEXT),
    observation_start_date    DATE,
    observation_end_date      DATE,
    source_row_count          BIGINT NOT NULL CHECK (source_row_count >= 0),
    persisted_row_count       BIGINT NOT NULL CHECK (persisted_row_count >= 0),
    metadata                  JSONB NOT NULL DEFAULT '{}'::JSONB
                              CHECK (jsonb_typeof(metadata) = 'object'),
    UNIQUE (
        dataset_key,
        dataset_schema_version,
        normalized_content_sha256
    ),
    UNIQUE (
        reference_data_release_id,
        dataset_kind,
        load_transaction_id
    ),
    CHECK (
        observation_end_date IS NULL
        OR observation_start_date IS NULL
        OR observation_end_date >= observation_start_date
    ),
    CHECK (persisted_row_count <= source_row_count)
);

CREATE TABLE vrp.reference_rate_observations (
    reference_rate_observation_id UUID PRIMARY KEY,
    reference_data_release_id     UUID NOT NULL,
    release_dataset_kind          TEXT
                                  GENERATED ALWAYS AS ('REFERENCE_RATE') STORED,
    load_transaction_id           TEXT NOT NULL
                                  DEFAULT (pg_current_xact_id()::TEXT),
    series_key                    TEXT NOT NULL DEFAULT 'SOFR'
                                  CHECK (series_key = 'SOFR'),
    observation_date             DATE NOT NULL,
    rate_percent                 NUMERIC(16, 12) NOT NULL
                                  CHECK (rate_percent BETWEEN -5.0 AND 25.0),
    rate_decimal                 NUMERIC(18, 14)
                                  GENERATED ALWAYS AS (
                                      rate_percent / 100.0
                                  ) STORED,
    source_unit                  TEXT NOT NULL
                                  DEFAULT 'ANNUAL_PERCENTAGE_POINTS'
                                  CHECK (
                                      source_unit =
                                      'ANNUAL_PERCENTAGE_POINTS'
                                  ),
    supersedes_observation_id    UUID,
    row_sha256                   TEXT NOT NULL
                                  CHECK (row_sha256 ~ '^[0-9a-f]{64}$'),
    created_at                   TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (
        reference_data_release_id,
        series_key,
        observation_date
    ),
    UNIQUE (
        reference_rate_observation_id,
        series_key,
        observation_date
    ),
    UNIQUE (
        reference_rate_observation_id,
        observation_date,
        rate_decimal
    ),
    UNIQUE (supersedes_observation_id),
    CHECK (
        supersedes_observation_id IS NULL
        OR supersedes_observation_id <> reference_rate_observation_id
    ),
    FOREIGN KEY (
        reference_data_release_id,
        release_dataset_kind,
        load_transaction_id
    ) REFERENCES vrp.reference_data_releases (
        reference_data_release_id,
        dataset_kind,
        load_transaction_id
    ),
    FOREIGN KEY (
        supersedes_observation_id,
        series_key,
        observation_date
    ) REFERENCES vrp.reference_rate_observations (
        reference_rate_observation_id,
        series_key,
        observation_date
    )
);

CREATE UNIQUE INDEX reference_rate_one_root_idx
    ON vrp.reference_rate_observations (series_key, observation_date)
    WHERE supersedes_observation_id IS NULL;

CREATE INDEX reference_rate_observation_history_idx
    ON vrp.reference_rate_observations (series_key, observation_date DESC, created_at DESC);

CREATE TABLE vrp.daily_market_feature_definitions (
    daily_market_feature_definition_id UUID PRIMARY KEY,
    definition_key                     TEXT NOT NULL
                                        CHECK (length(btrim(definition_key)) > 0),
    version_label                      TEXT NOT NULL
                                        CHECK (length(btrim(version_label)) > 0),
    content_sha256                     TEXT NOT NULL
                                        CHECK (content_sha256 ~ '^[0-9a-f]{64}$'),
    symbol                             TEXT NOT NULL DEFAULT 'SPY'
                                        CHECK (symbol = 'SPY'),
    exchange_calendar                  TEXT NOT NULL DEFAULT 'XNYS'
                                        CHECK (exchange_calendar = 'XNYS'),
    price_adjustment                   TEXT NOT NULL CHECK (
                                            price_adjustment IN (
                                                'UNADJUSTED',
                                                'SPLIT_ADJUSTED',
                                                'TOTAL_RETURN_ADJUSTED',
                                                'UNKNOWN'
                                            )
                                        ),
    close_unit                         TEXT NOT NULL DEFAULT 'USD_PER_SHARE'
                                        CHECK (close_unit = 'USD_PER_SHARE'),
    return_formula_version             TEXT NOT NULL
                                        CHECK (length(btrim(return_formula_version)) > 0),
    return_unit                        TEXT NOT NULL DEFAULT 'DECIMAL_LOG_RETURN'
                                        CHECK (return_unit = 'DECIMAL_LOG_RETURN'),
    rsi_period_sessions                SMALLINT NOT NULL DEFAULT 14
                                        CHECK (rsi_period_sessions = 14),
    rsi_formula_version                TEXT NOT NULL
                                        CHECK (length(btrim(rsi_formula_version)) > 0),
    rsi_unit                           TEXT NOT NULL DEFAULT 'INDEX_0_100'
                                        CHECK (rsi_unit = 'INDEX_0_100'),
    rv_window_sessions                 SMALLINT NOT NULL DEFAULT 21
                                        CHECK (rv_window_sessions = 21),
    rv_sample_ddof                     SMALLINT NOT NULL DEFAULT 1
                                        CHECK (rv_sample_ddof = 1),
    annualization_sessions             SMALLINT NOT NULL DEFAULT 252
                                        CHECK (annualization_sessions = 252),
    rv_formula_version                 TEXT NOT NULL
                                        CHECK (length(btrim(rv_formula_version)) > 0),
    rv_variance_unit                   TEXT NOT NULL
                                        DEFAULT 'ANNUALIZED_DECIMAL_VARIANCE'
                                        CHECK (
                                            rv_variance_unit =
                                            'ANNUALIZED_DECIMAL_VARIANCE'
                                        ),
    rv_volatility_unit                 TEXT NOT NULL
                                        DEFAULT 'ANNUALIZED_PERCENTAGE_POINTS'
                                        CHECK (
                                            rv_volatility_unit =
                                            'ANNUALIZED_PERCENTAGE_POINTS'
                                        ),
    definition                         JSONB NOT NULL
                                        CHECK (jsonb_typeof(definition) = 'object'),
    created_at                         TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (definition_key, version_label),
    UNIQUE (definition_key, content_sha256)
);

CREATE TABLE vrp.daily_market_features (
    daily_market_feature_id            UUID PRIMARY KEY,
    daily_market_feature_definition_id UUID NOT NULL
                                        REFERENCES
                                        vrp.daily_market_feature_definitions (
                                            daily_market_feature_definition_id
                                        ),
    reference_data_release_id          UUID NOT NULL,
    release_dataset_kind               TEXT
                                        GENERATED ALWAYS AS (
                                            'DAILY_MARKET_FEATURES'
                                        ) STORED,
    load_transaction_id                TEXT NOT NULL
                                        DEFAULT (pg_current_xact_id()::TEXT),
    symbol                             TEXT NOT NULL DEFAULT 'SPY'
                                        CHECK (symbol = 'SPY'),
    trade_date                         DATE NOT NULL,
    prior_trade_date                   DATE,
    spy_close                          NUMERIC(20, 8) NOT NULL
                                        CHECK (spy_close > 0),
    spy_change                         DOUBLE PRECISION,
    spy_log_return                     DOUBLE PRECISION,
    wilder_avg_gain_14                 DOUBLE PRECISION
                                        CHECK (
                                            wilder_avg_gain_14 IS NULL
                                            OR wilder_avg_gain_14 >= 0
                                        ),
    wilder_avg_loss_14                 DOUBLE PRECISION
                                        CHECK (
                                            wilder_avg_loss_14 IS NULL
                                            OR wilder_avg_loss_14 >= 0
                                        ),
    rsi14                              DOUBLE PRECISION
                                        CHECK (
                                            rsi14 IS NULL
                                            OR rsi14 BETWEEN 0 AND 100
                                        ),
    rv21d_variance                     DOUBLE PRECISION
                                        CHECK (
                                            rv21d_variance IS NULL
                                            OR rv21d_variance >= 0
                                        ),
    rv21d_volatility_pct               DOUBLE PRECISION
                                        CHECK (
                                            rv21d_volatility_pct IS NULL
                                            OR rv21d_volatility_pct >= 0
                                        ),
    calculation_status                 TEXT NOT NULL CHECK (
                                            calculation_status IN (
                                                'AVAILABLE',
                                                'WARMUP',
                                                'MISSING',
                                                'FAILED'
                                            )
                                        ),
    quality_status                     TEXT NOT NULL CHECK (
                                            quality_status IN ('PASS', 'WARN', 'FAIL')
                                        ),
    supersedes_daily_market_feature_id UUID,
    row_sha256                         TEXT NOT NULL
                                        CHECK (row_sha256 ~ '^[0-9a-f]{64}$'),
    computed_at                        TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    details                            JSONB NOT NULL DEFAULT '{}'::JSONB
                                        CHECK (jsonb_typeof(details) = 'object'),
    UNIQUE (
        reference_data_release_id,
        daily_market_feature_definition_id,
        symbol,
        trade_date
    ),
    UNIQUE (
        daily_market_feature_id,
        daily_market_feature_definition_id,
        symbol,
        trade_date
    ),
    UNIQUE (
        daily_market_feature_id,
        rv21d_variance,
        rv21d_volatility_pct
    ),
    UNIQUE (supersedes_daily_market_feature_id),
    CHECK (
        spy_close NOT IN (
            'NaN'::NUMERIC,
            'Infinity'::NUMERIC,
            '-Infinity'::NUMERIC
        )
    ),
    CHECK (
        spy_change IS NULL
        OR spy_change NOT IN (
            'NaN'::DOUBLE PRECISION,
            'Infinity'::DOUBLE PRECISION,
            '-Infinity'::DOUBLE PRECISION
        )
    ),
    CHECK (
        spy_log_return IS NULL
        OR spy_log_return NOT IN (
            'NaN'::DOUBLE PRECISION,
            'Infinity'::DOUBLE PRECISION,
            '-Infinity'::DOUBLE PRECISION
        )
    ),
    CHECK (
        wilder_avg_gain_14 IS NULL
        OR wilder_avg_gain_14 NOT IN (
            'NaN'::DOUBLE PRECISION,
            'Infinity'::DOUBLE PRECISION,
            '-Infinity'::DOUBLE PRECISION
        )
    ),
    CHECK (
        wilder_avg_loss_14 IS NULL
        OR wilder_avg_loss_14 NOT IN (
            'NaN'::DOUBLE PRECISION,
            'Infinity'::DOUBLE PRECISION,
            '-Infinity'::DOUBLE PRECISION
        )
    ),
    CHECK (
        rsi14 IS NULL
        OR rsi14 NOT IN (
            'NaN'::DOUBLE PRECISION,
            'Infinity'::DOUBLE PRECISION,
            '-Infinity'::DOUBLE PRECISION
        )
    ),
    CHECK (
        rv21d_variance IS NULL
        OR rv21d_variance NOT IN (
            'NaN'::DOUBLE PRECISION,
            'Infinity'::DOUBLE PRECISION,
            '-Infinity'::DOUBLE PRECISION
        )
    ),
    CHECK (
        rv21d_volatility_pct IS NULL
        OR rv21d_volatility_pct NOT IN (
            'NaN'::DOUBLE PRECISION,
            'Infinity'::DOUBLE PRECISION,
            '-Infinity'::DOUBLE PRECISION
        )
    ),
    CHECK (prior_trade_date IS NULL OR prior_trade_date < trade_date),
    CHECK (
        supersedes_daily_market_feature_id IS NULL
        OR supersedes_daily_market_feature_id <> daily_market_feature_id
    ),
    CHECK (
        calculation_status <> 'AVAILABLE'
        OR (
            prior_trade_date IS NOT NULL
            AND spy_change IS NOT NULL
            AND spy_log_return IS NOT NULL
            AND wilder_avg_gain_14 IS NOT NULL
            AND wilder_avg_loss_14 IS NOT NULL
            AND rsi14 IS NOT NULL
            AND rv21d_variance IS NOT NULL
            AND rv21d_volatility_pct IS NOT NULL
        )
    ),
    FOREIGN KEY (
        reference_data_release_id,
        release_dataset_kind,
        load_transaction_id
    ) REFERENCES vrp.reference_data_releases (
        reference_data_release_id,
        dataset_kind,
        load_transaction_id
    ),
    FOREIGN KEY (
        supersedes_daily_market_feature_id,
        daily_market_feature_definition_id,
        symbol,
        trade_date
    ) REFERENCES vrp.daily_market_features (
        daily_market_feature_id,
        daily_market_feature_definition_id,
        symbol,
        trade_date
    )
);

CREATE UNIQUE INDEX daily_market_feature_one_root_idx
    ON vrp.daily_market_features (
        daily_market_feature_definition_id,
        symbol,
        trade_date
    )
    WHERE supersedes_daily_market_feature_id IS NULL;

CREATE INDEX daily_market_feature_history_idx
    ON vrp.daily_market_features (
        daily_market_feature_definition_id,
        symbol,
        trade_date DESC,
        computed_at DESC
    );

CREATE VIEW vrp.current_reference_rate_observations AS
SELECT observation.*
FROM vrp.reference_rate_observations AS observation
WHERE NOT EXISTS (
    SELECT 1
    FROM vrp.reference_rate_observations AS successor
    WHERE successor.supersedes_observation_id =
          observation.reference_rate_observation_id
);

CREATE VIEW vrp.current_daily_market_features AS
SELECT feature.*
FROM vrp.daily_market_features AS feature
WHERE NOT EXISTS (
    SELECT 1
    FROM vrp.daily_market_features AS successor
    WHERE successor.supersedes_daily_market_feature_id =
          feature.daily_market_feature_id
);

ALTER TABLE vrp.market_snapshots
    ALTER COLUMN sofr_rate TYPE NUMERIC(18, 14);

ALTER TABLE vrp.market_snapshots
    ADD COLUMN sofr_observation_id UUID,
    ADD COLUMN daily_market_feature_id UUID,
    ADD CONSTRAINT market_snapshots_sofr_observation_id_fk
        FOREIGN KEY (sofr_observation_id)
        REFERENCES vrp.reference_rate_observations (
            reference_rate_observation_id
        ),
    ADD CONSTRAINT market_snapshots_sofr_observation_fk
        FOREIGN KEY (
            sofr_observation_id,
            sofr_observation_date,
            sofr_rate
        ) REFERENCES vrp.reference_rate_observations (
            reference_rate_observation_id,
            observation_date,
            rate_decimal
        ),
    ADD CONSTRAINT market_snapshots_daily_feature_id_fk
        FOREIGN KEY (daily_market_feature_id)
        REFERENCES vrp.daily_market_features (daily_market_feature_id),
    ADD CONSTRAINT market_snapshots_feature_identity_uq
        UNIQUE (market_snapshot_id, daily_market_feature_id),
    ADD CONSTRAINT market_snapshots_sofr_pin_complete_ck
        CHECK (
            sofr_observation_id IS NULL
            OR (sofr_observation_date IS NOT NULL AND sofr_rate IS NOT NULL)
        ),
    ADD CONSTRAINT market_snapshots_sofr_prior_date_ck
        CHECK (
            sofr_observation_date IS NULL
            OR sofr_observation_date < valuation_date
        );

ALTER TABLE vrp.signal_features
    ADD COLUMN daily_market_feature_id UUID,
    ADD COLUMN rsi14_source_kind TEXT CHECK (
        rsi14_source_kind IN ('DAILY_OFFICIAL', 'INTRADAY_ESTIMATE')
    ),
    ADD CONSTRAINT signal_features_daily_feature_fk
        FOREIGN KEY (
            market_snapshot_id,
            daily_market_feature_id
        ) REFERENCES vrp.market_snapshots (
            market_snapshot_id,
            daily_market_feature_id
        ),
    ADD CONSTRAINT signal_features_daily_rv_values_fk
        FOREIGN KEY (
            daily_market_feature_id,
            rv21d_variance,
            rv21d_volatility_pct
        ) REFERENCES vrp.daily_market_features (
            daily_market_feature_id,
            rv21d_variance,
            rv21d_volatility_pct
        ),
    ADD CONSTRAINT signal_features_daily_reference_complete_ck
        CHECK (
            daily_market_feature_id IS NULL
            OR (
                rv21d_variance IS NOT NULL
                AND rv21d_volatility_pct IS NOT NULL
                AND rsi14_source_kind IS NOT NULL
            )
        );

CREATE FUNCTION vrp.force_current_load_transaction()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $body$
BEGIN
    NEW.load_transaction_id := pg_current_xact_id()::TEXT;
    RETURN NEW;
END;
$body$;

CREATE TRIGGER reference_data_release_current_transaction
BEFORE INSERT ON vrp.reference_data_releases
FOR EACH ROW EXECUTE FUNCTION vrp.force_current_load_transaction();

CREATE TRIGGER reference_rate_current_transaction
BEFORE INSERT ON vrp.reference_rate_observations
FOR EACH ROW EXECUTE FUNCTION vrp.force_current_load_transaction();

CREATE TRIGGER daily_market_feature_current_transaction
BEFORE INSERT ON vrp.daily_market_features
FOR EACH ROW EXECUTE FUNCTION vrp.force_current_load_transaction();

CREATE FUNCTION vrp.assert_compatible_reference_data_releases(
    new_release_id UUID,
    prior_release_id UUID
)
RETURNS VOID
LANGUAGE plpgsql
AS $body$
DECLARE
    new_release   vrp.reference_data_releases%ROWTYPE;
    prior_release vrp.reference_data_releases%ROWTYPE;
BEGIN
    SELECT * INTO STRICT new_release
    FROM vrp.reference_data_releases
    WHERE reference_data_release_id = new_release_id;

    SELECT * INTO STRICT prior_release
    FROM vrp.reference_data_releases
    WHERE reference_data_release_id = prior_release_id;

    IF new_release.dataset_key <> prior_release.dataset_key
       OR new_release.dataset_kind <> prior_release.dataset_kind
       OR new_release.dataset_schema_version <>
          prior_release.dataset_schema_version
       OR new_release.source_system <> prior_release.source_system
       OR new_release.vintage_kind <> prior_release.vintage_kind THEN
        RAISE EXCEPTION
            'A reference-data successor must preserve dataset, schema, source, and vintage identity'
            USING ERRCODE = '23514';
    END IF;

    IF new_release.accepted_at < prior_release.accepted_at
       OR new_release.retrieved_at < prior_release.retrieved_at THEN
        RAISE EXCEPTION
            'A reference-data successor cannot precede the release it supersedes'
            USING ERRCODE = '23514';
    END IF;

    IF prior_release.source_published_at IS NOT NULL
       AND (
           new_release.source_published_at IS NULL
           OR new_release.source_published_at < prior_release.source_published_at
       ) THEN
        RAISE EXCEPTION
            'A reference-data successor cannot use an older source publication'
            USING ERRCODE = '23514';
    END IF;

    IF prior_release.observation_start_date IS NOT NULL
       AND (
           new_release.observation_start_date IS NULL
           OR new_release.observation_start_date >
              prior_release.observation_start_date
       ) THEN
        RAISE EXCEPTION
            'A reference-data successor cannot shrink the start of source coverage'
            USING ERRCODE = '23514';
    END IF;

    IF prior_release.observation_end_date IS NOT NULL
       AND (
           new_release.observation_end_date IS NULL
           OR new_release.observation_end_date <
              prior_release.observation_end_date
       ) THEN
        RAISE EXCEPTION
            'A reference-data successor cannot shrink the end of source coverage'
            USING ERRCODE = '23514';
    END IF;

    IF new_release.source_row_count < prior_release.source_row_count THEN
        RAISE EXCEPTION
            'A reference-data successor cannot contain fewer source rows'
            USING ERRCODE = '23514';
    END IF;
END;
$body$;

CREATE FUNCTION vrp.validate_reference_rate_successor()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $body$
DECLARE
    prior_release_id UUID;
BEGIN
    IF NEW.supersedes_observation_id IS NOT NULL THEN
        SELECT reference_data_release_id INTO STRICT prior_release_id
        FROM vrp.reference_rate_observations
        WHERE reference_rate_observation_id =
              NEW.supersedes_observation_id;
        PERFORM vrp.assert_compatible_reference_data_releases(
            NEW.reference_data_release_id,
            prior_release_id
        );
    END IF;
    RETURN NEW;
END;
$body$;

CREATE FUNCTION vrp.validate_daily_market_feature_successor()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $body$
DECLARE
    prior_release_id UUID;
BEGIN
    IF NEW.supersedes_daily_market_feature_id IS NOT NULL THEN
        SELECT reference_data_release_id INTO STRICT prior_release_id
        FROM vrp.daily_market_features
        WHERE daily_market_feature_id =
              NEW.supersedes_daily_market_feature_id;
        PERFORM vrp.assert_compatible_reference_data_releases(
            NEW.reference_data_release_id,
            prior_release_id
        );
    END IF;
    RETURN NEW;
END;
$body$;

CREATE TRIGGER reference_rate_successor_release_guard
BEFORE INSERT ON vrp.reference_rate_observations
FOR EACH ROW EXECUTE FUNCTION vrp.validate_reference_rate_successor();

CREATE TRIGGER daily_market_feature_successor_release_guard
BEFORE INSERT ON vrp.daily_market_features
FOR EACH ROW EXECUTE FUNCTION vrp.validate_daily_market_feature_successor();

CREATE FUNCTION vrp.reject_reference_data_mutation()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $body$
BEGIN
    RAISE EXCEPTION
        'VRP reference data is append-only; insert a superseding row instead';
END;
$body$;

CREATE TRIGGER reference_data_releases_immutable
BEFORE UPDATE OR DELETE ON vrp.reference_data_releases
FOR EACH ROW EXECUTE FUNCTION vrp.reject_reference_data_mutation();

CREATE TRIGGER reference_rate_observations_immutable
BEFORE UPDATE OR DELETE ON vrp.reference_rate_observations
FOR EACH ROW EXECUTE FUNCTION vrp.reject_reference_data_mutation();

CREATE TRIGGER daily_market_feature_definitions_immutable
BEFORE UPDATE OR DELETE ON vrp.daily_market_feature_definitions
FOR EACH ROW EXECUTE FUNCTION vrp.reject_reference_data_mutation();

CREATE TRIGGER daily_market_features_immutable
BEFORE UPDATE OR DELETE ON vrp.daily_market_features
FOR EACH ROW EXECUTE FUNCTION vrp.reject_reference_data_mutation();

COMMENT ON TABLE vrp.reference_data_releases IS
    'Immutable accepted versions of normalized compact history; child rows must be inserted in the same transaction, sealing each release at commit.';
COMMENT ON TABLE vrp.reference_rate_observations IS
    'Append-only SOFR observations in source percentage points, with a generated decimal rate.';
COMMENT ON TABLE vrp.daily_market_feature_definitions IS
    'Immutable definitions that pin SPY price, return, RSI14, and signal RV21D semantics.';
COMMENT ON TABLE vrp.daily_market_features IS
    'Compact append-only SPY close/return/RSI14/RV21D history; corrections insert successors.';
COMMENT ON COLUMN vrp.market_snapshots.sofr_rate IS
    'Decimal annual rate used by the run (for example 0.0357 for a 3.57 percent SOFR observation).';
COMMENT ON COLUMN vrp.daily_market_features.rv21d_volatility_pct IS
    'Signal RV21D: 21-session sample standard deviation of log returns, annualized by sqrt(252), in percentage points.';
COMMENT ON COLUMN vrp.signal_features.rsi14_source_kind IS
    'DAILY_OFFICIAL for the accepted close calculation or INTRADAY_ESTIMATE for a live preview; RV21D remains pinned to daily_market_feature_id.';

INSERT INTO vrp.schema_migrations (version, description)
VALUES ('0002', 'Revision-safe compact SOFR and SPY daily reference data');

COMMIT;
