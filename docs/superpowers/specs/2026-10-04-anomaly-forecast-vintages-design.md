# SmartRetail v0.6 — Forecast Vintages and Sales Anomaly Pipeline

Date: 2026-10-04
Branch: feature/v0.6-ml-platform

## Context

The SmartRetail ML flow already generates demand forecasts, detects anomalies from actual-vs-expected residuals, stages anomaly rows, and publishes them transactionally.

The next step is to make anomaly detection operational and temporally correct. The core issue is that the current `demand_forecast` primary key is `(product_id, forecast_date, model_version)`. This does not preserve multiple forecast vintages produced by the same model version for the same product/date on different training cutoffs.

If a later run overwrites an earlier forecast, the anomaly detector can no longer know which forecast was actually available before the observed sale date. That would introduce forecast leakage and weaken the business meaning of the anomaly score.

## Goal

Create a temporally valid anomaly-detection pipeline that:

1. preserves forecast history across repeated forecast runs;
2. selects only forecasts that existed before the actual sale being evaluated;
3. builds anomaly baselines only from prior residuals for the same product;
4. requires enough historical residuals before scoring;
5. publishes anomaly results without deleting historical anomaly rows.

## Forecast Vintage Model

A forecast vintage is uniquely identified by:

- `product_id`
- `forecast_date`
- `model_version`
- `training_cutoff_date`

The database schema will be evolved with a new Flyway migration rather than modifying the existing migration in place.

The effective uniqueness constraint becomes:

```text
(product_id, forecast_date, model_version, training_cutoff_date)
```

This allows the same model version to produce multiple forecasts for the same target date as the training cutoff advances.

Example:

```text
SKU-001 | 2026-10-10 | v7 | cutoff 2026-10-03 | 104
SKU-001 | 2026-10-10 | v7 | cutoff 2026-10-04 | 108
SKU-001 | 2026-10-10 | v7 | cutoff 2026-10-05 | 101
```

## Temporal Forecast Selection Rule

For an actual sale on `event_date`, only forecasts satisfying:

```text
training_cutoff_date < event_date
```

are eligible.

Among eligible rows for the same product and event date, choose the forecast with:

1. greatest `training_cutoff_date`;
2. if tied, greatest `generated_at`.

This selects the freshest forecast that was genuinely available before the observed sale date.

## Forecast Publication Semantics

Forecast publication must no longer delete all rows from `analytics.demand_forecast`.

The staging flow remains:

```text
score forecast
  -> analytics.demand_forecast_staging
  -> merge/upsert
  -> analytics.demand_forecast
```

Publication will insert new forecast vintages and update rows only when the full vintage key matches. Older vintages remain available for future anomaly analysis.

## Anomaly Baseline Policy

An anomaly continues to be defined from actual-vs-forecast residuals:

```text
residual = actual_units - expected_units
```

For each product, the baseline is built only from previous residuals. The current row is excluded to prevent temporal leakage.

Configuration:

```text
ANOMALY_THRESHOLD = 3.5
ANOMALY_MIN_HISTORY = 7
ANOMALY_HISTORY_WINDOW = 28
```

Rules:

- partition by `product_id`;
- order by `event_date`;
- retain at most the previous 28 residuals;
- do not score until at least 7 historical residuals exist;
- compute median and MAD from that history;
- classify anomaly when `abs(modified_z_score) > 3.5`.

The existing MAD=0 behavior remains explicit:

- residual equal to median -> score 0;
- residual different from median -> signed infinity and anomaly.

## Anomaly Publication Semantics

`analytics.sales_anomaly` is historical serving data and must not be globally replaced by each run.

The staging flow becomes:

```text
scored anomalies
  -> analytics.sales_anomaly_staging
  -> merge/upsert
  -> analytics.sales_anomaly
```

The existing target primary key remains:

```text
(event_date, product_id, model_version)
```

A matching row may be updated; unrelated historical rows are retained.

## End-to-End Job

A new executable job, `sales_anomaly_job.py`, will orchestrate:

1. build Spark session with existing S3/JDBC configuration;
2. read actual product-demand history from `PRODUCT_DEMAND_GOLD_PATH`;
3. read historical forecasts from PostgreSQL;
4. select temporally valid forecast vintages;
5. build actual-vs-forecast candidates;
6. calculate residuals;
7. attach rolling prior residual history by product;
8. enforce minimum history;
9. calculate MAD-based anomaly score;
10. stage anomaly rows;
11. publish anomaly rows through merge/upsert;
12. emit execution metrics such as candidate count, scored count, and anomaly count.

## Files Expected to Change

Primary implementation scope:

```text
backend/analytics-api/src/main/resources/db/migration/V3__forecast_vintage_key.sql
ml/src/main/python/serving_publish.py
ml/src/main/python/ml_config.py
ml/src/main/python/anomaly_detection_app.py
ml/src/main/python/sales_anomaly_job.py
ml/src/main/python/publish_forecast_app.py
ml/src/main/python/publish_anomaly_app.py
docker-compose.yml
.github/workflows/ml-ci.yml
ml/src/test/python/test_*.py
```

No existing Flyway migration will be rewritten.

## Test Strategy

Implementation will follow TDD.

Critical behaviors to prove first:

1. forecast publication does not erase older forecast vintages;
2. upsert conflict key includes `training_cutoff_date`;
3. anomaly publication does not erase unrelated historical anomaly rows;
4. forecast selection rejects `training_cutoff_date >= event_date`;
5. latest eligible cutoff is selected;
6. `generated_at` breaks ties deterministically;
7. current residual is excluded from baseline;
8. baseline is limited to 28 prior residuals;
9. rows with fewer than 7 prior residuals are not scored;
10. end-to-end anomaly job stages and publishes the expected contract.

Targeted test files will be run after each RED/GREEN cycle. The complete ML unittest suite and CI workflow will be used only for final verification.

## Compatibility and Migration

The migration must preserve existing forecast rows. The current primary key will be replaced with a new key including `training_cutoff_date`. Existing rows already contain `training_cutoff_date`, so no backfill column is required.

The staging table does not require a primary key.

Existing consumers that query `demand_forecast` by product/date may now see multiple vintages. Serving queries that intend to expose only the current forecast should explicitly select the latest relevant vintage rather than relying on one-row storage semantics.

## Success Criteria

The change is complete when:

- forecast vintages are retained across repeated runs;
- anomaly detection selects only temporally valid forecasts;
- anomaly scoring starts only after 7 prior residuals and uses at most 28;
- both forecast and anomaly publication use non-destructive merge/upsert behavior;
- the executable anomaly job runs with existing project infrastructure;
- targeted tests pass;
- the full ML test suite passes;
- GitHub Actions completes successfully.
