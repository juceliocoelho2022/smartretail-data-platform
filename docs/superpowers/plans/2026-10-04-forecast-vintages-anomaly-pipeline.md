# Forecast Vintages and Sales Anomaly Pipeline Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make forecast storage historical and anomaly detection operational, temporally valid, and non-destructive end to end.

**Architecture:** Forecast and anomaly publication will use staging plus PostgreSQL `ON CONFLICT` merge semantics instead of global replacement. Forecast vintages will be keyed by product, target date, model version, and training cutoff; the anomaly job will select the freshest forecast that existed before the actual sale date, build a bounded prior-residual baseline per product, score with MAD, stage results, and merge them into serving storage.

**Tech Stack:** Python 3, PySpark 4, PostgreSQL 17, psycopg2, Flyway, Docker Compose, GitHub Actions, unittest.

**Spec:** `docs/superpowers/specs/2026-10-04-anomaly-forecast-vintages-design.md`

## Global Constraints

- Preserve all existing forecast rows during schema migration.
- Forecast vintage uniqueness is `(product_id, forecast_date, model_version, training_cutoff_date)`.
- Eligible forecast rule is `training_cutoff_date < event_date`.
- Tie-breaking is greatest `training_cutoff_date`, then greatest `generated_at`.
- `ANOMALY_THRESHOLD = 3.5`.
- `ANOMALY_MIN_HISTORY = 7`.
- `ANOMALY_HISTORY_WINDOW = 28`.
- Baselines are partitioned by `product_id`, ordered by `event_date`, and exclude the current residual.
- MAD=0 remains: residual == median -> score 0; otherwise signed infinity and anomaly.
- Forecast and anomaly publication must retain unrelated historical target rows.
- Existing Flyway migrations must not be edited.

## Review Focus

- Duplicate rows in staging for the same conflict key must not make publication nondeterministic; tests must reject ambiguous duplicate vintages before merge.
- Forecast rows with `training_cutoff_date == event_date` or later must never be selected; temporal-selection tests cover both boundary cases.
- Multiple eligible vintages with equal cutoff must deterministically choose the latest `generated_at`; selection tests pin this behavior.
- Products with fewer than 7 previous residuals must produce no scored row; baseline tests cover 0 through 6 prior observations.
- A rerun of the anomaly job for already-published dates must update matching keys without deleting unrelated historical anomaly rows; publication tests and CI database assertions cover reruns.

---

### Task 1: Evolve the forecast vintage key without losing data

**Files:**
- Create: `backend/analytics-api/src/main/resources/db/migration/V3__forecast_vintage_key.sql`
- Test: `backend/analytics-api/src/test/java/...` only if an existing migration integration-test pattern exists; otherwise verify through Flyway startup in CI.

**Interfaces:**
- Consumes: existing `analytics.demand_forecast` from V2, including non-null `training_cutoff_date`.
- Produces: primary key `(product_id, forecast_date, model_version, training_cutoff_date)` while preserving rows and existing secondary indexes.

- [ ] **Step 1: Add a failing schema verification**

Use the existing analytics-api/Flyway test pattern if present; otherwise add a CI-safe SQL assertion executed after analytics-api startup that queries `pg_constraint`/`pg_attribute` and expects all four primary-key columns in order.

- [ ] **Step 2: Run the focused verification and confirm RED**

Run the existing analytics-api test command or equivalent migration assertion.
Expected: FAIL because the current primary key has only three columns.

- [ ] **Step 3: Create `V3__forecast_vintage_key.sql`**

Drop only the existing primary-key constraint on `analytics.demand_forecast`, then add the four-column primary key. Do not recreate the table and do not modify V2.

- [ ] **Step 4: Re-run focused verification**

Expected: PASS, with existing rows preserved.

- [ ] **Step 5: Commit**

```bash
git add backend/analytics-api/src/main/resources/db/migration/V3__forecast_vintage_key.sql
git commit -m "feat(ml): preserve forecast vintages in serving schema"
```

### Task 2: Add generic non-destructive staging merge semantics

**Files:**
- Modify: `ml/src/main/python/serving_publish.py`
- Modify: `ml/src/test/python/test_serving_publish.py`

**Interfaces:**
- Consumes: `target_table: str`, `staging_table: str`, `ordered_columns: list[str]`, `conflict_columns: list[str]`, `pg_dsn: str`.
- Produces: `merge_from_staging(...) -> None`, which inserts new keys, updates matching keys, truncates staging, and never globally deletes target rows.

- [ ] **Step 1: Write failing tests for `merge_from_staging`**

Add tests asserting: no `DELETE FROM target`; SQL contains `ON CONFLICT` using the supplied conflict columns; non-key columns are updated from `EXCLUDED`; staging is truncated; transaction commits once; SQL failure rolls back; empty columns/conflict columns raise `ValueError`; conflict columns not present in ordered columns raise `ValueError`.

- [ ] **Step 2: Run the focused test and confirm RED**

Run:
```bash
python -m unittest ml/src/test/python/test_serving_publish.py -v
```
Expected: FAIL because `merge_from_staging` does not exist.

- [ ] **Step 3: Implement `merge_from_staging(...) -> None`**

Reuse `_quote_identifier`; build one `INSERT ... SELECT ... ON CONFLICT (...) DO UPDATE SET ...` statement and then `TRUNCATE` staging in the same PostgreSQL transaction. Reject a merge where every ordered column is part of the conflict key rather than emitting an empty update list.

- [ ] **Step 4: Re-run focused tests**

Expected: all `test_serving_publish.py` tests PASS.

- [ ] **Step 5: Commit**

```bash
git add ml/src/main/python/serving_publish.py ml/src/test/python/test_serving_publish.py
git commit -m "feat(ml): merge serving staging non-destructively"
```

### Task 3: Switch forecast and anomaly publishers to merge/upsert

**Files:**
- Create: `ml/src/test/python/test_publish_forecast_app.py`
- Modify: `ml/src/test/python/test_publish_anomaly_app.py`
- Modify: `ml/src/main/python/publish_forecast_app.py`
- Modify: `ml/src/main/python/publish_anomaly_app.py`

**Interfaces:**
- Consumes: `merge_from_staging(...)` from Task 2.
- Produces: forecast publication with conflict key `product_id, forecast_date, model_version, training_cutoff_date`; anomaly publication with conflict key `event_date, product_id, model_version`.

- [ ] **Step 1: Write failing publisher tests**

Forecast test asserts `main()` calls `merge_from_staging` with `FORECAST_COLUMNS` and the four-column vintage key. Update anomaly test to assert `merge_from_staging` with `ANOMALY_COLUMNS` and the three-column anomaly key.

- [ ] **Step 2: Run publisher tests and confirm RED**

Run:
```bash
python -m unittest discover -s ml/src/test/python -p "test_publish_*_app.py" -v
```
Expected: FAIL because publishers still call `replace_from_staging`.

- [ ] **Step 3: Modify both publisher apps**

Replace `replace_from_staging` calls with `merge_from_staging` and pass exact conflict-key lists from the spec.

- [ ] **Step 4: Re-run publisher tests**

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add ml/src/main/python/publish_forecast_app.py ml/src/main/python/publish_anomaly_app.py ml/src/test/python/test_publish_forecast_app.py ml/src/test/python/test_publish_anomaly_app.py
git commit -m "feat(ml): publish forecast and anomalies with upsert"
```

### Task 4: Make temporal forecast selection and anomaly history policy explicit

**Files:**
- Modify: `ml/src/main/python/ml_config.py`
- Modify: `ml/src/main/python/anomaly_detection_app.py`
- Modify: `ml/src/test/python/test_anomaly_detection_app.py`

**Interfaces:**
- Produces: `select_temporally_valid_forecasts(forecast: DataFrame) -> DataFrame`.
- Produces: `attach_residual_history(candidates: DataFrame, history_window: int = ANOMALY_HISTORY_WINDOW) -> DataFrame`.
- Produces: `score_anomaly_candidates(candidates: DataFrame, threshold: float = ANOMALY_THRESHOLD, min_history: int = ANOMALY_MIN_HISTORY) -> DataFrame`.
- Adds config constants `ANOMALY_THRESHOLD`, `ANOMALY_MIN_HISTORY`, `ANOMALY_HISTORY_WINDOW` with exact values `3.5`, `7`, `28`.

- [ ] **Step 1: Write failing temporal-selection tests**

Test that cutoff equal to or after forecast/event date is rejected; greatest eligible cutoff wins; greatest `generated_at` breaks equal-cutoff ties; result contains one selected forecast per `product_id, forecast_date`.

- [ ] **Step 2: Run focused tests and confirm RED**

Run:
```bash
python -m unittest discover -s ml/src/test/python -p "test_anomaly_detection_app.py" -v
```
Expected: FAIL because temporal selection/config behavior is missing.

- [ ] **Step 3: Implement temporal selection minimally**

Use a Spark `Window.partitionBy("product_id", "forecast_date")` ordered by descending `training_cutoff_date`, then descending `generated_at`, after filtering `training_cutoff_date < forecast_date`.

- [ ] **Step 4: Write failing rolling-history/minimum-history tests**

Assert current residual is excluded; only the previous 28 residuals are retained when 29+ exist; 0 through 6 prior residuals produce no scored row; exactly 7 permits scoring.

- [ ] **Step 5: Implement rolling window and minimum-history policy**

Use `rowsBetween(-ANOMALY_HISTORY_WINDOW, -1)` and filter scoring input by `size(historical_residuals) >= ANOMALY_MIN_HISTORY`. Preserve existing MAD=0 behavior.

- [ ] **Step 6: Re-run anomaly app tests**

Expected: all tests PASS.

- [ ] **Step 7: Commit**

```bash
git add ml/src/main/python/ml_config.py ml/src/main/python/anomaly_detection_app.py ml/src/test/python/test_anomaly_detection_app.py
git commit -m "feat(ml): enforce temporal anomaly baseline policy"
```

### Task 5: Build the executable end-to-end sales anomaly job

**Files:**
- Create: `ml/src/main/python/sales_anomaly_job.py`
- Create: `ml/src/test/python/test_sales_anomaly_job.py`

**Interfaces:**
- Consumes: `PRODUCT_DEMAND_GOLD_PATH`, analytics JDBC settings, `FORECAST_TARGET_TABLE`, `ANOMALY_STAGING_TABLE`; Task 4 selection/scoring functions; `stage_anomaly_results`; Task 2 merge function.
- Produces: `build_spark_session() -> SparkSession`, `run_sales_anomaly_job(spark: SparkSession, jdbc_options: dict[str, str]) -> dict[str, int]`, and CLI `main()`.
- Metrics return keys: `candidate_count`, `scored_count`, `anomaly_count`.

- [ ] **Step 1: Write failing orchestration tests**

Inject/read-test doubles around data loading and publication boundaries. Assert the job reads actual product demand and historical forecast target, applies temporal selection before candidate construction, stages the exact nine-column anomaly contract, merges to target, and returns the three metric counts.

- [ ] **Step 2: Run focused test and confirm RED**

Run:
```bash
python -m unittest ml/src/test/python/test_sales_anomaly_job.py -v
```
Expected: FAIL because `sales_anomaly_job.py` does not exist.

- [ ] **Step 3: Implement the minimal job**

Build Spark with the same S3/JDBC configuration pattern as existing ML apps. Read `PRODUCT_DEMAND_GOLD_PATH` via parquet and `FORECAST_TARGET_TABLE` via Spark JDBC. Pipeline order: temporal selection -> candidate join/residual -> rolling history -> score -> stage -> merge. Print metrics in `main()` and always stop Spark.

- [ ] **Step 4: Add rerun/idempotency test at publication boundary**

Assert a second publication of the same anomaly key updates that key while unrelated target rows are retained; use the generic merge contract from Task 2 rather than a delete/replace path.

- [ ] **Step 5: Re-run focused job tests**

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add ml/src/main/python/sales_anomaly_job.py ml/src/test/python/test_sales_anomaly_job.py
git commit -m "feat(ml): add executable sales anomaly job"
```

### Task 6: Wire Docker Compose and CI for operational verification

**Files:**
- Modify: `docker-compose.yml`
- Modify: `.github/workflows/ml-ci.yml`
- Modify: `.env.example` only if the new anomaly settings are exposed there.

**Interfaces:**
- Produces: Docker service `ml-sales-anomaly` using the existing ML jobs image and common environment.
- Produces: CI execution after forecast publication that runs anomaly detection/publication and validates persisted forecast vintages and anomaly rows without destructive replacement.

- [ ] **Step 1: Add configuration/service validation first**

Add `ANOMALY_THRESHOLD=3.5`, `ANOMALY_MIN_HISTORY=7`, `ANOMALY_HISTORY_WINDOW=28`, `ANOMALY_TARGET_TABLE=analytics.sales_anomaly`, and `ANOMALY_STAGING_TABLE=analytics.sales_anomaly_staging` to the shared ML environment. Add `ml-sales-anomaly` with PostgreSQL, MinIO, analytics-api, and Spark Ivy dependencies and a `spark-submit` command for `sales_anomaly_job.py`.

- [ ] **Step 2: Run Compose validation**

Run:
```bash
docker compose config --quiet
```
Expected: exit code 0.

- [ ] **Step 3: Extend CI end-to-end assertions**

After generating/publishing forecast, run the forecast flow a second time with an advanced cutoff fixture/path supported by the test data, then assert historical vintages coexist. Run `docker compose run --rm ml-sales-anomaly`, query PostgreSQL, and assert anomaly publication does not clear unrelated history. Also assert the four-column forecast primary key from Task 1.

- [ ] **Step 4: Run targeted local tests before the full suite**

Run:
```bash
python -m unittest discover -s ml/src/test/python -p "test_serving_publish.py" -v
python -m unittest discover -s ml/src/test/python -p "test_publish_*_app.py" -v
python -m unittest discover -s ml/src/test/python -p "test_anomaly_detection_app.py" -v
python -m unittest discover -s ml/src/test/python -p "test_sales_anomaly_job.py" -v
```
Expected: all PASS.

- [ ] **Step 5: Run complete ML verification**

Run:
```bash
python -m unittest discover -s ml/src/test/python -p "test_*.py" -v
```
Expected: full suite PASS.

Then run/verify the `ml-ci` workflow and require GitHub Actions conclusion `success` before completion.

- [ ] **Step 6: Commit**

```bash
git add docker-compose.yml .github/workflows/ml-ci.yml .env.example
git commit -m "ci(ml): verify forecast vintage anomaly pipeline"
```
