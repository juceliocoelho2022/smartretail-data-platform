# ADR-003 — Medallion Architecture and the Airflow Boundary

- **Status:** Accepted
- **Scope:** implemented platform v0.2–v0.5
- **Related rules:** BR-004, BR-005, BR-006
- **Related requirements:** FR-007, FR-008, NFR-004, NFR-005

## Context

The platform needs both continuous event processing and finite analytical jobs. Treating every stage as the same workload would blur responsibilities and make recovery, quality control and orchestration harder to reason about.

The current pipeline already separates:

- continuous Kafka ingestion and Spark Structured Streaming;
- raw and curated Lakehouse layers;
- finite post-Silver jobs that build and publish analytical products.

## Decision

Use a **Medallion Architecture** with explicit responsibilities:

- **Bronze** — preserves raw/reprocessable event data and ingestion metadata;
- **Silver** — applies schema, normalization, validity rules and deduplication;
- **Gold** — materializes consumption-oriented analytical data products.

Keep Kafka → Bronze → Silver as continuous **Spark Structured Streaming** flows.

Use **Apache Airflow** only to orchestrate the finite jobs that begin after Silver is available:

```text
Silver
  ↓
silver_data_quality
  ↓
build_gold
  ↓
refresh_iceberg
  ↓
post_load_validation
  ↓
publish_analytics
```

## Why this boundary

Spark Structured Streaming is the processing engine responsible for continuous data flow. Airflow is the workflow orchestrator responsible for dependencies, retries and execution state of finite jobs.

Using Airflow as if it were the streaming engine would mix two different execution models and obscure ownership of checkpoints, continuous state and job scheduling.

## Data Quality semantics

The implemented `silver_data_quality` task validates invariants of the curated Silver layer, including required identifiers, valid quantity/price values and duplicates.

It is therefore described as a **Silver invariant gate**. It is not presented as a Bronze rejection counter unless a separate rejection/quarantine flow is implemented and evidenced.

## Alternatives considered

### Single streaming pipeline through all analytical outputs

Could reduce the number of execution models, but finite rebuilds of Gold, Iceberg refresh and analytical publication benefit from explicit dependency orchestration and independently visible task status.

### Airflow for all ingestion and transformation

Rejected because continuous Kafka ingestion and watermark/checkpoint semantics belong to the streaming engine rather than a batch scheduler.

### One undifferentiated data layer

Rejected because it would make the distinction between raw, trusted and consumption-ready data less explicit and would reduce traceability during reprocessing.

## Consequences

### Positive

- clear ownership between continuous processing and batch orchestration;
- raw data remains available for controlled reprocessing;
- trusted-data invariants are explicit before analytical products are rebuilt;
- each post-Silver stage is independently visible in the Airflow DAG;
- Gold can be reconstructed from curated upstream data.

### Costs and trade-offs

- the platform operates both streaming and batch execution models;
- multiple storage layers consume more space than a single mutable table;
- teams must understand when a rule belongs in streaming transformation, Data Quality or a batch product job;
- orchestration dependencies must remain synchronized with pipeline contracts.

## Evidence in the repository

The README documents Spark Structured Streaming for Kafka/Bronze/Silver, the Medallion paths, the Airflow DAG, Silver Data Quality output and successful post-load/analytics publication validation.

## Revisit when

Revisit if analytical requirements become fully streaming or if the workload changes so that the current batch boundary no longer provides operational value.
