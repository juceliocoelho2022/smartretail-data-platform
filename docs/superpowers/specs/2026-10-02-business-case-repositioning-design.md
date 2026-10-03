# SmartRetail Data Platform — Business-First Case Study Design

Date: 2026-10-02
Status: proposed design for portfolio repositioning

## 1. Intent

Reposition the SmartRetail Data Platform from a technology-centric portfolio project into a business-driven software/data engineering case study.

The project must make clear, before listing technologies, what retail problem is being solved, which business invariants the platform protects, why each architectural component exists, and which technical evidence demonstrates that the solution works.

## 2. Primary audience

- Java Backend recruiters and technical interviewers
- Backend/Software Engineers and Tech Leads
- Data Engineering recruiters and technical interviewers
- Engineers reviewing architecture, resilience, data quality and delivery decisions

## 3. Core business problem

A retail operation receives order events from channels such as web, mobile, stores and integrations. Network retries, integration failures and reprocessing can cause the same logical operation to be delivered more than once.

Without explicit protection, duplicate processing can create inconsistent projections, duplicated downstream work and unreliable analytical data.

The SmartRetail case therefore centers on reliable ingestion and processing of retail order events, with idempotency, asynchronous processing, data quality and traceability across transactional and analytical layers.

## 4. Business rules and invariants

### BR-001 — Idempotent ingestion

The same `Idempotency-Key` must not create two distinct logical order events.

Expected behavior:

- a new key creates and persists the event;
- a repeated key does not create another logical event;
- the original result is reused/replayed according to the API contract.

### BR-002 — Reliable event publication

An accepted transactional event must not be lost because the application persisted database state but failed to publish directly to Kafka.

The Transactional Outbox is the architectural mechanism used to separate durable persistence from broker publication while preserving recoverability.

### BR-003 — Idempotent downstream consumption

Kafka's at-least-once delivery semantics mean consumers must tolerate repeated delivery without duplicating the resulting business projection.

### BR-004 — Curated analytical data

Only data satisfying the Silver-layer invariants may advance to Gold analytical products.

Current Silver invariants include mandatory identifiers, positive quantity, non-negative unit price, normalization and deduplication by `eventId`.

### BR-005 — Reproducible analytical products

Gold datasets and serving models must be rebuildable from trusted upstream data rather than depending on manually edited analytical state.

### BR-006 — Operational traceability

The system must expose sufficient health, metrics and pipeline evidence to diagnose ingestion, publication, processing and data-quality failures.

## 5. Functional requirements

- FR-001: receive order events through the ingestion API.
- FR-002: validate the incoming payload.
- FR-003: enforce idempotency at ingestion.
- FR-004: persist transactional state and outbox state atomically.
- FR-005: publish accepted events to Kafka asynchronously.
- FR-006: consume and project Kafka events idempotently.
- FR-007: process event data through Bronze, Silver and Gold layers.
- FR-008: execute explicit Silver data-quality checks before downstream batch products are refreshed.
- FR-009: publish analytical products to the serving model.
- FR-010: expose analytical summaries through the Analytics API and dashboard.

## 6. Non-functional requirements

- NFR-001: ingestion must not be coupled to analytical processing completion.
- NFR-002: temporary broker/consumer failures must not silently lose accepted events.
- NFR-003: repeated delivery must be safe at the API and consumer boundaries.
- NFR-004: batch pipeline stages must be observable and independently diagnosable.
- NFR-005: schema/data evolution must be explicit and versioned.
- NFR-006: architecture and trade-offs must be documented as engineering decisions, not implied by the stack list.

## 7. Architecture narrative

The portfolio narrative will present the system in this order:

```text
Retail channels
    -> Ingestion API
    -> validation + idempotency
    -> PostgreSQL transaction + Transactional Outbox
    -> Kafka
    -> idempotent consumers / Spark Structured Streaming
    -> Bronze
    -> Silver
    -> Data Quality Gate
    -> Gold / Iceberg
    -> Analytics serving PostgreSQL
    -> Analytics API
    -> React dashboard
```

The README must explain the role of each stage in terms of a failure mode, business rule or consumption need.

## 8. Key architecture decisions to surface

### ADR theme A — Why Kafka

Kafka is justified by asynchronous decoupling, independent consumers, replayability, scalable consumption and durable event-stream processing. It must not be presented merely as a technology included in the stack.

### ADR theme B — Why Transactional Outbox

The Outbox addresses the dual-write problem between PostgreSQL and Kafka. The README must explicitly state that the application does not claim a global exactly-once transaction.

### ADR theme C — Why Medallion Architecture

Bronze preserves raw/reprocessable data; Silver applies trusted schema/quality invariants; Gold exposes consumption-oriented analytical products.

### ADR theme D — Why Airflow only after Silver

Continuous ingestion and Silver transformation remain streaming flows. Airflow orchestrates finite post-Silver batch work, avoiding misuse of an orchestrator as the streaming engine.

### ADR theme E — Why a separate analytics serving model

The dashboard/API should consume a dedicated read model instead of coupling the frontend to lake files or transactional ingestion tables.

## 9. README redesign

The top of the README will be reorganized into this sequence:

1. one-sentence product/case description;
2. `Business Problem`;
3. `Business Impact / Failure Modes`;
4. `Business Rules`;
5. `Solution Overview`;
6. architecture diagram/flow;
7. `Engineering Decisions & Trade-offs`;
8. evidence: tests, data-quality output and E2E validation;
9. only then the detailed technology stack and release history.

Existing technical documentation and validated implementation details must be preserved; the objective is to change emphasis and information architecture rather than rewrite history.

## 10. Evidence strategy

The case study must point to concrete evidence already present in the repository wherever possible:

- idempotency behavior and automated tests;
- Transactional Outbox flow;
- retry / DLT behavior;
- consumer idempotency;
- CI workflows;
- Silver Data Quality Gate output;
- post-load validation;
- Airflow DAG success;
- Analytics Export success;
- API/dashboard serving the last published analytical state.

Claims without repository evidence must not be added as if implemented.

## 11. Documentation changes planned

After approval of this design, the implementation plan will cover at least:

- update `README.md` with the business-first opening and explicit decision rationale;
- add `docs/BUSINESS_PROBLEM.md`;
- add `docs/BUSINESS_RULES.md`;
- add `docs/REQUIREMENTS.md`;
- add or extend ADR documentation for Kafka, Outbox, Medallion/Airflow and analytical serving where equivalent ADRs do not already exist;
- add a concise `docs/PORTFOLIO_CASE_STUDY.md` suitable for interview preparation;
- cross-link tests and operational evidence instead of inventing metrics;
- keep the existing release/version history intact.

## 12. Out of scope for this repositioning slice

This documentation/portfolio slice will not add new production technologies or claim unimplemented scale characteristics.

The following are separate future engineering changes and require their own design/spec if pursued:

- changing broker topology or partition strategy;
- adding new business services such as inventory or billing;
- migrating deployment to AWS/Kubernetes;
- changing Iceberg write strategy to incremental `MERGE INTO`;
- introducing new security/authentication flows;
- load/performance testing that produces formal throughput/SLO numbers.

## 13. Success criteria

A reviewer should be able to answer, after reading the first portion of the repository:

1. What retail problem does SmartRetail solve?
2. What is the primary business invariant?
3. Why are Kafka, Outbox, Spark, Airflow and the serving model used?
4. How does the system react to duplicate delivery and data-quality failures?
5. What repository evidence supports those claims?

If those answers are clear without reading implementation source code first, the repositioning is successful.
