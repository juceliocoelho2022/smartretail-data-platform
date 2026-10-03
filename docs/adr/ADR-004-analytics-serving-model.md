# ADR-004 — Dedicated Analytics Serving Model

- **Status:** Accepted
- **Scope:** implemented platform v0.5
- **Related rules:** BR-005, BR-006
- **Related requirements:** FR-009, FR-010, NFR-001, NFR-004

## Context

The analytical dashboard needs fast, stable access to curated metrics without depending directly on transactional ingestion tables or Lakehouse files.

Querying Bronze/Silver/Gold files from the browser would expose storage details to the presentation layer. Querying the ingestion database directly would couple analytical read patterns to transactional tables and their evolution.

## Decision

Use a dedicated **analytics serving model**:

```text
Gold Data Products
      ↓
Analytics Export
      ↓
PostgreSQL schema `analytics`
      ↓
Spring Boot Analytics API
      ↓
React Dashboard
```

The `analytics` schema contains read-oriented tables such as `sales_summary` and `sales_daily`, populated from Gold by the analytics export job.

The React dashboard consumes only the Analytics API.

## Why this decision

The serving model creates a stable read boundary between data engineering and the UI:

- the Lakehouse remains responsible for data processing/history;
- PostgreSQL provides a simple serving store for the current analytical use case;
- the API owns validation, filters and response contracts;
- the frontend is isolated from storage-format and transactional-schema changes.

## Alternatives considered

### Dashboard reads Gold/Lakehouse files directly

**Rejected because:** it would couple the frontend to object storage, file formats and data-engineering layout, while also bypassing a controlled API contract.

### Dashboard queries transactional ingestion tables

**Rejected because:** analytical workloads and domain queries would become coupled to the write model used for reliable event ingestion.

### Query engine directly over the Lakehouse

A distributed analytical query engine could be appropriate at larger production scale, but it is not required for the validated scope of this portfolio implementation. Adding one now would increase complexity without solving a demonstrated need.

## Consequences

### Positive

- clear separation between transactional and analytical workloads;
- stable HTTP contract for the frontend;
- simple local execution with PostgreSQL already present in the platform;
- serving tables can be refreshed as part of the orchestrated pipeline;
- frontend does not need credentials or knowledge of Lakehouse storage.

### Costs and trade-offs

- analytical data is duplicated from Gold into the serving database;
- freshness depends on the analytics publication step completing;
- schema changes in Gold may require coordinated changes in export, serving tables and API contracts;
- very large analytical workloads could eventually require a different serving technology.

## Evidence in the repository

The v0.5 implementation documents the `analytics` PostgreSQL schema, the `publish_analytics` Airflow task, the Spring Boot Analytics API endpoints and the React dashboard consuming the published state.

## Revisit when

Revisit if query volume, data size, freshness requirements or analytical complexity make PostgreSQL an unsuitable serving store. Any migration should preserve the separation between Lakehouse processing, API contract and presentation layer.
