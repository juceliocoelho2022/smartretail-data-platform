# ADR-001 — Event-driven ingestion with idempotency and Transactional Outbox

- **Status:** Accepted
- **Context:** SmartRetail Event Platform

## Context

The ingestion layer receives order events from multiple retail channels. Network retries, client retries and asynchronous redelivery mean the same logical operation can arrive more than once.

Publishing directly to Kafka inside the HTTP request would also create a dual-write problem: the database transaction could succeed while the broker publish fails, or the opposite.

## Decision

The ingestion flow uses:

1. an `idempotencyKey` to identify the logical operation;
2. PostgreSQL as the transactional source of truth for ingestion state;
3. Transactional Outbox to persist the outgoing event in the same local transaction;
4. an asynchronous publisher that sends committed Outbox records to Kafka;
5. idempotent downstream consumers because Kafka delivery is treated as `at-least-once`.

## Why Kafka

Kafka was selected because the platform needs asynchronous decoupling between ingestion and downstream processing, while allowing independent consumer scaling, retention, replay and multiple consumers over the same event stream.

## Alternatives considered

### Synchronous HTTP chaining

Rejected as the primary integration model because it increases temporal coupling and makes ingestion availability depend on downstream services.

### Direct database + Kafka dual write

Rejected because there is no atomic transaction spanning PostgreSQL and Kafka in this design. A partial failure can create data/event divergence.

### Global exactly-once assumption

Rejected as a system-wide guarantee. The design instead uses local atomicity plus idempotent processing to make redelivery safe.

## Consequences

### Positive

- duplicate requests do not create duplicate business operations;
- the API is decoupled from downstream processing time;
- committed database state is not lost when Kafka is temporarily unavailable;
- events can be replayed and consumers can evolve independently;
- failure modes are explicit and testable.

### Trade-offs

- the Outbox needs its own lifecycle and monitoring;
- event delivery may happen more than once;
- consumers must implement idempotency;
- eventual consistency becomes part of the system model;
- operational visibility over backlog, retry and DLT becomes mandatory.

## Validation

The architecture is considered valid when tests demonstrate that:

- two requests with the same `idempotencyKey` produce one logical event;
- an Outbox record is created atomically with accepted ingestion;
- temporary broker failure does not require recreating the business operation;
- downstream redelivery does not create duplicate effects.
