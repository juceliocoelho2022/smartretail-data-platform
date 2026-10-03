# ADR-002 — Transactional Outbox for Reliable Publication

- **Status:** Accepted
- **Scope:** implemented platform v0.1–v0.5
- **Related rules:** BR-002, BR-003, BR-006
- **Related requirements:** FR-004, FR-005, NFR-002, NFR-003

## Context

The ingestion flow needs to persist accepted business state and eventually publish an event to Kafka. Performing a database write and a broker publish as two independent operations inside the HTTP request creates a dual-write failure mode:

```text
1. database commit succeeds
2. Kafka publish fails
```

or the inverse ordering can create other inconsistencies.

The project does not use a distributed transaction spanning PostgreSQL and Kafka.

## Decision

Use the **Transactional Outbox Pattern**.

The application stores the transactional record and the outbound event record in the same local PostgreSQL transaction. A separate publisher reads pending Outbox records and publishes them to Kafka after the database commit.

Conceptually:

```text
HTTP request
   ↓
PostgreSQL transaction
   ├── business/idempotency state
   └── outbox_event
   ↓ commit
Outbox Publisher
   ↓
Kafka
```

## Why this decision

The Outbox makes the durable database state the recovery point for publication. If Kafka is temporarily unavailable after the transaction commits, the pending Outbox record still exists and can be retried without recreating the original business operation.

## Delivery semantics

The publisher may publish the same Outbox event more than once around retry/crash boundaries. Therefore:

- the architecture assumes semantics compatible with `at-least-once` delivery;
- consumers must be idempotent;
- the project does not claim a single global exactly-once transaction.

## Alternatives considered

### Publish directly to Kafka after database commit

**Rejected because:** a crash or broker failure between commit and publish can leave durable database state without the corresponding event.

### Publish to Kafka before database commit

**Rejected because:** the broker can receive an event for a database transaction that later fails.

### Distributed transaction / 2PC

**Not selected because:** it would add coordination and operational complexity that is unnecessary for this portfolio scenario and is not part of the current implementation.

## Consequences

### Positive

- local database consistency between accepted state and outbound-event intent;
- recoverable publication after temporary Kafka failures;
- HTTP ingestion is decoupled from immediate broker success;
- auditability of pending/published Outbox records.

### Costs and trade-offs

- an additional Outbox table/model must be maintained;
- a publisher process or scheduled loop is required;
- retry and failure states need monitoring;
- stale or poison Outbox records require operational treatment;
- duplicate publication remains possible, so idempotent consumers are mandatory.

## Evidence in the repository

The v0.1 documentation and current README show the PostgreSQL transaction containing the idempotency/outbox state, followed by the Outbox Publisher and Kafka. The same documentation also describes retry/DLT and consumer idempotency.

## Revisit when

Revisit if publication moves to a mechanism such as database CDC that replaces the custom Outbox publisher while preserving the same reliability guarantees and operational visibility.
