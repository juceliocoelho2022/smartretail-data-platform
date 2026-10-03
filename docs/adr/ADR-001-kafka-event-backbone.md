# ADR-001 — Kafka as the Event Backbone

- **Status:** Accepted
- **Scope:** implemented platform v0.1–v0.5
- **Related rules:** BR-002, BR-003, BR-006
- **Related requirements:** FR-005, FR-006, NFR-001, NFR-002, NFR-003

## Context

The ingestion API should not require every downstream consumer to complete before an order event is accepted. Different consumers can have different responsibilities and lifecycles, such as transactional projections, streaming analytics and later analytical processing.

A direct chain of synchronous HTTP calls would couple availability and latency across components and would make independent reprocessing harder.

## Decision

Use **Apache Kafka** as the event backbone between the transactional ingestion boundary and asynchronous consumers.

Kafka is used because the implemented design benefits from:

- asynchronous decoupling between producer and consumers;
- independent consumer evolution;
- retention and replay of event streams;
- consumer groups for independent processing responsibilities;
- partitioned consumption as the platform evolves;
- compatibility with Spark Structured Streaming as an event source.

## Important semantic boundary

This decision does **not** claim global exactly-once business processing.

The platform is designed around delivery semantics compatible with `at-least-once`, while idempotency is enforced at relevant boundaries. A repeated delivery must therefore be safe for downstream processing.

## Alternatives considered

### Synchronous HTTP calls

**Advantages**
- simpler request/response model;
- fewer infrastructure components.

**Rejected for the main event flow because**
- downstream availability would become more tightly coupled to ingestion;
- fan-out to multiple consumers would be harder to evolve;
- replay would require additional application mechanisms.

### Database polling without a broker

**Advantages**
- fewer infrastructure technologies;
- operational simplicity in a small system.

**Rejected for the event backbone because**
- the platform already has multiple streaming/downstream concerns;
- the broker provides a clearer event-consumption boundary and integrates directly with the implemented streaming pipeline.

### RabbitMQ

RabbitMQ could solve asynchronous messaging for many systems. Kafka was chosen here because event retention, replay and stream-processing integration are central to this portfolio case.

## Consequences

### Positive

- producers and consumers are less coupled in time;
- event streams can be replayed within configured retention;
- multiple consumer groups can process the same logical stream independently;
- Spark can consume the stream continuously;
- failures in one consumer do not require the HTTP producer to synchronously wait for that consumer.

### Costs and trade-offs

- Kafka adds operational complexity;
- topic design, partitioning, retention and consumer-group behavior must be understood;
- duplicate delivery remains possible and must be handled safely;
- observability must cover producer, broker and consumers;
- ordering guarantees are scoped to partitions rather than the entire distributed system.

## Evidence in the repository

The current README documents the order-event topic, Kafka consumers, retry/DLT behavior, Spark Structured Streaming integration and the end-to-end path from ingestion to analytics.

## Revisit when

Revisit this decision if the system becomes small enough that a broker no longer provides sufficient value, or if future business requirements require different messaging semantics that Kafka does not fit well.
