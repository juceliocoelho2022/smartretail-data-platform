# SmartRetail Spark Streaming

Módulo de processamento de eventos em tempo real da SmartRetail Data Platform.

## Objetivo

Consumir eventos de pedidos publicados no Kafka e gerar métricas de vendas em tempo real utilizando Apache Spark Structured Streaming.

## Stack

- Apache Spark 4.0.1
- PySpark
- Java 21
- Apache Kafka 3.9.1
- Docker Compose
- Python
- unittest

## Arquitetura

```text
Spring Boot
    |
    v
Transactional Outbox
    |
    v
Kafka
smartretail.orders.v1
    |
    v
Spark Structured Streaming
    |
    +-- JSON parsing
    +-- Event Time
    +-- Watermark: 2 minutes
    +-- Tumbling Window: 1 minute
    |
    v
Sales Metrics
    |
    +-- orders
    +-- items
    +-- revenue