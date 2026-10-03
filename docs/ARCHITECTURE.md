# Arquitetura — SmartRetail Data Platform

Este documento descreve a **arquitetura atualmente implementada até a v0.5** e separa explicitamente os itens que permanecem como roadmap.

> Problema e regras de negócio: [BUSINESS_PROBLEM.md](BUSINESS_PROBLEM.md) · [BUSINESS_RULES.md](BUSINESS_RULES.md)  
> Requisitos: [REQUIREMENTS.md](REQUIREMENTS.md)  
> Decisões: [ADR-001](adr/ADR-001-kafka-event-backbone.md) · [ADR-002](adr/ADR-002-transactional-outbox.md) · [ADR-003](adr/ADR-003-medallion-and-airflow-boundary.md) · [ADR-004](adr/ADR-004-analytics-serving-model.md)

<p align="center">
  <img src="assets/smartretail-data-platform-architecture.svg" alt="Arquitetura SmartRetail Data Platform" width="100%">
</p>

## 1. Objetivo arquitetural

A plataforma foi desenhada para receber eventos de varejo com proteção contra duplicação, publicar eventos de forma recuperável, processá-los de modo assíncrono e produzir dados analíticos confiáveis sem acoplar o fluxo transacional ao consumo analítico.

A arquitetura atual materializa os seguintes princípios:

1. **idempotência na entrada e no consumo**;
2. **persistência transacional antes da publicação assíncrona**;
3. **event-driven architecture com Kafka**;
4. **separação entre streaming contínuo e jobs batch finitos**;
5. **Medallion Architecture para distinguir dado bruto, curado e orientado ao consumo**;
6. **Data Quality explícita antes dos produtos analíticos posteriores à Silver**;
7. **serving analítico separado do modelo transacional**;
8. **evidência operacional por health checks, métricas, logs e status de pipeline**.

## 2. Arquitetura atual — v0.5

```text
Web / Mobile / PDV / Integrações
               │
               ▼
┌─────────────────────────────────────┐
│ Ingestion API                       │
│ Java 21 + Spring Boot               │
│ validation + Idempotency-Key        │
└──────────────────┬──────────────────┘
                   │
                   ▼
          PostgreSQL transaction
          ├── idempotency state
          └── outbox_event
                   │
                   ▼
            Outbox Publisher
                   │
                   ▼
              Apache Kafka
                   │
                   ▼
       Spark Structured Streaming
          ┌────────┴────────┐
          ▼                 ▼
     real-time KPIs       Bronze
                              │
                              ▼
                            Silver
                              │
                              ▼
                     Silver Data Quality
                              │
                              ▼
                             Gold
                         ┌─────┴─────┐
                         ▼           ▼
                    Apache Iceberg  Analytics Export
                                      │
                                      ▼
                              PostgreSQL `analytics`
                                      │
                                      ▼
                              Spring Boot Analytics API
                                      │
                                      ▼
                                React Dashboard
```

### Regra central

A mesma `Idempotency-Key` não deve criar dois eventos lógicos distintos (**BR-001**).

A persistência da operação aceita e do evento de saída ocorre na mesma transação local (**BR-002**), e consumidores devem tolerar reentrega em uma arquitetura compatível com `at-least-once` (**BR-003**).

## 3. Ingestion Layer

Tecnologia atual: **Java 21 + Spring Boot**.

Responsabilidades implementadas:

- receber eventos de pedidos;
- validar payload;
- aplicar `Idempotency-Key`;
- persistir estado transacional;
- registrar eventos na Transactional Outbox;
- expor health/metrics por Spring Boot Actuator e Micrometer.

### Fluxo transacional

```text
POST /api/v1/events/orders
        │
        ▼
validation
        │
        ▼
idempotency check
        │
        ▼
PostgreSQL transaction
├── idempotency state
└── outbox_event
        │
        ▼
commit
```

Esse desenho implementa **BR-001** e **BR-002** e está detalhado em:

- [ADR-001 — Kafka as the Event Backbone](adr/ADR-001-kafka-event-backbone.md)
- [ADR-002 — Transactional Outbox](adr/ADR-002-transactional-outbox.md)

## 4. Event Backbone

Tecnologia atual: **Apache Kafka**.

Responsabilidades:

- desacoplar produtores e consumidores;
- permitir processamento assíncrono;
- permitir consumidores independentes;
- reter eventos conforme configuração do broker;
- permitir replay controlado;
- servir como fonte do Spark Structured Streaming.

A arquitetura não assume exactly-once global. O modelo é compatível com entrega `at-least-once`, portanto consumidores devem ser idempotentes.

Veja [ADR-001](adr/ADR-001-kafka-event-backbone.md).

## 5. Transactional Outbox

A API não depende de um dual write ingênuo `PostgreSQL + Kafka` dentro da mesma requisição HTTP.

O evento de saída é registrado na mesma transação local do estado aceito. Depois do commit, o Outbox Publisher publica o evento no Kafka.

```text
PostgreSQL commit
       │
       ▼
outbox pending
       │
       ▼
Outbox Publisher
       │
       ▼
Kafka
```

Uma eventual repetição de publicação é tratada por idempotência no consumo; não há reivindicação de uma transação exactly-once ponta a ponta.

Veja [ADR-002](adr/ADR-002-transactional-outbox.md).

## 6. Stream Processing

Tecnologia atual: **Apache Spark Structured Streaming**.

O fluxo Kafka → Bronze → Silver permanece **contínuo**.

Capacidades implementadas:

- Kafka source;
- schema explícito;
- event time;
- watermark;
- janelas temporais para KPIs;
- checkpoints;
- transformação contínua para Bronze e Silver;
- deduplicação conforme o contrato da Silver.

### Limite de responsabilidade

Spark Structured Streaming é o engine do fluxo contínuo. Airflow não substitui esse papel.

## 7. Lakehouse e Medallion Architecture

Armazenamento atual: **MinIO/S3-compatible + Parquet + Apache Iceberg**.

### Bronze

Preserva o dado de entrada e metadados necessários para rastreabilidade/reprocessamento.

```text
s3a://smartretail-bronze/orders/
```

### Silver

Representa dado curado e tipado.

Invariantes atuais (**BR-004**):

- `eventId` obrigatório;
- `customerId` obrigatório;
- `productId` obrigatório;
- `quantity > 0`;
- `unitPrice >= 0`;
- normalização de campos de domínio;
- deduplicação por `eventId`.

```text
s3a://smartretail-silver/orders/
```

### Gold

Materializa Data Products orientados ao consumo analítico (**BR-005**).

```text
s3a://smartretail-gold/orders-daily/
s3a://smartretail-gold/orders-summary/
```

KPIs atuais incluem total de pedidos, itens, receita, ticket médio, clientes e produtos únicos, além de segmentações por data/canal/localização.

## 8. Data Quality

O gate atual roda sobre a **Silver já curada**.

Ele verifica invariantes como:

```text
nullEventId
nullCustomerId
nullProductId
invalidQuantity
invalidUnitPrice
duplicateRows
```

A semântica correta é: **checagem de invariantes da Silver antes dos jobs batch seguintes**.

Não há, neste documento, alegação de que esse gate seja um contador de rejeições da Bronze.

## 9. Orchestration — Airflow

Tecnologia atual: **Apache Airflow**.

Airflow começa no limite pós-Silver, coordenando jobs batch finitos:

```text
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

Responsabilidades:

- dependências entre tasks;
- retry/orquestração dos jobs batch;
- status de execução;
- logs das tasks;
- sequência explícita entre qualidade, Gold, Iceberg, validação e serving.

Veja [ADR-003 — Medallion Architecture and the Airflow Boundary](adr/ADR-003-medallion-and-airflow-boundary.md).

## 10. Apache Iceberg

Tabela atualmente documentada:

```text
smartretail.lakehouse.orders
```

Recursos já validados no projeto:

- JDBC Catalog no PostgreSQL;
- snapshots;
- Time Travel;
- Schema Evolution;
- atualização atual por `INSERT OVERWRITE`.

`MERGE INTO` incremental permanece fora da implementação atual.

## 11. Analytics Serving Layer

O frontend não consulta diretamente tabelas de ingestão nem arquivos do Lakehouse.

A v0.5 utiliza:

```text
Gold
  ↓
Analytics Export
  ↓
PostgreSQL schema analytics
├── sales_summary
└── sales_daily
  ↓
Spring Boot Analytics API
  ↓
React Dashboard
```

Esse read model evita acoplar a experiência de consumo ao modelo transacional ou ao layout físico do Lakehouse.

Veja [ADR-004 — Dedicated Analytics Serving Model](adr/ADR-004-analytics-serving-model.md).

## 12. Observabilidade atual

A plataforma utiliza, de acordo com o componente:

- Spring Boot Actuator;
- Micrometer;
- Prometheus;
- logs de serviços e jobs;
- status do Airflow;
- evidências explícitas de Data Quality e post-load validation.

A observabilidade suporta **BR-006 — Operational traceability**.

Métricas e números de amostra usados nas validações do README representam execuções do ambiente de demonstração, não benchmarks formais de produção.

## 13. Contratos de eventos

Eventos utilizam metadados versionáveis e identificadores de rastreabilidade quando definidos no contrato:

```json
{
  "eventId": "EVT-9821837",
  "eventType": "ORDER_CREATED",
  "eventVersion": 1,
  "occurredAt": "2026-09-29T18:14:32Z",
  "producer": "ingestion-api",
  "correlationId": "COR-123456"
}
```

Princípios:

- `eventId` identifica o evento;
- timestamps usam UTC;
- contratos devem ser versionados;
- evolução deve priorizar compatibilidade;
- correlation IDs devem ser preservados quando presentes no fluxo.

## 14. Estratégia de testes

A arquitetura é validada em diferentes níveis:

```text
Unit
Integration
API / MockMvc
Kafka / consumer behavior
Data Quality
Pipeline
End-to-End
```

Ferramentas presentes no projeto incluem JUnit 5, Mockito, MockMvc, Python `unittest` e CI no GitHub Actions.

## 15. Current implementation vs. roadmap

### Implementado até v0.5

- Ingestion API;
- idempotência HTTP;
- Transactional Outbox;
- Kafka;
- retry/DLT e consumer idempotente;
- Spark Structured Streaming;
- Bronze/Silver/Gold;
- Data Quality Gate;
- Airflow pós-Silver;
- Apache Iceberg;
- analytics serving PostgreSQL;
- Analytics API;
- React dashboard;
- CI e observabilidade descrita no README.

### Roadmap / futuro

Os seguintes itens pertencem à evolução futura e **não são apresentados como capacidades atuais**:

- ML operacionalizado/MLflow;
- novos domínios como inventory, billing ou shipping;
- expansão de autenticação, autorização e governança;
- topologia cloud de produção;
- Kubernetes/EKS;
- metas formais de throughput, latência, disponibilidade ou SLO;
- `MERGE INTO` incremental no Iceberg.

## 16. Princípio de evolução

Cada componente deve responder a uma pergunta de engenharia concreta:

> Qual problema de negócio, requisito não funcional ou risco operacional justifica esta tecnologia e seu custo?

Essa regra evita transformar o projeto em uma coleção de tecnologias sem relação explícita com o problema resolvido.
