# 🛒 SmartRetail Data Platform

<p align="center">
  <strong>Case de engenharia para ingestão confiável e processamento analítico de eventos de varejo, com proteção contra duplicação e falhas parciais.</strong>
</p>

<p align="center">
  Backend Java • Event-Driven Architecture • Engenharia de Dados • Lakehouse • Analytics
</p>

---

## Business Problem

Uma operação de varejo omnichannel recebe eventos de pedidos originados por web, aplicativo, PDV e integrações externas. Em sistemas distribuídos, retries, timeouts e reprocessamentos podem fazer a **mesma operação chegar mais de uma vez**.

Se a plataforma tratar cada entrega como uma nova operação, o resultado pode ser processamento duplicado, projeções inconsistentes e dados analíticos incorretos.

O SmartRetail foi construído para responder a uma pergunta de engenharia concreta:

> **Como receber e processar eventos de varejo de forma confiável, tolerando duplicação e falhas parciais, sem acoplar a ingestão transacional ao processamento analítico?**

Este repositório é uma **simulação de portfólio**. Ele demonstra decisões e comportamentos verificáveis no ambiente do projeto; não reivindica volume, throughput, latência ou disponibilidade de uma operação real sem testes específicos para isso.

➡️ [Problema de negócio completo](docs/BUSINESS_PROBLEM.md)

---

## Business Impact / Failure Modes

Sem mecanismos explícitos de confiabilidade, o fluxo pode falhar de diferentes maneiras:

| Falha | Impacto | Proteção usada no projeto |
|---|---|---|
| Retry envia o mesmo pedido novamente | duas operações lógicas | `Idempotency-Key` |
| Banco confirma e Kafka falha | estado persistido sem publicação | Transactional Outbox |
| Kafka reentrega a mensagem | efeito/projeção duplicada | consumer idempotente |
| dado inválido avança no pipeline | analytics inconsistente | Silver invariants + Data Quality Gate |
| frontend depende do lake/transacional | alto acoplamento | serving model + Analytics API |
| falha ocorre em pipeline distribuído | diagnóstico difícil | health, métricas, logs e status das tasks |

---

## Core Business Rules

A arquitetura é orientada por seis invariantes principais:

- **BR-001 — Idempotent ingestion:** a mesma `Idempotency-Key` não cria dois eventos lógicos distintos.
- **BR-002 — Reliable event publication:** um evento aceito não depende de um dual write ingênuo entre PostgreSQL e Kafka.
- **BR-003 — Idempotent downstream consumption:** reentrega Kafka não deve duplicar o efeito de negócio.
- **BR-004 — Curated analytical data:** apenas dados que satisfazem as invariantes da Silver avançam como dados confiáveis.
- **BR-005 — Reproducible analytical products:** Gold e serving são reconstruíveis a partir de dados curados.
- **BR-006 — Operational traceability:** ingestão, publicação, processamento e qualidade precisam produzir evidência operacional.

➡️ [Business Rules](docs/BUSINESS_RULES.md) · [Requirements](docs/REQUIREMENTS.md)

---

## Solution Overview

```text
Retail Channels
      │
      ▼
Spring Boot Ingestion API
      │
      ├── validation
      └── Idempotency-Key
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
      │
   Bronze → Silver
              │
              ▼
       Data Quality Gate
              │
              ▼
             Gold
          ┌───┴──────────┐
          ▼              ▼
      Iceberg      Analytics Export
                         │
                         ▼
                 PostgreSQL analytics
                         │
                         ▼
                 Spring Boot Analytics API
                         │
                         ▼
                    React Dashboard
```

**Streaming contínuo:** Kafka → Bronze → Silver é processado por Spark Structured Streaming.  
**Batch finito:** Airflow começa depois da Silver e orquestra Data Quality → Gold → Iceberg → validação → publicação analítica.

➡️ [Arquitetura detalhada](docs/ARCHITECTURE.md)

---

## Engineering Decisions & Trade-offs

### Kafka — desacoplamento e replay

**Problema resolvido:** a ingestão não deve esperar consumidores downstream, e múltiplos consumidores precisam evoluir independentemente.

**Decisão:** Kafka funciona como event backbone, oferecendo retenção, replay e consumer groups.

**Trade-off:** adiciona complexidade operacional e não elimina redelivery; consumidores continuam precisando ser idempotentes.

➡️ [ADR-001 — Kafka Event Backbone](docs/adr/ADR-001-kafka-event-backbone.md)

### Transactional Outbox — dual write

**Problema resolvido:** evitar o cenário em que PostgreSQL confirma a operação, mas a publicação no Kafka falha logo depois.

**Decisão:** estado transacional e `outbox_event` são persistidos na mesma transação local; um publisher envia a mensagem após o commit.

**Trade-off:** exige tabela Outbox, publisher, retry e monitoramento próprios.

➡️ [ADR-002 — Transactional Outbox](docs/adr/ADR-002-transactional-outbox.md)

### Medallion + Airflow boundary

**Problema resolvido:** separar processamento contínuo de etapas analíticas batch e distinguir dado bruto, curado e pronto para consumo.

**Decisão:** Bronze/Silver permanecem em Spark Structured Streaming. Airflow coordena somente jobs finitos posteriores à Silver.

**Trade-off:** a plataforma opera dois modelos de execução — streaming e batch — com contratos claros entre eles.

➡️ [ADR-003 — Medallion + Airflow](docs/adr/ADR-003-medallion-and-airflow-boundary.md)

### Dedicated analytics serving model

**Problema resolvido:** evitar que a UI consulte arquivos do Lakehouse ou tabelas transacionais da ingestão.

**Decisão:** Gold é publicado no schema PostgreSQL `analytics`, consumido por uma Spring Boot Analytics API e pelo dashboard React.

**Trade-off:** há duplicação controlada entre Gold e serving, e a freshness depende do passo de publicação.

➡️ [ADR-004 — Analytics Serving Model](docs/adr/ADR-004-analytics-serving-model.md)

### Semântica de entrega

O projeto **não reivindica exactly-once global**. Kafka e os fluxos assíncronos são tratados com semântica compatível com `at-least-once`, combinada com idempotência na entrada e no consumo.

---

## Evidence — comportamento validado

Os valores abaixo são **evidências de execução do ambiente de demonstração**, não benchmarks de produção.

### Data Quality Gate

```text
Silver Data Quality gate: PASSED
totalRows=5
nullEventId=0
nullCustomerId=0
nullProductId=0
invalidQuantity=0
invalidUnitPrice=0
duplicateRows=0
```

O gate valida invariantes da **Silver já curada**; ele não é apresentado como contador de rejeições da Bronze.

### Post-load validation

```text
Post-load validation: PASSED
Silver rows=5
Iceberg rows=5
Gold totalOrders=5
```

### Airflow v0.5

```text
silver_data_quality    success
build_gold             success
refresh_iceberg        success
post_load_validation   success
publish_analytics      success
DagRun                  success
```

### Analytics Export

```text
Analytics serving export: PASSED
Summary rows=1
Daily rows=4
```

### Estado analítico servido pela API

```text
totalOrders       = 5
totalItems        = 14
totalRevenue      = 2918.60
averageOrderValue = 583.72
uniqueCustomers   = 4
uniqueProducts    = 4
goldProcessedAt   = 2026-10-02 20:45:52
refreshedAt       = 2026-10-02 20:47:57
```

O `refreshedAt` foi usado para confirmar que a API estava servindo o estado publicado pelo último run do pipeline.

➡️ [Case para entrevistas](docs/PORTFOLIO_CASE_STUDY.md)

---

## Status atual

**v0.5 — Analytics API + Dashboard React concluída e validada de ponta a ponta no ambiente do projeto.**

<p align="center">
  <img alt="Java 21" src="https://img.shields.io/badge/Java-21-ED8B00?logo=openjdk&logoColor=white">
  <img alt="Spring Boot 3.5.5" src="https://img.shields.io/badge/Spring%20Boot-3.5.5-6DB33F?logo=springboot&logoColor=white">
  <img alt="Apache Kafka 3.9.1" src="https://img.shields.io/badge/Apache%20Kafka-3.9.1-231F20?logo=apachekafka&logoColor=white">
  <img alt="PostgreSQL 17" src="https://img.shields.io/badge/PostgreSQL-17-4169E1?logo=postgresql&logoColor=white">
  <img alt="Apache Spark 4.0.1" src="https://img.shields.io/badge/Apache%20Spark-4.0.1-E25A1C?logo=apachespark&logoColor=white">
  <img alt="Apache Airflow 3.3.2" src="https://img.shields.io/badge/Apache%20Airflow-3.3.2-017CEE?logo=apacheairflow&logoColor=white">
  <img alt="Apache Iceberg 1.11.0" src="https://img.shields.io/badge/Apache%20Iceberg-1.11.0-2D7FF9">
  <img alt="React" src="https://img.shields.io/badge/React-19-61DAFB?logo=react&logoColor=111">
  <img alt="Docker" src="https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white">
</p>

<p align="center">
  <img alt="Backend CI" src="https://github.com/juceliocoelho2022/smartretail-data-platform/actions/workflows/backend-ci.yml/badge.svg?branch=main">
  <img alt="Streaming CI" src="https://github.com/juceliocoelho2022/smartretail-data-platform/actions/workflows/streaming-ci.yml/badge.svg?branch=main">
  <img alt="Analytics CI" src="https://github.com/juceliocoelho2022/smartretail-data-platform/actions/workflows/analytics-ci.yml/badge.svg?branch=main">
  <img alt="Dashboard CI" src="https://github.com/juceliocoelho2022/smartretail-data-platform/actions/workflows/dashboard-ci.yml/badge.svg?branch=main">
  <img alt="v0.5" src="https://img.shields.io/badge/v0.5-Analytics%20conclu%C3%ADda-2EA44F">
</p>

Implementado até aqui:

- Event Platform com Java 21 + Spring Boot + Kafka;
- idempotência HTTP + consumer idempotente;
- Transactional Outbox;
- retry + Dead Letter Topic;
- Spark Structured Streaming;
- Bronze / Silver / Gold em MinIO/S3;
- Apache Iceberg com JDBC Catalog, snapshots, Time Travel e Schema Evolution;
- Apache Airflow + Data Quality + validação pós-carga;
- serving analítico em PostgreSQL;
- Analytics API com Spring Boot;
- dashboard React + TypeScript;
- testes automatizados e GitHub Actions.

---

# Release history

## ✅ v0.1 — Event Platform

A primeira release estabelece a base transacional e orientada a eventos.

### Capacidades

- Java 21 + Spring Boot 3.5.5;
- API REST para ingestão de pedidos;
- Bean Validation;
- `Idempotency-Key`;
- Transactional Outbox Pattern;
- Apache Kafka 3.9.1;
- retry + Dead Letter Topic;
- consumer idempotente;
- PostgreSQL 17;
- Flyway;
- Spring Boot Actuator;
- Micrometer + Prometheus;
- JUnit 5, Mockito, MockMvc e JaCoCo;
- Docker Compose;
- GitHub Actions.

### Fluxo

```text
POST /api/v1/events/orders
          ↓
Spring Boot
          ↓
PostgreSQL Transaction
├── idempotency_record
└── outbox_event
          ↓
Outbox Publisher
          ↓
smartretail.orders.v1
          ↓
Kafka Consumer
          ↓
order_event_projection
```

---

## ✅ v0.2 — Streaming Analytics

A v0.2 conecta Kafka ao Apache Spark Structured Streaming.

- Apache Spark 4.0.1;
- PySpark;
- Kafka Source;
- schema explícito;
- event time;
- watermark de 2 minutos;
- tumbling window de 1 minuto;
- KPIs por canal;
- checkpoints;
- testes automatizados;
- execução em Docker.

---

## ✅ v0.3 — Data Lakehouse

A v0.3 implementa Medallion Architecture sobre MinIO/S3.

### Bronze

```text
s3a://smartretail-bronze/orders/
```

Preserva payload, chave, tópico, partição, offset, timestamps e data de ingestão.

### Silver

```text
s3a://smartretail-silver/orders/
```

Regras atuais:

- `eventId`, `customerId` e `productId` obrigatórios;
- `quantity > 0`;
- `unitPrice >= 0`;
- normalização de `channel` e `location`;
- cálculo de receita;
- criação de `eventDate`;
- deduplicação por `eventId`.

### Gold

```text
s3a://smartretail-gold/orders-daily/
s3a://smartretail-gold/orders-summary/
```

KPIs:

- total de pedidos;
- total de itens;
- receita total;
- ticket médio;
- clientes únicos;
- produtos únicos;
- vendas por data, canal e localização.

### Apache Iceberg

Tabela gerenciada:

```text
smartretail.lakehouse.orders
```

Warehouse:

```text
s3a://smartretail-warehouse/iceberg
```

Recursos validados:

- JDBC Catalog no PostgreSQL;
- snapshots;
- Time Travel;
- Schema Evolution;
- preservação de colunas evoluídas;
- atualização atual via `INSERT OVERWRITE`.

> `MERGE INTO` incremental ainda não faz parte da implementação atual.

---

## ✅ v0.4 — Airflow + Data Quality

A v0.4 adiciona orquestração aos jobs batch posteriores à Silver.

```text
silver_data_quality
        ↓
build_gold
        ↓
refresh_iceberg
        ↓
post_load_validation
```

Cada etapa roda em container Spark isolado via `DockerOperator`.

A Silver Data Quality funciona como checagem das invariantes da camada curada antes dos jobs seguintes.

---

## ✅ v0.5 — Analytics API + Dashboard React

A v0.5 transforma os Data Products Gold em uma camada de consumo analítico por API e interface web.

### Pipeline Airflow

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

A task `publish_analytics` executa:

```text
streaming/spark-streaming/src/main/python/analytics_export_app.py
```

### Serving model PostgreSQL

```text
analytics
├── sales_summary
└── sales_daily
```

### Analytics API

Serviço:

```text
backend/analytics-api
```

Endpoints:

```http
GET /api/v1/analytics/summary
GET /api/v1/analytics/sales/daily
```

Filtros opcionais no endpoint diário:

```text
from
to
channel
location
limit
```

Exemplo:

```http
GET /api/v1/analytics/sales/daily?from=2026-10-01&to=2026-10-02&channel=WEB&limit=100
```

### Dashboard

Stack atual:

- React 19;
- TypeScript;
- Vite;
- Nginx no container de produção.

Exibe resumo e segmentações por data, canal e localização.

---

## Fluxo E2E atual

```text
Order Event
   ↓
Spring Boot Ingestion API
   ↓
Transactional Outbox
   ↓
Apache Kafka
   ↓
Spark Structured Streaming
   ↓
Bronze → Silver
          ↓
    Data Quality
          ↓
        Gold
       /    \
      /      \
Iceberg     Analytics Export
   │              ↓
   │       PostgreSQL analytics
   │              ↓
   │       Analytics API
   │              ↓
   └──────► React Dashboard
```

---

## 🚀 Execução local

### Pré-requisitos

```text
Java 21
Maven 3.9+
Docker Desktop
Docker Compose
Node.js 22+ (apenas desenvolvimento local do dashboard)
```

### Clonar

```bash
git clone https://github.com/juceliocoelho2022/smartretail-data-platform.git
cd smartretail-data-platform
```

### Subir a plataforma

```bash
docker compose up -d --build
```

### Portas locais

| Componente | Porta |
|---|---:|
| Ingestion API | `8080` |
| Airflow | `8081` |
| Analytics API | `8082` |
| React Dashboard | `3000` |
| PostgreSQL | `5433` |
| Kafka | `9092` |
| Spark Streaming UI | `4040` |
| Spark Silver UI | `4041` |
| MinIO S3 API | `9000` |
| MinIO Console | `9001` |

Dentro da rede Docker, PostgreSQL utiliza a porta interna `5432`.

### Export analítico manual

```bash
docker compose run --rm spark-analytics-export
```

Saída de validação esperada no ambiente do projeto:

```text
Analytics serving export: PASSED
Summary rows=1
Daily rows=4
```

### Disparar a DAG

```bash
docker exec smartretail-airflow \
  airflow dags trigger smartretail_lakehouse_pipeline
```

---

## 📨 Criar evento de pedido

```bash
curl -i -X POST http://localhost:8080/api/v1/events/orders \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: demo-order-001" \
  -d '{
    "customerId": "CUST-001",
    "productId": "PROD-001",
    "quantity": 2,
    "unitPrice": 149.90,
    "channel": "WEB",
    "location": "SAO_PAULO"
  }'
```

Mais exemplos: [docs/API_EXAMPLES.md](docs/API_EXAMPLES.md).

---

## 📊 Analytics API

```bash
curl http://localhost:8082/api/v1/analytics/summary
curl "http://localhost:8082/api/v1/analytics/sales/daily?limit=100"
```

Dashboard:

```text
http://localhost:3000
```

Airflow:

```text
http://localhost:8081
```

---

## ❤️ Health e métricas

Ingestion API:

```bash
curl http://localhost:8080/actuator/health
curl http://localhost:8080/actuator/prometheus
```

Analytics API:

```bash
curl http://localhost:8082/actuator/health
curl http://localhost:8082/actuator/prometheus
```

---

## 🧪 Testes e CI

Workflows principais:

```text
.github/workflows/backend-ci.yml
.github/workflows/streaming-ci.yml
.github/workflows/analytics-ci.yml
.github/workflows/dashboard-ci.yml
```

Cobertura por área:

- ingestion backend: JUnit 5, Mockito, MockMvc, JaCoCo;
- analytics API: JUnit 5, Mockito, JaCoCo;
- streaming/lakehouse: PySpark + Python `unittest`;
- dashboard: TypeScript build + Vite;
- Docker Compose validado no pipeline de streaming.

---

## 🧰 Stack tecnológica

A stack aparece aqui **depois** do problema, regras e decisões porque tecnologia é consequência da necessidade arquitetural.

| Área | Tecnologias / responsabilidade |
|---|---|
| Backend | Java 21, Spring Boot 3.5.5 |
| APIs | REST, Validation, Spring JDBC |
| Persistência | PostgreSQL 17, Spring Data JPA, JDBC |
| Event backbone | Apache Kafka 3.9.1 |
| Confiabilidade | Idempotência, Transactional Outbox, retry, DLT |
| Schema | Flyway |
| Stream processing | Apache Spark 4.0.1, PySpark, Structured Streaming |
| Lakehouse | MinIO/S3, Parquet, Bronze/Silver/Gold, Apache Iceberg 1.11.0 |
| Orquestração | Apache Airflow 3.3.2, DockerOperator |
| Data Quality | Silver invariant gate, post-load validation |
| Analytics | PostgreSQL serving model, Spring Boot Analytics API |
| Frontend | React 19, TypeScript, Vite, Nginx |
| Observabilidade | Actuator, Micrometer, Prometheus endpoints, pipeline status/logs |
| Testes | JUnit 5, Mockito, MockMvc, JaCoCo, Python `unittest` |
| Containers | Docker, Docker Compose |
| CI/CD | GitHub Actions |

---

## 📁 Estrutura do projeto

```text
smartretail-data-platform/
├── backend/
│   ├── ingestion-api/
│   └── analytics-api/
├── frontend/
│   └── analytics-dashboard/
├── streaming/
│   └── spark-streaming/
├── lakehouse/
├── orchestration/
│   └── airflow/
├── docs/
│   ├── BUSINESS_PROBLEM.md
│   ├── BUSINESS_RULES.md
│   ├── REQUIREMENTS.md
│   ├── ARCHITECTURE.md
│   ├── PORTFOLIO_CASE_STUDY.md
│   ├── API_EXAMPLES.md
│   ├── ROADMAP.md
│   └── adr/
│       ├── ADR-001-kafka-event-backbone.md
│       ├── ADR-002-transactional-outbox.md
│       ├── ADR-003-medallion-and-airflow-boundary.md
│       └── ADR-004-analytics-serving-model.md
├── .github/workflows/
├── docker-compose.yml
└── README.md
```

---

## 🚀 Roadmap

| Release | Entrega | Status |
|---|---|---|
| **v0.1** | Event Platform — Spring Boot + Kafka + PostgreSQL + Docker | ✅ Concluída |
| **v0.2** | Streaming Analytics — Spark Structured Streaming | ✅ Concluída |
| **v0.3** | Data Lakehouse — MinIO/S3 + Bronze/Silver/Gold + Iceberg | ✅ Concluída |
| **v0.4** | Data Engineering — Airflow + PySpark + Data Quality | ✅ Concluída |
| **v0.5** | Analytics — Serving PostgreSQL + Spring Boot API + Dashboard React | ✅ Concluída |
| **v0.6** | AI — MLflow + previsão de demanda + detecção de anomalias | ⏭️ Roadmap |

### v0.6 — futuro

Itens planejados, **não implementados como parte do estado atual**:

- MLflow para tracking de experimentos;
- previsão de demanda;
- detecção de anomalias;
- versionamento de modelos;
- integração dos resultados ao ecossistema analítico.

---

## 📚 Documentação técnica

### Negócio e requisitos

- [Business Problem](docs/BUSINESS_PROBLEM.md)
- [Business Rules](docs/BUSINESS_RULES.md)
- [Requirements](docs/REQUIREMENTS.md)
- [Portfolio Case Study](docs/PORTFOLIO_CASE_STUDY.md)

### Arquitetura

- [Arquitetura atual](docs/ARCHITECTURE.md)
- [ADR-001 — Kafka Event Backbone](docs/adr/ADR-001-kafka-event-backbone.md)
- [ADR-002 — Transactional Outbox](docs/adr/ADR-002-transactional-outbox.md)
- [ADR-003 — Medallion + Airflow](docs/adr/ADR-003-medallion-and-airflow-boundary.md)
- [ADR-004 — Analytics Serving Model](docs/adr/ADR-004-analytics-serving-model.md)

### Implementação

- [Exemplos da API](docs/API_EXAMPLES.md)
- [Implementação da v0.1](docs/V0.1_IMPLEMENTATION.md)
- [Roadmap](docs/ROADMAP.md)
- [Airflow](orchestration/airflow/README.md)
- [Analytics API](backend/analytics-api/README.md)
- [Analytics Dashboard](frontend/analytics-dashboard/README.md)

---

## 📐 Princípios de engenharia

1. **Problema antes da tecnologia**
2. **Regras e contratos explícitos**
3. **Idempotência nos limites necessários**
4. **Baixo acoplamento**
5. **At-least-once + efeitos idempotentes, não exactly-once global**
6. **Data Quality antes do consumo analítico**
7. **Observabilidade by design**
8. **Automação de testes**
9. **Infraestrutura reproduzível**
10. **Evolução incremental orientada por necessidade**

---

## 🎓 Como apresentar este projeto em entrevista

Uma versão de 60 segundos, explicação técnica de 3–5 minutos e perguntas/respostas estão disponíveis em:

➡️ **[SmartRetail Portfolio Case Study](docs/PORTFOLIO_CASE_STUDY.md)**

---

## 📄 Licença

Distribuído sob a licença **MIT**. Consulte [LICENSE](LICENSE).

---

## 👨‍💻 Autor

**Jucelio Coelho**

Backend Java • Engenharia de Dados • Big Data • Cloud • QA

GitHub: [@juceliocoelho2022](https://github.com/juceliocoelho2022)

---

<p align="center">
  <strong>SmartRetail Data Platform</strong><br>
  Do problema de negócio à evidência de engenharia: ingestão confiável, streaming, Lakehouse e Analytics.
</p>
