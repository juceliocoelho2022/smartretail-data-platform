# 🛒 SmartRetail Data Platform

<p align="center">
  <strong>Plataforma de dados orientada a eventos para varejo, construída com Java, Kafka, Spark, Airflow, PostgreSQL, Apache Iceberg e React.</strong>
</p>

<p align="center">
  Projeto de portfólio focado em <strong>Backend Java, Engenharia de Dados, Event Streaming, Lakehouse, Analytics e Machine Learning</strong>.
</p>

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

---

## 📌 Visão geral

O **SmartRetail Data Platform** simula uma plataforma de dados moderna para varejo digital e físico, cobrindo o ciclo completo do dado:

```text
geração → ingestão → mensageria → streaming → lakehouse → qualidade
       → orquestração → serving analítico → API → dashboard → ML
```

O projeto evolui por releases incrementais e cada fase adiciona capacidades reais sobre a arquitetura anterior.

> **Status atual:** ✅ **v0.5 — Analytics API + Dashboard React concluída, integrada à `main` e validada de ponta a ponta.**
>
> ✅ Event Platform com Java 21 + Spring Boot + Kafka  
> ✅ Spark Structured Streaming  
> ✅ Bronze / Silver / Gold em MinIO  
> ✅ Apache Iceberg com JDBC Catalog, snapshots, Time Travel e Schema Evolution  
> ✅ Apache Airflow + Data Quality + validação pós-carga  
> ✅ Serving analítico em PostgreSQL  
> ✅ Analytics API com Spring Boot  
> ✅ Dashboard React + TypeScript  
> ✅ Pipeline completo Airflow finalizado com `state=success`

---

## 🏗️ Arquitetura atual

```text
Web / Mobile / PDV / APIs
          │
          ▼
┌───────────────────────────────┐
│ Ingestion API                 │
│ Java 21 + Spring Boot         │
│ Idempotency + Outbox Pattern  │
└──────────────┬────────────────┘
               │
               ▼
        PostgreSQL 17
               │
               ▼
         Apache Kafka
               │
               ▼
  Spark Structured Streaming
               │
        ┌──────┴──────┐
        ▼             ▼
   Real-time KPIs   MinIO / S3
                      │
                      ▼
                  Bronze
                      │
                      ▼
                   Silver
                      │
             Data Quality Gate
                      │
                      ▼
                    Gold
                      │
             ┌────────┴────────┐
             ▼                 ▼
      Apache Iceberg      Analytics Export
             │                 │
     JDBC Catalog              ▼
        PostgreSQL      PostgreSQL / analytics
                               │
                               ▼
                     Spring Boot Analytics API
                               │
                               ▼
                      React Analytics Dashboard
```

A ingestão Kafka/Bronze e a Silver permanecem fluxos contínuos de **Spark Structured Streaming**. O Airflow orquestra os jobs batch finitos posteriores à Silver.

---

## ✅ v0.1 — Event Platform

A primeira release estabelece a base transacional e orientada a eventos.

### Principais capacidades

- Java 21 + Spring Boot 3.5.5;
- API REST para ingestão de pedidos;
- validação de payload;
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

### Implementado

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

A v0.3 implementa a Medallion Architecture sobre MinIO/S3.

### Bronze

Eventos Kafka brutos e reprocessáveis:

```text
s3a://smartretail-bronze/orders/
```

Preserva payload, chave, tópico, partição, offset, timestamps e data de ingestão.

### Silver

Dados tipados, normalizados, validados e deduplicados:

```text
s3a://smartretail-silver/orders/
```

Regras principais:

- `eventId`, `customerId` e `productId` obrigatórios;
- `quantity > 0`;
- `unitPrice >= 0`;
- normalização de `channel` e `location`;
- cálculo de receita;
- criação de `eventDate`;
- deduplicação por `eventId`.

### Gold

Data Products analíticos reconstruíveis:

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

Estado atualmente validado:

```text
totalOrders       = 5
totalItems        = 14
totalRevenue      = 2918.60
averageOrderValue = 583.72
uniqueCustomers   = 4
uniqueProducts    = 4
```

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

A v0.4 adiciona orquestração explícita sobre os jobs batch do Lakehouse.

### DAG

```text
smartretail_lakehouse_pipeline

silver_data_quality
        ↓
build_gold
        ↓
refresh_iceberg
        ↓
post_load_validation
```

Cada etapa roda em container Spark isolado via `DockerOperator`.

### Data Quality Gate

Validações sobre a Silver:

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

A Silver já é uma camada curada; portanto esse gate funciona como **checagem de invariantes da Silver**, e não como medição de rejeições da Bronze.

### Post-load validation

```text
Post-load validation: PASSED
Silver rows=5
Iceberg rows=5
Gold totalOrders=5
```

---

## ✅ v0.5 — Analytics API + Dashboard React

A v0.5 transforma os Data Products Gold em uma camada de consumo analítico acessível por API e interface web.

### Pipeline Airflow atualizado

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

A nova task `publish_analytics` executa o job Spark:

```text
streaming/spark-streaming/src/main/python/analytics_export_app.py
```

e publica os Data Products Gold no schema analítico do PostgreSQL.

### Serving model PostgreSQL

Schema isolado:

```text
analytics
├── sales_summary
└── sales_daily
```

Essa separação evita acoplamento entre as tabelas transacionais da Event Platform e o modelo de leitura analítico.

### Analytics API

Serviço dedicado:

```text
backend/analytics-api
```

Stack:

- Java 21;
- Spring Boot 3.5.5;
- Spring JDBC;
- Flyway;
- PostgreSQL;
- Actuator;
- Micrometer / Prometheus;
- JUnit 5 + Mockito.

Endpoints:

```http
GET /api/v1/analytics/summary
GET /api/v1/analytics/sales/daily
```

O endpoint diário aceita filtros opcionais:

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

### React Analytics Dashboard

Aplicação:

```text
frontend/analytics-dashboard
```

Stack:

- React 19;
- TypeScript;
- Vite;
- Nginx no container de produção.

KPIs exibidos:

```text
Receita total        R$ 2.918,60
Pedidos              5
Ticket médio         R$ 583,72
Itens vendidos       14
Clientes únicos      4
Produtos únicos      4
```

Também são exibidos:

- receita por segmento;
- data;
- canal;
- localização;
- número de pedidos;
- itens;
- receita;
- ticket médio.

### Validação E2E da v0.5

A execução final pelo Airflow confirmou:

```text
silver_data_quality    success
build_gold             success
refresh_iceberg        success
post_load_validation   success
publish_analytics      success
DagRun                  success
```

Analytics Export:

```text
Analytics serving export: PASSED
Summary rows=1
Daily rows=4
```

A API retornou o estado recém-publicado:

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

O `refreshedAt` confirma que a API estava servindo os dados publicados pelo último run do Airflow.

---

## 🔄 Fluxo E2E atual

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

## 🧠 Decisões arquiteturais importantes

### Transactional Outbox

A API não publica no Kafka dentro da mesma transação HTTP. Primeiro persiste o evento no PostgreSQL e, após o commit, o publisher envia ao broker.

### Idempotência ponta a ponta

A plataforma protege contra duplicação tanto na entrada HTTP quanto no consumo Kafka.

### Streaming + batch separados

Bronze e Silver são fluxos contínuos. Airflow coordena os jobs batch finitos posteriores à Silver.

### Medallion Architecture

A separação Bronze → Silver → Gold mantém rastreabilidade entre dados brutos, confiáveis e prontos para consumo.

### Serving model separado

O schema `analytics` funciona como read model para a API, evitando consultas diretas do frontend sobre arquivos Gold ou tabelas transacionais.

### Iceberg como tabela analítica versionada

A tabela Iceberg adiciona snapshots, Time Travel e Schema Evolution sem substituir os Data Products Gold usados pelo serving model.

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

Dentro da rede Docker, o PostgreSQL utiliza a porta interna `5432`.

### Executar o export analítico manualmente

```bash
docker compose run --rm spark-analytics-export
```

Saída esperada:

```text
Analytics serving export: PASSED
Summary rows=1
Daily rows=4
```

### Disparar a DAG completa

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

---

## 📊 Consultar Analytics API

Resumo:

```bash
curl http://localhost:8082/api/v1/analytics/summary
```

Vendas diárias:

```bash
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

Cobertura:

- backend de ingestão: JUnit 5, Mockito, MockMvc, JaCoCo;
- analytics API: JUnit 5, Mockito, JaCoCo;
- streaming/lakehouse: PySpark + `unittest`;
- dashboard: TypeScript build + Vite;
- Docker Compose validado no pipeline de streaming.

Após o merge da v0.5, os workflows `analytics-ci`, `dashboard-ci` e `streaming-ci` concluíram com sucesso na `main`.

---

## 🧰 Stack tecnológica

| Área | Tecnologias |
|---|---|
| Backend | Java 21, Spring Boot 3.5.5 |
| APIs | REST, Validation, Problem Details, Spring JDBC |
| Persistência | PostgreSQL 17, Spring Data JPA, JDBC |
| Streaming | Apache Kafka 3.9.1 |
| Resiliência | Retry, DLT, Idempotência |
| Mensageria | Transactional Outbox |
| Schema | Flyway |
| Big Data | Apache Spark 4.0.1, PySpark, Structured Streaming |
| Lakehouse | MinIO/S3, Parquet, Bronze, Silver, Gold, Apache Iceberg 1.11.0 |
| Data Engineering | Apache Airflow 3.3.2, DockerOperator, Data Quality, post-load validation |
| Analytics | PostgreSQL serving model, Spring Boot Analytics API |
| Frontend | React 19, TypeScript, Vite, Nginx |
| Observabilidade | Actuator, Micrometer, Prometheus endpoints |
| Testes | JUnit 5, Mockito, MockMvc, JaCoCo, Python `unittest` |
| Containers | Docker, Docker Compose |
| CI/CD | GitHub Actions |
| ML | MLflow — próxima fase |

---

## 📁 Estrutura do projeto

```text
smartretail-data-platform/
├── backend/
│   ├── ingestion-api/
│   │   ├── src/main/java/
│   │   ├── src/main/resources/
│   │   ├── src/test/java/
│   │   ├── Dockerfile
│   │   └── pom.xml
│   └── analytics-api/
│       ├── src/main/java/
│       ├── src/main/resources/
│       ├── src/test/java/
│       ├── Dockerfile
│       ├── pom.xml
│       └── README.md
├── frontend/
│   └── analytics-dashboard/
│       ├── src/
│       ├── Dockerfile
│       ├── package.json
│       └── README.md
├── streaming/
│   └── spark-streaming/
│       ├── src/main/python/
│       ├── src/test/python/
│       ├── Dockerfile.jobs
│       └── README.md
├── lakehouse/
│   └── README.md
├── orchestration/
│   └── airflow/
│       ├── dags/
│       │   ├── smartretail_airflow_smoke.py
│       │   └── smartretail_lakehouse_pipeline.py
│       ├── Dockerfile
│       ├── requirements.txt
│       └── README.md
├── docs/
│   ├── assets/
│   ├── API_EXAMPLES.md
│   ├── ARCHITECTURE.md
│   ├── ROADMAP.md
│   └── V0.1_IMPLEMENTATION.md
├── .github/
│   └── workflows/
│       ├── backend-ci.yml
│       ├── streaming-ci.yml
│       ├── analytics-ci.yml
│       └── dashboard-ci.yml
├── .env.example
├── docker-compose.yml
├── LICENSE
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
| **v0.6** | AI — MLflow + previsão de demanda + detecção de anomalias | ⏭️ Próxima |

---

## 🤖 v0.6 — Próxima fase

A próxima etapa adicionará uma camada de Machine Learning sobre os dados confiáveis da plataforma.

Escopo planejado:

- MLflow para tracking de experimentos;
- previsão de demanda;
- detecção de anomalias;
- versionamento de modelos;
- métricas de treinamento e avaliação;
- integração dos resultados ao ecossistema analítico.

A implementação da v0.6 ainda não foi iniciada neste README; os itens acima representam roadmap.

---

## 🎓 Competências demonstradas

As releases v0.1 até v0.5 demonstram na prática:

- Java 21 e Spring Boot;
- APIs REST;
- arquitetura orientada a eventos;
- Apache Kafka;
- Transactional Outbox;
- idempotência;
- retry e DLT;
- Apache Spark Structured Streaming;
- PySpark;
- event time, watermark e janelas;
- Medallion Architecture;
- MinIO / S3A;
- Parquet;
- Bronze / Silver / Gold;
- Apache Iceberg;
- JDBC Catalog;
- snapshots e Time Travel;
- Schema Evolution;
- Data Quality;
- Apache Airflow;
- DAGs;
- DockerOperator;
- validação pós-carga;
- serving model analítico;
- PostgreSQL;
- Spring JDBC;
- Flyway;
- React;
- TypeScript;
- Vite;
- Docker e Docker Compose;
- CI com GitHub Actions;
- testes automatizados;
- observabilidade com Actuator/Micrometer/Prometheus;
- documentação técnica;
- evolução incremental de arquitetura.

---

## 📚 Documentação técnica

- [Exemplos da API](docs/API_EXAMPLES.md)
- [Implementação da v0.1](docs/V0.1_IMPLEMENTATION.md)
- [Arquitetura](docs/ARCHITECTURE.md)
- [Roadmap](docs/ROADMAP.md)
- [Airflow](orchestration/airflow/README.md)
- [Analytics API](backend/analytics-api/README.md)
- [Analytics Dashboard](frontend/analytics-dashboard/README.md)

---

## 📐 Princípios de engenharia

1. **Clareza antes de complexidade**
2. **Contratos explícitos**
3. **Idempotência**
4. **Baixo acoplamento**
5. **Observabilidade by design**
6. **Automação de testes**
7. **Infraestrutura reproduzível**
8. **Dados confiáveis antes de IA**
9. **Documentação junto com o código**
10. **Evolução incremental**

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
  Do evento ao insight — streaming, Lakehouse, Analytics e evolução para IA.
</p>
