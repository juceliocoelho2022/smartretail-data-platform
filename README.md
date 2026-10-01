# 🛒 SmartRetail Data Platform

<p align="center">
  <strong>Plataforma orientada a eventos para varejo, construída com Java, Kafka, PostgreSQL e evolução planejada para Big Data, Lakehouse, Analytics e IA.</strong>
</p>

<p align="center">
  Projeto de portfólio focado em <strong>Backend Java, Engenharia de Dados, Event Streaming, Observabilidade e Machine Learning</strong>.
</p>

<p align="center">
  <img alt="Java 21" src="https://img.shields.io/badge/Java-21-ED8B00?logo=openjdk&logoColor=white">
  <img alt="Spring Boot 3.5.5" src="https://img.shields.io/badge/Spring%20Boot-3.5.5-6DB33F?logo=springboot&logoColor=white">
  <img alt="Apache Kafka 3.9.1" src="https://img.shields.io/badge/Apache%20Kafka-3.9.1-231F20?logo=apachekafka&logoColor=white">
  <img alt="PostgreSQL 17" src="https://img.shields.io/badge/PostgreSQL-17-4169E1?logo=postgresql&logoColor=white">
  <img alt="Docker" src="https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white">
  <img alt="CI" src="https://github.com/juceliocoelho2022/smartretail-data-platform/actions/workflows/backend-ci.yml/badge.svg?branch=main">
  <img alt="v0.1" src="https://img.shields.io/badge/v0.1-Event%20Platform%20conclu%C3%ADda-2EA44F">
</p>

---

## 📌 Visão geral

O **SmartRetail Data Platform** é uma plataforma de dados orientada a eventos criada para simular um cenário realista de varejo digital e físico.

O objetivo é demonstrar, de forma incremental, o ciclo completo do dado:

**geração → ingestão → streaming → processamento → armazenamento → qualidade → analytics → machine learning → consumo**

A primeira release funcional já implementa uma fatia vertical completa de ingestão e processamento assíncrono de pedidos.

> **Status atual:** ✅ **v0.1 — Event Platform concluída, validada localmente de ponta a ponta e aprovada pelo CI.**  
> **Próxima etapa:** v0.2 — Spark Structured Streaming.

---

## ✅ v0.1 — Event Platform

A v0.1 implementa uma arquitetura orientada a eventos com consistência transacional, idempotência e processamento assíncrono.

### Tecnologias implementadas

- **Java 21**
- **Spring Boot 3.5.5**
- Spring Web
- Spring Validation
- Spring Data JPA
- Apache Kafka 3.9.1
- PostgreSQL 17
- Flyway
- Spring Boot Actuator
- Micrometer + Prometheus
- Docker + Docker Compose
- JUnit 5
- Mockito
- MockMvc
- JaCoCo
- GitHub Actions

### Capacidades implementadas

- API REST para ingestão de eventos de pedidos
- validação de payload
- respostas HTTP com Problem Details
- suporte a **Idempotency-Key**
- persistência transacional de idempotência
- **Transactional Outbox Pattern**
- publicação assíncrona no Kafka
- tópico `smartretail.orders.v1`
- 3 partições
- retry no consumo
- Dead Letter Topic
- consumer idempotente
- controle de eventos já processados
- projeção de pedidos em PostgreSQL
- migrations versionadas com Flyway
- health checks
- métricas Prometheus
- testes automatizados
- relatório de cobertura com JaCoCo
- CI com Java 21 e Maven

---

## 🔄 Fluxo validado de ponta a ponta

```text
POST /api/v1/events/orders
          │
          ▼
   Spring Boot API
          │
          ├──► idempotency_record
          │
          └──► outbox_event
                     │
                     ▼
              Outbox Publisher
                     │
                     ▼
        smartretail.orders.v1
                     │
                     ▼
           Kafka Consumer
              │          │
              ▼          ▼
      processed_event   order_event_projection
                              │
                              ▼
             GET /api/v1/events/orders/{eventId}
```

O fluxo foi validado localmente com:

- criação de evento com HTTP `202 Accepted`;
- repetição da chamada com o mesmo `Idempotency-Key`;
- retorno do mesmo `eventId`;
- indicação `replayed=true`;
- publicação assíncrona no Kafka;
- consumo do evento;
- gravação da projeção;
- consulta posterior via endpoint GET.

---

## 🧠 Decisões arquiteturais

### Transactional Outbox

A API não publica diretamente no Kafka dentro da transação HTTP.

Em vez disso:

```text
Transação PostgreSQL
├── idempotency_record
└── outbox_event
```

Após o commit, o publisher lê o outbox e envia o evento ao Kafka.

Essa abordagem reduz o risco de inconsistência entre banco e broker.

### Idempotência na entrada

Chamadas repetidas com o mesmo `Idempotency-Key` retornam o mesmo evento em vez de gerar pedidos duplicados.

### Idempotência no consumer

A tabela `processed_event` registra os eventos já processados e protege a projeção contra reprocessamento duplicado.

### Retry + DLT

Falhas de consumo utilizam retry e, após as tentativas configuradas, o evento pode ser encaminhado para:

```text
smartretail.orders.v1.DLT
```

### Limitação consciente da v0.1

O polling do Transactional Outbox foi projetado para uma instância da aplicação.

Para escala horizontal, a evolução deverá utilizar uma estratégia de claim/locking, como `FOR UPDATE SKIP LOCKED`, ou CDC com Debezium.

---

## 🚀 Execução local

### Pré-requisitos

```text
Java 21
Maven 3.9+
Docker Desktop
Docker Compose
```

### Clonar o projeto

```bash
git clone https://github.com/juceliocoelho2022/smartretail-data-platform.git
cd smartretail-data-platform
```

### Subir PostgreSQL, Kafka e API

```bash
docker compose up --build
```

### Portas locais

| Componente | Porta |
|---|---:|
| API Spring Boot | `8080` |
| PostgreSQL | `5433` |
| Kafka | `9092` |

Dentro da rede Docker, o PostgreSQL continua disponível na porta interna `5432`.

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

Exemplo de resposta:

```json
{
  "eventId": "21e31d5c-0c1a-41b5-9ff8-0772e44c8cfd",
  "status": "ACCEPTED",
  "replayed": false,
  "acceptedAt": "2026-09-30T22:08:59Z"
}
```

Repetindo a mesma chamada com o mesmo `Idempotency-Key`:

```json
{
  "eventId": "21e31d5c-0c1a-41b5-9ff8-0772e44c8cfd",
  "status": "ACCEPTED",
  "replayed": true
}
```

---

## 🔎 Consultar projeção

```bash
curl http://localhost:8080/api/v1/events/orders/{eventId}
```

Exemplo:

```json
{
  "eventId": "21e31d5c-0c1a-41b5-9ff8-0772e44c8cfd",
  "customerId": "CUST-001",
  "productId": "PROD-001",
  "quantity": 2,
  "unitPrice": 149.90,
  "channel": "WEB",
  "location": "SAO_PAULO",
  "occurredAt": "2026-09-30T22:08:59Z",
  "processedAt": "2026-09-30T22:09:00Z"
}
```

Como o processamento é assíncrono, o GET pode retornar `404` por alguns instantes antes da projeção ser criada.

---

## ❤️ Health e métricas

### Health

```bash
curl http://localhost:8080/actuator/health
```

### Prometheus

```bash
curl http://localhost:8080/actuator/prometheus
```

---

## 🧪 Testes

A v0.1 possui testes automatizados para as principais responsabilidades da API.

### Executar testes

```bash
cd backend/ingestion-api
mvn clean test
```

### Executar validação completa

```bash
mvn clean verify
```

### Relatório JaCoCo

Após o `verify`:

```text
backend/ingestion-api/target/site/jacoco/index.html
```

Cobertura atual é utilizada como instrumento de inspeção. O quality gate mínimo será introduzido conforme a suíte de testes crescer.

---

## ⚙️ CI — GitHub Actions

O workflow:

```text
.github/workflows/backend-ci.yml
```

executa:

```text
Checkout
   ↓
Java 21
   ↓
Maven
   ↓
mvn -B verify
   ↓
Testes + JaCoCo
```

A v0.1 foi integrada à `main` após o pipeline concluir com sucesso.

---

## 📚 Documentação técnica

- [Exemplos da API](docs/API_EXAMPLES.md)
- [Implementação da v0.1](docs/V0.1_IMPLEMENTATION.md)
- [Arquitetura](docs/ARCHITECTURE.md)
- [Roadmap](docs/ROADMAP.md)

---

## 🏗️ Arquitetura alvo da plataforma

<p align="center">
  <img src="docs/assets/smartretail-data-platform-architecture.svg" alt="Arquitetura SmartRetail Data Platform" width="100%">
</p>

```text
Web / Mobile / PDV / APIs
          │
          ▼
 Spring Boot — Java 21
          │
          ▼
      Apache Kafka
       /        \
      /          \
     ▼            ▼
Spark Structured   Data Lakehouse
Streaming          MinIO/S3 + Iceberg
     │                    │
     ▼                    ▼
Real-time KPIs      Bronze → Silver → Gold
                           │
                           ▼
                        PySpark
                           │
                           ▼
                     MLflow + ML
                           │
                 ┌─────────┴─────────┐
                 ▼                   ▼
          Analytics API        Data Products
                 │
                 ▼
          Dashboard React
```

Os componentes além da Event Platform serão implementados incrementalmente nas próximas releases.

---

## 🎯 Casos de uso planejados

A plataforma evoluirá para responder perguntas como:

- Qual é a receita por minuto?
- Qual é o ticket médio por canal?
- Quais produtos possuem maior volume de vendas?
- Quais produtos apresentam risco de ruptura?
- Qual região possui maior conversão?
- Existem transações ou eventos anômalos?
- Qual é a demanda prevista para as próximas horas ou dias?

---

## 5️⃣ Big Data — os 5 Vs

| V | Aplicação no projeto |
|---|---|
| **Volume** | processamento de grandes quantidades de eventos de varejo |
| **Velocity** | ingestão contínua com Kafka e processamento com Spark |
| **Variety** | JSON, CSV, logs, dados relacionais, APIs e telemetria |
| **Veracity** | validação, deduplicação, consistência e Data Quality |
| **Value** | KPIs, alertas, previsões e suporte à decisão |

---

## 🏞️ Data Lakehouse — roadmap

A evolução analítica utilizará Medallion Architecture.

### Bronze

Dados brutos e imutáveis para rastreabilidade e reprocessamento.

### Silver

Dados tratados, deduplicados, normalizados e enriquecidos.

### Gold

Dados preparados para consumo analítico, por exemplo:

```text
sales_daily
sales_by_region
customer_360
product_performance
inventory_risk
sales_forecast
```

Tecnologias planejadas:

- MinIO / AWS S3
- Apache Iceberg
- Parquet
- PySpark

---

## 🔁 Data Engineering — roadmap

A camada de engenharia de dados deverá evoluir com:

- Apache Airflow
- PySpark
- Data Quality
- retries
- scheduling
- SLAs
- logs
- alertas
- pipelines Bronze → Silver → Gold

---

## 🤖 Machine Learning — roadmap

Casos de uso previstos:

- previsão de demanda;
- detecção de anomalias;
- risco de ruptura de estoque;
- análise de comportamento transacional.

O MLflow será utilizado para rastreamento de experimentos, métricas, artefatos e modelos.

---

## 🔭 Observabilidade

### Implementado na v0.1

- Spring Boot Actuator
- Micrometer
- endpoint Prometheus
- logs da aplicação

### Evolução planejada

- Prometheus server
- Grafana
- OpenTelemetry
- tracing distribuído
- correlação entre logs, métricas e traces

---

## 🛡️ Segurança e governança

Princípios adotados:

- nenhum segredo real versionado no repositório;
- configuração por variáveis de ambiente;
- segregação entre configuração local e código;
- evolução futura para autenticação e autorização;
- princípio do menor privilégio;
- auditoria;
- retenção e lineage.

---

## 🧰 Stack tecnológica

| Área | Tecnologias |
|---|---|
| Backend | Java 21, Spring Boot 3.5.5 |
| API | REST, Validation, Problem Details |
| Persistência | PostgreSQL 17, Spring Data JPA |
| Streaming | Apache Kafka 3.9.1 |
| Resiliência | Retry, DLT, Idempotência |
| Mensageria | Transactional Outbox |
| Schema | Flyway |
| Observabilidade | Actuator, Micrometer, Prometheus |
| Testes | JUnit 5, Mockito, MockMvc, JaCoCo |
| Containers | Docker, Docker Compose |
| CI | GitHub Actions |
| Big Data | Apache Spark — próxima release |
| Data Engineering | PySpark, Airflow — roadmap |
| Lakehouse | MinIO/S3, Iceberg, Parquet — roadmap |
| ML | MLflow — roadmap |
| Frontend | React/Vite — roadmap |

---

## 📁 Estrutura do projeto

```text
smartretail-data-platform/
├── backend/
│   └── ingestion-api/
│       ├── src/main/java/
│       ├── src/main/resources/
│       │   └── db/migration/
│       ├── src/test/java/
│       ├── Dockerfile
│       └── pom.xml
├── docs/
│   ├── assets/
│   ├── API_EXAMPLES.md
│   ├── ARCHITECTURE.md
│   ├── ROADMAP.md
│   └── V0.1_IMPLEMENTATION.md
├── .github/
│   └── workflows/
│       └── backend-ci.yml
├── .env.example
├── docker-compose.yml
├── LICENSE
└── README.md
```

A estrutura cresce junto com as releases; o repositório não apresenta componentes de roadmap como se já estivessem implementados.

---

## 🚀 Roadmap

| Release | Entrega | Status |
|---|---|---|
| **v0.1** | Event Platform — Spring Boot + Kafka + PostgreSQL + Docker | ✅ Concluída |
| **v0.2** | Streaming Analytics — Spark Structured Streaming | 🔜 Próxima |
| **v0.3** | Data Lakehouse — MinIO/S3 + Bronze/Silver/Gold + Iceberg | ⏳ Planejada |
| **v0.4** | Data Engineering — Airflow + PySpark + Data Quality | ⏳ Planejada |
| **v0.5** | Analytics — API + Dashboard React | ⏳ Planejada |
| **v0.6** | AI — MLflow + previsão de demanda + anomalias | ⏳ Planejada |

---

## 🎓 Competências demonstradas

A v0.1 já demonstra, na prática:

- desenvolvimento backend com Java 21;
- Spring Boot;
- desenho de APIs REST;
- arquitetura orientada a eventos;
- Apache Kafka;
- Transactional Outbox;
- idempotência;
- processamento assíncrono;
- persistência com PostgreSQL;
- versionamento de schema com Flyway;
- testes automatizados;
- cobertura de código;
- CI;
- Docker;
- observabilidade;
- documentação técnica;
- evolução incremental de arquitetura.

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
  Do evento ao insight — arquitetura orientada a dados, streaming e geração de valor.
</p>
