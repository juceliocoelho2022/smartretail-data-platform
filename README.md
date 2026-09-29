# 🛒 SmartRetail Data Platform

<p align="center">
  <strong>Plataforma de Big Data e Inteligência Artificial para vendas, clientes e estoque em tempo real.</strong>
</p>

<p align="center">
  Projeto de portfólio focado em <strong>Backend Java, Engenharia de Dados, Streaming, Lakehouse, Observabilidade e Machine Learning</strong>.
</p>

<p align="center">
  <img alt="Java 21" src="https://img.shields.io/badge/Java-21-ED8B00?logo=openjdk&logoColor=white">
  <img alt="Spring Boot" src="https://img.shields.io/badge/Spring%20Boot-3.x-6DB33F?logo=springboot&logoColor=white">
  <img alt="Apache Kafka" src="https://img.shields.io/badge/Apache%20Kafka-Event%20Streaming-231F20?logo=apachekafka&logoColor=white">
  <img alt="Apache Spark" src="https://img.shields.io/badge/Apache%20Spark-Streaming-E25A1C?logo=apachespark&logoColor=white">
  <img alt="Apache Airflow" src="https://img.shields.io/badge/Apache%20Airflow-Orchestration-017CEE?logo=apacheairflow&logoColor=white">
  <img alt="PostgreSQL" src="https://img.shields.io/badge/PostgreSQL-Analytics-4169E1?logo=postgresql&logoColor=white">
  <img alt="Docker" src="https://img.shields.io/badge/Docker-Containers-2496ED?logo=docker&logoColor=white">
  <img alt="Status" src="https://img.shields.io/badge/status-em%20desenvolvimento-F59E0B">
</p>

---

## 📌 Visão geral

O **SmartRetail Data Platform** é uma plataforma de dados orientada a eventos criada para simular um cenário realista de varejo digital e físico.

A proposta é demonstrar o ciclo completo do dado:

**geração → ingestão → streaming → processamento → armazenamento → qualidade → analytics → machine learning → consumo**

O sistema será construído de forma incremental, priorizando decisões arquiteturais justificadas, qualidade de código, observabilidade e documentação.

> **Status atual:** fase inicial da **v0.1 — Event Platform**.  
> Os componentes apresentados abaixo representam a **arquitetura alvo** e serão incorporados por releases.

---

## 🏗️ Arquitetura da solução

<p align="center">
  <img src="docs/assets/smartretail-data-platform-architecture.svg" alt="Arquitetura SmartRetail Data Platform" width="100%">
</p>

A arquitetura separa responsabilidades em camadas, evitando acoplamento entre geração, processamento, armazenamento e consumo dos dados.

### Fluxo principal

```text
Web / Mobile / Lojas / APIs / Logs
                 │
                 ▼
        Spring Boot API — Java 21
                 │
                 ▼
            Apache Kafka
             /         \
            /           \
           ▼             ▼
Spark Structured      Data Lake / Lakehouse
Streaming             MinIO / S3 + Iceberg
     │                  │
     ▼                  ▼
Real-time KPIs    Bronze → Silver → Gold
                        │
                        ▼
                     PySpark
                        │
                        ▼
                  MLflow + ML
                        │
             ┌──────────┴──────────┐
             ▼                     ▼
       PostgreSQL/API        Analytics
             │
             ▼
       Dashboard React
```

---

## 🎯 Problema de negócio

Uma operação de varejo moderna gera dados continuamente em vários canais.

O projeto considera eventos provenientes de:

- e-commerce;
- aplicativo mobile;
- pontos de venda;
- APIs de parceiros;
- sistemas de estoque;
- sensores IoT;
- logs de aplicações e infraestrutura.

A plataforma deverá transformar esses eventos em informações úteis para perguntas como:

- Qual é a receita por minuto?
- Qual é o ticket médio por canal?
- Quais produtos estão vendendo mais?
- Quais produtos possuem risco de ruptura?
- Quais clientes abandonaram o carrinho?
- Qual região apresenta maior conversão?
- Existem transações com comportamento anômalo?
- Qual é a demanda prevista para as próximas horas ou dias?

---

## 5️⃣ Big Data na prática — os 5 Vs

| V | Como será demonstrado |
|---|---|
| **Volume** | geração e processamento de grandes quantidades de eventos de vendas, clientes e estoque |
| **Velocity** | ingestão e processamento contínuo com Kafka e Spark Structured Streaming |
| **Variety** | eventos JSON, arquivos CSV, logs, dados relacionais, APIs e telemetria |
| **Veracity** | validação, deduplicação, tratamento de inconsistências e regras de Data Quality |
| **Value** | KPIs, alertas, previsões, detecção de anomalias e suporte à decisão |

---

## 🧩 Componentes da arquitetura

### 1. Fontes de dados

As fontes representam diferentes origens do ecossistema de varejo:

```text
Web
Mobile
PDV
APIs
IoT
Logs
Sistemas externos
```

Cada origem poderá produzir eventos com diferentes estruturas, frequência e criticidade.

---

### 2. Backend de ingestão — Java 21 + Spring Boot

Responsável por:

- receber eventos via REST;
- validar payloads;
- aplicar regras de entrada;
- gerar identificadores;
- publicar eventos no Kafka;
- expor health checks;
- registrar métricas e traces;
- padronizar erros de API.

Stack planejada:

```text
Java 21
Spring Boot
Spring Web
Spring Validation
Spring Data JPA
Spring Actuator
JUnit 5
Mockito
Testcontainers
```

---

### 3. Event Streaming — Apache Kafka

Kafka será o backbone de eventos da plataforma.

Tópicos planejados:

| Tópico | Responsabilidade |
|---|---|
| `customer-events` | eventos relacionados a clientes |
| `product-views` | visualizações de produtos |
| `cart-events` | adição, remoção e abandono de carrinho |
| `orders` | pedidos criados e atualizados |
| `payments` | eventos de pagamento |
| `inventory-events` | movimentações de estoque |
| `shipping-events` | eventos de entrega |

Práticas previstas:

- consumer groups;
- particionamento;
- retry;
- Dead Letter Topic;
- idempotência;
- versionamento de eventos;
- observabilidade do fluxo.

---

## 📨 Exemplo de evento

```json
{
  "eventId": "EVT-9821837",
  "eventType": "ORDER_CREATED",
  "customerId": "CUS-81921",
  "productId": "PROD-3321",
  "quantity": 2,
  "unitPrice": 249.90,
  "channel": "WEB",
  "location": "SAO_PAULO",
  "timestamp": "2026-09-29T18:14:32Z"
}
```

O contrato deverá evoluir de forma controlada para evitar breaking changes entre produtores e consumidores.

---

## ⚡ Streaming Analytics

O **Spark Structured Streaming** deverá consumir eventos Kafka e calcular métricas em janelas de tempo.

Exemplos:

- vendas por minuto;
- receita por canal;
- ticket médio;
- top produtos;
- conversão;
- estoque crítico;
- quantidade de eventos por segundo;
- anomalias operacionais.

Fluxo conceitual:

```text
Kafka
  │
  ▼
Spark Structured Streaming
  │
  ├── agregações
  ├── janelas de tempo
  ├── filtros
  ├── enriquecimento
  └── métricas
  │
  ▼
Analytics / Serving Layer
```

---

## 🏞️ Data Lakehouse

O armazenamento analítico seguirá o padrão **Medallion Architecture**.

### Bronze

Dados brutos e imutáveis.

Objetivos:

- preservar eventos originais;
- permitir reprocessamento;
- manter rastreabilidade.

### Silver

Dados tratados e padronizados.

Processamentos:

- remoção de duplicidades;
- normalização;
- validação;
- tratamento de campos inválidos;
- enriquecimento.

### Gold

Dados preparados para consumo analítico.

Exemplos:

```text
sales_daily
sales_by_region
customer_360
product_performance
inventory_risk
sales_forecast
```

Tecnologias planejadas:

- MinIO / AWS S3;
- Apache Iceberg;
- Parquet;
- PySpark.

---

## 🔁 Orquestração — Apache Airflow

O Airflow será responsável pela execução e observabilidade dos pipelines batch.

Exemplo de DAG:

```text
ingest
  │
  ▼
validate
  │
  ▼
bronze_to_silver
  │
  ▼
silver_to_gold
  │
  ▼
data_quality
  │
  ├──► train_model
  │
  └──► publish_metrics
```

Aspectos planejados:

- retries;
- dependências;
- scheduling;
- parametrização;
- SLA de tarefas;
- logs;
- alertas.

---

## 🧹 Data Quality e Veracidade

A camada de qualidade deverá validar regras como:

- `eventId` obrigatório;
- timestamps válidos;
- valores monetários não negativos;
- quantidade maior que zero;
- IDs consistentes;
- eventos duplicados;
- campos obrigatórios por tipo de evento;
- integridade entre produto, pedido e estoque.

Métricas de qualidade previstas:

```text
completeness
uniqueness
validity
consistency
freshness
```

---

## 🤖 Machine Learning

Após a consolidação da plataforma de dados, a camada de ML deverá explorar casos de uso como:

### Previsão de demanda

Entradas possíveis:

- histórico de vendas;
- produto;
- preço;
- promoção;
- região;
- dia da semana;
- horário;
- sazonalidade;
- estoque.

Saída:

```text
Produto: Notebook X
Demanda prevista — próximas 24h: 183
Estoque atual: 97
Risco de ruptura: ALTO
```

### Detecção de anomalias

Aplicável a:

- picos inesperados de vendas;
- pagamentos fora do padrão;
- eventos duplicados;
- comportamento atípico de estoque.

O **MLflow** será utilizado para rastreamento de experimentos, métricas, artefatos e versionamento de modelos.

---

## 📊 Serving Layer e Dashboard

Os resultados analíticos serão disponibilizados por meio de:

- PostgreSQL;
- API analítica Spring Boot;
- dashboard React.

KPIs previstos:

| KPI | Objetivo |
|---|---|
| Receita | acompanhar faturamento |
| Pedidos | monitorar volume transacional |
| Ticket médio | medir valor médio das compras |
| Conversão | avaliar eficiência dos canais |
| Estoque crítico | antecipar indisponibilidade |
| Top produtos | identificar demanda |
| Abandono | analisar comportamento |
| Forecast | apoiar planejamento |

---

## 🔭 Observabilidade

A plataforma prevê observabilidade distribuída desde o início.

### Prometheus

Métricas de:

- aplicações;
- consumidores;
- latência;
- throughput;
- erros.

### Grafana

Dashboards para:

- APIs;
- Kafka;
- Spark;
- infraestrutura;
- indicadores técnicos.

### OpenTelemetry

Coleta de:

- traces;
- métricas;
- logs;
- correlação entre serviços.

---

## 🛡️ Segurança e governança

Itens previstos para evolução:

- autenticação e autorização;
- segregação de ambientes;
- secrets fora do código;
- proteção de informações sensíveis;
- mascaramento de dados;
- trilha de auditoria;
- retenção;
- lineage;
- princípio do menor privilégio.

Nenhum segredo real deverá ser versionado no repositório.

---

## 🧪 Estratégia de testes

O projeto deverá possuir testes em diferentes níveis:

```text
Unit Tests
Integration Tests
Repository Tests
API Tests
Kafka Integration Tests
Pipeline Tests
Data Quality Tests
End-to-End Tests
```

Ferramentas previstas:

- JUnit 5;
- Mockito;
- MockMvc;
- Testcontainers;
- pytest na camada Python.

---

## 🐳 Infraestrutura

A evolução da infraestrutura está planejada em duas etapas.

### Desenvolvimento local

```text
Docker
Docker Compose
PostgreSQL
Kafka
MinIO
Redis
```

### Evolução de plataforma

```text
Kubernetes
Prometheus
Grafana
OpenTelemetry
CI/CD
Cloud
```

---

## 🧰 Stack tecnológica

| Área | Tecnologias |
|---|---|
| Backend | Java 21, Spring Boot |
| API | REST, Validation, Problem Details |
| Streaming | Apache Kafka |
| Big Data | Apache Spark, Structured Streaming |
| Data Engineering | Python, PySpark, Airflow |
| Data Lake | MinIO / AWS S3 |
| Lakehouse | Apache Iceberg, Parquet |
| Banco relacional | PostgreSQL |
| Cache | Redis |
| Machine Learning | MLlib, MLflow |
| Frontend | React, Vite |
| Containers | Docker, Docker Compose |
| Orquestração | Kubernetes |
| Observabilidade | Prometheus, Grafana, OpenTelemetry |
| Testes | JUnit, Mockito, MockMvc, Testcontainers, pytest |

---

## 📁 Estrutura planejada do repositório

```text
smartretail-data-platform/
├── backend/
│   ├── ingestion-api/
│   └── analytics-api/
├── streaming/
│   └── spark-streaming/
├── data-engineering/
│   ├── airflow/
│   ├── pyspark/
│   └── data-quality/
├── ml/
│   ├── training/
│   └── experiments/
├── frontend/
├── infra/
│   ├── docker/
│   ├── kubernetes/
│   └── observability/
├── docs/
│   ├── assets/
│   ├── ARCHITECTURE.md
│   └── ROADMAP.md
├── .github/
├── .gitignore
├── LICENSE
└── README.md
```

A estrutura será criada conforme cada componente passar a existir; o README não assume como implementado o que ainda está no roadmap.

---

## 🚀 Roadmap

| Release | Entrega | Status |
|---|---|---|
| **v0.1** | Event Platform — Spring Boot + Kafka + PostgreSQL + Docker | 🚧 Em desenvolvimento |
| **v0.2** | Streaming Analytics — Spark Structured Streaming | ⏳ Planejada |
| **v0.3** | Data Lakehouse — MinIO/S3 + Bronze/Silver/Gold + Iceberg | ⏳ Planejada |
| **v0.4** | Data Engineering — Airflow + PySpark + Data Quality | ⏳ Planejada |
| **v0.5** | Analytics — API + Dashboard React | ⏳ Planejada |
| **v0.6** | AI — MLflow + previsão de demanda + anomalias | ⏳ Planejada |

Roadmap detalhado: [docs/ROADMAP.md](docs/ROADMAP.md)

Arquitetura detalhada: [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)

---

## 🎓 Competências demonstradas

Este projeto foi desenhado para evidenciar competências em:

- arquitetura orientada a eventos;
- desenvolvimento backend Java;
- desenho de APIs;
- processamento de dados em tempo real;
- sistemas distribuídos;
- engenharia de dados;
- modelagem de pipelines;
- Data Lake/Lakehouse;
- qualidade de dados;
- observabilidade;
- containers;
- machine learning aplicado;
- documentação técnica;
- evolução incremental de arquitetura.

---

## 📐 Princípios de engenharia

O desenvolvimento deverá seguir os seguintes princípios:

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

## ▶️ Execução

A execução completa via Docker Compose será disponibilizada durante a **v0.1**.

Enquanto a implementação inicial está em construção, este repositório contém a documentação de arquitetura e o roadmap que orientarão as entregas.

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
