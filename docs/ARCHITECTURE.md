# Arquitetura — SmartRetail Data Platform

> Documento técnico da arquitetura alvo. Os componentes serão implementados progressivamente conforme o roadmap.

<p align="center">
  <img src="assets/smartretail-data-platform-architecture.svg" alt="Arquitetura SmartRetail Data Platform" width="100%">
</p>

## 1. Objetivo arquitetural

Construir uma plataforma orientada a eventos capaz de:

- receber dados de múltiplas fontes;
- processar eventos em baixa latência;
- manter histórico confiável em Data Lake/Lakehouse;
- executar pipelines batch e streaming;
- disponibilizar dados analíticos;
- evoluir para casos de Machine Learning;
- oferecer observabilidade ponta a ponta.

## 2. Princípios

1. **Event-driven by design**
2. **Idempotência**
3. **Contratos versionados**
4. **Separação entre workloads transacionais e analíticos**
5. **Data Quality antes de Machine Learning**
6. **Observabilidade by design**
7. **Infraestrutura reproduzível**
8. **Automação de testes**
9. **Evolução incremental**
10. **Segurança e governança como requisitos transversais**

## 3. Camadas

### 3.1 Producers

Fontes previstas:

- Web;
- Mobile;
- PDV;
- APIs externas;
- IoT;
- logs de aplicações;
- simuladores de carga.

### 3.2 Ingestion Layer

Tecnologia: **Java 21 + Spring Boot**.

Responsabilidades:

- validar entrada;
- normalizar payload;
- gerar metadados;
- aplicar idempotência;
- publicar eventos;
- expor health checks;
- gerar métricas e traces.

### 3.3 Event Backbone

Tecnologia: **Apache Kafka**.

Responsabilidades:

- desacoplar produtores e consumidores;
- suportar processamento assíncrono;
- particionar carga;
- permitir replay;
- organizar domínios em tópicos.

Tópicos iniciais:

```text
customer-events
product-views
cart-events
orders
payments
inventory-events
shipping-events
```

### 3.4 Stream Processing

Tecnologia: **Apache Spark Structured Streaming**.

Casos previstos:

- agregações por janela;
- enriquecimento;
- cálculo de KPIs;
- detecção de eventos fora do padrão;
- atualização de visões em tempo real.

### 3.5 Lakehouse

Tecnologias previstas:

- MinIO / AWS S3;
- Apache Iceberg;
- Parquet.

Camadas:

```text
Bronze -> Silver -> Gold
```

**Bronze:** preservação do dado original.  
**Silver:** padronização, limpeza e enriquecimento.  
**Gold:** dados analíticos e orientados ao negócio.

### 3.6 Batch Processing

Tecnologia: **PySpark**.

Responsabilidades:

- transformação;
- deduplicação;
- enriquecimento;
- joins;
- regras de qualidade;
- geração de datasets Gold.

### 3.7 Orchestration

Tecnologia: **Apache Airflow**.

Responsabilidades:

- scheduling;
- dependências;
- retry;
- SLA;
- logs;
- execução de DAGs;
- monitoramento de pipelines.

### 3.8 Machine Learning

Tecnologias previstas:

- MLlib;
- MLflow.

Casos iniciais:

- previsão de demanda;
- risco de ruptura;
- detecção de anomalias.

### 3.9 Serving Layer

Tecnologias:

- PostgreSQL;
- Spring Boot Analytics API;
- React.

Objetivo: disponibilizar dados consolidados sem expor diretamente o Lakehouse ao frontend.

## 4. Fluxos

### 4.1 Fluxo streaming

```text
Producer
   -> Spring Boot
   -> Kafka
   -> Spark Structured Streaming
   -> Real-time analytics
   -> Serving Layer
```

### 4.2 Fluxo lakehouse

```text
Kafka / arquivos / APIs
   -> Bronze
   -> Silver
   -> Gold
   -> PostgreSQL / Analytics API
```

### 4.3 Fluxo de ML

```text
Gold datasets
   -> Feature preparation
   -> Training
   -> MLflow
   -> Model version
   -> Prediction
   -> Analytics
```

## 5. Contratos de eventos

Todo evento deverá conter metadados mínimos:

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

Regras:

- `eventId` único;
- timestamps em UTC;
- versionamento explícito;
- correlation ID para rastreabilidade;
- payload específico por domínio;
- evolução compatível sempre que possível.

## 6. Resiliência

Padrões previstos:

- retry controlado;
- exponential backoff;
- Dead Letter Topic;
- idempotency key;
- timeouts;
- circuit breaker quando aplicável;
- replay seguro;
- consumer groups;
- tratamento de poison messages.

## 7. Data Quality

Dimensões monitoradas:

- completeness;
- uniqueness;
- validity;
- consistency;
- freshness.

Falhas de qualidade deverão gerar métricas, logs e possibilidade de quarentena.

## 8. Observabilidade

### Métricas
Prometheus.

### Dashboards
Grafana.

### Tracing
OpenTelemetry.

### Correlação
`traceId`, `correlationId` e `eventId`.

Indicadores planejados:

- throughput;
- consumer lag;
- erro por serviço;
- latência;
- duração de pipeline;
- registros rejeitados;
- freshness;
- disponibilidade.

## 9. Segurança

Requisitos previstos:

- secrets fora do repositório;
- autenticação e autorização;
- principle of least privilege;
- mascaramento de dados sensíveis;
- auditoria;
- segregação de ambientes;
- proteção de endpoints administrativos.

## 10. Não funcionais

Metas técnicas serão definidas conforme os componentes forem implementados.

Categorias:

- disponibilidade;
- latência;
- throughput;
- escalabilidade;
- recoverability;
- observabilidade;
- segurança;
- manutenibilidade.

## 11. Estratégia de testes

```text
Unit
Integration
Contract
Kafka
Data Quality
Pipeline
End-to-End
```

Ferramentas previstas:

- JUnit 5;
- Mockito;
- MockMvc;
- Testcontainers;
- pytest.

## 12. Evolução

A arquitetura será implementada em seis releases:

1. Event Platform
2. Streaming Analytics
3. Data Lakehouse
4. Data Engineering
5. Analytics
6. AI

Consulte [ROADMAP.md](ROADMAP.md) para o plano detalhado.
