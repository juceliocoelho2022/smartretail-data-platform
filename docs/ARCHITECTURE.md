# Arquitetura — SmartRetail Data Platform

## Visão geral

A plataforma será construída de forma incremental, com arquitetura orientada a eventos e pipelines de dados.

### 1. Data Producers
Fontes como Web, Mobile, PDV, APIs, logs e simuladores geram eventos de negócio.

### 2. Ingestion Layer
O backend em Java 21 + Spring Boot valida e publica eventos no Apache Kafka.

### 3. Event Streaming
Kafka desacopla produtores e consumidores e suporta processamento assíncrono em escala.

Tópicos planejados:
- customer-events
- product-views
- cart-events
- orders
- payments
- inventory-events
- shipping-events

### 4. Stream Processing
Spark Structured Streaming processa eventos em tempo real e calcula métricas operacionais.

### 5. Data Lakehouse
MinIO/S3 recebe dados crus e processados em camadas:
- Bronze: dados brutos;
- Silver: dados limpos e padronizados;
- Gold: dados preparados para consumo.

Apache Iceberg fornece camada de tabelas analíticas e evolução de schema.

### 6. Orchestration & Data Quality
Apache Airflow orquestra jobs. PySpark executa transformações, validações, deduplicação e enriquecimento.

### 7. Machine Learning
MLflow acompanha experimentos e modelos para:
- previsão de demanda;
- detecção de anomalias;
- risco de ruptura de estoque.

### 8. Serving Layer
PostgreSQL e APIs analíticas disponibilizam dados para o dashboard React.

### 9. Observabilidade
Prometheus, Grafana e OpenTelemetry fornecem métricas, traces e monitoramento da plataforma.
