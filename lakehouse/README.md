# SmartRetail Lakehouse — v0.3

A v0.3 adiciona uma arquitetura de **Data Lakehouse** ao SmartRetail Data Platform utilizando MinIO/S3, Apache Spark, PySpark e Parquet.

## Status

🚧 Em desenvolvimento

Implementado:

- MinIO compatível com S3;
- buckets Bronze, Silver, Gold e Warehouse;
- Kafka → Bronze;
- Bronze em Parquet;
- Bronze → Silver;
- parsing, tipagem, validação e normalização;
- deduplicação por `eventId`;
- Silver em Parquet;
- Silver → Gold;
- Data Products analíticos Gold;
- testes automatizados PySpark.

Próximo incremento:

- Apache Iceberg.

## Arquitetura

```text
Spring Boot
    │
    ▼
Apache Kafka
    │
    ▼
Spark Structured Streaming
    │
    ▼
Bronze
Raw Kafka Events
Parquet
    │
    ▼
Spark Silver
    │
    ├── parsing
    ├── typing
    ├── validation
    ├── normalization
    └── deduplication
    │
    ▼
Silver
Trusted Orders
Parquet
    │
    ▼
Spark Gold
    │
    ├── aggregations
    └── KPIs
    │
    ▼
Gold
Analytics Data Products
    │
    ▼
Apache Iceberg
🚧 Próximo incremento
```

## Buckets

| Camada | Bucket | Finalidade |
|---|---|---|
| Bronze | `smartretail-bronze` | eventos brutos, auditáveis e reprocessáveis |
| Silver | `smartretail-silver` | dados tipados, validados, normalizados e deduplicados |
| Gold | `smartretail-gold` | KPIs e Data Products analíticos |
| Warehouse | `smartretail-warehouse` | warehouse futuro do Apache Iceberg |

## Bronze

Origem:

```text
smartretail.orders.v1
```

Destino:

```text
s3a://smartretail-bronze/orders/
```

A Bronze preserva:

```text
payload
kafkaKey
kafkaTopic
kafkaPartition
kafkaOffset
kafkaTimestamp
ingestedAt
ingestionDate
```

Particionamento:

```text
ingestionDate=YYYY-MM-DD
```

## Silver

Origem:

```text
s3a://smartretail-bronze/orders/
```

Destino:

```text
s3a://smartretail-silver/orders/
```

Transformações:

```text
JSON
 ↓
Schema
 ↓
Typing
 ↓
Validation
 ↓
Normalization
 ↓
Revenue calculation
 ↓
Deduplication
 ↓
Silver
```

Qualidade aplicada:

- `eventId` obrigatório;
- `occurredAt` válido;
- `customerId` obrigatório;
- `productId` obrigatório;
- `quantity > 0`;
- `unitPrice >= 0`;
- normalização de `channel`;
- normalização de `location`;
- deduplicação por `eventId`.

Particionamento:

```text
eventDate=YYYY-MM-DD
```

## Gold

Origem:

```text
s3a://smartretail-silver/orders/
```

Datasets:

```text
s3a://smartretail-gold/orders-daily/
s3a://smartretail-gold/orders-summary/
```

### orders-daily

Granularidade:

```text
eventDate + channel + location
```

Métricas:

```text
totalOrders
totalItems
totalRevenue
averageOrderValue
```

### orders-summary

Métricas:

```text
totalOrders
totalItems
totalRevenue
averageOrderValue
uniqueCustomers
uniqueProducts
```

Execução validada:

```text
totalOrders       = 4
totalItems        = 12
totalRevenue      = 2218.80
averageOrderValue = 554.70
uniqueCustomers   = 3
uniqueProducts    = 3
```

A Gold é reconstruível a partir da Silver e utiliza escrita `overwrite` no estágio atual baseado em Parquet.

## Testes

Suíte PySpark validada:

```text
Streaming transforms: 3
Silver:               5
Gold:                 2
------------------------
Total:               10
```

Todos os testes concluíram com `OK`.

## MinIO local

Subir o MinIO:

```bash
docker compose up -d minio
```

Endpoints:

| Serviço | Endereço |
|---|---|
| S3 API | `http://localhost:9000` |
| MinIO Console | `http://localhost:9001` |

As credenciais locais de desenvolvimento estão documentadas no arquivo `.env.example`.

## Spark UI

| Processo | Endereço |
|---|---|
| Streaming / Bronze | `http://localhost:4040` |
| Silver | `http://localhost:4041` |

A Gold é um job batch e encerra após gerar os datasets.

## Próximo incremento — Apache Iceberg

A próxima evolução adicionará:

- Iceberg Catalog;
- warehouse no MinIO;
- tabelas Iceberg;
- schema evolution;
- snapshots;
- time travel;
- atomic commits.

## Princípio arquitetural

```text
Bronze = verdade bruta
Silver = verdade confiável
Gold   = informação para consumo
```

As camadas posteriores permanecem reconstruíveis a partir das anteriores.
