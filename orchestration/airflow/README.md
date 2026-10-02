# SmartRetail Orchestration — v0.4

A v0.4 adiciona **Apache Airflow**, **Data Quality** e validação pós-carga ao SmartRetail Data Platform.

## Status

Implementação concluída e validada localmente de ponta a ponta.

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

O DAG foi executado pelo Airflow com todos os passos concluídos em `success`.

## Componentes

- Apache Airflow 3.3.2;
- DockerOperator;
- Apache Spark 4.0.1;
- PySpark;
- MinIO compatível com S3;
- Hadoop S3A;
- Apache Iceberg;
- PostgreSQL JDBC Catalog;
- Docker Compose;
- GitHub Actions.

## DAGs

### `smartretail_airflow_smoke`

Valida o runtime local do Airflow, incluindo carregamento do DAG e execução de uma task simples.

### `smartretail_lakehouse_pipeline`

Orquestra os jobs batch da v0.4:

```text
Silver já materializada
        ↓
Data Quality Gate
        ↓
Gold
        ↓
Iceberg Refresh
        ↓
Post-load Validation
```

A ingestão Kafka/Bronze e a Silver permanecem como pipelines contínuos de Structured Streaming. O Airflow não inicia nem aguarda esses processos long-running nesta release.

## Data Quality Gate

Job:

```text
streaming/spark-streaming/src/main/python/data_quality_app.py
```

Regras:

```text
eventId obrigatório
customerId obrigatório
productId obrigatório
quantity > 0
unitPrice >= 0
eventId sem duplicidade
```

Uma falha encerra o job com erro e impede a execução das etapas downstream.

Execução validada:

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

## Gold

A task `build_gold` reconstrói os Data Products analíticos a partir da Silver.

Resultado validado:

```text
totalOrders=5
totalItems=14
totalRevenue=2918.60
averageOrderValue=583.72
uniqueCustomers=4
uniqueProducts=4
```

## Iceberg

A task `refresh_iceberg` materializa a Silver na tabela `smartretail.lakehouse.orders`, usando warehouse `s3a://smartretail-warehouse/iceberg` e JDBC Catalog no PostgreSQL.

A estratégia atual continua usando `INSERT OVERWRITE`. `MERGE INTO` incremental não faz parte da v0.4.

## Post-load Validation

Job:

```text
streaming/spark-streaming/src/main/python/post_load_validation.py
```

Resultado E2E:

```text
Post-load validation: PASSED
Silver rows=5
Iceberg rows=5
Gold totalOrders=5
```

## Imagem Spark para jobs

A imagem `smartretail-spark-jobs:0.4`, definida em `streaming/spark-streaming/Dockerfile.jobs`, empacota o código Python usado pelos jobs disparados pelo DockerOperator.

## Cache Ivy

As dependências Maven do Spark usam o volume `smartretail-spark-ivy`. O serviço `spark-ivy-init` cria e prepara `/ivy/cache` e `/ivy/jars`, evitando downloads repetidos.

## Rede Docker

Todos os serviços da v0.4 usam `smartretail-network`, permitindo que containers criados pelo DockerOperator resolvam `minio:9000` e `postgres:5432`.

## Execução local

```powershell
docker compose up -d minio postgres airflow
docker exec smartretail-airflow airflow dags list
docker exec smartretail-airflow airflow dags trigger smartretail_lakehouse_pipeline
docker exec smartretail-airflow airflow dags list-runs smartretail_lakehouse_pipeline
```

## Validação E2E registrada

```text
silver_data_quality    success
build_gold             success
refresh_iceberg        success
post_load_validation   success
DagRun                 success
```

O DagRun completo terminou em aproximadamente 206,5 segundos no ambiente local de desenvolvimento.

## Limites conscientes

- Bronze e Silver continuam sendo Structured Streaming de longa duração e não são gerenciadas pelo DAG batch.
- O refresh Iceberg usa `INSERT OVERWRITE`, não `MERGE INTO`.
- O ambiente Airflow usa configuração local de desenvolvimento.
- Alertas, SLAs e políticas avançadas de retry permanecem como evoluções futuras.
