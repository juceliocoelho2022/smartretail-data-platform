# SmartRetail Orchestration — v0.4

A v0.4 adiciona **Apache Airflow** e **Data Quality** ao SmartRetail Data Platform.

## Primeiro incremento

Este incremento estabelece:

- Apache Airflow 3.3.2;
- imagem Docker versionada;
- Airflow standalone para desenvolvimento local;
- DAG de smoke test;
- Data Quality gate PySpark para a camada Silver;
- regras automatizadas para nulidade, valores inválidos e duplicidade.

## Data Quality

O job:

```text
streaming/spark-streaming/src/main/python/data_quality_app.py
```

valida:

```text
eventId obrigatório
customerId obrigatório
productId obrigatório
quantity > 0
unitPrice >= 0
eventId sem duplicidade
```

Quando uma regra falha, o job encerra com erro e pode bloquear as etapas analíticas posteriores.

## Airflow

A interface local será exposta em:

```text
http://localhost:8081
```

O primeiro DAG é:

```text
smartretail_airflow_smoke
```

Ele existe apenas para validar scheduler, Dag Processor, API/UI e execução de tasks no ambiente local.

## Próximo incremento

A próxima etapa conectará a orquestração real:

```text
Silver
  ↓
Data Quality Gate
  ↓
Gold
  ↓
Iceberg Refresh
  ↓
Post-load Validation
```

A DAG de produção só será marcada como implementada após os jobs Spark serem executados de ponta a ponta pelo Airflow.
