# SmartRetail ML Platform — v0.6

The `ml/` module adds the machine-learning layer planned for SmartRetail v0.6.

The first increment provides a deterministic, Silver-compatible historical demand dataset used only for development, automated tests, demos, and model evaluation. It is synthetic data and must not be presented as real retail history.

## Initial dataset

Default acceptance configuration:

```text
startDate    = 2025-10-01
days         = 365
products     = 30
seed         = 20261002
output       = s3a://smartretail-silver/orders-ml-history
```

The generated rows use the curated order fields required by the product-demand Gold builder:

```text
eventId
occurredAt
customerId
productId
quantity
unitPrice
channel
location
revenue
eventDate
silverProcessedAt
```

The data contains product-specific demand, weekly seasonality, gradual trend, bounded deterministic noise, and controlled spikes in the final 28 days for later anomaly-detection validation.

## Local image

```bash
docker build -t smartretail-ml-jobs:0.6 ./ml
```

The Spark job image pins MLflow 3.16.1 and the PostgreSQL Python driver. S3A/Hadoop packages are supplied by the Spark submit command in Docker Compose/Airflow.

## Scope

The synthetic history is isolated from operational Silver at:

```text
s3a://smartretail-silver/orders-ml-history
```

Production ML data products continue to consume the normal curated Silver source through configurable paths.
