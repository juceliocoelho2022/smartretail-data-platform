# SmartRetail Lakehouse — v0.3

A v0.3 adiciona a camada de **Data Lakehouse** ao SmartRetail Data Platform.

## Status

🚧 Em desenvolvimento

Primeiro incremento:

- MinIO compatível com API S3
- console web do MinIO
- provisionamento automático de buckets
- separação Bronze / Silver / Gold
- bucket dedicado ao warehouse do Apache Iceberg

## Arquitetura

```text
Spring Boot
    |
    v
Apache Kafka
    |
    v
Spark Structured Streaming
    |
    +--------------------+
    |                    |
    v                    v
Real-time metrics     MinIO / S3
                         |
                         v
                    Bronze Layer
                         |
                         v
                    Silver Layer
                         |
                         v
                     Gold Layer
                         |
                         v
                 Apache Iceberg
```

## Buckets

| Camada | Bucket | Finalidade |
|---|---|---|
| Bronze | `smartretail-bronze` | eventos brutos e reprocessáveis |
| Silver | `smartretail-silver` | dados tratados, tipados e deduplicados |
| Gold | `smartretail-gold` | datasets e KPIs para consumo analítico |
| Warehouse | `smartretail-warehouse` | tabelas e metadados do Iceberg |

## MinIO local

Subir somente a infraestrutura do Lakehouse:

```bash
docker compose up -d minio minio-init
```

Verificar:

```bash
docker compose ps
```

### Endpoints

| Serviço | Endereço |
|---|---|
| S3 API | `http://localhost:9000` |
| MinIO Console | `http://localhost:9001` |

As credenciais locais de desenvolvimento estão documentadas no arquivo `.env.example`. Não devem ser substituídas por credenciais reais no repositório.

## Próximos incrementos da v0.3

1. conectar Spark ao endpoint S3 do MinIO;
2. persistir eventos Kafka na camada Bronze em Parquet;
3. introduzir Apache Iceberg;
4. criar transformações Bronze → Silver;
5. criar agregações Silver → Gold;
6. adicionar testes de leitura/escrita e idempotência;
7. adicionar CI específico do Lakehouse;
8. documentar reprocessamento e evolução de schema.

## Princípio

A camada Bronze preserva o evento original. As camadas posteriores derivam dados confiáveis sem eliminar a possibilidade de auditoria e reprocessamento.
