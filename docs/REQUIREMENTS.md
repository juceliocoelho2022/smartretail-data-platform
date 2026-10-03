# SmartRetail Data Platform — Requirements

Os requisitos abaixo traduzem o problema e as regras de negócio do SmartRetail em comportamentos verificáveis. O documento descreve apenas capacidades sustentadas pela implementação atual ou pela documentação desta evolução de portfólio.

## Convenções

- **Implemented** — existe evidência atual no repositório ou foi entregue por esta evolução documental;
- **Planned** — pertence ao roadmap e não deve ser apresentado como capacidade atual.

## Functional Requirements

| ID | Requisito | Status | Regras relacionadas |
|---|---|---|---|
| **FR-001** | Receber eventos de pedidos pela Ingestion API. | Implemented | BR-001, BR-006 |
| **FR-002** | Validar o payload recebido antes de aceitar o evento. | Implemented | BR-001, BR-004 |
| **FR-003** | Aplicar idempotência na ingestão usando `Idempotency-Key`. | Implemented | BR-001 |
| **FR-004** | Persistir o estado transacional e o registro da Outbox atomicamente na mesma transação local. | Implemented | BR-002 |
| **FR-005** | Publicar eventos aceitos no Kafka de forma assíncrona por meio da Transactional Outbox. | Implemented | BR-002, BR-006 |
| **FR-006** | Consumir eventos Kafka de forma idempotente, tolerando reentrega. | Implemented | BR-003 |
| **FR-007** | Processar eventos pelas camadas Bronze, Silver e Gold, mantendo responsabilidades distintas entre dados brutos, curados e analíticos. | Implemented | BR-004, BR-005 |
| **FR-008** | Executar Data Quality Gate sobre a Silver antes dos jobs batch analíticos posteriores. | Implemented | BR-004, BR-006 |
| **FR-009** | Publicar Data Products Gold em um modelo de serving analítico dedicado. | Implemented | BR-005 |
| **FR-010** | Expor os dados analíticos pelo Analytics API e pelo dashboard. | Implemented | BR-005, BR-006 |

## Non-Functional Requirements

| ID | Requisito | Status | Regras relacionadas |
|---|---|---|---|
| **NFR-001** | A conclusão da ingestão não deve depender da finalização do processamento analítico downstream. | Implemented | BR-002, BR-003 |
| **NFR-002** | Falhas temporárias de broker ou processamento não devem resultar em perda silenciosa de um evento já aceito pela camada transacional. | Implemented | BR-002, BR-006 |
| **NFR-003** | Reentrega deve ser segura tanto no limite HTTP quanto no limite de consumo Kafka. | Implemented | BR-001, BR-003 |
| **NFR-004** | Etapas batch e verificações de qualidade devem ser observáveis e diagnosticáveis de forma independente. | Implemented | BR-004, BR-006 |
| **NFR-005** | Evoluções de schema e dados devem ser explícitas e versionadas nos mecanismos apropriados da plataforma. | Implemented | BR-005, BR-006 |
| **NFR-006** | Decisões arquiteturais relevantes e seus trade-offs devem estar documentados e vinculados ao problema que resolvem. | Implemented | BR-001, BR-002, BR-003, BR-004, BR-005, BR-006 |

## Traceability Matrix

| Regra | Requisitos principais | Evidência / decisão associada |
|---|---|---|
| **BR-001** | FR-001, FR-002, FR-003, NFR-003 | Ingestion API, idempotência e testes de replay |
| **BR-002** | FR-004, FR-005, NFR-001, NFR-002 | PostgreSQL transaction + Transactional Outbox + Kafka |
| **BR-003** | FR-006, NFR-003 | Consumer idempotente, retry e DLT |
| **BR-004** | FR-002, FR-007, FR-008, NFR-004 | Silver invariants + Data Quality Gate |
| **BR-005** | FR-007, FR-009, FR-010, NFR-005 | Gold, Iceberg e modelo de serving analítico |
| **BR-006** | FR-001, FR-005, FR-008, FR-010, NFR-002, NFR-004, NFR-005 | Actuator/Micrometer, logs de execução, Airflow e validações E2E |

## Current vs. Planned Scope

### Current implementation

A documentação atual considera como entregues as capacidades das releases v0.1 a v0.5 descritas no README: Event Platform, streaming, Lakehouse, Airflow/Data Quality, Analytics API e dashboard.

### Roadmap / future

Os itens abaixo não são requisitos desta evolução e não devem aparecer como capacidades atuais sem implementação e evidência próprias:

- novos serviços de domínio como inventory ou billing;
- expansão de autenticação/autorização;
- deployment cloud de produção;
- Kubernetes/EKS;
- ML operacionalizado;
- metas formais de throughput, latência, disponibilidade ou SLO.

## Restrições de interpretação

- Kafka não implica garantia de exactly-once de negócio ponta a ponta;
- o Data Quality Gate atual valida invariantes da Silver e não é descrito como contador de rejeições da Bronze;
- Airflow orquestra os jobs batch finitos posteriores à Silver e não substitui o Spark Structured Streaming;
- números presentes nas validações do README são amostras de execução do pipeline, não benchmarks de escala.
