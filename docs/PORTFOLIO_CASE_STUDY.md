# SmartRetail Data Platform — Portfolio Case Study

Este documento resume o projeto em formato útil para entrevistas, revisão técnica e apresentação de portfólio.

> Problema: [BUSINESS_PROBLEM.md](BUSINESS_PROBLEM.md)  
> Regras: [BUSINESS_RULES.md](BUSINESS_RULES.md)  
> Requisitos: [REQUIREMENTS.md](REQUIREMENTS.md)  
> Arquitetura: [ARCHITECTURE.md](ARCHITECTURE.md)

## 1. Explicação de 60 segundos

O **SmartRetail Data Platform** é um case de Backend Java + Engenharia de Dados orientado a um problema comum em sistemas distribuídos de varejo: a mesma operação pode chegar mais de uma vez por retries, timeouts ou reprocessamentos.

A principal regra é que a mesma `Idempotency-Key` não pode criar dois eventos lógicos distintos. A Ingestion API valida e persiste a operação em PostgreSQL junto com uma Transactional Outbox. Depois do commit, o evento é publicado no Kafka e processado de forma assíncrona.

Kafka foi escolhido para desacoplar produtores e consumidores, permitir replay e suportar processamento independente. A Outbox resolve o risco de dual write entre banco e broker. Spark Structured Streaming mantém o fluxo contínuo até Bronze e Silver; Airflow entra apenas depois da Silver para orquestrar Data Quality, Gold, Iceberg, validação e publicação analítica.

O resultado é um pipeline em que duplicação, publicação, qualidade e serving têm limites explícitos e evidências verificáveis no repositório — sem reivindicar exactly-once global nem números de escala não medidos.

## 2. Explicação técnica de 3–5 minutos

### 2.1 Problema

Em uma operação omnichannel, eventos podem ser enviados por web, mobile, PDV ou integrações externas. Em um sistema distribuído, retries e falhas parciais tornam a duplicação uma possibilidade real.

O objetivo do projeto não é apenas "consumir eventos", mas responder a quatro perguntas:

1. Como impedir que a mesma operação seja criada duas vezes?
2. Como evitar perder um evento se o banco confirmar e o broker estiver indisponível?
3. Como processar reentregas sem duplicar efeitos downstream?
4. Como impedir que dados inconsistentes avancem para consumo analítico?

### 2.2 Entrada idempotente

A API recebe pedidos e aplica `Idempotency-Key`.

```text
Request
  ↓
Validation
  ↓
Idempotency check
  ↓
PostgreSQL transaction
```

Se a chave já existir, o sistema não cria outra operação lógica. Isso implementa **BR-001**.

### 2.3 Publicação confiável

Publicar diretamente no Kafka e depois gravar no banco — ou o contrário — criaria um dual write frágil.

Por isso o projeto usa Transactional Outbox:

```text
PostgreSQL transaction
├── idempotency/business state
└── outbox_event
        ↓ commit
Outbox Publisher
        ↓
Kafka
```

A transação local protege banco + intenção de publicação. O publisher pode tentar novamente se o broker falhar.

Trade-off: a Outbox adiciona tabela, publisher, retry e observabilidade operacional.

### 2.4 Kafka e processamento assíncrono

Kafka desacopla a ingestão dos consumidores e permite diferentes fluxos processarem o mesmo stream de forma independente.

A escolha é adequada ao case porque o projeto precisa de:

- processamento assíncrono;
- retenção e replay;
- consumers independentes;
- integração com Spark Structured Streaming.

Trade-off: Kafka aumenta a complexidade operacional e não elimina duplicação por si só.

### 2.5 Idempotência no consumo

O projeto considera entrega compatível com `at-least-once`.

Isso significa que o mesmo evento pode ser entregue novamente. Por isso, o consumer precisa reconhecer um evento já processado e evitar duplicar a projeção.

O projeto não afirma exactly-once global.

### 2.6 Streaming e Lakehouse

O fluxo contínuo é:

```text
Kafka
  ↓
Spark Structured Streaming
  ↓
Bronze
  ↓
Silver
```

Bronze preserva o dado de entrada e metadados. Silver aplica tipagem, normalização, regras de validade e deduplicação.

### 2.7 Data Quality e Airflow

Airflow não é usado como engine de streaming.

Ele entra somente depois da Silver, coordenando jobs finitos:

```text
silver_data_quality
        ↓
build_gold
        ↓
refresh_iceberg
        ↓
post_load_validation
        ↓
publish_analytics
```

O Data Quality Gate valida invariantes da Silver. Ele não é apresentado como contador de rejeições da Bronze.

### 2.8 Serving analítico

A UI não consulta diretamente Lakehouse nem tabelas transacionais.

```text
Gold
 ↓
Analytics Export
 ↓
PostgreSQL analytics
 ↓
Spring Boot Analytics API
 ↓
React Dashboard
```

Isso cria um read model próprio para consumo analítico.

Trade-off: há duplicação controlada de dados entre Gold e serving, e a freshness depende da publicação analítica.

## 3. Principais decisões e trade-offs

| Decisão | Problema resolvido | Trade-off |
|---|---|---|
| `Idempotency-Key` | retry/reenvio criando duas operações | armazenamento e controle adicional de chaves |
| Transactional Outbox | dual write PostgreSQL + Kafka | publisher e ciclo operacional adicional |
| Kafka | acoplamento síncrono e necessidade de replay/consumers independentes | complexidade operacional e semântica de reentrega |
| Spark Structured Streaming | processamento contínuo de eventos | estado/checkpoints e operação distribuída |
| Bronze/Silver/Gold | mistura entre dado bruto, confiável e analítico | mais camadas e armazenamento |
| Airflow pós-Silver | dependências entre jobs batch analíticos | segundo modelo de execução além do streaming |
| PostgreSQL `analytics` + API | frontend acoplado a lake/transacional | duplicação controlada do read model |

## 4. Perguntas de entrevista

### Por que Kafka?

Porque a ingestão não precisa esperar o processamento de todos os consumidores, e diferentes consumidores podem evoluir de forma independente. Kafka também permite retenção/replay e integra com o fluxo de Spark Structured Streaming.

**Trade-off:** mais operação, configuração de tópicos/partições e necessidade de idempotência no consumo.

### Por que Transactional Outbox?

Para tratar o risco de dual write. Sem Outbox, o banco poderia confirmar a operação e a publicação Kafka falhar logo depois.

Com Outbox, a operação e a intenção de publicação ficam na mesma transação PostgreSQL; a publicação ocorre depois e pode ser repetida.

### Por que não exactly-once?

Porque "exactly-once" ponta a ponta é uma afirmação muito mais forte do que a infraestrutura isoladamente garante.

O projeto assume entrega compatível com `at-least-once` e torna os limites idempotentes. Isso é mais explícito sobre o comportamento real em retries e redelivery.

### Como a duplicação é tratada?

Em dois limites:

1. **HTTP** — `Idempotency-Key` impede que o mesmo pedido gere duas operações lógicas;
2. **Kafka consumer** — o processamento precisa tolerar redelivery e não duplicar a projeção resultante.

### Por que Airflow começa depois da Silver?

Porque Kafka → Bronze → Silver é um fluxo contínuo, responsabilidade do Spark Structured Streaming. Airflow coordena tarefas batch finitas como Data Quality, Gold, Iceberg, validação e export analítico.

### O que o Data Quality Gate realmente valida?

Invariantes da Silver já curada, como campos obrigatórios, quantidade, preço e duplicidades. O case não diz que esse gate mede rejeições da Bronze.

### Por que não consultar o Lakehouse direto no dashboard?

Porque isso acoplaria a UI ao storage, aos formatos físicos e à estrutura da engenharia de dados. O serving model cria uma API e um contrato de leitura próprios.

### O que mudaria para produção em escala real?

Eu começaria medindo, não adicionando tecnologias por hipótese. Os próximos passos dependeriam de evidência e poderiam incluir:

- testes de carga e definição de SLOs;
- estratégia de particionamento Kafka baseada em volume e ordering keys;
- observabilidade de consumer lag e backlog de Outbox;
- estratégia de CDC para Outbox, se necessária;
- segurança/RBAC e gestão de secrets;
- infraestrutura cloud e autoscaling;
- engine analítico dedicado caso PostgreSQL deixe de atender ao serving;
- estratégia incremental do Iceberg em vez de `INSERT OVERWRITE`, se o volume justificar.

Nenhum desses itens é apresentado como já implementado sem evidência correspondente.

## 5. Evidências verificáveis

A documentação principal registra evidências concretas do ambiente de demonstração:

- idempotência e replay na v0.1;
- Transactional Outbox;
- retry/DLT e consumer idempotente;
- Kafka → Spark Structured Streaming;
- Bronze/Silver/Gold;
- Silver Data Quality Gate;
- post-load validation;
- snapshots/Time Travel/Schema Evolution com Iceberg;
- DAG Airflow com tasks em `success`;
- exportação dos Data Products Gold para PostgreSQL `analytics`;
- Analytics API servindo o estado recém-publicado;
- dashboard React consumindo a API.

Os números exibidos nessas validações são **dados de amostra do pipeline**, não benchmarks de throughput, disponibilidade ou latência.

## 6. Referências do case

- [README — visão principal do projeto](../README.md)
- [Business Problem](BUSINESS_PROBLEM.md)
- [Business Rules](BUSINESS_RULES.md)
- [Requirements](REQUIREMENTS.md)
- [Architecture](ARCHITECTURE.md)
- [API Examples](API_EXAMPLES.md)
- [ADR-001 — Kafka](adr/ADR-001-kafka-event-backbone.md)
- [ADR-002 — Transactional Outbox](adr/ADR-002-transactional-outbox.md)
- [ADR-003 — Medallion + Airflow](adr/ADR-003-medallion-and-airflow-boundary.md)
- [ADR-004 — Analytics Serving](adr/ADR-004-analytics-serving-model.md)

## 7. Frase de fechamento para entrevista

> O objetivo do SmartRetail não foi juntar Java, Kafka, Spark e Airflow. Cada componente foi introduzido para resolver uma falha ou responsabilidade específica: duplicação, dual write, processamento assíncrono, qualidade, reprocessamento ou serving. O valor do case está na relação entre regra de negócio, decisão arquitetural e evidência de implementação.
