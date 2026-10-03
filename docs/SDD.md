# SmartRetail Data Platform — Software Design Document

## 1. Contexto de negócio

Uma operação de varejo omnichannel recebe eventos de pedidos originados por e-commerce, aplicativo, marketplace, PDV e integrações externas. Em sistemas distribuídos, retries, timeouts e reprocessamentos podem fazer a mesma operação chegar mais de uma vez.

O risco não é apenas técnico: um evento duplicado pode provocar processamento repetido, inconsistência de estoque, divergência analítica e, em cenários financeiros, cobrança duplicada.

O SmartRetail Data Platform foi projetado para receber, validar, persistir, distribuir e transformar eventos de varejo com rastreabilidade, idempotência e qualidade de dados.

## 2. Objetivos

- impedir que a mesma operação lógica seja criada duas vezes;
- desacoplar ingestão HTTP do processamento assíncrono;
- preservar eventos para processamento e reprocessamento confiáveis;
- produzir dados Bronze, Silver e Gold com regras explícitas de qualidade;
- disponibilizar indicadores analíticos sem acoplar o frontend ao banco transacional;
- permitir diagnóstico operacional por health checks, métricas e evidências de execução.

## 3. Atores

- **Canal de venda**: envia eventos de pedido.
- **Ingestion API**: valida e registra o evento recebido.
- **Outbox Publisher**: publica eventos confirmados no Kafka.
- **Kafka Consumers / Spark Streaming**: processam eventos de forma assíncrona.
- **Airflow**: orquestra etapas batch posteriores à Silver.
- **Analytics API**: serve dados analíticos processados.
- **Dashboard**: apresenta KPIs ao usuário de negócio.

## 4. Regras de negócio

### BR-001 — Idempotência de entrada

Uma mesma `idempotencyKey` representa uma única operação lógica.

- primeira requisição: cria o evento;
- requisição repetida com a mesma chave: não cria novo evento;
- o identificador original deve ser preservado e retornado.

### BR-002 — Evento de pedido válido

Um pedido aceito pelo pipeline deve possuir os identificadores obrigatórios e valores coerentes com o domínio.

Na camada Silver, as invariantes incluem:

- `eventId` obrigatório;
- `customerId` obrigatório;
- `productId` obrigatório;
- `quantity > 0`;
- `unitPrice >= 0`;
- deduplicação por `eventId`.

### BR-003 — Publicação confiável

Uma alteração confirmada no banco não pode depender de uma publicação Kafka executada de forma não transacional dentro da requisição HTTP.

A persistência de domínio e o registro da mensagem de saída devem ocorrer na mesma transação local por meio do Transactional Outbox Pattern.

### BR-004 — Processamento assíncrono idempotente

Como Kafka trabalha com entrega `at-least-once`, consumidores devem tolerar reentrega e impedir efeitos duplicados.

### BR-005 — Qualidade antes do consumo analítico

Dados que violam as invariantes da Silver não devem ser tratados como dados confiáveis para Gold, Iceberg ou serving analítico.

### BR-006 — Separação transacional e analítica

O frontend analítico não deve consultar diretamente tabelas transacionais ou arquivos do Lakehouse. O consumo ocorre por meio de um modelo de serving dedicado no schema `analytics` e de uma API específica.

## 5. Requisitos funcionais

- **FR-001** Receber eventos de pedidos por API REST.
- **FR-002** Detectar replay por `idempotencyKey`.
- **FR-003** Persistir evento e Outbox atomicamente.
- **FR-004** Publicar eventos no Kafka após confirmação da transação.
- **FR-005** Consumir eventos de forma idempotente.
- **FR-006** Persistir dados brutos na Bronze.
- **FR-007** Normalizar, validar e deduplicar dados na Silver.
- **FR-008** Gerar Data Products Gold.
- **FR-009** Executar Data Quality Gate antes das etapas analíticas posteriores.
- **FR-010** Publicar dados Gold no serving analítico.
- **FR-011** Expor KPIs por API REST.
- **FR-012** Exibir KPIs no dashboard React.

## 6. Requisitos não funcionais

- **NFR-001 — Resiliência:** falhas transitórias não devem causar perda silenciosa de eventos.
- **NFR-002 — Escalabilidade:** produtores e consumidores devem poder evoluir de forma desacoplada.
- **NFR-003 — Rastreabilidade:** o mesmo evento deve poder ser acompanhado entre ingestão, persistência, mensageria e processamento.
- **NFR-004 — Consistência:** a solução não deve depender de exactly-once global; efeitos duplicados são controlados por idempotência.
- **NFR-005 — Observabilidade:** serviços críticos devem disponibilizar health checks e métricas.
- **NFR-006 — Evolução de schema:** o Lakehouse deve suportar evolução controlada e histórico analítico.
- **NFR-007 — Reprodutibilidade:** a stack local deve ser inicializável por Docker Compose.

## 7. Fluxo principal

```text
Canal de venda
    |
    v
Ingestion API
    |
    +--> valida payload
    |
    +--> verifica idempotencyKey
    |
    v
PostgreSQL transaction
    |-- idempotency record
    `-- outbox event
            |
            v
      Outbox Publisher
            |
            v
          Kafka
            |
            v
 Spark Structured Streaming
            |
      Bronze -> Silver
                  |
           Data Quality Gate
                  |
                  v
                 Gold
              /       \
             v         v
         Iceberg   Analytics Export
                        |
                        v
                  PostgreSQL analytics
                        |
                        v
                  Analytics API
                        |
                        v
                  React Dashboard
```

## 8. Cenários de aceitação

### AC-001 — Primeira ingestão

**Given** uma `idempotencyKey` ainda não registrada  
**When** o pedido é recebido  
**Then** um novo evento é persistido  
**And** a Outbox é criada  
**And** a resposta informa que não houve replay.

### AC-002 — Reenvio da mesma operação

**Given** uma `idempotencyKey` já processada  
**When** a mesma operação é reenviada  
**Then** nenhum segundo evento de negócio é criado  
**And** o identificador original é preservado  
**And** a resposta informa replay.

### AC-003 — Falha entre banco e broker

**Given** que a transação de banco foi confirmada  
**And** Kafka está temporariamente indisponível  
**When** o publisher tenta publicar o evento  
**Then** o registro permanece na Outbox  
**And** pode ser reenviado posteriormente sem recriar a operação de negócio.

### AC-004 — Data Quality

**Given** uma Silver disponível  
**When** o Data Quality Gate é executado  
**Then** nulos em campos obrigatórios, quantidades inválidas, preços inválidos e duplicidades são verificados antes da publicação analítica.

## 9. Decisões arquiteturais

### Kafka

Escolhido porque o processamento precisa ser desacoplado da requisição HTTP e diferentes consumidores podem evoluir e escalar independentemente. Retenção, replay, particionamento e consumer groups também são relevantes para o domínio de eventos.

### Transactional Outbox

Evita o dual-write ingênuo `database + broker`. A API confirma a transação local primeiro; a publicação é realizada a partir da Outbox.

### Spark Structured Streaming

Usado para ingestão contínua e transformação de eventos em camadas Bronze e Silver, mantendo checkpoints e processamento incremental.

### Airflow

Usado apenas nas etapas batch finitas posteriores à Silver, evitando misturar orquestração batch com o fluxo contínuo de streaming.

### Medallion Architecture

Bronze preserva o dado recebido, Silver aplica regras de confiabilidade e Gold materializa Data Products orientados ao consumo analítico.

### Apache Iceberg

Adiciona snapshots, Time Travel e Schema Evolution à camada analítica versionada.

### Serving analítico separado

Evita que clientes de leitura dependam de arquivos do Lakehouse ou do modelo transacional da ingestão.

## 10. Métricas de negócio e engenharia

Métricas que ajudam a demonstrar o comportamento do sistema:

- eventos recebidos;
- replays bloqueados por idempotência;
- eventos publicados no Kafka;
- falhas de publicação;
- eventos enviados para DLT;
- registros aprovados/reprovados por Data Quality;
- tempo de processamento ponta a ponta;
- pedidos, itens, receita e ticket médio publicados na Gold;
- defasagem entre processamento Gold e atualização do serving analítico.

## 11. Princípio de evolução

Cada nova versão deve responder a uma pergunta objetiva:

> Qual problema de negócio ou risco operacional esta mudança resolve?

Tecnologias são adicionadas somente quando existe uma necessidade arquitetural, de domínio ou operacional que justifique seu custo.