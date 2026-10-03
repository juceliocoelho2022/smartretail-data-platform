# SmartRetail Data Platform — Business Rules

Este documento define as invariantes de negócio e de confiabilidade que orientam a implementação atual do SmartRetail Data Platform.

## BR-001 — Idempotent ingestion

Uma mesma `Idempotency-Key` representa uma única operação lógica de entrada.

### Regra

- uma chave ainda não registrada pode criar um novo evento;
- o reenvio da mesma chave não deve criar um segundo evento lógico;
- o resultado original deve ser reutilizado conforme o contrato já implementado pela API;
- a resposta deve permitir identificar que a operação foi tratada como replay.

### Risco mitigado

Duplicação de operações causada por retries, timeouts ou reenvio do cliente.

### Evidência atual

A v0.1 implementa idempotência na API de ingestão e mantém testes para criação e replay da mesma chave.

---

## BR-002 — Reliable event publication

Um evento aceito pela camada transacional não deve ser perdido porque o banco confirmou a operação e a publicação no Kafka falhou em seguida.

### Regra

- a alteração transacional e o registro do evento de saída são persistidos na mesma transação local;
- a publicação no Kafka acontece a partir da Transactional Outbox após o commit;
- uma falha temporária de publicação não exige recriar a operação de negócio.

### Risco mitigado

O problema de dual write entre PostgreSQL e Kafka.

### Observação semântica

A solução **não reivindica exactly-once global**. O desenho combina persistência transacional local, publicação recuperável e idempotência nos limites necessários.

---

## BR-003 — Idempotent downstream consumption

Consumidores devem tolerar reentrega de mensagens sem duplicar o efeito de negócio ou a projeção resultante.

### Regra

- o processamento Kafka considera semântica compatível com entrega `at-least-once`;
- um evento já processado não deve produzir uma segunda projeção lógica;
- retry e reprocessamento devem preservar a segurança contra duplicação.

### Risco mitigado

Efeitos duplicados causados por redelivery, retry do consumer ou reprocessamento.

---

## BR-004 — Curated analytical data

Dados só podem ser tratados como confiáveis para consumo analítico quando satisfazem as invariantes da camada Silver.

### Invariantes atuais da Silver

- `eventId` obrigatório;
- `customerId` obrigatório;
- `productId` obrigatório;
- `quantity > 0`;
- `unitPrice >= 0`;
- normalização de campos de domínio;
- deduplicação por `eventId`.

### Regra

O Data Quality Gate valida essas invariantes antes dos jobs batch posteriores à Silver.

### Limite da evidência

O gate atual é uma **checagem das invariantes da Silver**. Ele não deve ser descrito como contador de rejeições da Bronze sem evidência específica desse comportamento.

---

## BR-005 — Reproducible analytical products

Produtos analíticos devem ser reconstruíveis a partir de dados confiáveis e versionados, sem depender de alterações manuais no estado de serving.

### Regra

- a Gold é derivada das camadas anteriores;
- o serving analítico é publicado a partir dos Data Products Gold;
- o estado analítico deve poder ser atualizado novamente executando o pipeline definido;
- tabelas e artefatos derivados não são a fonte primária do evento de negócio.

### Risco mitigado

Drift entre dados processados e dados expostos ao consumidor analítico.

---

## BR-006 — Operational traceability

A plataforma deve produzir evidências suficientes para diagnosticar falhas no fluxo de ingestão, publicação, processamento e qualidade de dados.

### Regra

A implementação deve expor, de acordo com cada componente:

- health checks;
- métricas técnicas;
- logs de execução;
- status das etapas do pipeline;
- evidências de Data Quality e validação pós-carga;
- identificadores que permitam correlacionar o processamento quando disponíveis no fluxo.

### Risco mitigado

Falhas silenciosas ou difíceis de localizar em um pipeline distribuído.

---

## Relação entre as regras

```text
BR-001  protege a entrada contra duplicação
   ↓
BR-002  protege a transição entre banco e broker
   ↓
BR-003  protege o consumo contra reentrega
   ↓
BR-004  protege a qualidade dos dados curados
   ↓
BR-005  protege a reprodutibilidade dos produtos analíticos
   ↓
BR-006  fornece evidência operacional sobre todo o fluxo
```

## Princípio de evolução

Uma nova tecnologia, serviço ou camada só deve ser incorporada ao case quando houver uma regra de negócio, um requisito não funcional ou um risco operacional que justifique seu custo e complexidade.
