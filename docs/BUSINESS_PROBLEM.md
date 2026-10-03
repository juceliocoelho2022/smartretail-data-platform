# SmartRetail Data Platform — Business Problem

## 1. Contexto

O SmartRetail Data Platform é uma **simulação de portfólio** de uma plataforma de dados para varejo omnichannel. O cenário considera eventos de pedidos originados por canais como web, aplicativo, PDV e integrações externas.

Em sistemas distribuídos, uma mesma operação pode ser reenviada por causa de retries, timeouts, reprocessamentos ou falhas temporárias de comunicação. Por isso, receber o mesmo evento mais de uma vez é um comportamento esperado que precisa ser tratado explicitamente.

## 2. Quem tem o problema

Os principais atores do cenário são:

- **Canais de venda** — web, mobile, PDV e integrações que enviam eventos de pedidos;
- **Serviços de ingestão** — recebem e validam operações de entrada;
- **Processadores assíncronos** — consomem eventos e atualizam projeções;
- **Pipeline analítico** — transforma dados brutos em dados confiáveis e produtos analíticos;
- **Consumidores de analytics** — API e dashboard que utilizam os dados processados.

## 3. O que pode falhar

Sem mecanismos explícitos de confiabilidade, os principais riscos são:

1. **Duplicação na entrada** — a mesma operação lógica pode ser enviada novamente e criar dois eventos distintos;
2. **Dual write inconsistente** — o banco pode confirmar uma alteração enquanto a publicação no broker falha;
3. **Reentrega no consumo** — consumidores podem receber o mesmo evento novamente em um modelo de entrega at-least-once;
4. **Dados analíticos inválidos** — registros incompletos, inconsistentes ou duplicados podem avançar para camadas de consumo;
5. **Acoplamento entre ingestão e analytics** — processamento analítico síncrono aumentaria o acoplamento e dificultaria evolução independente;
6. **Baixa rastreabilidade operacional** — falhas podem se tornar difíceis de diagnosticar sem health checks, métricas e evidências de pipeline.

## 4. Por que isso importa

Uma duplicação ou inconsistência não é apenas um detalhe técnico. Dependendo do domínio, ela pode causar:

- projeções de pedidos incorretas;
- processamento downstream repetido;
- divergências em indicadores e relatórios;
- reprocessamentos desnecessários;
- perda de confiança nos dados analíticos;
- maior dificuldade de diagnóstico e recuperação.

O projeto não assume números reais de volume, throughput, latência ou disponibilidade. Qualquer afirmação desse tipo exigiria evidência específica de teste de carga ou operação em produção.

## 5. Resultado esperado

A plataforma deve permitir que um evento de pedido seja recebido, validado, persistido e distribuído de forma confiável, mantendo as seguintes propriedades:

- a mesma `Idempotency-Key` não cria duas operações lógicas distintas;
- persistência e publicação assíncrona não dependem de um dual write ingênuo entre PostgreSQL e Kafka;
- consumidores toleram reentrega sem produzir efeitos duplicados;
- dados avançam para consumo analítico somente após validações explícitas de qualidade;
- o frontend consome um modelo analítico dedicado, sem depender de tabelas transacionais ou arquivos do Lakehouse;
- o fluxo pode ser inspecionado por evidências operacionais e de qualidade.

## 6. Escopo deste case

Este case demonstra decisões de Backend Java, arquitetura orientada a eventos e Engenharia de Dados. A implementação atual cobre ingestão, idempotência, Transactional Outbox, Kafka, Spark Structured Streaming, camadas Bronze/Silver/Gold, Apache Iceberg, Airflow, Data Quality, serving analítico, API e dashboard.

Recursos de roadmap — como novos domínios de negócio, ML avançado, expansão de segurança e uma eventual topologia cloud de produção — não são tratados como capacidades já entregues.

## 7. Pergunta de engenharia que orienta o projeto

> Como receber e processar eventos de varejo de forma confiável, tolerando duplicação e falhas parciais, sem acoplar a ingestão transacional ao processamento analítico?

Essa pergunta orienta as regras de negócio, os requisitos e as decisões arquiteturais documentadas neste repositório.
