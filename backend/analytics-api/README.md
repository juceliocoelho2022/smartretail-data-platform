# SmartRetail Analytics API — v0.5

A Analytics API é a camada de serving da v0.5.

Ela expõe Data Products derivados da Gold por uma API REST Java 21 + Spring Boot.

## Endpoints

```text
GET /api/v1/analytics/summary

GET /api/v1/analytics/sales/daily
    ?from=2026-10-01
    &to=2026-10-02
    &channel=WEB
    &location=SAO_PAULO
    &limit=100
```

## Serving model

O PostgreSQL utiliza um schema separado:

```text
analytics
├── sales_summary
└── sales_daily
```

A separação evita acoplar o modelo analítico às tabelas transacionais da Event Platform.

## Porta local

```text
8082
```

## Próximo passo

Um job Spark publicará os Data Products Gold no serving model PostgreSQL.

Depois o Dashboard React consumirá exclusivamente esta API.
