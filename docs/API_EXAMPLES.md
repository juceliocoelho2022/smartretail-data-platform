# API Examples — v0.1

## Criar evento de pedido

~~~bash
curl -i -X POST http://localhost:8080/api/v1/events/orders \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: order-demo-001" \
  -d '{
    "customerId": "CUS-81921",
    "productId": "PROD-3321",
    "quantity": 2,
    "unitPrice": 249.90,
    "channel": "WEB",
    "location": "SAO_PAULO"
  }'
~~~

Repetir a chamada com o mesmo Idempotency-Key retorna o mesmo eventId com replayed=true.

## Consultar projeção

~~~bash
curl http://localhost:8080/api/v1/events/orders/{eventId}
~~~

O GET pode retornar 404 por alguns instantes porque o processamento é assíncrono.

## Health

~~~bash
curl http://localhost:8080/actuator/health
~~~

## Prometheus

~~~bash
curl http://localhost:8080/actuator/prometheus
~~~
