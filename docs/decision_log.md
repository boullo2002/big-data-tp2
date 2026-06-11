# Decision Log TP2

## Arquitectura

Se mantiene la arquitectura Lambda definida en el TP1 porque la consigna requiere batch para maestros/facturacion y Structured Streaming para eventos de uso. El MVP usa un unico motor, PySpark, para reducir duplicacion operacional.

## Particiones

- Bronze batch: particion por `ingest_date`.
- Bronze streaming: particion por `usage_date`.
- Silver events: particion por `usage_date`.
- Gold FinOps: particion por `usage_date`.
- Gold revenue mensual: particion por `month`.
- Gold tickets: particion por `ticket_date`.
- Gold anomalias y GenAI: particion por `usage_date`.

Estas particiones permiten reprocesar periodos puntuales y habilitan partition pruning en Spark.

## Calidad de datos

Reglas activas:

1. `event_id` no nulo.
2. `event_ts` parseable y no nulo.
3. `unit` no nulo cuando existe `value`.
4. `cost_usd_increment >= -0.01`; valores menores se envian a quarantine.

Los costos negativos leves se toleran como ajustes o creditos operativos, pero se marca anomalia cuando superan el umbral.

## Cassandra

La tabla principal del MVP se modela query-first para responder consultas por organizacion y rango de fechas. La primary key `((org_id), usage_date, service)` permite leer una organizacion ordenada por fecha descendente y servicio. Las dos consultas de aceptacion del segundo parcial se responden desde `org_daily_usage_by_service`: costo/uso por rango de fechas y detalle de servicios con `anomaly_event_count` para una fecha puntual.

Para cubrir los marts definidos en el TP1, Serving tambien expone tablas query-first adicionales:

- `revenue_by_org_month`: `PRIMARY KEY ((org_id), month)`.
- `cost_anomaly_mart`: `PRIMARY KEY ((org_id), usage_date, service)`.
- `tickets_by_org_date`: `PRIMARY KEY ((org_id), ticket_date, category, severity)`.
- `genai_tokens_by_org_date`: `PRIMARY KEY ((org_id), usage_date)`.

## Idempotencia

Bronze batch, Silver y Gold se escriben en modo `overwrite`. Streaming usa checkpointing y dedupe por `event_id`. En Cassandra, las primary keys permiten upsert natural. La re-ejecucion del pipeline no duplica registros porque cada capa reemplaza su salida o escribe sobre claves deterministicas.

## Trade-offs de Colab

Colab no es un ambiente productivo: las sesiones expiran, los recursos son variables y no hay cluster Spark real. Para el parcial es adecuado porque permite demostrar el recorrido tecnico completo con el dataset sintetico.

## JSONL como stream simulado

Los archivos JSONL representan micro-lotes de eventos. No reemplazan a Kafka ni a un broker real, pero permiten ejercitar Structured Streaming, watermark, checkpointing y dedupe con una fuente reproducible.
