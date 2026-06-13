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

Para evitar el problema de small files, todos los writes batch usan `repartition(partition_col).coalesce(1)` antes de escribir, resultando en 1 archivo por partición. El streaming Bronze genera muchos archivos chicos (1 por micro-batch por fecha) por lo que se aplica un paso de reparquet inmediatamente después de que el stream termina, consolidando también a 1 archivo por fecha. Para datasets más grandes el target sería `ceil(size / 128 MB)` archivos por partición.

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

## Entorno de ejecución

El pipeline corre localmente con `uv` como gestor de dependencias y entorno virtual. Se eligió `uv` sobre `pip`/`venv` por su resolución determinista de dependencias (lockfile) y velocidad de instalación. El código es reproducible en cualquier máquina con Python 3.10+ y Java 11-21 sin configuración adicional.

## JSONL como stream simulado

Los archivos JSONL representan micro-lotes de eventos. No reemplazan a Kafka ni a un broker real, pero permiten ejercitar Structured Streaming, watermark, checkpointing y dedupe con una fuente reproducible.
