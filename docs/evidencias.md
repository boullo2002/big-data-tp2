# Evidencias TP2

Completar este archivo con capturas o salidas copiadas desde el notebook.

## 1. Batch -> Bronze

- Conteo de `bronze/customers_orgs`.
- Conteo de `bronze/users`.
- Conteo de `bronze/billing_monthly`.
- Schema impreso con `ingest_ts`, `ingest_date` y `source_file`.

## 2. Streaming -> Bronze

- Conteo de `bronze/usage_events_stream`.
- Schema con `event_ts`, `usage_date`, `schema_version`, `carbon_kg` y `genai_tokens`.
- Ruta de checkpoint `checkpoints/usage_events_bronze`.

## 3. Silver y Quarantine

- Conteo de `silver/usage_events`.
- Conteo de `quarantine/usage_events`.
- Muestra de registros quarantined con `dq_reason`.

## 4. Gold

- Conteo de `gold/org_daily_usage_by_service`.
- Muestra con `org_id`, `usage_date`, `service`, `requests`, `daily_cost_usd`, `genai_tokens` y `carbon_kg`.

## 5. Idempotencia

Pegar el output:

```text
Conteos antes: {...}
Conteos despues: {...}
Idempotencia OK: True
```

## 6. Cassandra / AstraDB

- Captura de ejecucion de `01_create_keyspace.cql`.
- Captura de ejecucion de `02_create_tables.cql`.
- Captura de tabla `org_daily_usage_by_service` poblada.
- Resultado de consulta 1.
- Resultado de consulta 2.
