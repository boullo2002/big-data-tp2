# Big Data TP2 - Cloud Provider Analytics

Entrega del segundo parcial basada en el diseno del TP1. El objetivo es demostrar un MVP end-to-end:

`Landing -> Bronze -> Silver -> Gold -> Serving Cassandra/AstraDB`

## Estructura del repo

- `notebooks/segundo_parcial_big_data.ipynb`: notebook principal para Colab.
- `src/tp2_pipeline.py`: modulo reutilizable con la logica PySpark.
- `cql/01_create_keyspace.cql`: creacion de keyspace.
- `cql/02_create_tables.cql`: tablas query-first.
- `cql/03_queries_demo.cql`: consultas de demo.
- `docs/decision_log.md`: decisiones tecnicas.
- `docs/evidencias.md`: checklist de evidencias.
- `cloud_provider_challenge_dataset_v1/`: dataset provisto, si esta presente localmente.

## Quickstart en Google Colab

1. Subir el repo a Colab o montar Drive con esta carpeta.
2. Si el dataset no esta presente, subir `cloud_provider_challenge_dataset_v1.zip` y descomprimirlo para que exista:

```text
cloud_provider_challenge_dataset_v1/datalake/landing/
```

3. Instalar dependencias:

```bash
pip install -r requirements.txt
```

4. Abrir y ejecutar:

```text
notebooks/segundo_parcial_big_data.ipynb
```

El notebook esta dividido en:

0. Setup
1. Paths y configuracion
2. SparkSession
3. Schemas explicitos
4. Batch -> Bronze
5. Streaming -> Bronze
6. Silver + calidad + quarantine
7. Gold mart FinOps
8. Idempotencia
9. Cassandra / AstraDB
10. Consultas CQL
11. Evidencias finales

## Ejecucion alternativa como script

```bash
python src/tp2_pipeline.py \
  --landing cloud_provider_challenge_dataset_v1/datalake/landing \
  --datalake-out cloud_provider_challenge_dataset_v1/datalake \
  --checkpoint-out cloud_provider_challenge_dataset_v1/checkpoints
```

## Validacion de zonas

```bash
find cloud_provider_challenge_dataset_v1/datalake/bronze -maxdepth 3 -type f | head
find cloud_provider_challenge_dataset_v1/datalake/silver -maxdepth 3 -type f | head
find cloud_provider_challenge_dataset_v1/datalake/gold -maxdepth 3 -type f | head
find cloud_provider_challenge_dataset_v1/datalake/quarantine -maxdepth 3 -type f | head
```

## AstraDB / Cassandra

Configurar variables de entorno en Colab:

```python
import os
os.environ["ASTRA_CLIENT_ID"] = "<client_id>"
os.environ["ASTRA_CLIENT_SECRET"] = "<client_secret>"
os.environ["ASTRA_SECURE_CONNECT_BUNDLE"] = "/content/secure-connect.zip"
os.environ["ASTRA_KEYSPACE"] = "cloud_analytics"
```

Luego ejecutar en AstraDB los scripts:

```text
cql/01_create_keyspace.cql
cql/02_create_tables.cql
cql/03_queries_demo.cql
```

El notebook incluye funciones para cargar:

- `org_daily_usage_by_service`
- `org_service_cost_last_14d`

## Como validar la entrega

El notebook debe imprimir:

- rutas Bronze/Silver/Gold/Quarantine generadas;
- cantidad de registros por capa;
- schema de tablas importantes;
- ejemplos quarantined con `dq_reason`;
- mart Gold `org_daily_usage_by_service`;
- conteos antes/despues de reprocesar Silver/Gold;
- CQL ejecutado;
- resultados de las dos consultas sobre AstraDB.

## Decisiones principales

- Arquitectura Lambda porque el caso combina batch y near real-time.
- Parquet como formato intermedio por compresion, lectura columnar y particionamiento.
- Structured Streaming con `availableNow=True`, watermark y checkpointing para demo en Colab.
- Cassandra/AstraDB modelado query-first, no normalizado como RDBMS.
- Idempotencia por `dropDuplicates`, `overwrite` en Parquet, checkpointing y upserts por primary key.
