# Big Data TP2 - Cloud Provider Analytics

Entrega del segundo parcial basada en el diseno del TP1. El objetivo es demostrar un MVP tecnico end-to-end minimo:

`Landing -> Bronze -> Silver -> Gold -> Serving Cassandra/AstraDB`

## Estructura del repo

- `notebooks/segundo_parcial_big_data.ipynb`: notebook principal para Colab.
- `src/tp2_pipeline.py`: modulo reutilizable con la logica PySpark.
- `cql/01_create_keyspace.cql`: creacion de keyspace.
- `cql/02_create_tables.cql`: tablas query-first para los marts Gold.
- `cql/03_queries_demo.cql`: consultas de demo.
- `dashboard/index.html`: visualizacion estatica basada en los marts Gold.
- `dashboard/data/marts.json`: snapshot liviano exportado desde Gold para el dashboard.
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

### Local (repo)

1. Dejar el bundle en la raiz del repo, por ejemplo `secure-connect-tp2-bigdata.zip` (esta en `.gitignore`).
2. Copiar `.env.example` a `.env` y completar `ASTRA_CLIENT_ID` y `ASTRA_CLIENT_SECRET`.
3. El notebook detecta el zip automaticamente al correr la seccion 9.

### Colab

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

El notebook incluye la carga del mart requerido:

- `org_daily_usage_by_service`

Ademas, el pipeline materializa los marts extra planteados en el TP1:

- `revenue_by_org_month`
- `cost_anomaly_mart`
- `tickets_by_org_date`
- `genai_tokens_by_org_date`

## Dashboard de visualizacion

La capa de visualizacion del TP1 queda materializada en `dashboard/index.html`, consumiendo `dashboard/data/marts.json`, que se exporta desde los marts Gold. Para verlo:

```bash
python3 -m http.server 8000 --directory dashboard
```

Luego abrir `http://localhost:8000`.

## Como validar la entrega

El notebook debe imprimir:

- rutas Bronze/Silver/Gold/Quarantine generadas;
- cantidad de registros por capa;
- schema de tablas importantes;
- ejemplos quarantined con `dq_reason`;
- mart Gold `org_daily_usage_by_service`;
- marts extra TP1 en Gold (`revenue_by_org_month`, `cost_anomaly_mart`, `tickets_by_org_date`, `genai_tokens_by_org_date`);
- dashboard estatico con KPIs y vistas FinOps, Billing, Soporte, Producto y Anomalias;
- conteos antes/despues de reprocesar Silver/Gold;
- CQL ejecutado;
- resultados de dos consultas sobre `org_daily_usage_by_service` en AstraDB.

## Decisiones principales

- Arquitectura Lambda porque el caso combina batch y near real-time.
- Parquet como formato intermedio por compresion, lectura columnar y particionamiento.
- Structured Streaming con `availableNow=True`, watermark y checkpointing para demo en Colab.
- Cassandra/AstraDB modelado query-first, no normalizado como RDBMS.
- Idempotencia por `dropDuplicates`, `overwrite` en Parquet, checkpointing y upserts por primary key.
