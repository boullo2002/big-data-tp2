#!/usr/bin/env python3
"""TP2 Cloud Provider Analytics MVP.

Pipeline end-to-end:
Landing -> Bronze -> Silver -> Gold -> Cassandra/AstraDB.
"""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
import time
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, Iterator, Optional

# ---------------------------------------------------------------------------
# Output helpers
# ---------------------------------------------------------------------------

_USE_COLOR = sys.stdout.isatty()
_W = 68


def _c(code: str, text: str) -> str:
    return f"\033[{code}m{text}\033[0m" if _USE_COLOR else text


def _banner() -> None:
    print(_c("1;36", "=" * _W))
    title = "TP2  ·  Cloud Provider Analytics  ·  Big Data ITBA"
    print(_c("1;36", f"  {title}"))
    print(_c("1;36", "=" * _W))
    print()


def _header(step: int, total: int, label: str) -> None:
    tag = _c("1;33", f"[{step}/{total}]")
    print(f"\n{tag} {_c('1', label)}")
    print(_c("90", "─" * _W))


def _ok(label: str, rows: int, elapsed: float) -> None:
    rows_str = _c("32", f"{rows:>8,} filas")
    time_str = _c("90", f"({elapsed:.1f}s)")
    print(f"  {_c('32', '✓')}  {label:<38} {rows_str}  {time_str}")


def _info(msg: str) -> None:
    print(f"  {_c('36', '→')}  {msg}")


def _done(label: str, elapsed: float) -> None:
    print(_c("90", f"  └─ {label} completado en {elapsed:.1f}s"))


@contextmanager
def _step(step: int, total: int, label: str) -> Iterator[None]:
    _header(step, total, label)
    t0 = time.perf_counter()
    yield
    _done(label, time.perf_counter() - t0)


_TOTAL_STEPS = 6

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


@dataclass(frozen=True)
class Paths:
    landing: Path
    datalake_out: Path
    checkpoint_out: Path

    @property
    def bronze(self) -> Path:
        return self.datalake_out / "bronze"

    @property
    def silver(self) -> Path:
        return self.datalake_out / "silver"

    @property
    def gold(self) -> Path:
        return self.datalake_out / "gold"

    @property
    def quarantine(self) -> Path:
        return self.datalake_out / "quarantine"


CSV_SPECS: Dict[str, tuple[StructType, Iterable[str]]] = {
    "customers_orgs": (
        StructType(
            [
                StructField("org_id", StringType()),
                StructField("org_name", StringType()),
                StructField("industry", StringType()),
                StructField("hq_region", StringType()),
                StructField("plan_tier", StringType()),
                StructField("is_enterprise", StringType()),
                StructField("signup_date", StringType()),
                StructField("sales_rep", StringType()),
                StructField("lifecycle_stage", StringType()),
                StructField("marketing_source", StringType()),
                StructField("nps_score", StringType()),
            ]
        ),
        ("org_id",),
    ),
    "users": (
        StructType(
            [
                StructField("user_id", StringType()),
                StructField("org_id", StringType()),
                StructField("email", StringType()),
                StructField("role", StringType()),
                StructField("active", StringType()),
                StructField("created_at", StringType()),
                StructField("last_login", StringType()),
            ]
        ),
        ("user_id",),
    ),
    "resources": (
        StructType(
            [
                StructField("resource_id", StringType()),
                StructField("org_id", StringType()),
                StructField("service", StringType()),
                StructField("region", StringType()),
                StructField("created_at", StringType()),
                StructField("state", StringType()),
                StructField("tags_json", StringType()),
            ]
        ),
        ("resource_id",),
    ),
    "billing_monthly": (
        StructType(
            [
                StructField("invoice_id", StringType()),
                StructField("org_id", StringType()),
                StructField("month", StringType()),
                StructField("subtotal", StringType()),
                StructField("credits", StringType()),
                StructField("taxes", StringType()),
                StructField("currency", StringType()),
                StructField("exchange_rate_to_usd", StringType()),
            ]
        ),
        ("invoice_id",),
    ),
    "support_tickets": (
        StructType(
            [
                StructField("ticket_id", StringType()),
                StructField("org_id", StringType()),
                StructField("category", StringType()),
                StructField("severity", StringType()),
                StructField("created_at", StringType()),
                StructField("resolved_at", StringType()),
                StructField("csat", StringType()),
                StructField("sla_breached", StringType()),
            ]
        ),
        ("ticket_id",),
    ),
    "nps_surveys": (
        StructType(
            [
                StructField("org_id", StringType()),
                StructField("survey_date", StringType()),
                StructField("nps_score", StringType()),
                StructField("comment", StringType()),
            ]
        ),
        ("org_id", "survey_date"),
    ),
    "marketing_touches": (
        StructType(
            [
                StructField("touch_id", StringType()),
                StructField("org_id", StringType()),
                StructField("campaign", StringType()),
                StructField("channel", StringType()),
                StructField("timestamp", StringType()),
                StructField("clicked", StringType()),
                StructField("converted", StringType()),
            ]
        ),
        ("touch_id",),
    ),
}


USAGE_SCHEMA = StructType(
    [
        StructField("event_id", StringType()),
        StructField("timestamp", StringType()),
        StructField("org_id", StringType()),
        StructField("resource_id", StringType()),
        StructField("service", StringType()),
        StructField("region", StringType()),
        StructField("metric", StringType()),
        StructField("value", StringType()),
        StructField("unit", StringType()),
        StructField("cost_usd_increment", StringType()),
        StructField("schema_version", IntegerType()),
        StructField("carbon_kg", StringType()),
        StructField("genai_tokens", StringType()),
    ]
)


def configure_java_for_spark() -> None:
    """Prefer a Spark-compatible Java when macOS defaults to a too-new JDK."""
    if os.environ.get("JAVA_HOME"):
        return

    def java_major(java_home: str) -> Optional[int]:
        try:
            output = subprocess.check_output(
                [str(Path(java_home, "bin", "java")), "-version"],
                text=True,
                stderr=subprocess.STDOUT,
            )
        except Exception:
            return None
        version_line = output.splitlines()[0]
        version = version_line.split('"')[1]
        if version.startswith("1."):
            return int(version.split(".")[1])
        return int(version.split(".")[0])

    candidates = [
        "/Users/juan/Library/Java/JavaVirtualMachines/corretto-21.0.2/Contents/Home",
        "/Users/juan/Library/Java/JavaVirtualMachines/corretto-1.8.0_352/Contents/Home",
    ]

    if Path("/usr/libexec/java_home").exists():
        for version in ("17", "21", "11", "1.8", "8"):
            try:
                java_home = subprocess.check_output(
                    ["/usr/libexec/java_home", "-v", version],
                    text=True,
                    stderr=subprocess.DEVNULL,
                ).strip()
            except Exception:
                continue
            if java_home:
                candidates.append(java_home)

    for java_home in candidates:
        major = java_major(java_home)
        if major is not None and 8 <= major <= 21:
            os.environ["JAVA_HOME"] = java_home
            os.environ["PATH"] = f"{java_home}/bin:{os.environ.get('PATH', '')}"
            break


def build_spark(app_name: str) -> SparkSession:
    configure_java_for_spark()
    spark = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    java_version = spark.sparkContext._jvm.java.lang.System.getProperty("java.version")
    _info(f"Spark {spark.version}  |  Java {java_version}  |  local[*]")
    return spark


def read_csv(spark: SparkSession, path: Path, schema: StructType) -> DataFrame:
    return spark.read.option("header", True).schema(schema).csv(str(path))


def add_technical_columns(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("ingest_ts", F.current_timestamp())
        .withColumn("ingest_date", F.to_date("ingest_ts"))
        .withColumn("source_file", F.input_file_name())
    )


def normalize_batch_df(name: str, df: DataFrame) -> DataFrame:
    if name == "customers_orgs":
        return (
            df.withColumn("is_enterprise", F.col("is_enterprise").cast(BooleanType()))
            .withColumn("signup_date", F.to_date("signup_date"))
            .withColumn("nps_score", F.col("nps_score").cast(DoubleType()))
        )
    if name == "users":
        return (
            df.withColumn("active", F.col("active").cast(BooleanType()))
            .withColumn("created_at", F.to_date("created_at"))
            .withColumn("last_login", F.to_date("last_login"))
        )
    if name == "resources":
        return df.withColumn("created_at", F.to_date("created_at"))
    if name == "billing_monthly":
        return (
            df.withColumn("month", F.to_date("month"))
            .withColumn("subtotal", F.col("subtotal").cast(DoubleType()))
            .withColumn("credits", F.coalesce(F.col("credits").cast(DoubleType()), F.lit(0.0)))
            .withColumn("taxes", F.coalesce(F.col("taxes").cast(DoubleType()), F.lit(0.0)))
            .withColumn("exchange_rate_to_usd", F.col("exchange_rate_to_usd").cast(DoubleType()))
        )
    if name == "support_tickets":
        return (
            df.withColumn("created_at", F.to_timestamp("created_at"))
            .withColumn("resolved_at", F.to_timestamp("resolved_at"))
            .withColumn("csat", F.col("csat").cast(DoubleType()))
            .withColumn("sla_breached", F.col("sla_breached").cast(BooleanType()))
        )
    if name == "nps_surveys":
        return (
            df.withColumn("survey_date", F.to_date("survey_date"))
            .withColumn("nps_score", F.col("nps_score").cast(DoubleType()))
        )
    if name == "marketing_touches":
        return (
            df.withColumn("timestamp", F.to_timestamp("timestamp"))
            .withColumn("clicked", F.col("clicked").cast(BooleanType()))
            .withColumn("converted", F.col("converted").cast(BooleanType()))
        )
    return df


def write_parquet(df: DataFrame, path: Path, partition_cols: Iterable[str]) -> None:
    cols = list(partition_cols)
    # Repartition by partition key garantiza que todos los registros de la misma
    # partición queden en el mismo task; coalesce(1) los consolida en 1 archivo.
    # Para este dataset (< 500 MB) es óptimo. En producción: ceil(size / 128 MB).
    out = df.repartition(*[F.col(c) for c in cols]) if cols else df
    out = out.coalesce(1)
    writer = out.write.mode("overwrite")
    if cols:
        writer = writer.partitionBy(*cols)
    writer.parquet(str(path))


def read_parquet_dir(spark: SparkSession, path: Path) -> DataFrame:
    """Lee Parquet desde un directorio, tolerando metadatos de streaming sink."""
    resolved = path.resolve()
    if not any(resolved.rglob("*.parquet")):
        raise FileNotFoundError(f"Sin archivos Parquet en {resolved}")
    if (resolved / "_spark_metadata").exists():
        parquet_glob = str(resolved / "*" / "*.parquet")
        return spark.read.option("basePath", str(resolved)).parquet(parquet_glob)
    return spark.read.parquet(str(resolved))


def ingest_batch_to_bronze(spark: SparkSession, paths: Paths) -> None:
    for name, (schema, dedupe_keys) in CSV_SPECS.items():
        input_path = paths.landing / f"{name}.csv"
        if not input_path.exists():
            _info(f"[SKIP] {name}.csv no encontrado")
            continue
        t0 = time.perf_counter()
        df = read_csv(spark, input_path, schema)
        df = normalize_batch_df(name, add_technical_columns(df)).dropDuplicates(list(dedupe_keys))
        output_path = paths.bronze / name
        write_parquet(df, output_path, ["ingest_date"])
        _ok(name, df.count(), time.perf_counter() - t0)


def ingest_usage_stream_to_bronze(spark: SparkSession, paths: Paths) -> None:
    input_path = paths.landing / "usage_events_stream"
    output_path = paths.bronze / "usage_events_stream"
    checkpoint_path = paths.checkpoint_out / "usage_events_bronze"

    stream_df = (
        spark.readStream.schema(USAGE_SCHEMA)
        .option("maxFilesPerTrigger", 10)
        .json(str(input_path))
        .withColumn("event_ts", F.to_timestamp("timestamp"))
        .withColumn("usage_date", F.to_date("event_ts"))
        .withColumn("ingest_ts", F.current_timestamp())
        .withColumn("source_file", F.input_file_name())
        .withWatermark("event_ts", "2 days")
        .dropDuplicates(["event_id"])
    )

    query = (
        stream_df.writeStream.format("parquet")
        .option("path", str(output_path))
        .option("checkpointLocation", str(checkpoint_path))
        .option("maxRecordsPerFile", 100_000)
        .partitionBy("usage_date")
        .outputMode("append")
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()

    # Reparquet: el stream genera muchos archivos chicos (1 por micro-batch por fecha).
    # Compactamos a 1 archivo por partición de fecha antes de que Silver lo lea.
    # Escribimos en un path temporal: overwrite sobre el mismo directorio borra la
    # fuente antes de que Spark termine de leerla.
    _info("compactando bronze stream (reparquet) ...")
    if not any(output_path.rglob("*.parquet")):
        _info("[SKIP] usage_events_stream vacío, sin reparquet")
        return

    compact_path = output_path.parent / f"{output_path.name}__compact"
    if compact_path.exists():
        shutil.rmtree(compact_path)

    read_parquet_dir(spark, output_path) \
        .repartition(F.col("usage_date")) \
        .coalesce(1) \
        .write.mode("overwrite") \
        .partitionBy("usage_date") \
        .parquet(str(compact_path))

    shutil.rmtree(output_path)
    compact_path.rename(output_path)
    _info(f"usage_events_stream  →  {output_path}")


def build_silver_events(spark: SparkSession, paths: Paths) -> None:
    events = read_parquet_dir(spark, paths.bronze / "usage_events_stream")
    orgs = spark.read.parquet(str(paths.bronze / "customers_orgs")).select(
        "org_id", "org_name", "industry", "hq_region", "plan_tier", "lifecycle_stage"
    )
    resources = spark.read.parquet(str(paths.bronze / "resources")).select(
        F.col("resource_id").alias("resource_id_dim"),
        F.col("org_id").alias("resource_org_id"),
        F.col("service").alias("resource_service"),
        F.col("region").alias("resource_region"),
        "state",
    )

    conformed = (
        events.withColumn("usage_date", F.to_date("event_ts"))
        .withColumn("value_num", F.col("value").cast(DoubleType()))
        .withColumn("cost_usd_increment", F.coalesce(F.col("cost_usd_increment").cast(DoubleType()), F.lit(0.0)))
        .withColumn("genai_tokens", F.coalesce(F.col("genai_tokens").cast(DoubleType()), F.lit(0.0)))
        .withColumn("carbon_kg", F.coalesce(F.col("carbon_kg").cast(DoubleType()), F.lit(0.0)))
        .join(orgs, "org_id", "left")
        .join(resources, F.col("resource_id") == F.col("resource_id_dim"), "left")
        .withColumn("service", F.coalesce(F.col("service"), F.col("resource_service")))
        .withColumn("region", F.coalesce(F.col("region"), F.col("resource_region"), F.col("hq_region")))
    )

    invalid_condition = (
        F.col("event_id").isNull()
        | F.col("event_ts").isNull()
        | (F.col("value_num").isNotNull() & F.col("unit").isNull())
        | (F.col("cost_usd_increment") < F.lit(-0.01))
        | F.col("schema_version").isNull()
        | (~F.col("schema_version").isin(1, 2))
    )

    quarantine = conformed.filter(invalid_condition).withColumn(
        "dq_reason",
        F.concat_ws(
            ";",
            F.when(F.col("event_id").isNull(), F.lit("event_id_null")),
            F.when(F.col("event_ts").isNull(), F.lit("event_ts_invalid")),
            F.when(F.col("value_num").isNotNull() & F.col("unit").isNull(), F.lit("unit_null_with_value")),
            F.when(F.col("cost_usd_increment") < F.lit(-0.01), F.lit("cost_below_threshold")),
            F.when(F.col("schema_version").isNull() | (~F.col("schema_version").isin(1, 2)), F.lit("schema_version_invalid")),
        ),
    )

    valid = (
        conformed.filter(~invalid_condition)
        .withColumn("requests", F.when(F.col("metric") == "requests", F.col("value_num")).otherwise(F.lit(0.0)))
        .withColumn("cpu_hours", F.when(F.col("metric") == "cpu_hours", F.col("value_num")).otherwise(F.lit(0.0)))
        .withColumn(
            "storage_gb_hours",
            F.when(F.col("metric") == "storage_gb_hours", F.col("value_num")).otherwise(F.lit(0.0)),
        )
        .withColumn("daily_cost_usd", F.col("cost_usd_increment"))
        .withColumn("total_value", F.coalesce(F.col("value_num"), F.lit(0.0)))
        .withColumn("is_cost_anomaly", (F.col("cost_usd_increment") < 0) | (F.col("cost_usd_increment") > 50))
        .drop("resource_id_dim", "resource_org_id", "resource_service", "resource_region")
    )

    write_parquet(valid, paths.silver / "usage_events", ["usage_date"])
    write_parquet(quarantine, paths.quarantine / "usage_events", ["usage_date"])
    valid_count = valid.count()
    quar_count = quarantine.count()
    _ok("usage_events  (válidos)", valid_count, 0)
    _ok("usage_events  (quarantine)", quar_count, 0)


def build_gold_finops(spark: SparkSession, paths: Paths) -> DataFrame:
    events = spark.read.parquet(str(paths.silver / "usage_events"))
    mart = (
        events.groupBy("org_id", "usage_date", "service", "org_name", "plan_tier", "hq_region")
        .agg(
            F.count("*").alias("event_count"),
            F.sum("requests").alias("requests"),
            F.sum("total_value").alias("total_value"),
            F.sum("cpu_hours").alias("cpu_hours"),
            F.sum("storage_gb_hours").alias("storage_gb_hours"),
            F.sum("genai_tokens").alias("genai_tokens"),
            F.sum("carbon_kg").alias("carbon_kg"),
            F.round(F.sum("daily_cost_usd"), 4).alias("daily_cost_usd"),
            F.sum(F.col("is_cost_anomaly").cast("long")).alias("anomaly_event_count"),
        )
        .withColumn("updated_at", F.current_timestamp())
    )
    write_parquet(mart, paths.gold / "org_daily_usage_by_service", ["usage_date"])
    _ok("org_daily_usage_by_service", mart.count(), 0)
    return mart


def build_gold_revenue_by_org_month(spark: SparkSession, paths: Paths) -> DataFrame:
    billing = spark.read.parquet(str(paths.bronze / "billing_monthly"))
    orgs = spark.read.parquet(str(paths.bronze / "customers_orgs")).select(
        "org_id", "org_name", "industry", "plan_tier", "hq_region", "lifecycle_stage"
    )

    mart = (
        billing.join(orgs, "org_id", "left")
        .withColumn("gross_revenue_usd", F.col("subtotal") * F.col("exchange_rate_to_usd"))
        .withColumn("credits_usd", F.col("credits") * F.col("exchange_rate_to_usd"))
        .withColumn("taxes_usd", F.col("taxes") * F.col("exchange_rate_to_usd"))
        .withColumn(
            "net_revenue_usd",
            (F.col("subtotal") - F.col("credits") + F.col("taxes")) * F.col("exchange_rate_to_usd"),
        )
        .groupBy("org_id", "month", "org_name", "industry", "plan_tier", "hq_region", "lifecycle_stage")
        .agg(
            F.countDistinct("invoice_id").alias("invoice_count"),
            F.round(F.sum("gross_revenue_usd"), 4).alias("gross_revenue_usd"),
            F.round(F.sum("credits_usd"), 4).alias("credits_usd"),
            F.round(F.sum("taxes_usd"), 4).alias("taxes_usd"),
            F.round(F.sum("net_revenue_usd"), 4).alias("net_revenue_usd"),
        )
        .withColumn("updated_at", F.current_timestamp())
    )
    write_parquet(mart, paths.gold / "revenue_by_org_month", ["month"])
    _ok("revenue_by_org_month", mart.count(), 0)
    return mart


def build_gold_cost_anomaly_mart(spark: SparkSession, paths: Paths) -> DataFrame:
    events = spark.read.parquet(str(paths.silver / "usage_events"))
    mart = (
        events.filter(F.col("is_cost_anomaly"))
        .groupBy("org_id", "usage_date", "service", "org_name", "plan_tier", "hq_region")
        .agg(
            F.count("*").alias("anomaly_event_count"),
            F.sum(F.when(F.col("cost_usd_increment") < 0, 1).otherwise(0)).alias("negative_cost_event_count"),
            F.sum(F.when(F.col("cost_usd_increment") > 50, 1).otherwise(0)).alias("high_cost_event_count"),
            F.round(F.sum("daily_cost_usd"), 4).alias("anomaly_cost_usd"),
            F.round(F.max("cost_usd_increment"), 4).alias("max_event_cost_usd"),
            F.round(F.min("cost_usd_increment"), 4).alias("min_event_cost_usd"),
        )
        .withColumn("updated_at", F.current_timestamp())
    )
    write_parquet(mart, paths.gold / "cost_anomaly_mart", ["usage_date"])
    _ok("cost_anomaly_mart", mart.count(), 0)
    return mart


def build_gold_tickets_by_org_date(spark: SparkSession, paths: Paths) -> DataFrame:
    tickets = spark.read.parquet(str(paths.bronze / "support_tickets"))
    orgs = spark.read.parquet(str(paths.bronze / "customers_orgs")).select(
        "org_id", "org_name", "industry", "plan_tier", "hq_region"
    )

    mart = (
        tickets.join(orgs, "org_id", "left")
        .withColumn("ticket_date", F.to_date("created_at"))
        .withColumn("is_resolved", F.col("resolved_at").isNotNull())
        .groupBy("org_id", "ticket_date", "category", "severity", "org_name", "industry", "plan_tier", "hq_region")
        .agg(
            F.countDistinct("ticket_id").alias("ticket_count"),
            F.sum(F.col("is_resolved").cast("long")).alias("resolved_ticket_count"),
            F.sum((~F.col("is_resolved")).cast("long")).alias("open_ticket_count"),
            F.sum(F.col("sla_breached").cast("long")).alias("sla_breach_count"),
            F.round(F.avg(F.col("sla_breached").cast("double")), 4).alias("sla_breach_rate"),
            F.round(F.avg("csat"), 4).alias("avg_csat"),
        )
        .withColumn("updated_at", F.current_timestamp())
    )
    write_parquet(mart, paths.gold / "tickets_by_org_date", ["ticket_date"])
    _ok("tickets_by_org_date", mart.count(), 0)
    return mart


def build_gold_genai_tokens_by_org_date(spark: SparkSession, paths: Paths) -> DataFrame:
    events = spark.read.parquet(str(paths.silver / "usage_events"))
    mart = (
        events.filter((F.col("service") == "genai") | (F.col("genai_tokens") > 0))
        .groupBy("org_id", "usage_date", "org_name", "plan_tier", "hq_region")
        .agg(
            F.count("*").alias("event_count"),
            F.sum("requests").alias("requests"),
            F.sum("genai_tokens").alias("genai_tokens"),
            F.round(F.sum("daily_cost_usd"), 4).alias("estimated_token_cost_usd"),
            F.sum("carbon_kg").alias("carbon_kg"),
        )
        .withColumn("updated_at", F.current_timestamp())
    )
    write_parquet(mart, paths.gold / "genai_tokens_by_org_date", ["usage_date"])
    _ok("genai_tokens_by_org_date", mart.count(), 0)
    return mart


def write_cassandra_table(
    df: DataFrame,
    session,
    table: str,
    columns: Iterable[str],
    batch_size: int = 500,
    concurrency: int = 8,
) -> int:
    from cassandra.concurrent import execute_concurrent_with_args

    cols = list(columns)
    placeholders = ", ".join(["?"] * len(cols))
    column_list = ", ".join(cols)
    prepared = session.prepare(f"INSERT INTO {table} ({column_list}) VALUES ({placeholders})")

    def flush(batch):
        if not batch:
            return 0
        results = execute_concurrent_with_args(
            session,
            prepared,
            batch,
            concurrency=concurrency,
            raise_on_first_error=False,
        )
        failures = [result for success, result in results if not success]
        if failures:
            sample = failures[0]
            raise RuntimeError(f"Cassandra load failed for {len(failures)} rows in {table}. First error: {sample}")
        return len(batch)

    written = 0
    batch = []
    for row in df.select(*cols).toLocalIterator():
        batch.append(tuple(getattr(row, col) for col in cols))
        if len(batch) >= batch_size:
            written += flush(batch)
            batch = []
    written += flush(batch)
    return written


def load_to_cassandra_foreachbatch(
    spark: SparkSession,
    parquet_path: Path,
    session,
    table: str,
    columns: Iterable[str],
    checkpoint_path: Path,
    batch_size: int = 500,
    concurrency: int = 8,
) -> int:
    cols = list(columns)
    total: list[int] = [0]

    def write_batch(batch_df: DataFrame, batch_id: int) -> None:
        written = write_cassandra_table(batch_df, session, table, cols, batch_size, concurrency)
        total[0] += written

    schema = spark.read.parquet(str(parquet_path)).schema
    query = (
        spark.readStream.schema(schema)
        .parquet(str(parquet_path))
        .writeStream.foreachBatch(write_batch)
        .trigger(availableNow=True)
        .option("checkpointLocation", str(checkpoint_path))
        .start()
    )
    query.awaitTermination()
    return total[0]


def write_cassandra(
    spark: SparkSession,
    paths: Paths,
    hosts: str,
    username: Optional[str],
    password: Optional[str],
    keyspace: str,
    table: str,
) -> None:
    from cassandra.auth import PlainTextAuthProvider
    from cassandra.cluster import Cluster

    host_list = [host.strip() for host in hosts.split(",") if host.strip()]
    auth_provider = PlainTextAuthProvider(username, password) if username and password else None
    cluster = Cluster(
        host_list,
        auth_provider=auth_provider,
        connect_timeout=30,
        control_connection_timeout=30,
    )
    session = cluster.connect(keyspace)
    session.default_timeout = 60

    columns = [
        "org_id", "usage_date", "service", "org_name", "plan_tier", "hq_region",
        "event_count", "requests", "total_value", "cpu_hours", "storage_gb_hours",
        "genai_tokens", "carbon_kg", "daily_cost_usd", "anomaly_event_count", "updated_at",
    ]
    try:
        load_to_cassandra_foreachbatch(
            spark,
            paths.gold / table,
            session,
            table,
            columns,
            paths.checkpoint_out / "cassandra" / table,
        )
    finally:
        session.shutdown()
        cluster.shutdown()


def print_idempotence_evidence(spark: SparkSession, paths: Paths) -> None:
    targets = {
        "bronze / usage_events_stream": paths.bronze / "usage_events_stream",
        "silver / usage_events": paths.silver / "usage_events",
        "gold  / org_daily_usage_by_service": paths.gold / "org_daily_usage_by_service",
        "gold  / revenue_by_org_month": paths.gold / "revenue_by_org_month",
        "gold  / cost_anomaly_mart": paths.gold / "cost_anomaly_mart",
        "gold  / tickets_by_org_date": paths.gold / "tickets_by_org_date",
        "gold  / genai_tokens_by_org_date": paths.gold / "genai_tokens_by_org_date",
    }
    for name, path in targets.items():
        if path.exists():
            if name == "bronze / usage_events_stream":
                count = read_parquet_dir(spark, path).count()
            else:
                count = spark.read.parquet(str(path)).count()
            _ok(name, count, 0)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run TP2 Cloud Provider Analytics pipeline.")
    parser.add_argument("--landing", default="cloud_provider_challenge_dataset_v1/datalake/landing")
    parser.add_argument("--datalake-out", default="cloud_provider_challenge_dataset_v1/datalake")
    parser.add_argument("--checkpoint-out", default="cloud_provider_challenge_dataset_v1/checkpoints")
    parser.add_argument("--write-cassandra", action="store_true")
    parser.add_argument("--cassandra-hosts", default="")
    parser.add_argument("--cassandra-username", default=None)
    parser.add_argument("--cassandra-password", default=None)
    parser.add_argument("--cassandra-keyspace", default="cloud_analytics")
    parser.add_argument("--cassandra-table", default="org_daily_usage_by_service")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    paths = Paths(Path(args.landing), Path(args.datalake_out), Path(args.checkpoint_out))

    _banner()
    pipeline_start = time.perf_counter()

    with _step(1, _TOTAL_STEPS, "Spark"):
        spark = build_spark("big-data-tp2-cloud-provider")

    with _step(2, _TOTAL_STEPS, "Bronze · Batch CSV"):
        ingest_batch_to_bronze(spark, paths)

    with _step(3, _TOTAL_STEPS, "Bronze · Streaming (usage_events)"):
        ingest_usage_stream_to_bronze(spark, paths)

    with _step(4, _TOTAL_STEPS, "Silver · Validación y quarantine"):
        build_silver_events(spark, paths)

    with _step(5, _TOTAL_STEPS, "Gold · Marts"):
        build_gold_finops(spark, paths)
        build_gold_revenue_by_org_month(spark, paths)
        build_gold_cost_anomaly_mart(spark, paths)
        build_gold_tickets_by_org_date(spark, paths)
        build_gold_genai_tokens_by_org_date(spark, paths)

    with _step(6, _TOTAL_STEPS, "Evidencia de idempotencia"):
        print_idempotence_evidence(spark, paths)

    if args.write_cassandra:
        if not args.cassandra_hosts:
            raise ValueError("--cassandra-hosts is required when --write-cassandra is enabled")
        write_cassandra(
            spark,
            paths,
            args.cassandra_hosts,
            args.cassandra_username,
            args.cassandra_password,
            args.cassandra_keyspace,
            args.cassandra_table,
        )

    spark.stop()
    total = time.perf_counter() - pipeline_start
    print()
    print(_c("1;32", "=" * _W))
    print(_c("1;32", f"  Pipeline completado en {total:.1f}s"))
    print(_c("1;32", "=" * _W))


if __name__ == "__main__":
    main()
