#!/usr/bin/env python3
"""TP2 Cloud Provider Analytics MVP.

Pipeline end-to-end:
Landing -> Bronze -> Silver -> Gold -> Cassandra/AstraDB.
"""

from __future__ import annotations

import argparse
import os
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, Optional

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
    print(f"Spark {spark.version} inicializado con Java {java_version}")
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
    writer = df.write.mode("overwrite")
    cols = list(partition_cols)
    if cols:
        writer = writer.partitionBy(*cols)
    writer.parquet(str(path))


def ingest_batch_to_bronze(spark: SparkSession, paths: Paths) -> None:
    for name, (schema, dedupe_keys) in CSV_SPECS.items():
        input_path = paths.landing / f"{name}.csv"
        if not input_path.exists():
            continue
        df = read_csv(spark, input_path, schema)
        df = normalize_batch_df(name, add_technical_columns(df)).dropDuplicates(list(dedupe_keys))
        output_path = paths.bronze / name
        write_parquet(df, output_path, ["ingest_date"])
        print(f"bronze_batch.{name}: {df.count()} rows -> {output_path}")


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
        .partitionBy("usage_date")
        .outputMode("append")
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()
    print(f"bronze_stream.usage_events -> {output_path}")


def build_silver_events(spark: SparkSession, paths: Paths) -> None:
    events = spark.read.parquet(str(paths.bronze / "usage_events_stream"))
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
    print(f"silver.usage_events: {valid.count()} rows")
    print(f"quarantine.usage_events: {quarantine.count()} rows")


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
    print(f"gold.org_daily_usage_by_service: {mart.count()} rows")
    return mart


def write_cassandra(
    mart: DataFrame,
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
    cluster = Cluster(host_list, auth_provider=auth_provider)
    session = cluster.connect(keyspace)

    insert_cql = f"""
        INSERT INTO {table} (
            org_id, usage_date, service, org_name, plan_tier, hq_region,
            event_count, requests, total_value, cpu_hours, storage_gb_hours,
            genai_tokens, carbon_kg, daily_cost_usd, anomaly_event_count, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    prepared = session.prepare(insert_cql)

    for row in mart.collect():
        session.execute(
            prepared,
            (
                row.org_id,
                row.usage_date,
                row.service,
                row.org_name,
                row.plan_tier,
                row.hq_region,
                row.event_count,
                row.requests,
                row.total_value,
                row.cpu_hours,
                row.storage_gb_hours,
                row.genai_tokens,
                row.carbon_kg,
                row.daily_cost_usd,
                row.anomaly_event_count,
                row.updated_at,
            ),
        )
    session.shutdown()
    cluster.shutdown()
    print(f"serving.cassandra: wrote {mart.count()} rows to {keyspace}.{table}")


def print_idempotence_evidence(spark: SparkSession, paths: Paths) -> None:
    targets = {
        "bronze_usage_events_stream": paths.bronze / "usage_events_stream",
        "silver_usage_events": paths.silver / "usage_events",
        "gold_org_daily_usage_by_service": paths.gold / "org_daily_usage_by_service",
    }
    for name, path in targets.items():
        if path.exists():
            count = spark.read.parquet(str(path)).count()
            print(f"idempotence_count.{name}: {count}")


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
    spark = build_spark("big-data-tp2-cloud-provider")

    ingest_batch_to_bronze(spark, paths)
    ingest_usage_stream_to_bronze(spark, paths)
    build_silver_events(spark, paths)
    mart = build_gold_finops(spark, paths)
    print_idempotence_evidence(spark, paths)

    if args.write_cassandra:
        if not args.cassandra_hosts:
            raise ValueError("--cassandra-hosts is required when --write-cassandra is enabled")
        write_cassandra(
            mart,
            args.cassandra_hosts,
            args.cassandra_username,
            args.cassandra_password,
            args.cassandra_keyspace,
            args.cassandra_table,
        )

    spark.stop()


if __name__ == "__main__":
    main()
