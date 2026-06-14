#!/usr/bin/env python3
"""Carga los marts Gold a AstraDB usando foreachBatch."""

from __future__ import annotations

import argparse
import shutil
import sys
import time
from pathlib import Path

# ---------------------------------------------------------------------------
# Output helpers
# ---------------------------------------------------------------------------

_USE_COLOR = sys.stdout.isatty()
_W = 68


def _c(code: str, text: str) -> str:
    return f"\033[{code}m{text}\033[0m" if _USE_COLOR else text


def _banner() -> None:
    print(_c("1;36", "=" * _W))
    print(_c("1;36", "  Carga a AstraDB"))
    print(_c("1;36", "=" * _W))
    print()


def _header(label: str) -> None:
    print(f"\n{_c('1;33', '●')} {_c('1', label)}")
    print(_c("90", "─" * _W))


def _ok(label: str, rows: int, elapsed: float) -> None:
    rows_str = _c("32", f"{rows:>8,} filas")
    throughput = f"{rows / elapsed:,.0f} filas/s" if elapsed > 0 else "—"
    time_str = _c("90", f"({elapsed:.1f}s · {throughput})")
    print(f"  {_c('32', '✓')}  {label:<38} {rows_str}  {time_str}")


def _skip(label: str) -> None:
    print(f"  {_c('33', '○')}  {label:<38} {_c('90', '(parquet no encontrado)')}")


def _info(msg: str) -> None:
    print(f"  {_c('36', '→')}  {msg}")


# ---------------------------------------------------------------------------

TABLES_REQUIRING_TRUNCATE: set[str] = {"org_service_cost_14d"}

CASSANDRA_TABLES: dict[str, list[str]] = {
    "org_service_cost_14d": [
        "org_id", "total_cost_usd", "service", "org_name", "plan_tier",
        "hq_region", "event_count", "requests", "updated_at",
    ],
    "org_daily_usage_by_service": [
        "org_id", "usage_date", "service", "org_name", "plan_tier", "hq_region",
        "event_count", "requests", "total_value", "cpu_hours", "storage_gb_hours",
        "genai_tokens", "carbon_kg", "daily_cost_usd", "anomaly_event_count", "updated_at",
    ],
    "revenue_by_org_month": [
        "org_id", "month", "org_name", "industry", "plan_tier", "hq_region",
        "lifecycle_stage", "invoice_count", "gross_revenue_usd", "credits_usd",
        "taxes_usd", "net_revenue_usd", "updated_at",
    ],
    "cost_anomaly_mart": [
        "org_id", "usage_date", "service", "org_name", "plan_tier", "hq_region",
        "anomaly_event_count", "negative_cost_event_count", "high_cost_event_count",
        "anomaly_cost_usd", "max_event_cost_usd", "min_event_cost_usd", "updated_at",
    ],
    "tickets_critical_by_org_date": [
        "org_id", "ticket_date", "org_name", "industry", "plan_tier", "hq_region",
        "ticket_count", "resolved_ticket_count", "open_ticket_count",
        "sla_breach_count", "sla_breach_rate", "avg_csat", "updated_at",
    ],
    "tickets_by_org_date": [
        "org_id", "ticket_date", "category", "severity", "org_name", "industry",
        "plan_tier", "hq_region", "ticket_count", "resolved_ticket_count",
        "open_ticket_count", "sla_breach_count", "sla_breach_rate", "avg_csat", "updated_at",
    ],
    "genai_tokens_by_org_date": [
        "org_id", "usage_date", "org_name", "plan_tier", "hq_region",
        "event_count", "requests", "genai_tokens", "estimated_token_cost_usd",
        "carbon_kg", "updated_at",
    ],
}


# ---------------------------------------------------------------------------
# Derived table loaders (no intermediate Parquet — transform at load time)
# ---------------------------------------------------------------------------

def _derive_org_service_cost_14d(spark, paths):
    """Costo acumulado por org+service en los últimos 14 días, desde el Gold diario."""
    from pyspark.sql import functions as F

    df = spark.read.parquet(str(paths.gold / "org_daily_usage_by_service"))
    max_date = df.agg(F.max("usage_date")).collect()[0][0]
    cutoff = F.date_sub(F.lit(max_date), 13)
    return (
        df.filter(F.col("usage_date") >= cutoff)
        .groupBy("org_id", "service", "org_name", "plan_tier", "hq_region")
        .agg(
            F.sum("daily_cost_usd").cast("double").alias("total_cost_usd"),
            F.sum("event_count").alias("event_count"),
            F.sum("requests").cast("double").alias("requests"),
        )
        .withColumn("updated_at", F.current_timestamp())
    )


def _derive_tickets_critical_by_org_date(spark, paths):
    """Agrega tickets de severidad critical por org+fecha, desde el Gold de tickets."""
    from pyspark.sql import functions as F

    df = spark.read.parquet(str(paths.gold / "tickets_by_org_date"))
    return (
        df.filter(F.col("severity") == "critical")
        .groupBy("org_id", "ticket_date", "org_name", "industry", "plan_tier", "hq_region")
        .agg(
            F.sum("ticket_count").alias("ticket_count"),
            F.sum("resolved_ticket_count").alias("resolved_ticket_count"),
            F.sum("open_ticket_count").alias("open_ticket_count"),
            F.sum("sla_breach_count").alias("sla_breach_count"),
            (F.sum("sla_breach_count") / F.sum("ticket_count")).cast("double").alias("sla_breach_rate"),
            (F.sum(F.col("avg_csat") * F.col("ticket_count")) / F.sum("ticket_count"))
            .cast("double")
            .alias("avg_csat"),
        )
        .withColumn("updated_at", F.current_timestamp())
    )


DERIVED_TABLE_LOADERS = {
    "org_service_cost_14d": _derive_org_service_cost_14d,
    "tickets_critical_by_org_date": _derive_tickets_critical_by_org_date,
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Carga marts Gold a AstraDB via foreachBatch.")
    parser.add_argument("--datalake", default="cloud_provider_challenge_dataset_v1/datalake")
    parser.add_argument("--checkpoint-out", default="cloud_provider_challenge_dataset_v1/checkpoints")
    parser.add_argument("--client-id", default=None)
    parser.add_argument("--client-secret", default=None)
    parser.add_argument("--bundle", default=None, help="Path al secure-connect-*.zip")
    parser.add_argument("--keyspace", default=None)
    parser.add_argument("--tables", nargs="+", choices=list(CASSANDRA_TABLES), default=None,
                        help="Tablas a cargar (default: todas)")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    repo_root = Path(__file__).parent.parent

    _banner()
    pipeline_start = time.perf_counter()

    from src.astra_config import configure_astra_env
    from src.astra_schema import ensure_astra_schema
    from src.run_pipeline import Paths, build_spark, load_to_cassandra_foreachbatch, write_cassandra_table

    _header("Conexión · AstraDB")
    astra_env = configure_astra_env(repo_root)
    client_id = args.client_id or astra_env["client_id"]
    client_secret = args.client_secret or astra_env["client_secret"]
    bundle = args.bundle or astra_env["secure_connect_bundle"]
    keyspace = args.keyspace or astra_env["keyspace"]

    if not (client_id and client_secret and bundle):
        raise SystemExit(
            "Faltan credenciales Astra. Completar .env o pasar --client-id / --client-secret / --bundle."
        )

    from cassandra.auth import PlainTextAuthProvider
    from cassandra.cluster import Cluster

    t0 = time.perf_counter()
    cluster = Cluster(
        cloud={"secure_connect_bundle": bundle},
        auth_provider=PlainTextAuthProvider(client_id, client_secret),
        connect_timeout=30,
        control_connection_timeout=30,
    )
    session, _ = ensure_astra_schema(cluster, keyspace, bundle, repo_root)
    session.default_timeout = 60
    _info(f"keyspace={keyspace}  ({time.perf_counter() - t0:.1f}s)")

    _header("Spark")
    spark = build_spark("tp2-load-cassandra")
    spark.sparkContext._jvm.org.apache.log4j.LogManager \
        .getLogger("org.apache.spark.sql.catalyst.analysis.ResolveWriteToStream") \
        .setLevel(spark.sparkContext._jvm.org.apache.log4j.Level.ERROR)

    paths = Paths(
        landing=Path(args.datalake) / "landing",
        datalake_out=Path(args.datalake),
        checkpoint_out=Path(args.checkpoint_out),
    )

    tables_to_load = args.tables or list(CASSANDRA_TABLES)
    _header(f"Carga · {len(tables_to_load)} tabla(s)")

    from tqdm import tqdm

    total_rows = 0
    try:
        for table in tables_to_load:
            if table in TABLES_REQUIRING_TRUNCATE:
                session.execute(f"TRUNCATE {table}")
            t1 = time.perf_counter()

            if table in DERIVED_TABLE_LOADERS:
                df = DERIVED_TABLE_LOADERS[table](spark, paths)
                df.cache()
                total_count = df.count()
                print("\n", end="")
                with tqdm(
                    total=total_count,
                    desc=f"  {table}",
                    unit="filas",
                    colour="green",
                    dynamic_ncols=True,
                    leave=False,
                    file=sys.stdout,
                    disable=not _USE_COLOR,
                    bar_format="{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} filas",
                ) as pbar:
                    rows = write_cassandra_table(
                        df,
                        session,
                        table,
                        CASSANDRA_TABLES[table],
                        on_progress=pbar.update,
                    )
                df.unpersist()
            else:
                parquet_path = paths.gold / table
                if not parquet_path.exists():
                    _skip(table)
                    continue
                checkpoint_path = paths.checkpoint_out / "cassandra" / table
                if checkpoint_path.exists():
                    shutil.rmtree(checkpoint_path)
                total_count = spark.read.parquet(str(parquet_path)).count()
                print("\n", end="")
                with tqdm(
                    total=total_count,
                    desc=f"  {table}",
                    unit="filas",
                    colour="green",
                    dynamic_ncols=True,
                    leave=False,
                    file=sys.stdout,
                    disable=not _USE_COLOR,
                    bar_format="{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} filas",
                ) as pbar:
                    rows = load_to_cassandra_foreachbatch(
                        spark,
                        parquet_path,
                        session,
                        table,
                        CASSANDRA_TABLES[table],
                        checkpoint_path,
                        on_progress=pbar.update,
                    )

            total_rows += rows
            _ok(table, rows, time.perf_counter() - t1)
            print()
    finally:
        session.shutdown()
        cluster.shutdown()
        spark.stop()

    elapsed = time.perf_counter() - pipeline_start
    print()
    print(_c("1;32", "=" * _W))
    print(_c("1;32", f"  Carga completada · {total_rows:,} filas totales · {elapsed:.1f}s"))
    print(_c("1;32", "=" * _W))


if __name__ == "__main__":
    main()
