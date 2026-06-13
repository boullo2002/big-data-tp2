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
    print(_c("1;36", "  TP2  ·  Carga a AstraDB  ·  Big Data ITBA"))
    print(_c("1;36", "=" * _W))
    print()


def _header(label: str) -> None:
    print(f"\n{_c('1;33', '●')} {_c('1', label)}")
    print(_c("90", "─" * _W))


def _ok(label: str, rows: int, elapsed: float) -> None:
    rows_str = _c("32", f"{rows:>8,} filas")
    time_str = _c("90", f"({elapsed:.1f}s)")
    print(f"  {_c('32', '✓')}  {label:<38} {rows_str}  {time_str}")


def _skip(label: str) -> None:
    print(f"  {_c('33', '○')}  {label:<38} {_c('90', '(parquet no encontrado)')}")


def _info(msg: str) -> None:
    print(f"  {_c('36', '→')}  {msg}")


# ---------------------------------------------------------------------------

CASSANDRA_TABLES: dict[str, list[str]] = {
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
    from src.tp2_pipeline import Paths, build_spark, load_to_cassandra_foreachbatch

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

    paths = Paths(
        landing=Path(args.datalake) / "landing",
        datalake_out=Path(args.datalake),
        checkpoint_out=Path(args.checkpoint_out),
    )

    tables_to_load = args.tables or list(CASSANDRA_TABLES)
    _header(f"Carga · {len(tables_to_load)} tabla(s)")

    total_rows = 0
    try:
        for table in tables_to_load:
            parquet_path = paths.gold / table
            if not parquet_path.exists():
                _skip(table)
                continue
            checkpoint_path = paths.checkpoint_out / "cassandra" / table
            if checkpoint_path.exists():
                shutil.rmtree(checkpoint_path)
            t1 = time.perf_counter()
            rows = load_to_cassandra_foreachbatch(
                spark,
                parquet_path,
                session,
                table,
                CASSANDRA_TABLES[table],
                checkpoint_path,
            )
            total_rows += rows
            _ok(table, rows, time.perf_counter() - t1)
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
