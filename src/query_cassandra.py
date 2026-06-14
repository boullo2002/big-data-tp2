#!/usr/bin/env python3
"""Ejecuta las consultas demo contra AstraDB con parámetros configurables."""

from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path

logging.getLogger("cassandra").setLevel(logging.ERROR)

# ---------------------------------------------------------------------------
# Output helpers
# ---------------------------------------------------------------------------

_USE_COLOR = sys.stdout.isatty()
_W = 68


def _c(code: str, text: str) -> str:
    return f"\033[{code}m{text}\033[0m" if _USE_COLOR else text


def _banner() -> None:
    print(_c("1;36", "=" * _W))
    print(_c("1;36", "  Consultas AstraDB"))
    print(_c("1;36", "=" * _W))
    print()


_CONSIGNA_QUERIES = {"1", "2", "3", "4", "5"}


def _section_header(label: str) -> None:
    print(f"\n{_c('1;35', f'▸ {label}')}")
    print(_c("35", "─" * _W))


def _query_header(num: int, total: int, title: str, org_id: str, min_date: str, max_date: str) -> None:
    tag = _c("1;33", f"[{num}/{total}]")
    print(f"\n{tag} {_c('1', title)}")
    print(_c("90", "─" * _W))
    date_str = f"{min_date} → {max_date}" if min_date else "últimos 14 días"
    print(
        f"  {_c('90', 'org:')} {_c('36', org_id)}"
        f"  {_c('90', 'rango:')} {_c('36', date_str)}"
    )


def _info(msg: str) -> None:
    print(f"  {_c('36', '→')}  {msg}")


# ---------------------------------------------------------------------------
# Defaults dinámicos desde Gold Parquet (sin Spark)
# ---------------------------------------------------------------------------

def _read_gold_defaults(datalake: str, table: str, date_col: str | None) -> tuple[str, str, str]:
    """Devuelve (org_id, min_date, max_date) leyendo el parquet de la tabla Gold."""
    import pandas as pd
    parquet_path = Path(datalake) / "gold" / table
    if not parquet_path.exists():
        raise FileNotFoundError(
            f"No se encontró el parquet Gold en '{parquet_path}'.\n"
            f"Ejecutá desde la raíz del repo o pasá --datalake con la ruta correcta."
        )
    cols = ["org_id"] + ([date_col] if date_col else [])
    df = pd.read_parquet(parquet_path, columns=cols)
    org_id = str(df["org_id"].iloc[0])
    if not date_col:
        return (org_id, "", "")
    dates = df[date_col].astype(str)
    return (org_id, dates.min()[:10], dates.max()[:10])


def _point_date(max_date: str) -> str:
    return max_date


def _subtract_days(date_str: str, days: int) -> str:
    from datetime import date, timedelta
    d = date.fromisoformat(date_str)
    return (d - timedelta(days=days)).isoformat()


# ---------------------------------------------------------------------------
# Helpers de resultado
# ---------------------------------------------------------------------------

def _rows_to_str(rows) -> str:
    """Convierte resultados de Cassandra a tabla formateada con tabulate."""
    from tabulate import tabulate

    if not rows:
        return _c("33", "  (sin resultados)")

    records = []
    for row in rows:
        record: dict = {}
        for k, v in row._asdict().items():
            if hasattr(v, "date") and callable(v.date):
                record[k] = v.date().isoformat()
            else:
                record[k] = v
        records.append(record)

    headers = list(records[0].keys())
    rows_data = [[r[h] for h in headers] for r in records]
    return tabulate(rows_data, headers=headers, tablefmt="rounded_outline", floatfmt=".4f")


# ---------------------------------------------------------------------------
# Definición de las 7 consultas
# ---------------------------------------------------------------------------

QUERY_DEFS = {
    "1": {
        "title": "Costos y requests diarios por org y servicio (rango de fechas)",
        "table": "org_daily_usage_by_service",
        "date_col": "usage_date",
        "cql": (
            "SELECT org_id, usage_date, service, requests, cpu_hours, "
            "storage_gb_hours, daily_cost_usd, event_count "
            "FROM org_daily_usage_by_service "
            "WHERE org_id = %s AND usage_date >= %s AND usage_date <= %s"
        ),
        "params": lambda p: (p["org_id"], p["min_date"], p["max_date"]),
    },
    "2": {
        "title": "Top-N servicios por costo acumulado (últimos 14 días)",
        "table": "org_service_cost_14d",
        "defaults_table": "org_daily_usage_by_service",
        "date_col": None,
        "cql": (
            "SELECT org_id, service, total_cost_usd, event_count, requests "
            "FROM org_service_cost_14d "
            "WHERE org_id = %s LIMIT %s"
        ),
        "params": lambda p: (p["org_id"], p["top_n"]),
    },
    "3": {
        "title": "Evolución de tickets críticos y SLA breach por día (últimos 30 días)",
        "table": "tickets_critical_by_org_date",
        "defaults_table": "tickets_by_org_date",
        "date_col": "ticket_date",
        "window_days": 30,
        "cql": (
            "SELECT org_id, ticket_date, ticket_count, resolved_ticket_count, "
            "open_ticket_count, sla_breach_count, sla_breach_rate, avg_csat "
            "FROM tickets_critical_by_org_date "
            "WHERE org_id = %s AND ticket_date >= %s AND ticket_date <= %s"
        ),
        "params": lambda p: (p["org_id"], p["min_date"], p["max_date"]),
    },
    "4": {
        "title": "Revenue mensual por organización",
        "table": "revenue_by_org_month",
        "date_col": "month",
        "cql": (
            "SELECT org_id, month, gross_revenue_usd, credits_usd, taxes_usd, "
            "net_revenue_usd, invoice_count "
            "FROM revenue_by_org_month "
            "WHERE org_id = %s AND month >= %s AND month <= %s"
        ),
        "params": lambda p: (p["org_id"], p["min_date"], p["max_date"]),
    },
    "5": {
        "title": "Tokens GenAI y costo estimado por día",
        "table": "genai_tokens_by_org_date",
        "date_col": "usage_date",
        "cql": (
            "SELECT org_id, usage_date, genai_tokens, estimated_token_cost_usd, "
            "requests, carbon_kg "
            "FROM genai_tokens_by_org_date "
            "WHERE org_id = %s AND usage_date >= %s AND usage_date <= %s"
        ),
        "params": lambda p: (p["org_id"], p["min_date"], p["max_date"]),
    },
    "6": {
        "title": "Detalle de servicios y anomalías en una fecha puntual",
        "table": "org_daily_usage_by_service",
        "date_col": "usage_date",
        "cql": (
            "SELECT org_id, usage_date, service, requests, cpu_hours, storage_gb_hours, "
            "genai_tokens, carbon_kg, daily_cost_usd, anomaly_event_count, event_count "
            "FROM org_daily_usage_by_service "
            "WHERE org_id = %s AND usage_date = %s"
        ),
        "params": lambda p: (p["org_id"], p["point_date"]),
    },
    "7": {
        "title": "Eventos anómalos de costo",
        "table": "cost_anomaly_mart",
        "date_col": "usage_date",
        "cql": (
            "SELECT org_id, usage_date, service, anomaly_event_count, "
            "negative_cost_event_count, high_cost_event_count, "
            "anomaly_cost_usd, max_event_cost_usd, min_event_cost_usd "
            "FROM cost_anomaly_mart "
            "WHERE org_id = %s AND usage_date >= %s AND usage_date <= %s"
        ),
        "params": lambda p: (p["org_id"], p["min_date"], p["max_date"]),
    },
}


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Ejecuta consultas demo contra AstraDB.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Ejemplos:\n"
            "  python -m src.query_cassandra\n"
            "  python -m src.query_cassandra --query 1 3 --org-id org_4zw9xa3k\n"
            "  python -m src.query_cassandra --min-date 2025-08-01 --max-date 2025-08-31\n"
        ),
    )
    parser.add_argument("--datalake", default="cloud_provider_challenge_dataset_v1/datalake",
                        help="Ruta al datalake (para defaults dinámicos desde Gold Parquet)")
    parser.add_argument("--client-id", default=None)
    parser.add_argument("--client-secret", default=None)
    parser.add_argument("--bundle", default=None, help="Path al secure-connect-*.zip")
    parser.add_argument("--keyspace", default=None)
    parser.add_argument("--org-id", default=None, help="org_id a consultar (default: primero del mart)")
    parser.add_argument("--min-date", default=None, help="Fecha mínima YYYY-MM-DD")
    parser.add_argument("--max-date", default=None, help="Fecha máxima YYYY-MM-DD")
    parser.add_argument("--point-date", default=None, help="Fecha puntual para consulta 6 (default: max_date)")
    parser.add_argument("--top-n", type=int, default=5, metavar="N",
                        help="Servicios a mostrar en query Top-N (consulta 2, default: 5)")
    parser.add_argument(
        "--query", nargs="+", type=str, choices=list(QUERY_DEFS), default=None,
        metavar="ID", help="Consultas a ejecutar: 1 2 3 4 5 6 7 (default: todas)",
    )
    return parser.parse_args()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    args = parse_args()
    repo_root = Path(__file__).parent.parent

    _banner()
    t_start = time.perf_counter()

    from src.astra_config import configure_astra_env

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

    _info(f"Conectando a AstraDB · keyspace={keyspace} ...")
    cluster = Cluster(
        cloud={"secure_connect_bundle": bundle},
        auth_provider=PlainTextAuthProvider(client_id, client_secret),
        connect_timeout=30,
        control_connection_timeout=30,
    )
    session = cluster.connect(keyspace)
    session.default_timeout = 60
    _info(f"Conexión establecida ({time.perf_counter() - t_start:.1f}s)")

    queries_to_run = args.query or list(QUERY_DEFS)
    total_rows = 0
    _printed_consigna_header = False
    _printed_extra_header = False

    try:
        for q_num in queries_to_run:
            q = QUERY_DEFS[q_num]
            if q_num in _CONSIGNA_QUERIES and not _printed_consigna_header:
                _section_header("Consultas de la consigna")
                _printed_consigna_header = True
            elif q_num not in _CONSIGNA_QUERIES and not _printed_extra_header:
                _section_header("Consultas extra")
                _printed_extra_header = True

            org_id, dyn_min, dyn_max = _read_gold_defaults(args.datalake, q.get("defaults_table", q["table"]), q["date_col"])
            effective_max = args.max_date or dyn_max
            if not args.min_date and q.get("window_days") and effective_max:
                effective_min = _subtract_days(effective_max, q["window_days"] - 1)
            else:
                effective_min = args.min_date or dyn_min
            resolved = {
                "org_id":     args.org_id or org_id,
                "min_date":   effective_min,
                "max_date":   effective_max,
                "point_date": args.point_date or _point_date(effective_max),
                "top_n":      args.top_n,
            }

            _query_header(q_num, len(queries_to_run), q["title"],
                          resolved["org_id"], resolved["min_date"], resolved["max_date"])

            try:
                t1 = time.perf_counter()
                rows = list(session.execute(q["cql"], q["params"](resolved)))
                elapsed = time.perf_counter() - t1
                total_rows += len(rows)
                print(f"\n{_rows_to_str(rows)}")
                print(f"\n  {_c('90', f'{len(rows)} fila(s)  ·  {elapsed:.2f}s')}")
            except Exception as exc:
                print(f"  {_c('31', 'ERROR')}: {exc}", file=sys.stderr)

    finally:
        session.shutdown()
        cluster.shutdown()

    elapsed_total = time.perf_counter() - t_start
    print()
    print(_c("1;32", "=" * _W))
    print(_c("1;32", f"  {len(queries_to_run)} consulta(s) · {total_rows:,} filas totales · {elapsed_total:.1f}s"))
    print(_c("1;32", "=" * _W))


if __name__ == "__main__":
    main()
