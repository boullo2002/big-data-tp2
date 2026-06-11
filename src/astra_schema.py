"""Crea keyspace/tablas en Astra si faltan (con fallback a default_keyspace)."""

from __future__ import annotations

import json
import zipfile
from pathlib import Path

from cassandra.cluster import Cluster, Session


def local_dc_from_bundle(bundle_path: str) -> str | None:
    with zipfile.ZipFile(bundle_path) as bundle:
        config = json.loads(bundle.read("config.json"))
    return config.get("localDC")


def keyspace_exists(session: Session, keyspace: str) -> bool:
    rows = session.execute(
        "SELECT keyspace_name FROM system_schema.keyspaces WHERE keyspace_name = %s",
        (keyspace,),
    )
    return any(row.keyspace_name == keyspace for row in rows)


def create_keyspace_if_missing(
    session: Session,
    keyspace: str,
    bundle_path: str,
) -> str:
    if keyspace_exists(session, keyspace):
        print(f"Keyspace '{keyspace}' ya existe.")
        return keyspace

    local_dc = local_dc_from_bundle(bundle_path)
    attempts: list[str] = []
    if local_dc:
        attempts.append(
            f"CREATE KEYSPACE IF NOT EXISTS {keyspace} "
            f"WITH replication = {{'class': 'NetworkTopologyStrategy', '{local_dc}': 3}}"
        )
    attempts.append(
        f"CREATE KEYSPACE IF NOT EXISTS {keyspace} "
        "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}"
    )

    for statement in attempts:
        try:
            session.execute(statement)
            if keyspace_exists(session, keyspace):
                print(f"Keyspace '{keyspace}' creado.")
                return keyspace
        except Exception as exc:
            print(f"No se pudo crear keyspace '{keyspace}': {exc}")

    fallback = "default_keyspace"
    if keyspace_exists(session, fallback):
        print(
            f"Usando '{fallback}' (el token no puede crear keyspaces nuevos en Astra; "
            "las tablas se crean ahi)."
        )
        return fallback

    raise RuntimeError(
        f"No existe '{keyspace}' y no se pudo crear ni usar '{fallback}'."
    )


def create_tables_if_missing(session: Session, repo_root: Path) -> None:
    ddl_path = repo_root / "cql" / "02_create_tables.cql"
    for raw_statement in ddl_path.read_text(encoding="utf-8").split(";"):
        statement = raw_statement.strip()
        if not statement or statement.upper().startswith("USE "):
            continue
        session.execute(statement)
        table_name = statement.split("EXISTS", 1)[-1].split("(", 1)[0].replace("CREATE TABLE IF NOT EXISTS", "").strip()
        print(f"Tabla lista: {table_name}")


def ensure_astra_schema(
    cluster: Cluster,
    keyspace: str,
    bundle_path: str,
    repo_root: Path,
) -> tuple[Session, str]:
    bootstrap = cluster.connect()
    try:
        resolved_keyspace = create_keyspace_if_missing(bootstrap, keyspace, bundle_path)
    finally:
        bootstrap.shutdown()

    session = cluster.connect(resolved_keyspace)
    create_tables_if_missing(session, repo_root)
    return session, resolved_keyspace
