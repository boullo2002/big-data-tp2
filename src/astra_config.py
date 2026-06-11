"""Carga configuracion local de AstraDB sin commitear secretos."""

from __future__ import annotations

import os
from pathlib import Path


def _load_env_file(env_path: Path) -> None:
    if not env_path.is_file():
        return

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        key, separator, value = line.partition("=")
        if not separator:
            continue
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def _default_bundle_path(repo_root: Path) -> Path | None:
    explicit = repo_root / "secure-connect-tp2-bigdata.zip"
    if explicit.is_file():
        return explicit

    matches = sorted(repo_root.glob("secure-connect-*.zip"))
    if len(matches) == 1:
        return matches[0]
    return None


def configure_astra_env(repo_root: Path) -> dict[str, str | None]:
    """Carga `.env` y resuelve el Secure Connect Bundle en el repo."""
    _load_env_file(repo_root / ".env")

    bundle_path = os.getenv("ASTRA_SECURE_CONNECT_BUNDLE")
    if not bundle_path:
        default_bundle = _default_bundle_path(repo_root)
        if default_bundle is not None:
            bundle_path = str(default_bundle)
            os.environ["ASTRA_SECURE_CONNECT_BUNDLE"] = bundle_path

    if not os.getenv("ASTRA_KEYSPACE"):
        os.environ["ASTRA_KEYSPACE"] = "cloud_analytics"

    return {
        "client_id": os.getenv("ASTRA_CLIENT_ID"),
        "client_secret": os.getenv("ASTRA_CLIENT_SECRET"),
        "secure_connect_bundle": bundle_path,
        "keyspace": os.getenv("ASTRA_KEYSPACE", "cloud_analytics"),
    }
