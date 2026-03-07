"""
proxy/config.py
Loads Weir proxy configuration from environment variables.
"""

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class ProxyConfig:
    listen_host: str
    listen_port: int
    target_host: str
    target_port: int
    target_db: str

    def __str__(self) -> str:
        return (
            f"Weir Proxy  listen={self.listen_host}:{self.listen_port}  "
            f"→  target={self.target_host}:{self.target_port}/{self.target_db}"
        )


def load_config() -> ProxyConfig:
    """
    Read configuration from environment variables.

    Required env vars (with defaults):
        WEIR_LISTEN_HOST   interface to bind on        (default: 0.0.0.0)
        WEIR_LISTEN_PORT    port to listen on           (default: 5433)
        WEIR_TARGET_HOST    upstream PostgreSQL host    (default: localhost)
        WEIR_TARGET_PORT    upstream PostgreSQL port    (default: 5432)
        WEIR_TARGET_DB      upstream database name      (default: postgres)
    """
    return ProxyConfig(
        listen_host=os.getenv("WEIR_LISTEN_HOST", "0.0.0.0"),
        listen_port=int(os.getenv("WEIR_LISTEN_PORT", "5433")),
        target_host=os.getenv("WEIR_TARGET_HOST", "localhost"),
        target_port=int(os.getenv("WEIR_TARGET_PORT", "5432")),
        target_db=os.getenv("WEIR_TARGET_DB", "postgres"),
    )