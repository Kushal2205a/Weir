"""
proxy/config.py — Weir local edition
"""

import os
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class ProxyConfig:
    listen_host: str
    listen_port: int
    target_host: str
    target_port: int
    target_db: str
    target_user: str
    target_password: str
    dashboard_url: str
    approval_timeout: int

    def __str__(self) -> str:
        return (
            f"Weir Proxy  listen={self.listen_host}:{self.listen_port}  "
            f"→  {self.target_user}@{self.target_host}:{self.target_port}/{self.target_db}"
        )


def load_config() -> ProxyConfig:
    return ProxyConfig(
        listen_host=os.getenv("WEIR_LISTEN_HOST", "0.0.0.0"),
        listen_port=int(os.getenv("WEIR_LISTEN_PORT", "5455")),
        target_host=os.getenv("WEIR_TARGET_HOST", "localhost"),
        target_port=int(os.getenv("WEIR_TARGET_PORT", "5432")),
        target_db=os.getenv("WEIR_TARGET_DB", "postgres"),
        target_user=os.getenv("WEIR_TARGET_USER", "postgres"),
        target_password=os.getenv("WEIR_TARGET_PASSWORD", ""),
        dashboard_url=os.getenv("WEIR_DASHBOARD_URL", "http://localhost:8000"),
        approval_timeout=int(os.getenv("WEIR_APPROVAL_TIMEOUT", "60")),
    )