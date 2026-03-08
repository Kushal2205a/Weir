"""
proxy/approval.py
Weir — Task 0.5: Approval gate.

Inserts a pending intercept record into Supabase, then polls until a
developer approves or blocks it from the dashboard, or until the
configured timeout expires.
"""

import asyncio
import json
import logging

import aiohttp

from config import ProxyConfig
from impact import generate_impact

log = logging.getLogger("weir.approval")

POLL_INTERVAL_SECONDS = 0.5
SUPABASE_INTERCEPTS_PATH = "/rest/v1/intercepts"


def _supabase_headers(cfg: ProxyConfig) -> dict[str, str]:
    return {
        "apikey": cfg.supabase_key,
        "Authorization": f"Bearer {cfg.supabase_key}",
        "Content-Type": "application/json",
        "Prefer": "return=representation",
    }


async def _insert_intercept(
    session: aiohttp.ClientSession,
    sql: str,
    query_type: str,
    dry_run_result: dict,
    impact: str,
    cfg: ProxyConfig,
    agent_classification: str,
) -> str | None:
    """
    POST the intercept record to Supabase.
    Returns the new record's UUID, or None if the insert failed.
    """
    payload = {
        "query_type": query_type,
        "original_sql": sql,
        "impact": impact,
        "dry_run": dry_run_result,
        "status": "pending",
        "agent_classification": agent_classification,
    }

    url = cfg.supabase_url + SUPABASE_INTERCEPTS_PATH
    try:
        async with session.post(url, headers=_supabase_headers(cfg), json=payload) as resp:
            if resp.status not in (200, 201):
                body = await resp.text()
                log.error("Supabase insert returned %d: %s", resp.status, body)
                return None

            records = await resp.json()
            return records[0]["id"]

    except Exception as exc:
        log.error("Supabase insert failed: %s", exc)
        return None


async def _poll_for_decision(
    session: aiohttp.ClientSession,
    intercept_id: str,
    cfg: ProxyConfig,
) -> str:
    """
    Poll Supabase every 500 ms until the status leaves 'pending'
    or the approval timeout expires.

    Returns the final status string: "approved" | "blocked" | "timeout".
    """
    url = (
        cfg.supabase_url
        + SUPABASE_INTERCEPTS_PATH
        + f"?id=eq.{intercept_id}&select=status"
    )
    # Strip the Prefer header for plain GET requests
    headers = {k: v for k, v in _supabase_headers(cfg).items() if k != "Prefer"}

    elapsed = 0.0
    while elapsed < cfg.approval_timeout:
        await asyncio.sleep(POLL_INTERVAL_SECONDS)
        elapsed += POLL_INTERVAL_SECONDS

        try:
            async with session.get(url, headers=headers) as resp:
                if resp.status != 200:
                    log.warning("Supabase poll returned %d — retrying", resp.status)
                    continue

                records = await resp.json()
                if not records:
                    log.warning("Supabase poll returned empty list for id=%s", intercept_id)
                    continue

                status = records[0]["status"]
                if status != "pending":
                    return status

        except Exception as exc:
            log.warning("Supabase poll error: %s — retrying", exc)

    return "timeout"


async def _mark_timeout(
    session: aiohttp.ClientSession,
    intercept_id: str,
    cfg: ProxyConfig,
) -> None:
    """PATCH the intercept record to status='timeout' after the deadline expires."""
    url = cfg.supabase_url + SUPABASE_INTERCEPTS_PATH + f"?id=eq.{intercept_id}"
    headers = {k: v for k, v in _supabase_headers(cfg).items() if k != "Prefer"}
    try:
        async with session.patch(url, headers=headers, json={"status": "timeout"}) as resp:
            if resp.status not in (200, 204):
                log.warning("Failed to mark intercept %s as timeout: %d", intercept_id, resp.status)
    except Exception as exc:
        log.warning("Could not update timeout status for %s: %s", intercept_id, exc)


async def request_approval(
    sql: str,
    query_type: str,
    dry_run_result: dict,
    cfg: ProxyConfig,
    agent_classification: str = "UNKNOWN",
) -> str:
    """
    Insert a pending intercept into Supabase and wait for a developer decision.

    Returns "approved" | "blocked" | "timeout".

    If Supabase is unreachable, logs a clear warning and returns "approved"
    so Weir's own infrastructure failure never silently blocks a query.
    """
    if not cfg.supabase_url or not cfg.supabase_key:
        log.warning("Supabase unavailable — allowing query through (no credentials configured)")
        return "approved"

    impact = generate_impact(
        query_type=query_type,
        tables_affected=dry_run_result.get("tables_affected", []),
        affected_count=dry_run_result.get("affected_count", -1),
        sample_rows=dry_run_result.get("sample_rows", []),
    )

    async with aiohttp.ClientSession() as session:
        intercept_id = await _insert_intercept(
            session, sql, query_type, dry_run_result, impact, cfg, agent_classification
        )

        if intercept_id is None:
            log.warning("Supabase unavailable — allowing query through")
            return "approved"

        log.info("Waiting for approval  intercept_id=%s  timeout=%ds", intercept_id, cfg.approval_timeout)

        decision = await _poll_for_decision(session, intercept_id, cfg)

        if decision == "timeout":
            log.warning("TIMEOUT — no decision after %ds, auto-blocking", cfg.approval_timeout)
            await _mark_timeout(session, intercept_id, cfg)

        return decision