"""
proxy/approval.py
Weir — Approval gate.

Insert flow (Task 3.2):
  proxy → POST {WEIR_DASHBOARD_URL}/api/intercept (X-API-Key: wk_...)
        ← { id, over_quota }

Poll flow (unchanged structurally):
  proxy → GET Supabase /rest/v1/intercepts?id=eq.{id} (service_role key)
        ← { status }
"""

import asyncio
import logging

import aiohttp

from config import ProxyConfig
from impact import generate_impact

log = logging.getLogger("weir.approval")

POLL_INTERVAL_SECONDS = 0.5
SUPABASE_INTERCEPTS_PATH = "/rest/v1/intercepts"


def _service_headers(cfg: ProxyConfig) -> dict[str, str]:
    """Headers for direct Supabase calls using the service_role key."""
    return {
        "apikey": cfg.service_key,
        "Authorization": f"Bearer {cfg.service_key}",
        "Content-Type": "application/json",
    }


async def _post_to_dashboard(
    session: aiohttp.ClientSession,
    sql: str,
    query_type: str,
    dry_run_result: dict,
    impact: str,
    agent_classification: str,
    cfg: ProxyConfig,
) -> tuple[str | None, bool]:
    """
    POST the intercept to the dashboard's /api/intercept endpoint.
    Returns (intercept_id, over_quota). Returns (None, False) on failure.

    The dashboard validates the api_key, enforces quota, and inserts
    the record with the correct user_id — the proxy never touches
    Supabase directly for inserts.
    """
    url = f"{cfg.dashboard_url}/api/intercept"
    payload = {
        "query_type": query_type,
        "original_sql": sql,
        "impact": impact,
        "dry_run": dry_run_result,
        "agent_classification": agent_classification,
    }
    try:
        async with session.post(
            url,
            headers={"X-API-Key": cfg.api_key, "Content-Type": "application/json"},
            json=payload,
        ) as resp:
            if resp.status == 401:
                log.error("Invalid API key — check WEIR_API_KEY in .env")
                return None, False
            if resp.status not in (200, 201):
                body = await resp.text()
                log.error("Dashboard /api/intercept returned %d: %s", resp.status, body[:200])
                return None, False

            data = await resp.json()
            return data.get("id"), data.get("over_quota", False)

    except Exception as exc:
        log.error("Could not reach dashboard at %s: %s", cfg.dashboard_url, exc)
        return None, False


async def _poll_for_decision(
    session: aiohttp.ClientSession,
    intercept_id: str,
    cfg: ProxyConfig,
) -> str:
    """
    Poll Supabase every 500ms until the intercept status leaves 'pending'
    or the approval timeout expires.

    Uses the service_role key — this is internal Weir infrastructure,
    not a user-facing call.
    """
    url = (
        cfg.supabase_url
        + SUPABASE_INTERCEPTS_PATH
        + f"?id=eq.{intercept_id}&select=status"
    )
    headers = _service_headers(cfg)

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
    """PATCH the intercept record to status='timeout' using the service_role key."""
    url = cfg.supabase_url + SUPABASE_INTERCEPTS_PATH + f"?id=eq.{intercept_id}"
    try:
        async with session.patch(
            url,
            headers=_service_headers(cfg),
            json={"status": "timeout"},
        ) as resp:
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
    Submit the intercept to the dashboard and wait for a developer decision.

    Returns "approved" | "blocked" | "timeout".

    Fail-open: if the dashboard or Supabase is unreachable, the query is
    allowed through so Weir's infrastructure never silently blocks production.
    """
    if not cfg.api_key:
        log.warning("No WEIR_API_KEY configured — allowing query through")
        return "approved"

    if not cfg.supabase_url or not cfg.service_key:
        log.warning("Supabase not configured — allowing query through")
        return "approved"

    impact = generate_impact(
        query_type=query_type,
        tables_affected=dry_run_result.get("tables_affected", []),
        affected_count=dry_run_result.get("affected_count", -1),
        sample_rows=dry_run_result.get("sample_rows", []),
    )

    async with aiohttp.ClientSession() as session:
        intercept_id, over_quota = await _post_to_dashboard(
            session, sql, query_type, dry_run_result, impact, agent_classification, cfg
        )

        if intercept_id is None:
            log.warning("Dashboard unavailable — allowing query through")
            return "approved"

        if over_quota:
            log.warning("User is over free quota — intercept logged but forwarding")

        log.info(
            "Waiting for approval  intercept_id=%s  timeout=%ds",
            intercept_id, cfg.approval_timeout,
        )

        decision = await _poll_for_decision(session, intercept_id, cfg)

        if decision == "timeout":
            log.warning("TIMEOUT — no decision after %ds, auto-blocking", cfg.approval_timeout)
            await _mark_timeout(session, intercept_id, cfg)

        return decision