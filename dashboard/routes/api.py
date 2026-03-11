"""
dashboard/routes/api.py
Weir — POST /api/intercept

The proxy calls this endpoint instead of writing to Supabase directly.
This gives us: API key auth, quota enforcement, and proper user_id attribution
all in one place before anything touches the database.
"""

import logging
import os
from datetime import datetime, timezone

import aiohttp
from dotenv import load_dotenv
from fastapi import APIRouter, Header, HTTPException
from pydantic import BaseModel

load_dotenv()

log = logging.getLogger("weir.api")
router = APIRouter()

SUPABASE_URL = os.getenv("WEIR_SUPABASE_URL", "")
SERVICE_KEY  = os.getenv("WEIR_SERVICE_KEY", "")

FREE_TIER_LIMIT = 50


def _svc_headers(prefer_repr: bool = False) -> dict:
    h = {
        "apikey": SERVICE_KEY,
        "Authorization": f"Bearer {SERVICE_KEY}",
        "Content-Type": "application/json",
    }
    if prefer_repr:
        h["Prefer"] = "return=representation"
    return h


def _current_month() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m")


class InterceptPayload(BaseModel):
    query_type: str
    original_sql: str
    impact: str
    dry_run: dict
    agent_classification: str = "UNKNOWN"


@router.post("/api/intercept")
async def create_intercept(
    payload: InterceptPayload,
    x_api_key: str = Header(default="", alias="X-API-Key"),
):
    """
    Called by the proxy for every intercepted destructive query.

    1. Validate API key → look up user
    2. Check + update monthly quota
    3. Insert intercept with user_id
    4. Return { id, over_quota }
    """
    if not x_api_key or not x_api_key.startswith("wk_"):
        raise HTTPException(status_code=401, detail="Missing or invalid API key")

    async with aiohttp.ClientSession() as session:
        # ── 1. Look up user ──────────────────────────────────────────────────
        user = await _get_user_by_api_key(session, x_api_key)
        if not user:
            raise HTTPException(status_code=401, detail="Unknown API key")

        user_id = user["id"]
        plan    = user.get("plan", "free")

        # ── 2. Quota check + increment ───────────────────────────────────────
        month     = _current_month()
        usage_row = await _get_or_create_usage(session, user_id, month)
        count     = usage_row.get("intercept_count", 0)
        over_quota = False

        if plan == "free" and count >= FREE_TIER_LIMIT:
            over_quota = True
            log.warning("User %s over free quota (%d/%d)", user_id, count, FREE_TIER_LIMIT)
        else:
            await _increment_usage(session, user_id, month, count)

        # ── 3. Insert intercept ──────────────────────────────────────────────
        intercept_id = await _insert_intercept(session, payload, user_id)
        if not intercept_id:
            raise HTTPException(status_code=502, detail="Failed to write intercept to database")

        return {"id": intercept_id, "over_quota": over_quota}


# ── Supabase helpers ─────────────────────────────────────────────────────────

async def _get_user_by_api_key(session: aiohttp.ClientSession, api_key: str) -> dict | None:
    url = f"{SUPABASE_URL}/rest/v1/users?api_key=eq.{api_key}&select=id,plan&limit=1"
    try:
        async with session.get(url, headers=_svc_headers()) as resp:
            if resp.status == 200:
                rows = await resp.json()
                return rows[0] if rows else None
    except Exception as exc:
        log.error("User lookup failed: %s", exc)
    return None


async def _get_or_create_usage(
    session: aiohttp.ClientSession, user_id: str, month: str
) -> dict:
    """Return the monthly_usage row, creating it (intercept_count=0) if missing."""
    url = (
        f"{SUPABASE_URL}/rest/v1/monthly_usage"
        f"?user_id=eq.{user_id}&month=eq.{month}&select=intercept_count&limit=1"
    )
    try:
        async with session.get(url, headers=_svc_headers()) as resp:
            if resp.status == 200:
                rows = await resp.json()
                if rows:
                    return rows[0]
    except Exception as exc:
        log.error("Usage fetch failed: %s", exc)

    # Row doesn't exist — create it
    try:
        create_url = f"{SUPABASE_URL}/rest/v1/monthly_usage"
        async with session.post(
            create_url,
            headers=_svc_headers(prefer_repr=True),
            json={"user_id": user_id, "month": month, "intercept_count": 0},
        ) as resp:
            if resp.status in (200, 201):
                rows = await resp.json()
                return rows[0] if rows else {"intercept_count": 0}
    except Exception as exc:
        log.error("Usage create failed: %s", exc)

    return {"intercept_count": 0}


async def _increment_usage(
    session: aiohttp.ClientSession, user_id: str, month: str, current: int
) -> None:
    url = (
        f"{SUPABASE_URL}/rest/v1/monthly_usage"
        f"?user_id=eq.{user_id}&month=eq.{month}"
    )
    try:
        async with session.patch(
            url,
            headers=_svc_headers(),
            json={"intercept_count": current + 1},
        ) as resp:
            if resp.status not in (200, 204):
                log.warning("Usage increment returned %d", resp.status)
    except Exception as exc:
        log.error("Usage increment failed: %s", exc)


async def _insert_intercept(
    session: aiohttp.ClientSession,
    payload: InterceptPayload,
    user_id: str,
) -> str | None:
    url = f"{SUPABASE_URL}/rest/v1/intercepts"
    body = {
        "user_id": user_id,
        "query_type": payload.query_type,
        "original_sql": payload.original_sql,
        "impact": payload.impact,
        "dry_run": payload.dry_run,
        "agent_classification": payload.agent_classification,
        "status": "pending",
    }
    try:
        async with session.post(
            url,
            headers=_svc_headers(prefer_repr=True),
            json=body,
        ) as resp:
            if resp.status in (200, 201):
                rows = await resp.json()
                return rows[0]["id"] if rows else None
            body_text = await resp.text()
            log.error("Intercept insert returned %d: %s", resp.status, body_text[:200])
    except Exception as exc:
        log.error("Intercept insert failed: %s", exc)
    return None