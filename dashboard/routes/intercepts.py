"""
dashboard/routes/intercepts.py
Weir — Intercept listing, HTMX partial, approve/block, setup page, upgrade page.
"""

import logging
import os
from datetime import datetime, timezone

import aiohttp
from dotenv import load_dotenv
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from itsdangerous import BadSignature, SignatureExpired, TimestampSigner

load_dotenv()

log = logging.getLogger("weir.intercepts")
router = APIRouter()
templates = Jinja2Templates(directory="templates")

SUPABASE_URL  = os.getenv("WEIR_SUPABASE_URL", "")
SERVICE_KEY   = os.getenv("WEIR_SERVICE_KEY", "")
SECRET_KEY    = os.getenv("WEIR_SECRET_KEY", "dev-secret-change-me")
SESSION_COOKIE = "weir_session"
SESSION_MAX_AGE = 604800  # 7 days


def _svc_headers() -> dict:
    return {
        "apikey": SERVICE_KEY,
        "Authorization": f"Bearer {SERVICE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=representation",
    }


def _get_session(request: Request) -> tuple[str, str]:
    """
    Unsign the session cookie.
    Returns (email, api_key). Both are "" on failure or expiry.

    Cookie format: TimestampSigner signs "email|api_key" using sep="~"
    so the pipe in the email address doesn't collide with our separator.
    """
    cookie = request.cookies.get(SESSION_COOKIE, "")
    if not cookie:
        return "", ""
    try:
        signer = TimestampSigner(SECRET_KEY, sep="~")
        payload = signer.unsign(cookie, max_age=SESSION_MAX_AGE).decode()
        # payload = "email|api_key"  — split on the LAST pipe to be safe
        idx = payload.rfind("|")
        if idx == -1:
            return payload, ""
        return payload[:idx], payload[idx + 1:]
    except (BadSignature, SignatureExpired):
        return "", ""


async def _fetch_pending(user_id: str) -> list[dict]:
    url = (
        f"{SUPABASE_URL}/rest/v1/intercepts"
        f"?user_id=eq.{user_id}&status=eq.pending&order=created_at.asc"
    )
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=_svc_headers()) as resp:
                if resp.status == 200:
                    return await resp.json()
    except Exception:
        pass
    return []


async def _fetch_history(user_id: str) -> list[dict]:
    url = (
        f"{SUPABASE_URL}/rest/v1/intercepts"
        f"?user_id=eq.{user_id}&status=neq.pending&order=created_at.desc&limit=20"
    )
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=_svc_headers()) as resp:
                if resp.status == 200:
                    return await resp.json()
    except Exception:
        pass
    return []


async def _fetch_usage(user_id: str) -> dict | None:
    month = datetime.now(timezone.utc).strftime("%Y-%m")
    url = (
        f"{SUPABASE_URL}/rest/v1/monthly_usage"
        f"?user_id=eq.{user_id}&month=eq.{month}&select=intercept_count&limit=1"
    )
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=_svc_headers()) as resp:
                if resp.status == 200:
                    rows = await resp.json()
                    return rows[0] if rows else None
    except Exception:
        pass
    return None


async def _fetch_user_by_api_key(api_key: str) -> dict | None:
    """Fetch user row by api_key — used to get user_id and created_at."""
    url = f"{SUPABASE_URL}/rest/v1/users?api_key=eq.{api_key}&select=id,created_at&limit=1"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=_svc_headers()) as resp:
                if resp.status == 200:
                    rows = await resp.json()
                    return rows[0] if rows else None
    except Exception:
        pass
    return None


async def _patch_intercept(intercept_id: str, status: str) -> bool:
    url = f"{SUPABASE_URL}/rest/v1/intercepts?id=eq.{intercept_id}"
    resolved_at = datetime.now(timezone.utc).isoformat()
    try:
        async with aiohttp.ClientSession() as session:
            async with session.patch(
                url,
                headers=_svc_headers(),
                json={"status": status, "resolved_at": resolved_at},
            ) as resp:
                return resp.status in (200, 204)
    except Exception:
        return False


def _is_new_user(user: dict, history: list) -> bool:
    """True if account created within last 5 minutes and no resolved intercepts yet."""
    if history:
        return False
    try:
        created = datetime.fromisoformat(user["created_at"].replace("Z", "+00:00"))
        age_seconds = (datetime.now(timezone.utc) - created).total_seconds()
        return age_seconds < 300
    except Exception:
        return False


# ── Routes ───────────────────────────────────────────────────────────────────

@router.get("/")
async def root():
    return RedirectResponse("/intercepts", status_code=302)


@router.get("/intercepts")
async def intercepts_page(request: Request):
    email, api_key = _get_session(request)
    if not email:
        return RedirectResponse("/login", status_code=302)

    user = await _fetch_user_by_api_key(api_key) if api_key else None
    user_id = user["id"] if user else None

    if not user_id:
        # api_key invalid or missing — still show the page with empty data
        return templates.TemplateResponse(
            "intercepts.html",
            {"request": request, "email": email, "pending": [], "history": [], "usage": None},
        )

    pending, history, usage = (
        await _fetch_pending(user_id),
        await _fetch_history(user_id),
        await _fetch_usage(user_id),
    )

    # Redirect brand-new users to the setup page
    if user and _is_new_user(user, history):
        return RedirectResponse("/setup", status_code=302)

    return templates.TemplateResponse(
        "intercepts.html",
        {
            "request": request,
            "email": email,
            "api_key": api_key,
            "pending": pending,
            "history": history,
            "usage": usage,
        },
    )


@router.get("/intercepts/pending", response_class=HTMLResponse)
async def pending_partial(request: Request):
    email, api_key = _get_session(request)
    user = await _fetch_user_by_api_key(api_key) if api_key else None
    user_id = user["id"] if user else None
    pending = await _fetch_pending(user_id) if user_id else []
    return templates.TemplateResponse("_pending.html", {"request": request, "pending": pending})


@router.post("/intercepts/{intercept_id}/allow", response_class=HTMLResponse)
async def allow_intercept(intercept_id: str):
    await _patch_intercept(intercept_id, "approved")
    return HTMLResponse("")


@router.post("/intercepts/{intercept_id}/block", response_class=HTMLResponse)
async def block_intercept(intercept_id: str):
    await _patch_intercept(intercept_id, "blocked")
    return HTMLResponse("")


@router.get("/setup")
async def setup_page(request: Request):
    email, api_key = _get_session(request)
    if not email:
        return RedirectResponse("/login", status_code=302)
    return templates.TemplateResponse(
        "setup.html",
        {"request": request, "email": email, "api_key": api_key},
    )


@router.get("/upgrade")
async def upgrade_page(request: Request):
    email, api_key = _get_session(request)
    if not email:
        return RedirectResponse("/login", status_code=302)
    return templates.TemplateResponse(
        "upgrade.html",
        {"request": request, "email": email},
    )