"""
dashboard/routes/intercepts.py
Weir — Intercept listing, HTMX partial, and approve/block actions.
"""

import os
from datetime import datetime, timezone

import aiohttp
from dotenv import load_dotenv
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from itsdangerous import BadSignature, SignatureExpired, TimestampSigner

load_dotenv()

router = APIRouter()
templates = Jinja2Templates(directory="dashboard/templates")

SUPABASE_URL = os.getenv("WEIR_SUPABASE_URL", "")
SUPABASE_KEY = os.getenv("WEIR_SUPABASE_KEY", "")
SECRET_KEY = os.getenv("WEIR_SECRET_KEY", "dev-secret-change-me")
SESSION_COOKIE = "weir_session"

SUPABASE_HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=representation",
}


def _get_email(request: Request) -> str:
    cookie = request.cookies.get(SESSION_COOKIE, "")
    try:
        signer = TimestampSigner(SECRET_KEY, sep="|")
        return signer.unsign(cookie, max_age=3600).decode()
    except (BadSignature, SignatureExpired):
        return ""


async def _fetch_pending() -> list[dict]:
    url = (
        f"{SUPABASE_URL}/rest/v1/intercepts"
        "?status=eq.pending&order=created_at.asc"
    )
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=SUPABASE_HEADERS) as resp:
                if resp.status == 200:
                    return await resp.json()
    except Exception:
        pass
    return []


async def _fetch_history() -> list[dict]:
    url = (
        f"{SUPABASE_URL}/rest/v1/intercepts"
        "?status=neq.pending&order=created_at.desc&limit=20"
    )
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=SUPABASE_HEADERS) as resp:
                if resp.status == 200:
                    return await resp.json()
    except Exception:
        pass
    return []


async def _patch_intercept(intercept_id: str, status: str) -> bool:
    url = f"{SUPABASE_URL}/rest/v1/intercepts?id=eq.{intercept_id}"
    resolved_at = datetime.now(timezone.utc).isoformat()
    try:
        async with aiohttp.ClientSession() as session:
            async with session.patch(
                url,
                headers=SUPABASE_HEADERS,
                json={"status": status, "resolved_at": resolved_at},
            ) as resp:
                return resp.status in (200, 204)
    except Exception:
        return False


@router.get("/")
async def root():
    return RedirectResponse("/intercepts", status_code=302)


@router.get("/intercepts")
async def intercepts_page(request: Request):
    email = _get_email(request)
    pending = await _fetch_pending()
    history = await _fetch_history()
    return templates.TemplateResponse(
        "intercepts.html",
        {"request": request, "email": email, "pending": pending, "history": history},
    )


@router.get("/intercepts/pending", response_class=HTMLResponse)
async def pending_partial(request: Request):
    pending = await _fetch_pending()
    return templates.TemplateResponse(
        "_pending.html",
        {"request": request, "pending": pending},
    )


@router.post("/intercepts/{intercept_id}/allow", response_class=HTMLResponse)
async def allow_intercept(intercept_id: str):
    await _patch_intercept(intercept_id, "approved")
    return HTMLResponse("")  # HTMX swaps the card out with empty content


@router.post("/intercepts/{intercept_id}/block", response_class=HTMLResponse)
async def block_intercept(intercept_id: str):
    await _patch_intercept(intercept_id, "blocked")
    return HTMLResponse("")