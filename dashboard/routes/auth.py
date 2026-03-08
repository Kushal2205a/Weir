"""
dashboard/routes/auth.py
Weir — Magic-link authentication via Supabase.

Flow:
  1. User submits email → POST /auth/login → Supabase sends magic link
  2. User clicks link → Supabase redirects to /auth/callback with token in URL hash
  3. JS on callback page extracts hash token → redirects to /auth/session?access_token=...
  4. /auth/session decodes JWT email, signs it, sets HttpOnly cookie → redirect to /
"""

import base64
import json
import os

import aiohttp
from dotenv import load_dotenv
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from itsdangerous import TimestampSigner

load_dotenv()

router = APIRouter()
templates = Jinja2Templates(directory="dashboard/templates")

SUPABASE_URL = os.getenv("WEIR_SUPABASE_URL", "")
SUPABASE_KEY = os.getenv("WEIR_SUPABASE_KEY", "")
SECRET_KEY = os.getenv("WEIR_SECRET_KEY", "dev-secret-change-me")
SESSION_COOKIE = "weir_session"


def _sign_email(email: str) -> str:
    signer = TimestampSigner(SECRET_KEY, sep="|")
    return signer.sign(email.encode()).decode()


def _decode_jwt_email(token: str) -> str:
    """Decode email from a Supabase JWT without verification (we trust Supabase delivery)."""
    try:
        payload_b64 = token.split(".")[1]
        payload_b64 += "=" * (4 - len(payload_b64) % 4)
        payload = json.loads(base64.urlsafe_b64decode(payload_b64))
        return payload.get("email", "")
    except Exception:
        return ""


@router.get("/login")
async def login_page(request: Request):
    return templates.TemplateResponse("login.html", {"request": request})


@router.post("/auth/login")
async def send_magic_link(request: Request):
    form = await request.form()
    email = str(form.get("email", "")).strip()

    if not email:
        return HTMLResponse('<p class="auth-error">Please enter a valid email address.</p>')

    base_url = str(request.base_url).rstrip("/")
    redirect_to = f"{base_url}/auth/callback"

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{SUPABASE_URL}/auth/v1/magiclink",
                headers={"apikey": SUPABASE_KEY, "Content-Type": "application/json"},
                json={"email": email, "options": {"emailRedirectTo": redirect_to}},
            ) as resp:
                if resp.status not in (200, 204):
                    body = await resp.text()
                    return HTMLResponse(f'<p class="auth-error">Supabase error: {body[:120]}</p>')
    except Exception as exc:
        return HTMLResponse(f'<p class="auth-error">Could not reach Supabase: {exc}</p>')

    return HTMLResponse(
        '<p class="auth-success">Magic link sent — check your email.</p>'
    )


@router.get("/auth/callback")
async def auth_callback(
    request: Request,
    code: str = "",
    token_hash: str = "",
    type: str = "",
):
    """
    Supabase redirects here after the user clicks the magic link.

    The Supabase REST API (/auth/v1/magiclink) sends:
        ?token_hash=<hash>&type=magiclink
    
    The Supabase JS SDK PKCE flow sends:
        ?code=<code>

    We handle both. Hash-fragment fallback handles legacy flows.
    """

    # Path 1: REST API magic link — token_hash + type
    if token_hash and type:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{SUPABASE_URL}/auth/v1/verify",
                    headers={"apikey": SUPABASE_KEY, "Content-Type": "application/json"},
                    json={"token_hash": token_hash, "type": type},
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        access_token = data.get("access_token", "")
                        email = _decode_jwt_email(access_token)
                        if email:
                            signed = _sign_email(email)
                            response = RedirectResponse("/", status_code=302)
                            response.set_cookie(
                                SESSION_COOKIE,
                                signed,
                                httponly=True,
                                samesite="lax",
                                max_age=3600,
                            )
                            return response
        except Exception:
            pass
        return RedirectResponse("/login", status_code=302)

    # Path 2: PKCE code exchange
    if code:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{SUPABASE_URL}/auth/v1/token?grant_type=pkce",
                    headers={"apikey": SUPABASE_KEY, "Content-Type": "application/json"},
                    json={"auth_code": code},
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        access_token = data.get("access_token", "")
                        email = _decode_jwt_email(access_token)
                        if email:
                            signed = _sign_email(email)
                            response = RedirectResponse("/", status_code=302)
                            response.set_cookie(
                                SESSION_COOKIE,
                                signed,
                                httponly=True,
                                samesite="lax",
                                max_age=3600,
                            )
                            return response
        except Exception:
            pass
        return RedirectResponse("/login", status_code=302)

    # Path 3: Hash-fragment fallback (legacy Supabase, token in #access_token=...)
    return HTMLResponse("""
<!doctype html>
<html>
<head><title>Signing in…</title></head>
<body>
<script>
  const hash = window.location.hash.slice(1);
  const params = new URLSearchParams(hash);
  const token = params.get("access_token");
  if (token) {
    window.location.href = "/auth/session?access_token=" + encodeURIComponent(token);
  } else {
    window.location.href = "/login";
  }
</script>
<p>Signing you in…</p>
</body>
</html>
""")


@router.get("/auth/session")
async def create_session(request: Request, access_token: str = ""):
    email = _decode_jwt_email(access_token)
    if not email:
        return RedirectResponse("/login", status_code=302)

    signed = _sign_email(email)
    response = RedirectResponse("/", status_code=302)
    response.set_cookie(
        SESSION_COOKIE,
        signed,
        httponly=True,
        samesite="lax",
        max_age=3600,
    )
    return response


@router.get("/auth/logout")
async def logout():
    response = RedirectResponse("/login", status_code=302)
    response.delete_cookie(SESSION_COOKIE)
    return response


# REMOVE BEFORE DEPLOY — local dev only, bypasses magic link auth entirely
@router.get("/auth/dev-login")
async def dev_login():
    if os.getenv("WEIR_ENV") != "development":
        from fastapi import HTTPException
        raise HTTPException(status_code=404)
    signed = _sign_email("dev@weir.local")
    response = RedirectResponse("/", status_code=302)
    response.set_cookie(SESSION_COOKIE, signed, httponly=True, samesite="lax", max_age=3600)
    return response