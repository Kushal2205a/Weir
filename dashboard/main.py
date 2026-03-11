"""
dashboard/main.py
Weir — FastAPI dashboard entry point.
"""

import os

from dotenv import load_dotenv
from fastapi import FastAPI, Request
from fastapi.responses import RedirectResponse
from itsdangerous import BadSignature, SignatureExpired, TimestampSigner

load_dotenv()

from routes import auth, intercepts, api

app = FastAPI(docs_url=None, redoc_url=None)

app.include_router(auth.router)
app.include_router(intercepts.router)
app.include_router(api.router)

SESSION_MAX_AGE = 604800  # 7 days — must match auth.py


def _verified_session(cookie: str) -> bool:
    """Return True if the session cookie is valid and not expired."""
    secret = os.getenv("WEIR_SECRET_KEY", "dev-secret-change-me")
    try:
        signer = TimestampSigner(secret, sep="~")
        signer.unsign(cookie, max_age=SESSION_MAX_AGE)
        return True
    except (BadSignature, SignatureExpired):
        return False


# /api/* is authenticated via X-API-Key header, not the session cookie
UNPROTECTED_PREFIXES = ("/login", "/auth/", "/api/")


@app.middleware("http")
async def require_session(request: Request, call_next):
    path = request.url.path
    if any(path.startswith(prefix) for prefix in UNPROTECTED_PREFIXES):
        return await call_next(request)

    cookie = request.cookies.get("weir_session", "")
    if not cookie or not _verified_session(cookie):
        return RedirectResponse("/login", status_code=302)

    return await call_next(request)