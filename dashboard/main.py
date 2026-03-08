"""
dashboard/main.py
Weir — FastAPI dashboard entry point.

Starts the approval UI, wires up auth + intercept routes,
and enforces session authentication on every protected path.
"""

import os

from dotenv import load_dotenv
from fastapi import FastAPI, Request
from fastapi.responses import RedirectResponse
from fastapi.templating import Jinja2Templates
from itsdangerous import BadSignature, SignatureExpired, TimestampSigner

load_dotenv()

from dashboard.routes import auth, intercepts

app = FastAPI(docs_url=None, redoc_url=None)

templates = Jinja2Templates(directory="dashboard/templates")

app.include_router(auth.router)
app.include_router(intercepts.router)


def _verified_email(cookie: str) -> str | None:
    """Return the signed email from the session cookie, or None if invalid/expired."""
    secret = os.getenv("WEIR_SECRET_KEY", "dev-secret-change-me")
    try:
        signer = TimestampSigner(secret, sep="|")
        return signer.unsign(cookie, max_age=3600).decode()
    except (BadSignature, SignatureExpired):
        return None


UNPROTECTED_PREFIXES = ("/login", "/auth/")


@app.middleware("http")
async def require_session(request: Request, call_next):
    path = request.url.path
    if any(path.startswith(prefix) for prefix in UNPROTECTED_PREFIXES):
        return await call_next(request)

    cookie = request.cookies.get("weir_session")
    if not cookie or _verified_email(cookie) is None:
        return RedirectResponse("/login", status_code=302)

    return await call_next(request)