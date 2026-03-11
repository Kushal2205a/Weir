# Weir — Database Firewall for AI Agents

Weir sits between your AI agent and your PostgreSQL database. Every destructive
query — `DELETE`, `DROP`, `TRUNCATE`, `ALTER DROP COLUMN`, `UPDATE` without
`WHERE` — is intercepted, dry-run in a transaction, described in plain English,
and held for your approval in a real-time dashboard.

---

## Quick Start (Docker)

1. **Sign up** at [weir.dev](https://weir.dev) and copy your `wk_...` API key
   from the setup page.

2. **Run the proxy** in front of your database:

```bash
docker run -d \
  -e WEIR_API_KEY=wk_your_key_here \
  -e WEIR_TARGET_HOST=host.docker.internal \
  -e WEIR_TARGET_PORT=5432 \
  -p 5433:5433 \
  weir/proxy:latest
```

> **Note:** `host.docker.internal` resolves to your Mac/Windows localhost from
> inside the container. On Linux, use your machine's LAN IP instead.

3. **Change your agent's connection string** — swap port `5432` → `5433`:

```
# Before
postgresql://user:pass@localhost:5432/mydb

# After
postgresql://user:pass@localhost:5433/mydb
```

4. **Open the dashboard** and watch queries arrive within 2 seconds.

---

## Local Development Setup

### Prerequisites
- Python 3.12+
- PostgreSQL running locally
- A Supabase project (free tier is fine)

### 1. Clone and install

```bash
git clone https://github.com/yourname/weir
cd weir

# Dashboard
pip install fastapi uvicorn aiohttp itsdangerous jinja2 python-dotenv

# Proxy
pip install asyncpg sqlglot aiohttp python-dotenv
```

### 2. Configure environment

```bash
cp .env.example proxy/.env
cp .env.example dashboard/.env
# Edit both files with your actual values
```

Key variables:
| Variable | Where used | Description |
|---|---|---|
| `WEIR_API_KEY` | proxy | Your `wk_...` key from the dashboard |
| `WEIR_DASHBOARD_URL` | proxy | `http://localhost:8000` for local dev |
| `WEIR_SERVICE_KEY` | proxy + dashboard | Supabase `service_role` key — never share |
| `WEIR_SUPABASE_KEY` | dashboard | Supabase `anon` key — magic link emails only |
| `WEIR_SECRET_KEY` | dashboard | Random hex — generate with `secrets.token_hex(32)` |

### 3. Run Supabase migrations

Open the [Supabase SQL editor](https://app.supabase.com) and run `supabase_setup.sql`.

### 4. Start the dashboard

```bash
python -m uvicorn dashboard.main:app --reload
```

### 5. Start the proxy

```bash
cd proxy && python main.py
```

### 6. Dev login (skip magic link)

Add `WEIR_ENV=development` to your `.env`, then visit:
```
http://localhost:8000/auth/dev-login
```

---

## Architecture

```
Agent / psql
    │
    ▼ :5433
┌─────────────┐
│ Weir Proxy  │  asyncio TCP proxy
│             │  • Intercepts Q messages
│             │  • Classifies HUMAN/AGENT
│             │  • Runs dry-run in savepoint
│             │  POST /api/intercept
└──────┬──────┘
       │ X-API-Key: wk_...
       ▼
┌─────────────┐
│  Dashboard  │  FastAPI + HTMX
│             │  • Validates API key
│             │  • Enforces quota
│             │  • Writes to Supabase
│             │  • Shows ALLOW/BLOCK UI
└──────┬──────┘
       │ service_role key
       ▼
┌─────────────┐
│  Supabase   │  intercepts table
└─────────────┘
       │ polls every 500ms
       ▲
  Weir Proxy
```

## Pricing

| Plan | Intercepts/mo | Price |
|---|---|---|
| Free | 50 | $0 |
| Pro | Unlimited | $19/mo |

Email [kushal@weir.dev](mailto:kushal@weir.dev) to upgrade.
