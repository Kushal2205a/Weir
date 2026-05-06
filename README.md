# Weir - Database Security Layer for AI Agents 

A TCP proxy that sits in front of PostgreSQL and intercepts destructive queries before they execute. Every `DELETE`, `DROP`, `TRUNCATE`, or `UPDATE` without a `WHERE` clause gets held for manual approval in a web dashboard. The query only goes through if you click Allow.

![The dashboard showing a pending DELETE intercept with BLOCK and ALLOW buttons](screenshots.png)

## How it works

Your agent connects to Weir on port 5455 instead of PostgreSQL directly. Weir forwards everything through untouched, except destructive queries. When it catches one, it:

1. Runs the query inside a savepoint transaction to see what it would actually do
2. Posts the query, the impact, and a row preview to the dashboard
3. Blocks the connection until you click Allow or Block
4. If you don't decide within 60 seconds, it auto-blocks

The proxy also tracks whether a session looks like it's coming from an agent or a human based on connection timing and query patterns.

## Running it

**Prerequisites:** Docker Desktop, PostgreSQL running locally.

```bash
git clone https://github.com/Kushal2205a/database-security-layer-for-ai-agents
cd Weir
cp .env.example .env
```

Edit `.env` with your database credentials:

```
WEIR_TARGET_HOST=host.docker.internal
WEIR_TARGET_PORT=5432
WEIR_TARGET_USER=postgres
WEIR_TARGET_PASSWORD=yourpassword
WEIR_TARGET_DB=postgres
```

> On Linux, replace `host.docker.internal` with your machine's local IP.

```bash
docker compose up --build
```

Dashboard runs at `http://localhost:8000`. Proxy listens on port `5455`.

## Connecting through it

Change one port in your connection string:

```
# before
postgresql://user:pass@localhost:5432/mydb

# after
postgresql://user:pass@localhost:5455/mydb
```

Then run a destructive query from your agent or terminal. It shows up in the dashboard within 2 seconds.

## What gets intercepted

| Query | Why |
|---|---|
| `DELETE FROM ...` | Permanent row deletion |
| `DROP TABLE/DATABASE/SCHEMA` | Irreversible |
| `TRUNCATE ...` | Empties the entire table |
| `UPDATE ... (no WHERE)` | Modifies every row |
| `ALTER TABLE ... DROP COLUMN` | Drops data permanently |

Non-destructive queries (`SELECT`, `INSERT`, `UPDATE` with a `WHERE`) pass through instantly with no delay.

## Agent detection

Each connection gets scored across four signals:

- Application name contains `python`, `node`, `agent`, `claude`, `cursor`, etc. (+40)
- First query fires within 200ms of connecting (+20)
- 3 or more queries arrive within a 500ms window (+20)
- More than 10 queries on a single connection (+15)

Score 60+ -> `AGENT`. Score 30–59 -> `LIKELY AI`. Under 30 -> `HUMAN`. The badge shows up on every pending card and in the history table.

## Running without Docker

```bash
# terminal 1
cd dashboard
pip install -r requirements.txt
uvicorn main:app --reload

# terminal 2
cd proxy
pip install -r requirements.txt
python main.py
```

## Stack

- **Proxy** — Python asyncio, sqlglot, asyncpg, aiohttp
- **Dashboard** — FastAPI, HTMX, Jinja2, aiosqlite
- **Storage** — SQLite (zero config, file created on first run)