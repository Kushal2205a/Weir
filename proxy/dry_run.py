"""
proxy/dry_run.py
Weir — Task 0.3: Dry-run engine.

Opens a fresh asyncpg connection (independent of the proxied connection),
executes the destructive query inside a savepoint, captures affected rows
and count, then rolls back so the database is left unchanged.
"""

import logging
import re

import asyncpg
import sqlglot
import sqlglot.expressions as exp

from config import ProxyConfig

log = logging.getLogger("weir.dry_run")

MAX_SAMPLE_ROWS = 5
SAVEPOINT_NAME = "weir_dryrun"

# Query types whose impact is best measured by row count before execution
# rather than RETURNING * (DDL statements don't support RETURNING).
COUNT_ONLY_TYPES = {"DROP", "TRUNCATE", "ALTER"}


def _add_returning_star(sql: str) -> str:
    """
    Rewrite *sql* to include RETURNING * if it doesn't already have one.
    Uses the sqlglot AST so the rewrite is syntactically correct.
    Falls back to appending the clause as a string if sqlglot fails.
    """
    try:
        statements = sqlglot.parse(sql, dialect="postgres")
        if not statements or statements[0] is None:
            raise ValueError("empty parse result")

        statement = statements[0]
        if statement.args.get("returning"):
            return sql  # already has RETURNING, leave it alone

        statement.set("returning", exp.Returning(expressions=[exp.Star()]))
        return statement.sql(dialect="postgres")
    except Exception:
        # Safe string fallback — good enough for the cases sqlglot can't handle
        return sql.rstrip().rstrip(";") + " RETURNING *"


def _extract_table_names(sql: str) -> list[str]:
    """
    Pull the table name(s) the query targets from the sqlglot AST.
    Falls back to a regex scan of FROM/TABLE/INTO keywords if AST fails.
    """
    try:
        statements = sqlglot.parse(sql, dialect="postgres")
        if statements and statements[0] is not None:
            tables = [
                table.name
                for table in statements[0].find_all(exp.Table)
                if table.name
            ]
            if tables:
                return tables
    except Exception:
        pass

    # Regex fallback: grab the word after FROM, TABLE, or INTO
    matches = re.findall(r"(?:FROM|TABLE|INTO)\s+([a-zA-Z_][a-zA-Z0-9_.]*)", sql, re.IGNORECASE)
    return list(dict.fromkeys(matches))  # deduplicate while preserving order


def _serialise_row(row: asyncpg.Record) -> dict:
    """
    Convert an asyncpg Record to a plain dict with all values cast to str.
    asyncpg returns typed Python objects (UUID, Decimal, datetime, etc.) that
    are not JSON-serialisable by default — stringifying here prevents surprises
    when the result is written to Supabase in Task 0.5.
    """
    return {key: str(value) for key, value in dict(row).items()}


async def _count_rows(conn: asyncpg.Connection, table: str) -> int:
    """Return the current row count for *table*, or -1 if the query fails."""
    try:
        result = await conn.fetchval(f"SELECT COUNT(*) FROM {table}")
        return int(result)
    except Exception:
        return -1


async def run_dry_run(sql: str, query_type: str, cfg: ProxyConfig) -> dict:
    """
    Execute *sql* inside a savepoint on a fresh connection and roll it back.

    Returns a dict describing the impact:
        affected_count  – number of rows that would be changed
        sample_rows     – up to 5 rows as serialisable dicts
        tables_affected – list of table name strings
        query_type      – passed through from the classifier
        original_query  – the unmodified SQL string
        error           – present only when the dry run itself fails
    """
    tables_affected = _extract_table_names(sql)

    conn: asyncpg.Connection | None = None
    try:
        conn = await asyncpg.connect(
            host=cfg.target_host,
            port=cfg.target_port,
            database=cfg.target_db,
            user=cfg.target_user,
            password=cfg.target_password,
        )

        await conn.execute("BEGIN")
        await conn.execute(f"SAVEPOINT {SAVEPOINT_NAME}")

        affected_count = 0
        sample_rows: list[dict] = []

        if query_type in COUNT_ONLY_TYPES:
            # DDL can't use RETURNING — snapshot row counts before the statement runs
            first_table = tables_affected[0] if tables_affected else None
            pre_count = await _count_rows(conn, first_table) if first_table else 0

            await conn.execute(sql)

            post_count = await _count_rows(conn, first_table) if first_table else 0
            # For DROP/TRUNCATE the table will be gone or empty after execution,
            # so the delta is how many rows would have been lost.
            affected_count = max(pre_count - post_count, pre_count)

        else:
            # DELETE / UPDATE — rewrite to capture affected rows via RETURNING *
            rewritten_sql = _add_returning_star(sql)
            rows = await conn.fetch(rewritten_sql)
            affected_count = len(rows)
            sample_rows = [_serialise_row(r) for r in rows[:MAX_SAMPLE_ROWS]]

        await conn.execute(f"ROLLBACK TO SAVEPOINT {SAVEPOINT_NAME}")
        await conn.execute("ROLLBACK")

        return {
            "affected_count": affected_count,
            "sample_rows": sample_rows,
            "tables_affected": tables_affected,
            "query_type": query_type,
            "original_query": sql,
        }

    except Exception as exc:
        log.error("Dry run failed for %s query: %s", query_type, exc)

        # Best-effort rollback — the connection may already be in an error state
        if conn is not None:
            try:
                await conn.execute(f"ROLLBACK TO SAVEPOINT {SAVEPOINT_NAME}")
                await conn.execute("ROLLBACK")
            except Exception:
                pass

        return {
            "affected_count": -1,
            "sample_rows": [],
            "tables_affected": tables_affected,
            "query_type": query_type,
            "original_query": sql,
            "error": str(exc),
        }

    finally:
        if conn is not None:
            await conn.close()