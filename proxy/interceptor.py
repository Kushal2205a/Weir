"""
proxy/interceptor.py
Weir — Task 0.2: Query interception and classification.

Sits between the raw pipe() byte relay and the upstream PostgreSQL server.
Only the client→server direction passes through here; the server→client
direction continues to use the unmodified pipe().
"""

import asyncio
import logging
import struct
from typing import Optional

import sqlglot
import sqlglot.expressions as exp

from config import ProxyConfig
from dry_run import run_dry_run
from approval import request_approval

log = logging.getLogger("weir.interceptor")

CHUNK_SIZE = 65_536

# PostgreSQL wire protocol constants
QUERY_MESSAGE_TAG = 0x51          # ASCII 'Q'
MSG_TAG_LENGTH = 1                # type tag is always 1 byte
MSG_PAYLOAD_LENGTH_SIZE = 4       # the length field itself is 4 bytes big-endian
MSG_HEADER_SIZE = MSG_TAG_LENGTH + MSG_PAYLOAD_LENGTH_SIZE

# Destructive query types Weir tracks
DESTRUCTIVE = "DESTRUCTIVE"
SAFE = "SAFE"


def parse_queries(data: bytes) -> list[str]:
    """
    Extract every SQL string from a raw PostgreSQL wire-protocol chunk.

    A PostgreSQL Query message has the layout:
        [0x51][int32 length (includes itself, excludes tag)][SQL string \0]

    Returns an empty list for chunks that contain no Query messages
    (e.g. startup/auth frames, binary protocol frames, pure noise).
    """
    queries: list[str] = []
    offset = 0

    while offset < len(data):
        if offset + MSG_HEADER_SIZE > len(data):
            break  # not enough bytes for a full header

        tag = data[offset]
        if tag != QUERY_MESSAGE_TAG:
            # Not a simple Query message; skip one byte and keep scanning.
            # During startup there are no type tags, so this drops us through
            # the whole chunk safely without crashing.
            offset += 1
            continue

        payload_length: int = struct.unpack_from(">I", data, offset + MSG_TAG_LENGTH)[0]
        message_end = offset + MSG_TAG_LENGTH + payload_length

        if message_end > len(data):
            break  # message is split across chunks — skip for now (Task 0.3 will buffer)

        # payload_length counts its own 4 bytes, so SQL starts at offset+5
        sql_bytes = data[offset + MSG_HEADER_SIZE : message_end]
        sql = sql_bytes.rstrip(b"\x00").decode("utf-8", errors="replace").strip()

        if sql:
            queries.append(sql)

        offset = message_end

    return queries


def _classify_with_ast(sql: str) -> Optional[tuple[str, str]]:
    """
    Use sqlglot to parse *sql* and return (classification, query_type).
    Returns None if sqlglot cannot parse the statement.
    """
    try:
        statements = sqlglot.parse(sql, dialect="postgres")
    except Exception:
        return None

    if not statements or statements[0] is None:
        return None

    statement = statements[0]

    if isinstance(statement, exp.Drop):
        droppable_kinds = {"TABLE", "DATABASE", "SCHEMA"}
        kind = (statement.args.get("kind") or "").upper()
        if kind in droppable_kinds:
            return DESTRUCTIVE, "DROP"

    if isinstance(statement, exp.Delete):
        # DELETE is always destructive — even with a WHERE clause it mutates rows
        return DESTRUCTIVE, "DELETE"

    if isinstance(statement, exp.TruncateTable):
        return DESTRUCTIVE, "TRUNCATE"

    if isinstance(statement, exp.AlterTable):
        for action in statement.args.get("actions", []):
            if isinstance(action, exp.Drop):
                # ALTER TABLE ... DROP COLUMN
                return DESTRUCTIVE, "ALTER"

    if isinstance(statement, exp.Update):
        if statement.args.get("where") is None:
            return DESTRUCTIVE, "UPDATE_NO_WHERE"

    return SAFE, "SAFE"


def _classify_with_fallback(sql: str) -> tuple[str, str]:
    """
    Keyword-based fallback used only when sqlglot cannot parse the SQL.
    Matches on the uppercased statement to catch the same destructive patterns.
    """
    uppercased = sql.upper()

    if "DROP TABLE" in uppercased or "DROP DATABASE" in uppercased or "DROP SCHEMA" in uppercased:
        return DESTRUCTIVE, "DROP"
    if "DELETE FROM" in uppercased:
        return DESTRUCTIVE, "DELETE"
    if "TRUNCATE" in uppercased:
        return DESTRUCTIVE, "TRUNCATE"
    if "ALTER TABLE" in uppercased and "DROP COLUMN" in uppercased:
        return DESTRUCTIVE, "ALTER"
    # UPDATE without WHERE: simplistic but good enough as a last resort
    if "UPDATE" in uppercased and "WHERE" not in uppercased:
        return DESTRUCTIVE, "UPDATE_NO_WHERE"

    return SAFE, "SAFE"


def classify(sql: str) -> tuple[str, str]:
    """
    Classify *sql* as SAFE or DESTRUCTIVE using the sqlglot AST.

    Returns a (classification, query_type) tuple, e.g.:
        ("DESTRUCTIVE", "DELETE")
        ("SAFE",        "SAFE")

    Falls back to keyword matching if sqlglot cannot parse the statement.
    """
    result = _classify_with_ast(sql)
    if result is not None:
        return result

    log.debug("sqlglot could not parse query; using keyword fallback: %.120s", sql)
    return _classify_with_fallback(sql)


async def intercept_pipe(
    client_reader: asyncio.StreamReader,
    server_writer: asyncio.StreamWriter,
    label: str,
    cfg: ProxyConfig,
) -> None:
    """
    Replacement for pipe() in the client→server direction.

    Reads each chunk, classifies any SQL queries inside it, runs a dry-run
    for destructive ones, then waits for developer approval before forwarding.
    Non-destructive chunks are forwarded immediately.
    """
    try:
        while True:
            data: bytes = await client_reader.read(CHUNK_SIZE)
            if not data:
                log.debug("%s  EOF", label)
                break

            should_forward = True

            for sql in parse_queries(data):
                classification, query_type = classify(sql)
                if classification == DESTRUCTIVE:
                    log.warning("INTERCEPTED %s: %.120s", query_type, sql)
                    dry = await run_dry_run(sql, query_type, cfg)
                    log.warning(
                        "DRY RUN RESULT: affected=%d tables=%s",
                        dry["affected_count"],
                        dry["tables_affected"],
                    )
                    decision = await request_approval(sql, query_type, dry, cfg)

                    if decision == "approved":
                        log.info("APPROVED — forwarding query")
                    elif decision == "blocked":
                        log.warning("BLOCKED — query will not reach PostgreSQL")
                        should_forward = False
                        break
                    elif decision == "timeout":
                        log.warning("TIMEOUT — auto-blocked after %ds", cfg.approval_timeout)
                        should_forward = False
                        break

            if should_forward:
                server_writer.write(data)
                await server_writer.drain()

    except (asyncio.IncompleteReadError, ConnectionResetError) as exc:
        log.debug("%s  connection reset: %s", label, exc)
    except Exception as exc:  # noqa: BLE001
        log.warning("%s  unexpected error: %s", label, exc)
    finally:
        try:
            server_writer.close()
            await server_writer.wait_closed()
        except Exception:
            pass