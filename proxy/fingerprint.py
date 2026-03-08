"""
proxy/fingerprint.py
Weir — Tasks 2.1 + 2.2: Session metadata collection and agent/human classification.

Extracts connection metadata (application_name, timings, query cadence) and
scores each session to determine whether it looks like a human developer or
an AI agent firing queries programmatically.
"""

import logging
import struct
import time

log = logging.getLogger("weir.fingerprint")

# PostgreSQL wire protocol: the startup packet uses protocol version 196608
PG_PROTOCOL_VERSION = 196608

# application_name substrings that strongly suggest an AI agent or script
AGENT_NAME_SIGNALS = {
    "claude", "cursor", "replit", "copilot", "gpt", "agent",
    "bot", "script", "python", "node", "langchain", "openai",
}

# application_name substrings that strongly suggest a human GUI tool
HUMAN_NAME_SIGNALS = {"psql", "pgadmin", "tableplus", "datagrip", "dbeaver", "postico"}

# Scoring thresholds
AGENT_THRESHOLD = 60
LIKELY_AGENT_THRESHOLD = 30

# Scoring weights
SCORE_AGENT_APP_NAME = 40
SCORE_FAST_FIRST_QUERY = 20   # connection→first query < 200ms
SCORE_BURST_QUERIES = 20      # 3+ queries within any 500ms window
SCORE_HIGH_QUERY_COUNT = 15   # > 10 queries on one connection

FAST_FIRST_QUERY_MS = 200
BURST_WINDOW_MS = 500
BURST_MIN_QUERIES = 3
HIGH_QUERY_THRESHOLD = 10


def new_session(client_ip: str) -> dict:
    """
    Create a fresh in-memory session dict for a newly accepted connection.
    Nothing is persisted — this lives only for the duration of the connection.
    """
    return {
        "application_name": "",
        "client_ip": client_ip,
        "connected_at_ms": _now_ms(),
        "first_query_at_ms": 0,
        "query_count": 0,
        "query_timestamps_ms": [],
        "classification": "HUMAN",
    }


def extract_application_name(startup_data: bytes) -> str:
    """
    Parse the PostgreSQL startup message and return the value of the
    'application_name' key, or an empty string if it cannot be found.

    Startup message layout:
        [int32 total_length][int32 protocol_version][key\\0value\\0 ... \\0]

    The entire function is wrapped in a broad try/except so a malformed or
    split startup packet never crashes the proxy — we just fall back to "".
    """
    try:
        if len(startup_data) < 8:
            return ""

        protocol = struct.unpack_from(">I", startup_data, 4)[0]
        if protocol != PG_PROTOCOL_VERSION:
            # SSLRequest or CancelRequest — not a real startup message
            return ""

        # Key-value pairs start at byte 8
        kv_bytes = startup_data[8:]
        pairs = kv_bytes.split(b"\x00")

        # pairs is now [key, value, key, value, ..., ""] — zip in steps of 2
        i = 0
        while i + 1 < len(pairs):
            key = pairs[i].decode("utf-8", errors="replace")
            value = pairs[i + 1].decode("utf-8", errors="replace")
            if key == "application_name":
                return value
            if key == "":
                break  # terminating null pair
            i += 2

        return ""
    except Exception:
        return ""


def record_query(session: dict) -> None:
    """
    Update session timing and counter on each query seen.
    Call this once per intercepted SQL string before classify_session().
    """
    now = _now_ms()

    if session["first_query_at_ms"] == 0:
        session["first_query_at_ms"] = now

    session["query_count"] += 1
    session["query_timestamps_ms"].append(now)

    # Keep the timestamp list bounded — only the last 50 matter for burst detection
    if len(session["query_timestamps_ms"]) > 50:
        session["query_timestamps_ms"] = session["query_timestamps_ms"][-50:]

    session["classification"] = classify_session(session)


def classify_session(session: dict) -> str:
    """
    Score the session and return "HUMAN" | "LIKELY_AGENT" | "AGENT".

    Scoring is additive — multiple weak signals combine to a confident verdict.
    """
    score = 0

    app_name = session.get("application_name", "").lower()
    if any(signal in app_name for signal in AGENT_NAME_SIGNALS):
        score += SCORE_AGENT_APP_NAME
        log.debug("fingerprint: +%d agent app_name=%r", SCORE_AGENT_APP_NAME, app_name)

    first_query_ms = session.get("first_query_at_ms", 0)
    connected_ms = session.get("connected_at_ms", 0)
    if first_query_ms > 0 and (first_query_ms - connected_ms) < FAST_FIRST_QUERY_MS:
        score += SCORE_FAST_FIRST_QUERY
        log.debug("fingerprint: +%d fast first query (%dms)", SCORE_FAST_FIRST_QUERY, first_query_ms - connected_ms)

    if _has_query_burst(session.get("query_timestamps_ms", [])):
        score += SCORE_BURST_QUERIES
        log.debug("fingerprint: +%d burst queries detected", SCORE_BURST_QUERIES)

    if session.get("query_count", 0) > HIGH_QUERY_THRESHOLD:
        score += SCORE_HIGH_QUERY_COUNT
        log.debug("fingerprint: +%d high query count (%d)", SCORE_HIGH_QUERY_COUNT, session["query_count"])

    if score >= AGENT_THRESHOLD:
        return "AGENT"
    if score >= LIKELY_AGENT_THRESHOLD:
        return "LIKELY_AGENT"
    return "HUMAN"


def _has_query_burst(timestamps_ms: list[int]) -> bool:
    """
    Return True if any 3 consecutive timestamps fall within a 500ms window.
    Consecutive here means adjacent entries in the list, which is insertion-ordered.
    """
    if len(timestamps_ms) < BURST_MIN_QUERIES:
        return False

    for i in range(len(timestamps_ms) - BURST_MIN_QUERIES + 1):
        window = timestamps_ms[i : i + BURST_MIN_QUERIES]
        if window[-1] - window[0] <= BURST_WINDOW_MS:
            return True

    return False


def _now_ms() -> int:
    return int(time.monotonic() * 1000)