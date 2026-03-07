"""
proxy/impact.py
Weir — Task 0.4: Plain-English impact statement generator.

Pure Python — no API calls, no external dependencies.
Converts dry-run results into human-readable descriptions
that the developer sees in the approval dashboard.
"""


def generate_impact(
    query_type: str,
    tables_affected: list[str],
    affected_count: int,
    sample_rows: list[dict],
) -> str:
    """
    Return a plain-English description of what a destructive query would do.

    Never raises — always returns a string even for unknown query types
    or missing data.
    """
    table = tables_affected[0] if tables_affected else "unknown"
    count_phrase = f"{affected_count} row(s)" if affected_count != -1 else "an unknown number of rows"

    templates = {
        "DELETE": (
            f"Your AI agent is about to permanently delete {count_phrase} "
            f"from '{table}'. This cannot be undone."
        ),
        "DROP": (
            f"Your AI agent is about to permanently destroy the entire "
            f"'{table}' table and all {count_phrase} in it. "
            f"This cannot be undone."
        ),
        "TRUNCATE": (
            f"Your AI agent is about to wipe all {count_phrase} from "
            f"'{table}', keeping the table structure but deleting everything inside."
        ),
        "UPDATE_NO_WHERE": (
            f"Your AI agent is about to modify all {count_phrase} in "
            f"'{table}' with no WHERE clause — every row will be changed."
        ),
        "ALTER": (
            f"Your AI agent is about to alter the structure of '{table}'. "
            f"This may permanently remove column data."
        ),
    }

    statement = templates.get(
        query_type,
        f"Destructive operation on '{table}' affecting {count_phrase}.",
    )

    if sample_rows:
        sample_preview = ", ".join(str(row) for row in sample_rows[:3])
        statement += f"\nSample affected: {sample_preview}"

    return statement