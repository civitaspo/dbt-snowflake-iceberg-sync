from __future__ import annotations

import json
from pathlib import Path

from jinja2 import Environment


def test_json_sql_literal_round_trips_single_quotes_for_snowflake_sql():
    payload = {
        "incremental_predicate": "event_date = '20240111'",
        "quoted_incremental_predicate": '"event_date" = \'20240111\'',
    }

    literal = _render_json_sql_literal(payload)

    assert "\\u0027" not in literal
    assert _decode_sql_string_literal(literal) == payload


def _render_json_sql_literal(value: dict[str, str]) -> str:
    macro_path = Path(__file__).resolve().parents[2] / "macros/iceberg_sync/json.sql"
    template = Environment().from_string(
        macro_path.read_text(encoding="utf-8")
        + "\n{{ iceberg_sync_json_sql_literal(value) }}"
    )
    return template.render({"value": value, "return": lambda item: item}).strip()


def _decode_sql_string_literal(literal: str) -> dict[str, str]:
    assert literal.startswith("'") and literal.endswith("'")
    json_text = literal[1:-1].replace("''", "'")
    return json.loads(json_text)
