from __future__ import annotations

import json
from pathlib import Path

from jinja2 import Environment


def test_json_sql_literal_round_trips_single_quotes_for_snowflake_sql():
    payload = {
        "incremental_predicate": "event_date = '20240111'",
        "model_sql": "select\n  *\nfrom `project.dataset.table`",
        "quoted_incremental_predicate": '"event_date" = \'20240111\'',
    }

    literal = _render_json_sql_literal(payload)

    assert "\\u0027" not in literal
    assert "\\\\n" in literal
    assert _decode_sql_string_literal(literal) == payload


def test_create_view_macro_quotes_uppercase_aliases():
    macro_path = Path(__file__).resolve().parents[2] / "macros/iceberg_sync/relations.sql"
    template = Environment(extensions=["jinja2.ext.do"]).from_string(
        macro_path.read_text(encoding="utf-8")
        + "\n{{ iceberg_sync_create_view_sql(target, internal, view_columns) }}"
    )

    rendered = template.render(
        {
            "target": '"DB"."SCHEMA"."VIEW"',
            "internal": '"DB"."SCHEMA"."TABLE"',
            "view_columns": [{"source_name": "OrderID", "alias": "select"}],
            "adapter": _FakeAdapter(),
        }
    )

    assert '"OrderID" AS "SELECT"' in rendered


class _FakeAdapter:
    def quote(self, value: str) -> str:
        return '"' + value.replace('"', '""') + '"'


def _render_json_sql_literal(value: dict[str, str]) -> str:
    macro_path = Path(__file__).resolve().parents[2] / "macros/iceberg_sync/json.sql"
    template = Environment().from_string(
        macro_path.read_text(encoding="utf-8")
        + "\n{{ iceberg_sync_json_sql_literal(value) }}"
    )
    return template.render({"value": value, "return": lambda item: item}).strip()


def _decode_sql_string_literal(literal: str) -> dict[str, str]:
    assert literal.startswith("'") and literal.endswith("'")
    json_text = literal[1:-1].replace("''", "'").replace("\\\\", "\\")
    return json.loads(json_text)
