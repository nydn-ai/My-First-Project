"""
Python dict → Avro record coercion.

Validates types against the parsed fastavro schema and coerces common
mismatches (e.g. nanosecond ISO strings → epoch ms, float → int).
Resolves Avro named-type string references via ``named_types`` dict.
"""

from __future__ import annotations

import re
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

# Nanosecond ISO timestamp: "2026-05-14T16:50:40.357419719Z"
_NS_TS_RE = re.compile(r"(\.\d{6})\d+(Z|[+-]\d{2}:\d{2})$")


class AvroMappingError(Exception):
    pass


# ──────────────────────────────────────────────────────────────────────────────
# Public entry point
# ──────────────────────────────────────────────────────────────────────────────


def map_to_avro(
    data: Any,
    schema: Any,
    named_types: Optional[Dict[str, Any]] = None,
    field_path: str = "",
) -> Any:
    """
    Recursively coerce *data* to match *schema*.

    Parameters
    ----------
    data:
        Python value to coerce.
    schema:
        fastavro-parsed (or raw dict) Avro schema node.
    named_types:
        Dict mapping fully-qualified Avro names → schema dicts.
        Required when schemas use string references like
        ``"com.bumble.avro.ml.vertex.VertexMLLog"``.
    field_path:
        Dot-separated path for error messages.
    """
    nt = named_types or {}
    return _coerce(data, schema, nt, field_path)


# ──────────────────────────────────────────────────────────────────────────────
# Core coercion
# ──────────────────────────────────────────────────────────────────────────────


def _coerce(value: Any, schema: Any, named_types: Dict, path: str) -> Any:
    # ── Resolve string reference ───────────────────────────────────────────
    if isinstance(schema, str):
        if schema in (
            "null",
            "string",
            "int",
            "long",
            "float",
            "double",
            "boolean",
            "bytes",
            "fixed",
        ):
            return _coerce_primitive(value, schema, path)
        resolved = named_types.get(schema)
        if resolved is None:
            raise AvroMappingError(
                f"[{path}] Unknown named type '{schema}'. "
                f"Known: {list(named_types.keys())[:10]}"
            )
        return _coerce(value, resolved, named_types, path)

    # ── Union ──────────────────────────────────────────────────────────────
    if isinstance(schema, list):
        return _coerce_union(value, schema, named_types, path)

    if not isinstance(schema, dict):
        raise AvroMappingError(f"[{path}] Unexpected schema node type: {type(schema)}")

    # ── Logical types ──────────────────────────────────────────────────────
    logical = schema.get("logicalType")
    if logical == "uuid":
        return _coerce_uuid(value, path)
    if logical in ("timestamp-millis",):
        return _coerce_timestamp(value, "millis", path)
    if logical in ("timestamp-micros",):
        return _coerce_timestamp(value, "micros", path)

    avro_type = schema.get("type")

    # ── Record ────────────────────────────────────────────────────────────
    if avro_type == "record":
        return _coerce_record(value, schema, named_types, path)

    # ── Array ─────────────────────────────────────────────────────────────
    if avro_type == "array":
        if value is None:
            return []
        if not isinstance(value, (list, tuple)):
            raise AvroMappingError(
                f"[{path}] Expected list, got {type(value).__name__}"
            )
        items_schema = schema["items"]
        return [
            _coerce(v, items_schema, named_types, f"{path}[{i}]")
            for i, v in enumerate(value)
        ]

    # ── Map ───────────────────────────────────────────────────────────────
    if avro_type == "map":
        if value is None:
            return {}
        if not isinstance(value, dict):
            raise AvroMappingError(
                f"[{path}] Expected dict, got {type(value).__name__}"
            )
        values_schema = schema["values"]
        return {
            k: _coerce(v, values_schema, named_types, f"{path}.{k}")
            for k, v in value.items()
        }

    # ── Enum ──────────────────────────────────────────────────────────────
    if avro_type == "enum":
        symbols = schema.get("symbols", [])
        val = str(value) if value is not None else symbols[0] if symbols else ""
        if val not in symbols:
            raise AvroMappingError(f"[{path}] Enum value '{val}' not in {symbols}")
        return val

    # ── Primitives wrapped in dict (e.g. {"type": "string"}) ─────────────
    if isinstance(avro_type, str):
        return _coerce_primitive(value, avro_type, path)

    # ── Nested union inside dict ──────────────────────────────────────────
    if isinstance(avro_type, list):
        return _coerce_union(value, avro_type, named_types, path)

    raise AvroMappingError(f"[{path}] Cannot coerce schema: {schema}")


def _coerce_record(value: Any, schema: Dict, named_types: Dict, path: str) -> Dict:
    if not isinstance(value, dict):
        raise AvroMappingError(
            f"[{path}] Expected dict for record '{schema.get('name')}', "
            f"got {type(value).__name__}"
        )
    result = {}
    for field in schema.get("fields", []):
        fname = field["name"]
        fpath = f"{path}.{fname}" if path else fname
        fschema = field["type"]
        raw = value.get(fname)

        if raw is None:
            # Use default if present
            if "default" in field:
                result[fname] = field["default"]
            elif _is_nullable(fschema):
                result[fname] = None
            else:
                raise AvroMappingError(
                    f"[{fpath}] Required field missing (no default, not nullable)"
                )
        else:
            result[fname] = _coerce(raw, fschema, named_types, fpath)
    return result


def _coerce_union(value: Any, union_schema: List, named_types: Dict, path: str) -> Any:
    """Try each branch of a union in order; null always matches None."""
    if value is None:
        if "null" in union_schema:
            return None
        raise AvroMappingError(f"[{path}] None value not allowed in non-nullable union")

    non_null = [s for s in union_schema if s != "null"]
    for branch in non_null:
        try:
            return _coerce(value, branch, named_types, path)
        except (AvroMappingError, TypeError, ValueError):
            continue

    raise AvroMappingError(
        f"[{path}] Value {value!r} did not match any union branch: {union_schema}"
    )


def _coerce_primitive(value: Any, avro_type: str, path: str) -> Any:
    if avro_type == "null":
        return None
    if avro_type == "string":
        return str(value) if value is not None else ""
    if avro_type == "boolean":
        if isinstance(value, bool):
            return value
        return bool(value)
    if avro_type == "int":
        return _to_int(value, path)
    if avro_type == "long":
        return _to_long(value, path)
    if avro_type in ("float", "double"):
        return float(value)
    if avro_type == "bytes":
        if isinstance(value, (bytes, bytearray)):
            return value
        return str(value).encode()
    raise AvroMappingError(f"[{path}] Unknown primitive type '{avro_type}'")


# ──────────────────────────────────────────────────────────────────────────────
# Logical type helpers
# ──────────────────────────────────────────────────────────────────────────────


def _coerce_uuid(value: Any, path: str) -> str:
    if value is None:
        return str(uuid.uuid4())
    s = str(value)
    try:
        return str(uuid.UUID(s))
    except ValueError:
        raise AvroMappingError(f"[{path}] Invalid UUID: {s!r}")


def _coerce_timestamp(value: Any, unit: str, path: str) -> int:
    """
    Convert a timestamp value to epoch milliseconds or microseconds.

    Accepts:
      - int / float (already epoch ms or µs)
      - ISO 8601 string, including nanosecond precision
        ("2026-05-14T16:50:40.357419719Z")
    """
    if value is None:
        epoch = int(datetime.now(timezone.utc).timestamp() * 1000)
        return epoch if unit == "millis" else epoch * 1000

    if isinstance(value, (int, float)):
        return int(value)

    s = str(value)
    # Truncate nanoseconds to microseconds for Python fromisoformat
    s = _NS_TS_RE.sub(r"\1\2", s)
    s = s.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(s)
        epoch_s = dt.timestamp()
        if unit == "millis":
            return int(epoch_s * 1_000)
        return int(epoch_s * 1_000_000)
    except Exception as exc:
        raise AvroMappingError(f"[{path}] Cannot parse timestamp '{value}': {exc}")


# ──────────────────────────────────────────────────────────────────────────────
# Utilities
# ──────────────────────────────────────────────────────────────────────────────


def _is_nullable(schema: Any) -> bool:
    if isinstance(schema, list):
        return "null" in schema
    return schema == "null"


def _to_int(value: Any, path: str) -> int:
    try:
        if isinstance(value, float):
            return int(value)
        return int(value)
    except (TypeError, ValueError) as exc:
        raise AvroMappingError(f"[{path}] Cannot convert {value!r} to int: {exc}")


def _to_long(value: Any, path: str) -> int:
    return _to_int(value, path)
