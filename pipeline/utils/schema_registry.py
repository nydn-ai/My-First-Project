"""
Confluent-compatible Schema Registry client for Google Managed Kafka.

Supports:
  - Google ADC bearer-token auth (for GCP-hosted registries)
  - No-auth mode for local Docker / localhost URLs
  - Schema reference resolution (depth-first, cached)
  - Avro Confluent wire-format serialization / deserialization
"""

from __future__ import annotations

import logging
import struct
import threading
import time
from typing import Any, Dict, List, Optional, Tuple

import fastavro
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

# Retry strategy for transient Schema Registry errors.
# Retries on 429 (rate-limit) and 5xx server errors with exponential back-off.
# total=3 means up to 3 retries (4 attempts total).
# backoff_factor=0.5 → waits 0.5 s, 1 s, 2 s between retries.
_RETRY_STRATEGY = Retry(
    total=3,
    backoff_factor=0.5,
    status_forcelist={429, 500, 502, 503, 504},
    allowed_methods={"GET", "POST"},
    raise_on_status=False,
)

_MAGIC_BYTE = b"\x00"
_SCHEMA_ID_FMT = ">I"  # big-endian uint32
_SCHEMA_ID_SIZE = 4


class SchemaRegistryError(Exception):
    pass


class SchemaRegistryClient:
    """
    Thin client for a Confluent-compatible Schema Registry.

    Parameters
    ----------
    base_url:
        Registry base URL, e.g.
        ``https://managedkafka.googleapis.com/v1/projects/P/locations/L/schemaRegistries/R``
    cache_ttl_seconds:
        How long to cache resolved schemas before re-fetching.
    """

    def __init__(self, base_url: str, cache_ttl_seconds: int = 300):
        self._base_url = base_url.rstrip("/")
        self._cache_ttl = cache_ttl_seconds

        # Two separate locks to prevent deadlock:
        #   _schema_lock  — guards the schema cache (get_latest_schema / _cache)
        #   _token_lock   — guards ADC token refresh (_get_google_token)
        #
        # IMPORTANT: _get_google_token is called from _headers() which is called
        # from HTTP methods inside _fetch_and_resolve(), which is itself called
        # while _schema_lock is held.  Using a single lock for both would deadlock
        # because threading.Lock is not reentrant.
        self._schema_lock = threading.Lock()
        self._token_lock = threading.Lock()

        # subject → (schema_id, parsed_schema, named_types, fetched_at)
        self._cache: Dict[str, Tuple[int, Any, Dict, float]] = {}

        # Token cache — initialised here so _get_google_token never needs getattr()
        self._gcp_credentials = None
        self._token_value: Optional[str] = None
        self._token_expiry: float = 0.0

        # Shared requests.Session with retry — created once per worker instance
        # so connections to the Schema Registry are reused and transient 429/5xx
        # errors are retried with exponential back-off instead of going to DLQ.
        self._session = requests.Session()
        self._session.mount("https://", HTTPAdapter(max_retries=_RETRY_STRATEGY))
        self._session.mount("http://", HTTPAdapter(max_retries=_RETRY_STRATEGY))

    # ── Public API ────────────────────────────────────────────────────────────

    def get_latest_schema(self, subject: str) -> Tuple[int, Any, Dict[str, Any]]:
        """
        Return ``(schema_id, fastavro_parsed_schema, named_types_dict)``.

        ``named_types_dict`` maps fully-qualified Avro record names to their
        raw schema dicts so that ``avro_mapper`` can resolve string references.
        """
        with self._schema_lock:
            cached = self._cache.get(subject)
            if cached and time.monotonic() - cached[3] < self._cache_ttl:
                return cached[:3]

            schema_id, parsed, named_types = self._fetch_and_resolve(subject)
            self._cache[subject] = (schema_id, parsed, named_types, time.monotonic())
            return schema_id, parsed, named_types

    def register_schema(
        self,
        subject: str,
        schema_str: str,
        references: Optional[List[Dict]] = None,
        schema_type: str = "AVRO",
    ) -> int:
        """Register a schema and return the assigned schema ID."""
        payload: Dict[str, Any] = {
            "schema": schema_str,
            "schemaType": schema_type,
        }
        if references:
            payload["references"] = references

        url = f"{self._base_url}/subjects/{subject}/versions"
        resp = self._session.post(url, json=payload, headers=self._headers(), timeout=30)
        if resp.status_code not in (200, 201):
            raise SchemaRegistryError(
                f"register_schema failed [{resp.status_code}]: {resp.text}"
            )
        return resp.json()["id"]

    def get_schema_by_id(self, schema_id: int) -> Dict:
        """Fetch raw schema dict by ID."""
        url = f"{self._base_url}/schemas/ids/{schema_id}"
        resp = self._session.get(url, headers=self._headers(), timeout=15)
        if resp.status_code != 200:
            raise SchemaRegistryError(
                f"get_schema_by_id({schema_id}) failed [{resp.status_code}]: {resp.text}"
            )
        return resp.json()

    # ── Internal: reference resolution ───────────────────────────────────────

    def _fetch_and_resolve(self, subject: str) -> Tuple[int, Any, Dict[str, Any]]:
        """
        Depth-first resolution of schema references.

        Returns a fastavro-parsed schema and a dict of named types
        suitable for passing to avro_mapper.
        """
        import json

        url = f"{self._base_url}/subjects/{subject}/versions/latest"
        resp = self._session.get(url, headers=self._headers(), timeout=15)
        if resp.status_code != 200:
            raise SchemaRegistryError(
                f"Fetch subject '{subject}' failed [{resp.status_code}]: {resp.text}"
            )
        data = resp.json()
        schema_id: int = data["id"]
        raw_schema: Dict = _strip_non_avro_keys(json.loads(data["schema"]))
        references: List[Dict] = data.get("references", []) or []

        # Collect all referenced schemas first (depth-first)
        ref_schemas: List[Dict] = []
        named_types: Dict[str, Any] = {}
        for ref in references:
            _, _, ref_named = self._fetch_and_resolve(ref["subject"])
            named_types.update(ref_named)
            # Collect the raw schema for fastavro ordering
            ref_data = self._fetch_raw_schema(ref["subject"])
            ref_schemas.append(ref_data)

        # Parse with fastavro.
        # When there are no cross-schema references (our common case — all four
        # schemas are self-contained), pass the dict directly so fastavro
        # receives a single schema rather than a one-element list.  Some
        # fastavro versions handle single-element lists differently and the
        # direct form is unambiguous.
        if ref_schemas:
            parsed = fastavro.parse_schema(ref_schemas + [raw_schema])
        else:
            parsed = fastavro.parse_schema(raw_schema)

        # Build named_types from this schema
        self._collect_named_types(raw_schema, named_types)

        return schema_id, parsed, named_types

    def _fetch_raw_schema(self, subject: str) -> Dict:
        import json

        url = f"{self._base_url}/subjects/{subject}/versions/latest"
        resp = self._session.get(url, headers=self._headers(), timeout=15)
        if resp.status_code != 200:
            raise SchemaRegistryError(
                f"Fetch raw '{subject}' failed [{resp.status_code}]: {resp.text}"
            )
        return _strip_non_avro_keys(json.loads(resp.json()["schema"]))

    @staticmethod
    def _collect_named_types(schema: Any, out: Dict[str, Any]) -> None:
        """Recursively collect all named Avro types into ``out``."""
        if isinstance(schema, dict):
            if schema.get("type") == "record":
                ns = schema.get("namespace", "")
                name = schema.get("name", "")
                fqn = f"{ns}.{name}" if ns else name
                out[fqn] = schema
                out[name] = schema  # also store short name
                for field in schema.get("fields", []):
                    SchemaRegistryClient._collect_named_types(field.get("type"), out)
            elif schema.get("type") in ("array", "map"):
                SchemaRegistryClient._collect_named_types(schema.get("items"), out)
                SchemaRegistryClient._collect_named_types(schema.get("values"), out)
        elif isinstance(schema, list):
            for item in schema:
                SchemaRegistryClient._collect_named_types(item, out)

    # ── Auth ──────────────────────────────────────────────────────────────────

    @property
    def _is_local(self) -> bool:
        """True when the registry is running locally (Docker / CI)."""
        host = self._base_url.lower()
        return any(
            marker in host
            for marker in ("localhost", "127.0.0.1", "0.0.0.0", "host.docker.internal")
        )

    def _headers(self) -> Dict[str, str]:
        if self._is_local:
            return {"Content-Type": "application/vnd.schemaregistry.v1+json"}
        token = self._get_google_token()
        return {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/vnd.schemaregistry.v1+json",
        }

    def _get_google_token(self) -> str:
        """
        Return a valid Google OAuth2 token, refreshing only when expired.

        Tokens are cached on the instance to avoid an ADC round-trip on every
        HTTP call.  Uses a dedicated ``_token_lock`` — NOT ``_schema_lock`` —
        so this method is safe to call from inside ``get_latest_schema()``
        (which holds ``_schema_lock``) without deadlocking.
        """
        import time as _time

        with self._token_lock:
            now = _time.monotonic()
            # Serve from cache if still valid (60 s safety margin built into expiry)
            if self._token_value is not None and now < self._token_expiry:
                return self._token_value

            import google.auth
            import google.auth.transport.requests

            # Initialise credentials once; reuse across refreshes
            if self._gcp_credentials is None:
                self._gcp_credentials, _ = google.auth.default(
                    scopes=["https://www.googleapis.com/auth/cloud-platform"]
                )

            self._gcp_credentials.refresh(google.auth.transport.requests.Request())
            self._token_value = self._gcp_credentials.token

            if self._gcp_credentials.expiry is not None:
                import datetime

                expiry_epoch = self._gcp_credentials.expiry.replace(
                    tzinfo=datetime.timezone.utc
                ).timestamp()
                ttl = max(0.0, expiry_epoch - _time.time() - 60)
                self._token_expiry = now + ttl
            else:
                self._token_expiry = now + 55 * 60  # default 55-min TTL

            logger.debug(
                "Schema Registry token refreshed; next refresh in %.0f s",
                self._token_expiry - now,
            )
            return self._token_value


# ──────────────────────────────────────────────────────────────────────────────
# Confluent wire-format serializer / deserializer
# ──────────────────────────────────────────────────────────────────────────────


class AvroConfluentSerializer:
    """
    Serialize / deserialize using the Confluent wire format:

        [0x00] [4-byte schema_id big-endian] [fastavro binary payload]
    """

    def __init__(self, client: SchemaRegistryClient):
        self._client = client

    def serialize(
        self, record: Dict[str, Any], schema_id: int, parsed_schema: Any
    ) -> bytes:
        import io

        buf = io.BytesIO()
        buf.write(_MAGIC_BYTE)
        buf.write(struct.pack(_SCHEMA_ID_FMT, schema_id))
        fastavro.schemaless_writer(buf, parsed_schema, record)
        return buf.getvalue()

    def deserialize(self, data: bytes) -> Dict[str, Any]:
        import io

        if data[0:1] != _MAGIC_BYTE:
            raise ValueError("Invalid Confluent magic byte")
        (schema_id,) = struct.unpack(_SCHEMA_ID_FMT, data[1:5])
        schema_data = self._client.get_schema_by_id(schema_id)
        import json

        parsed = fastavro.parse_schema(
            _strip_non_avro_keys(json.loads(schema_data["schema"]))
        )
        buf = io.BytesIO(data[5:])
        return fastavro.schemaless_reader(buf, parsed)


# ──────────────────────────────────────────────────────────────────────────────
# Schema sanitizer
# ──────────────────────────────────────────────────────────────────────────────

# Avro spec keys that fastavro understands at the record level.
# Any other top-level keys (e.g. "x-meta") are stripped before parsing
# so fastavro does not raise on unknown properties.
_AVRO_RECORD_KEYS = frozenset(
    {
        "type",
        "name",
        "namespace",
        "doc",
        "fields",
        "aliases",
        "items",
        "values",
        "symbols",
        "size",
        "default",
        "order",
        "logicalType",
        "precision",
        "scale",
    }
)


def _strip_non_avro_keys(schema: Any) -> Any:
    """
    Recursively strip non-standard keys (e.g. ``x-meta``) from a schema dict.

    This is needed for schemas that carry vendor metadata extensions which
    fastavro does not recognise and may raise on.
    """
    if isinstance(schema, dict):
        cleaned = {}
        for k, v in schema.items():
            if k.startswith("x-"):
                continue  # drop extension keys
            cleaned[k] = _strip_non_avro_keys(v)
        # NOTE: the recursive call above already processes "fields" values.
        # Do NOT add a second pass here — it would be redundant and confusing.
        return cleaned
    elif isinstance(schema, list):
        return [_strip_non_avro_keys(item) for item in schema]
    return schema
