#!/usr/bin/env python3
"""
Test Kafka consumer for the dev-enriched-vertex-logs topic.

Connects to Google Managed Kafka using ADC (OAUTHBEARER), reads messages
from the topic, deserializes the Confluent Avro wire format, and prints
the decoded record in a readable format.

Usage:
  python scripts/test_kafka_consumer.py \\
    --bootstrap_servers "bootstrap.dev-vertex-log-kafka.us-east1.managedkafka.PROJECT.cloud.goog:9092" \\
    --topic dev-enriched-vertex-logs \\
    --registry_url "https://managedkafka.googleapis.com/v1/projects/P/locations/L/schemaRegistries/R" \\
    --timeout 30 \\
    --max_messages 10
"""
from __future__ import annotations

import argparse
import json
import logging
import struct
import time
from datetime import datetime, timezone

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s — %(message)s",
)
logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────────────────────────
# Token provider (mirrors kafka_auth.py — standalone for this script)
# ──────────────────────────────────────────────────────────────────────────────


class _TokenProvider:
    """
    Thread-safe ADC token provider for kafka-python and schema registry calls.

    Caches the token and refreshes 60 s before expiry to avoid a round-trip
    on every connection attempt.  A single instance is shared across the Kafka
    consumer and schema registry fetches so credentials are initialised once.
    """

    def __init__(self):
        import threading

        self._lock = threading.Lock()
        self._credentials = None
        self._token: str | None = None
        self._expiry: float = 0.0  # monotonic deadline

    def token(self) -> str:
        import time

        import google.auth
        import google.auth.transport.requests

        with self._lock:
            if self._token is not None and time.monotonic() < self._expiry:
                return self._token

            # Initialise credentials once; subsequent calls just refresh
            if self._credentials is None:
                self._credentials, _ = google.auth.default(
                    scopes=["https://www.googleapis.com/auth/cloud-platform"]
                )

            self._credentials.refresh(google.auth.transport.requests.Request())
            self._token = self._credentials.token

            if self._credentials.expiry is not None:
                import datetime

                expiry_epoch = self._credentials.expiry.replace(
                    tzinfo=datetime.timezone.utc
                ).timestamp()
                ttl = max(0.0, expiry_epoch - time.time() - 60)
                self._expiry = time.monotonic() + ttl
            else:
                self._expiry = time.monotonic() + 55 * 60

            logger.debug(
                "ADC token refreshed; next refresh in %.0f s",
                self._expiry - time.monotonic(),
            )
            return self._token


# Module-level shared token provider — one ADC credential shared between the
# Kafka consumer and schema registry HTTP calls; avoids redundant ADC refreshes.
_shared_token_provider = _TokenProvider()

# ──────────────────────────────────────────────────────────────────────────────
# Avro deserializer
# ──────────────────────────────────────────────────────────────────────────────


def _get_schema(registry_url: str, schema_id: int) -> dict:
    """Fetch a schema by ID from the registry, using the shared token provider."""
    import requests

    base = registry_url.lower()
    is_local = any(m in base for m in ("localhost", "127.0.0.1", "0.0.0.0"))
    if is_local:
        headers: dict = {}
    else:
        # Reuse cached token — no google.auth.default() round-trip per call
        headers = {"Authorization": f"Bearer {_shared_token_provider.token()}"}

    url = f"{registry_url.rstrip('/')}/schemas/ids/{schema_id}"
    resp = requests.get(url, headers=headers, timeout=15)
    resp.raise_for_status()
    return resp.json()


_schema_cache: dict = {}


def deserialize_avro(data: bytes, registry_url: str) -> dict:
    """Deserialize Confluent wire-format Avro bytes."""
    import io

    import fastavro

    if data[0:1] != b"\x00":
        raise ValueError(f"Expected Confluent magic byte 0x00, got 0x{data[0]:02x}")

    (schema_id,) = struct.unpack(">I", data[1:5])

    if schema_id not in _schema_cache:
        schema_data = _get_schema(registry_url, schema_id)
        parsed = fastavro.parse_schema(json.loads(schema_data["schema"]))
        _schema_cache[schema_id] = parsed

    parsed_schema = _schema_cache[schema_id]
    buf = io.BytesIO(data[5:])
    return fastavro.schemaless_reader(buf, parsed_schema)


# ──────────────────────────────────────────────────────────────────────────────
# Pretty-printer
# ──────────────────────────────────────────────────────────────────────────────


def _ts_to_iso(epoch_ms: int) -> str:
    """Convert epoch milliseconds to readable ISO string."""
    try:
        dt = datetime.fromtimestamp(epoch_ms / 1000, tz=timezone.utc)
        return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    except Exception:
        return str(epoch_ms)


def _pretty_print(record: dict, offset: int, partition: int, key: bytes | None) -> None:
    print()
    print(f"  {'─' * 60}")
    print(f"  Partition: {partition}  Offset: {offset}")
    print(f"  Key:       {key.decode('utf-8', errors='replace') if key else '(none)'}")
    print()

    # Handle nested common field
    if "common" in record:
        common = record["common"]
        print("  [common]")
        print(f"    id             : {common.get('id', '?')}")
        ts = common.get("ts")
        print(f"    ts             : {_ts_to_iso(ts) if ts else '?'}")
        print(f"    endpoint_id    : {common.get('endpoint_id', '?')}")
        print(f"    model_id       : {common.get('model_id', '?')}")
        print(f"    model_version  : {common.get('model_version', '?')}")
        print(f"    severity       : {common.get('severity', '?')}")
        print(f"    resource_type  : {common.get('resource_type', '?')}")

        specific = {k: v for k, v in record.items() if k != "common"}
        if specific:
            print("  [specific]")
            for k, v in specific.items():
                if v is not None:
                    print(f"    {k:<20}: {v}")
    else:
        # Flat record (VertexMLLog directly)
        for k, v in record.items():
            if k in ("ts", "insert_time") and isinstance(v, int):
                print(f"    {k:<20}: {_ts_to_iso(v)} ({v})")
            elif k == "raw_payload_json":
                print(f"    {k:<20}: [len={len(str(v))} chars]")
            else:
                print(f"    {k:<20}: {v}")


# ──────────────────────────────────────────────────────────────────────────────
# Consumer
# ──────────────────────────────────────────────────────────────────────────────


def consume(
    bootstrap_servers: str,
    topic: str,
    registry_url: str,
    timeout_seconds: int = 30,
    max_messages: int = 100,
    from_beginning: bool = False,
    group_id: str = "vertex-ml-logs-consumer-dev",
) -> None:
    import datetime

    from confluent_kafka import Consumer, KafkaError

    # OAUTHBEARER token refresh callback — Google Managed Kafka requires a
    # specific 3-part base64-encoded token format (GOOG_OAUTH2_TOKEN), not a
    # raw Bearer access token.  See kafka_sink.py for the full explanation.
    def _oauth_cb(config_str):
        import base64
        import json
        import time as _time

        import google.auth
        import google.auth.transport.requests

        creds, _ = google.auth.default(
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        creds.refresh(google.auth.transport.requests.Request())

        expiry = creds.expiry
        if expiry is None:
            expiry = datetime.datetime.utcnow() + datetime.timedelta(hours=1)
        if expiry.tzinfo is None:
            utc_expiry = expiry.replace(tzinfo=datetime.timezone.utc)
        else:
            utc_expiry = expiry
        expiry_ts = utc_expiry.timestamp()

        sa_email = (
            getattr(creds, "service_account_email", None)
            or getattr(creds, "_service_account_email", None)
            or ""
        )

        def _b64url(s: str) -> str:
            return (
                base64.urlsafe_b64encode(s.encode("utf-8")).decode("utf-8").rstrip("=")
            )

        header_b64 = _b64url(json.dumps({"typ": "JWT", "alg": "GOOG_OAUTH2_TOKEN"}))
        claims_b64 = _b64url(
            json.dumps(
                {
                    "exp": expiry_ts,
                    "iss": "Google",
                    "iat": _time.time(),
                    "scope": "kafka",
                    "sub": sa_email,
                }
            )
        )
        token_b64 = _b64url(creds.token or "")

        kafka_token = f"{header_b64}.{claims_b64}.{token_b64}"
        logger.debug(
            "OAUTHBEARER token built — sa=%s prefix=%s",
            sa_email or "n/a",
            (creds.token or "")[:8],
        )
        return kafka_token, expiry_ts

    auto_offset = "earliest" if from_beginning else "latest"

    logger.info("Connecting to Kafka bootstrap: %s", bootstrap_servers)
    logger.info("Topic: %s", topic)
    logger.info(
        "Offset: %s  |  Timeout: %ds  |  Max messages: %d",
        auto_offset,
        timeout_seconds,
        max_messages,
    )

    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms": "OAUTHBEARER",
            "oauth_cb": _oauth_cb,
            "group.id": group_id,
            "auto.offset.reset": auto_offset,
            "enable.auto.commit": True,
        }
    )
    consumer.subscribe([topic])

    count = 0
    errors = 0
    start = time.monotonic()
    deadline = start + timeout_seconds

    print()
    print(f"  Listening on topic '{topic}' for {timeout_seconds}s ...")
    print("  Send test messages with: make generate-test-log")
    print()

    try:
        while time.monotonic() < deadline and count < max_messages:
            remaining = deadline - time.monotonic()
            msg = consumer.poll(timeout=min(1.0, remaining))
            if msg is None:
                continue

            if msg.error():
                err = msg.error()
                if err.code() == KafkaError._PARTITION_EOF:
                    # End of partition — normal, keep polling
                    continue
                logger.warning("Consumer error: %s", err)
                errors += 1
                continue

            count += 1
            raw = msg.value()
            try:
                record = deserialize_avro(raw, registry_url)
                _pretty_print(record, msg.offset(), msg.partition(), msg.key())
            except Exception as exc:
                errors += 1
                logger.warning(
                    "Deserialization error (offset %d): %s",
                    msg.offset(),
                    exc,
                )
                print(f"  [raw bytes, len={len(raw)}]: {raw[:80]!r}...")

            if count >= max_messages:
                logger.info("Reached max_messages=%d, stopping.", max_messages)
                break

    finally:
        consumer.close()

    elapsed = time.monotonic() - start
    print()
    print(f"  {'─' * 60}")
    print(f"  Consumed: {count} message(s) in {elapsed:.1f}s  |  Errors: {errors}")
    if count == 0:
        print()
        print("  ⚠ No messages received. Possible causes:")
        print("    1. Dataflow job is not running yet.")
        print(
            "    2. No test messages have been published (run: make generate-test-log)."
        )
        print(
            "    3. Consumer is reading from 'latest' — publish AFTER starting consumer."
        )
        print("       Use --from_beginning to read all existing messages.")
    print()


# ──────────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(
        description="Consume and display Avro messages from a Kafka topic."
    )
    parser.add_argument(
        "--bootstrap_servers",
        default=(
            "bootstrap.dev-vertex-log-kafka.us-east1.managedkafka."
            "project-1c03ae00-17f3-43f4-86a.cloud.goog:9092"
        ),
        help="Kafka bootstrap server(s).",
    )
    parser.add_argument(
        "--topic",
        default="dev-enriched-vertex-logs",
        help="Kafka topic to consume from.",
    )
    parser.add_argument(
        "--registry_url",
        default=(
            "https://managedkafka.googleapis.com/v1/projects/"
            "project-1c03ae00-17f3-43f4-86a/locations/us-east1/"
            "schemaRegistries/dev_schema_registry"
        ),
        help="Schema Registry base URL.",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=30,
        help="Seconds to wait for messages before exiting (default: 30).",
    )
    parser.add_argument(
        "--max_messages",
        type=int,
        default=20,
        help="Maximum messages to consume (default: 20).",
    )
    parser.add_argument(
        "--from_beginning",
        action="store_true",
        help="Read from the earliest offset (replay all messages).",
    )
    parser.add_argument(
        "--group_id",
        default="vertex-ml-logs-consumer-dev",
        help="Kafka consumer group ID.",
    )
    args = parser.parse_args()

    consume(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        registry_url=args.registry_url,
        timeout_seconds=args.timeout,
        max_messages=args.max_messages,
        from_beginning=args.from_beginning,
        group_id=args.group_id,
    )


if __name__ == "__main__":
    main()
