"""
Time-related helpers shared across the pipeline.

Kept in a proper sub-module (pipeline.utils.time_utils) so that Beam/dill
can serialise references to these functions by their stable module path
instead of as __main__.xxx — the latter breaks on Dataflow workers which
run the beam harness (not main.py) as __main__.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone

# Nanosecond timestamp: truncate to 6 fractional digits for Python datetime
_NS_RE = re.compile(r"(\.\d{6})\d+(Z|[+-]\d{2}:\d{2})$")


def pubsub_time_to_millis(publish_time) -> int:
    """Convert a Pub/Sub publish_time (Timestamp protobuf or ISO string) to epoch ms."""
    if publish_time is None:
        return int(datetime.now(timezone.utc).timestamp() * 1000)
    if hasattr(publish_time, "seconds"):
        return publish_time.seconds * 1000 + publish_time.nanos // 1_000_000
    s = str(publish_time)
    s = _NS_RE.sub(r"\1\2", s)
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        return int(dt.timestamp() * 1000)
    except Exception:
        return int(datetime.now(timezone.utc).timestamp() * 1000)
