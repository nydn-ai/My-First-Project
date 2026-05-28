"""
Google Managed Kafka OAUTHBEARER token provider.

Google Managed Kafka uses IAM (roles/managedkafka.client) with
Application Default Credentials — no username/password is required.
This module provides the token provider that kafka-python calls before
each SASL handshake.
"""

from __future__ import annotations

import logging
import threading
import time
from typing import Optional

logger = logging.getLogger(__name__)

# Google Managed Kafka OAuth scope
_KAFKA_SCOPE = "https://www.googleapis.com/auth/cloud-platform"


class GoogleManagedKafkaTokenProvider:
    """
    Thread-safe ADC-based OAuth2 token provider for kafka-python.

    kafka-python's SASL OAUTHBEARER support calls `token()` before each
    new connection.  We cache the token and refresh it 60 s before expiry
    to avoid mid-stream expiration.
    """

    def __init__(self, scopes: Optional[list] = None):
        self._scopes = scopes or [_KAFKA_SCOPE]
        self._lock = threading.Lock()
        self._credentials = None
        self._token_value: Optional[str] = None
        self._expiry: float = 0.0  # epoch seconds

    # ── kafka-python interface ────────────────────────────────────────────────

    def token(self) -> str:
        """Return a valid OAuth2 bearer token string."""
        with self._lock:
            if self._token_value is None or time.monotonic() >= self._expiry:
                self._refresh()
            return self._token_value

    # ── Internal ──────────────────────────────────────────────────────────────

    def _refresh(self) -> None:
        import google.auth
        import google.auth.transport.requests

        if self._credentials is None:
            self._credentials, _ = google.auth.default(scopes=self._scopes)

        request = google.auth.transport.requests.Request()
        self._credentials.refresh(request)

        self._token_value = self._credentials.token

        # ── Diagnostics: log credentials type so auth failures are debuggable ──
        cred_type = type(self._credentials).__name__
        email = getattr(self._credentials, "service_account_email", None) or getattr(
            self._credentials, "_service_account_email", None
        )
        logger.info(
            "Kafka token refreshed — credentials type=%s service_account=%s "
            "token_prefix=%s",
            cred_type,
            email or "n/a",
            (self._token_value or "")[:8],
        )

        if self._credentials.expiry is not None:
            import datetime

            expiry_epoch = self._credentials.expiry.replace(
                tzinfo=datetime.timezone.utc
            ).timestamp()
            # Refresh 60 s before actual expiry
            self._expiry = time.monotonic() + max(0, expiry_epoch - time.time() - 60)
        else:
            # Default: refresh every 55 minutes
            self._expiry = time.monotonic() + 55 * 60

        logger.debug(
            "Google Managed Kafka token refreshed; next refresh in %.0f s",
            self._expiry - time.monotonic(),
        )
