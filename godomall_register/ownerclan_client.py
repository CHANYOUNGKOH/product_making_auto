"""Ownerclan GraphQL API client with JWT auth, token caching, and retry."""

import base64
import json
import logging
import time
import urllib.parse
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

logger = logging.getLogger(__name__)

# ── URL constants ────────────────────────────────────────────────────────────
AUTH_URL = "https://auth.ownerclan.com/auth"
AUTH_SANDBOX_URL = "https://auth.sandbox.ownerclan.com/auth"
API_URL = "https://api.ownerclan.com/graphql"
API_SANDBOX_URL = "https://api.sandbox.ownerclan.com/graphql"

# ── GraphQL field-set constants ──────────────────────────────────────────────
ITEM_FIELDS_FULL = (
    "key name model price(currency: KRW) fixedPrice pricePolicy "
    "status images(size: large) "
    "options { price quantity status optionAttributes { name value } } "
    "category { key name fullName } "
    "content searchKeywords "
    "shippingFee shippingType taxFree adultOnly "
    "origin production openmarketSellable returnable "
    "createdAt updatedAt metadata"
)
ITEM_FIELDS_LIGHT = "key name price(currency: KRW) status updatedAt"
ITEM_FIELDS_MONITOR = "key name price(currency: KRW) status metadata"
HISTORY_FIELDS = "itemKey kind title valueBefore valueAfter createdAt"
ORDER_FIELDS_LIST = "key status createdAt"
ORDER_FIELDS_DETAIL = (
    "key status createdAt updatedAt ordererNote sellerNote isBeingMediated "
    "products { itemKey quantity price(currency: KRW) "
    "trackingNumber shippingCompanyName "
    "itemOptionInfo { optionAttributes { name value } } } "
    "shippingInfo { recipient { name phoneNumber "
    "destinationAddress { addr1 addr2 postalCode } } "
    "shippingFee(currency: KRW) }"
)
CATEGORY_FIELDS = "key name fullName"


# ── Exceptions ───────────────────────────────────────────────────────────────
class OwnerclanApiError(Exception):
    """Raised on GraphQL or HTTP errors from the Ownerclan API."""

    def __init__(self, message: str, errors: list | None = None):
        super().__init__(message)
        self.errors = errors or []


class OwnerclanAuthError(OwnerclanApiError):
    """Raised when authentication fails."""


# ── Client ───────────────────────────────────────────────────────────────────
class OwnerclanClient:
    """Thin wrapper around the Ownerclan GraphQL API."""

    MAX_KEYS_PER_CALL = 5000
    MAX_HISTORY_DAYS = 7
    POST_THRESHOLD = 4000

    # ── init / config ────────────────────────────────────────────────────
    def __init__(self, config_path: str | Path | None = None, sandbox: bool = False):
        if config_path is None:
            config_path = Path(__file__).parent / "ownerclan_config.json"
        self.config_path = Path(config_path)

        self.auth_url = AUTH_SANDBOX_URL if sandbox else AUTH_URL
        self.api_url = API_SANDBOX_URL if sandbox else API_URL

        self.session = requests.Session()
        self.token: str | None = None
        self.token_expires_at: datetime | None = None
        self.username: str = ""
        self.password: str = ""

        self._load_config()

    def _load_config(self) -> None:
        """Read config JSON; restore cached token when present."""
        with open(self.config_path, "r", encoding="utf-8") as f:
            cfg = json.load(f)

        self.username = cfg["username"]
        self.password = cfg["password"]

        cached_token = cfg.get("token")
        cached_expires = cfg.get("token_expires_at")

        if cached_token and cached_expires:
            try:
                self.token_expires_at = datetime.fromisoformat(cached_expires)
                self.token = cached_token
                logger.info("Restored cached token (expires %s)", self.token_expires_at)
            except (ValueError, TypeError):
                logger.warning("Invalid cached token_expires_at; ignoring cached token")
                self.token = None
                self.token_expires_at = None

    def _save_token(self) -> None:
        """Persist current token and expiry back to config JSON."""
        with open(self.config_path, "r", encoding="utf-8") as f:
            cfg = json.load(f)

        cfg["token"] = self.token
        cfg["token_expires_at"] = (
            self.token_expires_at.isoformat() if self.token_expires_at else None
        )

        with open(self.config_path, "w", encoding="utf-8") as f:
            json.dump(cfg, f, indent=4, ensure_ascii=False)
            f.write("\n")

        logger.debug("Token saved to %s", self.config_path)

    # ── authentication ───────────────────────────────────────────────────
    def authenticate(self) -> str:
        """Obtain a new JWT from the Ownerclan auth endpoint.

        Returns the raw token string.
        """
        payload = {
            "service": "ownerclan",
            "userType": "seller",
            "username": self.username,
            "password": self.password,
        }
        try:
            resp = self.session.post(self.auth_url, json=payload, timeout=10)
        except requests.RequestException as exc:
            raise OwnerclanAuthError(f"Auth request failed: {exc}") from exc

        if resp.status_code != 200:
            raise OwnerclanAuthError(
                f"Authentication failed (HTTP {resp.status_code}): {resp.text}"
            )

        token = resp.text.strip()
        if not token:
            raise OwnerclanAuthError("Empty token received from auth endpoint")

        # Parse JWT exp claim (second segment, base64-url decoded)
        try:
            parts = token.split(".")
            # Add padding for base64url
            payload_b64 = parts[1] + "=" * (-len(parts[1]) % 4)
            jwt_payload = json.loads(base64.urlsafe_b64decode(payload_b64))
            exp_ts = jwt_payload["exp"]
            self.token_expires_at = datetime.fromtimestamp(exp_ts, tz=timezone.utc)
            logger.info("JWT expires at %s", self.token_expires_at)
        except Exception:
            # Fallback: 30 days from now
            self.token_expires_at = datetime.now(timezone.utc) + timedelta(days=30)
            logger.warning("Could not parse JWT exp; defaulting to 30 days")

        self.token = token
        self.session.headers["Authorization"] = f"Bearer {self.token}"
        self._save_token()
        logger.info("Authenticated as %s", self.username)
        return token

    def _ensure_token(self) -> None:
        """Ensure a valid token is present; re-authenticate if needed."""
        now = datetime.now(timezone.utc)

        if self.token and self.token_expires_at:
            # Normalise naive expiry to UTC for comparison
            expires = self.token_expires_at
            if expires.tzinfo is None:
                expires = expires.replace(tzinfo=timezone.utc)

            if expires - now > timedelta(days=1):
                # Token still valid — just make sure the header is set
                if "Authorization" not in self.session.headers:
                    self.session.headers["Authorization"] = f"Bearer {self.token}"
                return

        # Token missing or about to expire
        logger.info("Token missing or expiring soon — re-authenticating")
        self.authenticate()
