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
AUTH_URL = "https://auth.ownerclan.com/auth/authenticate"
AUTH_SANDBOX_URL = "https://auth.sandbox.ownerclan.com/auth/authenticate"
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
