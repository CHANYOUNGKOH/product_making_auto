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
API_URL = "https://api.ownerclan.com/v1/graphql"
API_SANDBOX_URL = "https://api.sandbox.ownerclan.com/v1/graphql"

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

    # ── GraphQL transport ────────────────────────────────────────────────
    def _graphql(self, query: str, variables: dict | None = None, timeout: int = 30):
        """Execute a GraphQL query with GET/POST auto-switch and retry.

        Returns the value of the first key in the ``data`` dict.
        """
        self._ensure_token()

        max_retries = 3

        for attempt in range(max_retries):
            try:
                # Decide GET vs POST based on URL length
                encoded_query = urllib.parse.quote(query, safe="")
                get_url = f"{self.api_url}?query={encoded_query}"

                if len(get_url) > self.POST_THRESHOLD:
                    # POST with JSON body
                    body: dict = {"query": query}
                    if variables:
                        body["variables"] = variables
                    resp = self.session.post(
                        self.api_url, json=body, timeout=timeout
                    )
                else:
                    # GET
                    resp = self.session.get(get_url, timeout=timeout)

            except requests.Timeout as exc:
                raise OwnerclanApiError(
                    f"Request timed out after {timeout}s"
                ) from exc
            except requests.RequestException as exc:
                raise OwnerclanApiError(
                    f"Request failed: {exc}"
                ) from exc

            # Retryable HTTP errors
            if resp.status_code == 429:
                wait = 3 * (attempt + 1)
                logger.warning(
                    "Rate-limited (429), waiting %ds (attempt %d/%d)",
                    wait, attempt + 1, max_retries,
                )
                time.sleep(wait)
                continue

            if 500 <= resp.status_code < 600:
                wait = 2 * (attempt + 1)
                logger.warning(
                    "Server error (%d), waiting %ds (attempt %d/%d)",
                    resp.status_code, wait, attempt + 1, max_retries,
                )
                time.sleep(wait)
                continue

            # Non-retryable HTTP errors
            if resp.status_code != 200:
                raise OwnerclanApiError(
                    f"HTTP {resp.status_code}: {resp.text[:500]}"
                )

            # Parse JSON
            try:
                data = resp.json()
            except ValueError as exc:
                raise OwnerclanApiError(
                    f"Invalid JSON response: {resp.text[:500]}"
                ) from exc

            # GraphQL-level errors
            if "errors" in data:
                messages = [
                    e.get("message", str(e)) for e in data["errors"]
                ]
                raise OwnerclanApiError(
                    f"GraphQL errors: {'; '.join(messages)}",
                    errors=data["errors"],
                )

            # Extract data — return value of the first key
            result = data.get("data", {})
            if not result:
                raise OwnerclanApiError(
                    f"No data in response: {json.dumps(data)[:500]}"
                )
            first_key = next(iter(result))
            return result[first_key]

        # Exhausted all retries
        raise OwnerclanApiError(
            f"Max retries ({max_retries}) exceeded for GraphQL request"
        )

    # ── metadata parsing ──────────────────────────────────────────────────
    def _parse_metadata(self, item: dict) -> dict:
        """metadata JSON 스칼라 → dict 파싱. 실패 시 빈 dict."""
        if "metadata" in item and isinstance(item["metadata"], str):
            try:
                item["metadata"] = json.loads(item["metadata"])
            except (json.JSONDecodeError, TypeError):
                item["metadata"] = {}
        return item

    # ── single item query ─────────────────────────────────────────────────
    def get_item(self, key: str, fields: str = None) -> dict:
        """item(key) — 단건 상세 조회. metadata 자동 파싱."""
        if fields is None:
            fields = ITEM_FIELDS_FULL
        query = f'{{ item(key: "{key}") {{ {fields} }} }}'
        result = self._graphql(query)
        return self._parse_metadata(result) if result else result

    # ── pagination helper ─────────────────────────────────────────────────
    def _paginate(self, query_template: str, connection_name: str,
                  first: int = 100, max_pages: int = None,
                  timeout: int = 30) -> list:
        """cursor-based pagination 공통 처리. 전체 node 리스트 반환."""
        all_nodes = []
        cursor = None
        page = 0

        while True:
            after_clause = f', after: "{cursor}"' if cursor else ""
            query = query_template.format(first=first, after_clause=after_clause)
            result = self._graphql(f"{{ {query} }}", timeout=timeout)

            if not result:
                break

            page_info = result.get("pageInfo", {})
            edges = result.get("edges", [])

            for edge in edges:
                node = edge.get("node", edge)
                all_nodes.append(node)

            if not page_info.get("hasNextPage", False):
                break

            cursor = page_info.get("endCursor")
            if not cursor:
                break

            page += 1
            if max_pages and page >= max_pages:
                break

            logger.debug("Page %d done, %d items so far", page, len(all_nodes))

        return all_nodes

    # ── search items ──────────────────────────────────────────────────────
    def search_items(self, *, search: str = None, status: str = None,
                     vendor: str = None, grade: str = None,
                     category: str = None, min_price: int = None,
                     max_price: int = None, sort_by: str = None,
                     date_from: int = None, date_to: int = None,
                     first: int = 100, max_pages: int = None,
                     fields: str = None, timeout: int = 30) -> list[dict]:
        """allItems — 필터 조합 검색 + cursor pagination."""
        if fields is None:
            fields = ITEM_FIELDS_LIGHT

        params = []
        params.append("first: {first}")
        params.append("{after_clause}")
        if search:
            params.append(f'search: "{search}"')
        if status:
            params.append(f"status: {status}")
        if vendor:
            params.append(f'vendor: "{vendor}"')
        if grade:
            params.append(f"grade: {grade}")
        if category:
            params.append(f'category: "{category}"')
        if min_price is not None:
            params.append(f"minPrice: {min_price}")
        if max_price is not None:
            params.append(f"maxPrice: {max_price}")
        if sort_by:
            params.append(f"sortBy: {sort_by}")
        if date_from is not None:
            params.append(f"dateFrom: {date_from}")
        if date_to is not None:
            params.append(f"dateTo: {date_to}")

        param_str = ", ".join(params)
        # Double-escape braces that must survive .format() in _paginate
        query_template = (
            f"allItems({param_str}) {{{{ "
            f"pageInfo {{{{ hasNextPage endCursor }}}} "
            f"edges {{{{ cursor node {{{{ {fields} }}}} }}}} }}}}"
        )

        items = self._paginate(query_template, "allItems",
                               first=first, max_pages=max_pages, timeout=timeout)

        if "metadata" in (fields or ""):
            items = [self._parse_metadata(item) for item in items]

        return items

    # ── bulk key lookup ───────────────────────────────────────────────────
    def get_items_by_keys(self, keys: list[str], fields: str = None,
                          timeout: int = 60) -> list[dict]:
        """itemsByKeys — 5000개 초과 시 자동 분할. metadata 자동 파싱."""
        if fields is None:
            fields = ITEM_FIELDS_MONITOR
        if not keys:
            return []

        all_items = []
        for i in range(0, len(keys), self.MAX_KEYS_PER_CALL):
            chunk = keys[i:i + self.MAX_KEYS_PER_CALL]
            keys_str = ", ".join(f'"{k}"' for k in chunk)
            query = f'{{ itemsByKeys(keys: [{keys_str}]) {{ {fields} }} }}'
            result = self._graphql(query, timeout=timeout)
            if isinstance(result, list):
                all_items.extend(result)
            elif result:
                all_items.append(result)

            if len(keys) > self.MAX_KEYS_PER_CALL:
                logger.info("Batch %d/%d: %d keys",
                            i // self.MAX_KEYS_PER_CALL + 1,
                            (len(keys) - 1) // self.MAX_KEYS_PER_CALL + 1,
                            len(chunk))

        if "metadata" in (fields or ""):
            all_items = [self._parse_metadata(item) for item in all_items]

        return all_items

    # ── item histories ────────────────────────────────────────────────────
    def get_item_histories(self, date_from: int, date_to: int = None,
                           kind: str = None, item_key: str = None,
                           first: int = 100, max_pages: int = None,
                           fields: str = None, timeout: int = 30) -> list[dict]:
        """itemHistories — 7일 제한, 밀리초 타임스탬프."""
        if fields is None:
            fields = HISTORY_FIELDS
        if date_to is None:
            date_to = int(time.time() * 1000)

        diff_days = (date_to - date_from) / (1000 * 60 * 60 * 24)
        if diff_days > self.MAX_HISTORY_DAYS:
            raise OwnerclanApiError(
                f"itemHistories max range is {self.MAX_HISTORY_DAYS} days, "
                f"requested {diff_days:.1f} days"
            )

        params = []
        params.append("first: {first}")
        params.append("{after_clause}")
        params.append(f"dateFrom: {date_from}")
        params.append(f"dateTo: {date_to}")
        if kind:
            params.append(f"kind: {kind}")
        if item_key:
            params.append(f'itemKey: "{item_key}"')

        param_str = ", ".join(params)
        # Double-escape braces for .format() in _paginate
        query_template = (
            f"itemHistories({param_str}) {{{{ "
            f"pageInfo {{{{ hasNextPage endCursor }}}} "
            f"edges {{{{ node {{{{ {fields} }}}} }}}} }}}}"
        )

        return self._paginate(query_template, "itemHistories",
                              first=first, max_pages=max_pages, timeout=timeout)

    # ── orders ────────────────────────────────────────────────────────────
    def get_orders(self, *, status: str = None, date_from: int = None,
                   date_to: int = None, first: int = 100,
                   max_pages: int = None, fields: str = None,
                   timeout: int = 30) -> list[dict]:
        """allOrders — 주문 목록 + pagination."""
        if fields is None:
            fields = ORDER_FIELDS_LIST

        params = []
        params.append("first: {first}")
        params.append("{after_clause}")
        if status:
            params.append(f"status: {status}")
        if date_from is not None:
            params.append(f"dateFrom: {date_from}")
        if date_to is not None:
            params.append(f"dateTo: {date_to}")

        param_str = ", ".join(params)
        # Double-escape braces for .format() in _paginate
        query_template = (
            f"allOrders({param_str}) {{{{ "
            f"pageInfo {{{{ hasNextPage endCursor }}}} "
            f"edges {{{{ node {{{{ {fields} }}}} }}}} }}}}"
        )

        return self._paginate(query_template, "allOrders",
                              first=first, max_pages=max_pages, timeout=timeout)

    def get_order(self, key: str, fields: str = None) -> dict:
        """order(key) — 주문 상세."""
        if fields is None:
            fields = ORDER_FIELDS_DETAIL
        query = f'{{ order(key: "{key}") {{ {fields} }} }}'
        return self._graphql(query)

    # ── categories ────────────────────────────────────────────────────────
    def get_categories(self, first: int = 100, fields: str = None) -> list[dict]:
        """allCategories — 전체 카테고리 수집 후 리스트 반환."""
        if fields is None:
            fields = CATEGORY_FIELDS
        # Double-escape braces for .format() in _paginate
        query_template = (
            f"allCategories(first: {{first}}{{after_clause}}) {{{{ "
            f"pageInfo {{{{ hasNextPage endCursor }}}} "
            f"edges {{{{ node {{{{ {fields} }}}} }}}} }}}}"
        )
        return self._paginate(query_template, "allCategories", first=first)


if __name__ == "__main__":
    """Smoke test — 실제 API 호출로 전체 기능 검증"""
    import sys
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    client = OwnerclanClient()

    print("=" * 60)
    print("오너클랜 API 클라이언트 Smoke Test")
    print("=" * 60)

    # 1. 인증
    print("\n[1] Authentication")
    client.authenticate()
    print(f"  Token: {client.token[:30]}...")
    print(f"  Expires: {client.token_expires_at}")

    # 2. 단건 조회
    print("\n[2] get_item('WFNJKLQ')")
    item = client.get_item("WFNJKLQ")
    if item:
        print(f"  Name: {item.get('name')}")
        print(f"  Price: {item.get('price')}원")
        print(f"  Status: {item.get('status')}")
        print(f"  Grade: {item.get('metadata', {}).get('grade')}")
    else:
        print("  Item not found")

    # 3. 검색
    print("\n[3] search_items(search='텀블러', max_pages=1)")
    items = client.search_items(search="텀블러", first=5, max_pages=1)
    print(f"  Found: {len(items)} items")
    for it in items[:3]:
        print(f"    {it['key']} - {it['name'][:40]}")

    # 4. 배치 조회
    print("\n[4] get_items_by_keys()")
    if items:
        test_keys = [it["key"] for it in items[:3]]
        batch = client.get_items_by_keys(test_keys)
        print(f"  Requested: {len(test_keys)}, Got: {len(batch)}")
        for it in batch:
            print(f"    {it['key']} - {it['status']}")

    # 5. 변경 이력
    print("\n[5] get_item_histories(3 days, priceDecreased)")
    import time as _time
    now_ms = int(_time.time() * 1000)
    three_days_ms = now_ms - (3 * 24 * 60 * 60 * 1000)
    histories = client.get_item_histories(
        date_from=three_days_ms, kind="priceDecreased", max_pages=1
    )
    print(f"  Price decreases: {len(histories)}")
    for h in histories[:3]:
        print(f"    {h['itemKey']}: {h['valueBefore']} -> {h['valueAfter']}")

    # 6. 카테고리
    print("\n[6] get_categories()")
    cats = client.get_categories()
    print(f"  Categories: {len(cats)}")
    for cat in cats[:5]:
        print(f"    {cat['key']} - {cat.get('fullName', cat.get('name'))}")

    # 7. 주문
    print("\n[7] get_orders(max_pages=1)")
    orders = client.get_orders(first=5, max_pages=1)
    print(f"  Orders: {len(orders)}")
    for o in orders[:3]:
        print(f"    {o['key']} - {o['status']}")

    print("\n" + "=" * 60)
    print("Smoke test complete!")
    print("=" * 60)
