# 오너클랜 API 클라이언트 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `godomall_register/ownerclan_client.py` — 오너클랜 GraphQL API를 래핑하는 단일 클라이언트 클래스 구현

**Architecture:** 단일 파일, 단일 클래스 (`OwnerclanClient`). JWT 인증 + 토큰 파일 캐싱, GraphQL GET/POST 자동 전환, cursor pagination 공통 처리, metadata JSON 자동 파싱. 비즈니스 로직 없음 — 순수 API 래퍼만.

**Tech Stack:** Python 3, `requests` (2.31.0), 표준 라이브러리 (`json`, `time`, `urllib.parse`, `logging`, `pathlib`, `base64`)

**Design Spec:** `docs/superpowers/specs/2026-04-05-ownerclan-client-design.md`
**API Reference:** `godomall_register/docs/ownerclan_api_spec.md`

---

## File Map

| File | Action | Responsibility |
|------|--------|---------------|
| `godomall_register/ownerclan_client.py` | Create | API 클라이언트 클래스 전체 |
| `godomall_register/ownerclan_config.json` | Create | 인증 정보 + 토큰 캐시 |

기존 파일 수정 없음.

---

### Task 1: Exceptions + Config 로딩

**Files:**
- Create: `godomall_register/ownerclan_client.py`
- Create: `godomall_register/ownerclan_config.json`

- [ ] **Step 1: Create config file**

Create `godomall_register/ownerclan_config.json`:

```json
{
    "username": "shiningmall",
    "password": "shine140917!",
    "token": null,
    "token_expires_at": null
}
```

- [ ] **Step 2: Write exception classes + config loading**

Create `godomall_register/ownerclan_client.py` with the foundation:

```python
# -*- coding: utf-8 -*-
"""
오너클랜 Seller GraphQL API 클라이언트
- JWT 인증 + 30일 토큰 캐싱
- 상품/주문/카테고리 조회 (READ 전용)
- cursor pagination 자동 처리
- metadata JSON 스칼라 자동 파싱
"""

import json
import time
import logging
import urllib.parse
import base64
from pathlib import Path
from datetime import datetime, timezone, timedelta

import requests

logger = logging.getLogger(__name__)

# ── 엔드포인트 ──

AUTH_URL = "https://auth.ownerclan.com/auth"
AUTH_SANDBOX_URL = "https://auth-sandbox.ownerclan.com/auth"
API_URL = "https://api.ownerclan.com/v1/graphql"
API_SANDBOX_URL = "https://api-sandbox.ownerclan.com/v1/graphql"

# ── 기본 필드 세트 ──

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


# ── 예외 ──

class OwnerclanApiError(Exception):
    """GraphQL 에러 또는 HTTP 에러"""
    def __init__(self, message: str, errors: list = None):
        super().__init__(message)
        self.errors = errors or []


class OwnerclanAuthError(OwnerclanApiError):
    """인증 실패"""
    pass


# ── 클라이언트 ──

class OwnerclanClient:
    """오너클랜 GraphQL API 클라이언트 (READ 전용)"""

    MAX_KEYS_PER_CALL = 5000
    MAX_HISTORY_DAYS = 7
    POST_THRESHOLD = 4000  # URL 길이 이 이상이면 POST로 전환

    def __init__(self, config_path: str = None, sandbox: bool = False):
        if config_path is None:
            config_path = str(Path(__file__).parent / "ownerclan_config.json")
        self.config_path = Path(config_path)
        self.sandbox = sandbox

        self.auth_url = AUTH_SANDBOX_URL if sandbox else AUTH_URL
        self.api_url = API_SANDBOX_URL if sandbox else API_URL

        self.session = requests.Session()
        self.token = None
        self.token_expires_at = None

        self._load_config()

    def _load_config(self):
        """config.json에서 인증 정보 + 캐시 토큰 로드"""
        if not self.config_path.exists():
            raise FileNotFoundError(
                f"Config file not found: {self.config_path}\n"
                "Create it with: {\"username\": \"...\", \"password\": \"...\"}"
            )
        with open(self.config_path, "r", encoding="utf-8") as f:
            config = json.load(f)

        self.username = config["username"]
        self.password = config["password"]

        # 캐시된 토큰 복원
        if config.get("token") and config.get("token_expires_at"):
            self.token = config["token"]
            self.token_expires_at = datetime.fromisoformat(
                config["token_expires_at"]
            )
            logger.info("Cached token loaded (expires: %s)", self.token_expires_at)

    def _save_token(self):
        """토큰을 config.json에 저장"""
        with open(self.config_path, "r", encoding="utf-8") as f:
            config = json.load(f)
        config["token"] = self.token
        config["token_expires_at"] = (
            self.token_expires_at.isoformat() if self.token_expires_at else None
        )
        with open(self.config_path, "w", encoding="utf-8") as f:
            json.dump(config, f, indent=4, ensure_ascii=False)
```

- [ ] **Step 3: Verify syntax**

Run: `cd "C:/Users/kohaz/Desktop/Python/파이썬자동화파일/상품가공프로그램" && python -c "from godomall_register.ownerclan_client import OwnerclanClient; print('OK')"`

Expected: `OK`

- [ ] **Step 4: Commit**

```bash
git add godomall_register/ownerclan_client.py godomall_register/ownerclan_config.json
git commit -m "feat(ownerclan): add client skeleton with config loading and exceptions"
```

---

### Task 2: Authentication + Token Caching

**Files:**
- Modify: `godomall_register/ownerclan_client.py`

- [ ] **Step 1: Add authenticate() and _ensure_token()**

Add these methods to `OwnerclanClient` class:

```python
    def authenticate(self) -> str:
        """JWT 토큰 발급. 반환: 토큰 문자열"""
        logger.info("Authenticating as %s...", self.username)
        resp = requests.post(
            self.auth_url,
            json={
                "service": "ownerclan",
                "userType": "seller",
                "username": self.username,
                "password": self.password,
            },
            timeout=10,
        )
        if resp.status_code != 200:
            raise OwnerclanAuthError(
                f"Authentication failed: HTTP {resp.status_code} - {resp.text}"
            )

        self.token = resp.text.strip().strip('"')
        if not self.token:
            raise OwnerclanAuthError("Empty token received")

        # JWT exp 파싱 (payload는 base64 두 번째 세그먼트)
        try:
            payload_b64 = self.token.split(".")[1]
            # base64 패딩 보정
            payload_b64 += "=" * (4 - len(payload_b64) % 4)
            payload = json.loads(base64.urlsafe_b64decode(payload_b64))
            exp = payload.get("exp", 0)
            self.token_expires_at = datetime.fromtimestamp(exp, tz=timezone.utc)
        except (IndexError, ValueError, json.JSONDecodeError):
            # JWT 파싱 실패 시 30일 기본값
            self.token_expires_at = datetime.now(timezone.utc) + timedelta(days=30)

        self.session.headers["Authorization"] = f"Bearer {self.token}"
        self._save_token()
        logger.info("Authenticated. Token expires: %s", self.token_expires_at)
        return self.token

    def _ensure_token(self):
        """토큰 유효성 체크 → 필요 시 자동 갱신"""
        now = datetime.now(timezone.utc)
        if self.token and self.token_expires_at:
            # 만료 1일 전까지는 유효
            if now < self.token_expires_at - timedelta(days=1):
                if "Authorization" not in self.session.headers:
                    self.session.headers["Authorization"] = f"Bearer {self.token}"
                return
            logger.info("Token expired or expiring soon, re-authenticating...")
        self.authenticate()
```

- [ ] **Step 2: Test authentication with real API**

Run: `cd "C:/Users/kohaz/Desktop/Python/파이썬자동화파일/상품가공프로그램" && python -c "
from godomall_register.ownerclan_client import OwnerclanClient
c = OwnerclanClient()
c.authenticate()
print('Token:', c.token[:20] + '...')
print('Expires:', c.token_expires_at)
"`

Expected: Token 발급 성공, 만료일 30일 후 표시

- [ ] **Step 3: Test token caching**

Run: `cd "C:/Users/kohaz/Desktop/Python/파이썬자동화파일/상품가공프로그램" && python -c "
from godomall_register.ownerclan_client import OwnerclanClient
c = OwnerclanClient()
c._ensure_token()
print('Reused cached token:', c.token[:20] + '...')
"`

Expected: "Cached token loaded" 로그, 재인증 없이 기존 토큰 사용

- [ ] **Step 4: Commit**

```bash
git add godomall_register/ownerclan_client.py
git commit -m "feat(ownerclan): add JWT authentication with 30-day token caching"
```

---

### Task 3: GraphQL Wrapper + Retry

**Files:**
- Modify: `godomall_register/ownerclan_client.py`

- [ ] **Step 1: Add _graphql() method**

Add to `OwnerclanClient`:

```python
    def _graphql(self, query: str, variables: dict = None,
                 timeout: int = 30) -> dict:
        """
        GraphQL 요청 실행.
        GET 기본, URL 4000자 초과 시 POST 자동 전환.
        응답에서 data 추출, errors 시 예외.
        HTTP 429/5xx 자동 재시도.
        """
        self._ensure_token()

        encoded_query = urllib.parse.quote(query)
        url = f"{self.api_url}?query={encoded_query}"

        use_post = len(url) > self.POST_THRESHOLD

        last_error = None
        for attempt in range(3):
            try:
                if use_post:
                    body = {"query": query}
                    if variables:
                        body["variables"] = variables
                    resp = self.session.post(
                        self.api_url, json=body, timeout=timeout
                    )
                else:
                    resp = self.session.get(url, timeout=timeout)

                if resp.status_code == 429:
                    wait = 3 * (attempt + 1)
                    logger.warning("Rate limited (429), waiting %ds...", wait)
                    time.sleep(wait)
                    continue

                if resp.status_code >= 500:
                    wait = 2 * (attempt + 1)
                    logger.warning("Server error (%d), waiting %ds...",
                                   resp.status_code, wait)
                    time.sleep(wait)
                    continue

                resp.raise_for_status()

                data = resp.json()

                if "errors" in data:
                    error_msgs = [e.get("message", str(e)) for e in data["errors"]]
                    raise OwnerclanApiError(
                        f"GraphQL errors: {'; '.join(error_msgs)}",
                        errors=data["errors"],
                    )

                result = data.get("data", {})
                # data의 첫 번째 키 값 반환
                if result:
                    first_key = next(iter(result))
                    return result[first_key]
                return result

            except requests.exceptions.Timeout:
                raise OwnerclanApiError(
                    f"Request timed out after {timeout}s"
                )
            except requests.exceptions.RequestException as e:
                last_error = e
                if attempt < 2:
                    time.sleep(2)
                    continue
                raise OwnerclanApiError(f"Request failed: {e}")

        raise OwnerclanApiError(f"Max retries exceeded: {last_error}")
```

- [ ] **Step 2: Test with a simple query**

Run: `cd "C:/Users/kohaz/Desktop/Python/파이썬자동화파일/상품가공프로그램" && python -c "
from godomall_register.ownerclan_client import OwnerclanClient
c = OwnerclanClient()
result = c._graphql('{ item(key: \"WFNJKLQ\") { key name status } }')
print(result)
"`

Expected: `{'key': 'WFNJKLQ', 'name': '...', 'status': '...'}`

- [ ] **Step 3: Commit**

```bash
git add godomall_register/ownerclan_client.py
git commit -m "feat(ownerclan): add GraphQL wrapper with GET/POST auto-switch and retry"
```

---

### Task 4: Metadata Parsing + get_item()

**Files:**
- Modify: `godomall_register/ownerclan_client.py`

- [ ] **Step 1: Add _parse_metadata() and get_item()**

```python
    def _parse_metadata(self, item: dict) -> dict:
        """metadata JSON 스칼라 → dict 파싱. 실패 시 빈 dict."""
        if "metadata" in item and isinstance(item["metadata"], str):
            try:
                item["metadata"] = json.loads(item["metadata"])
            except (json.JSONDecodeError, TypeError):
                item["metadata"] = {}
        return item

    def get_item(self, key: str, fields: str = None) -> dict:
        """
        item(key) — 단건 상세 조회.
        fields: GraphQL 필드 문자열. None이면 ITEM_FIELDS_FULL 사용.
        metadata는 자동 파싱.
        """
        if fields is None:
            fields = ITEM_FIELDS_FULL
        query = f'{{ item(key: "{key}") {{ {fields} }} }}'
        result = self._graphql(query)
        return self._parse_metadata(result) if result else result
```

- [ ] **Step 2: Test get_item()**

Run: `cd "C:/Users/kohaz/Desktop/Python/파이썬자동화파일/상품가공프로그램" && python -c "
import json
from godomall_register.ownerclan_client import OwnerclanClient
c = OwnerclanClient()
item = c.get_item('WFNJKLQ')
print('name:', item.get('name'))
print('price:', item.get('price'))
print('status:', item.get('status'))
print('metadata type:', type(item.get('metadata')))
print('grade:', item.get('metadata', {}).get('grade'))
"`

Expected: 상품명, 가격, 상태 출력. metadata는 dict 타입, grade 값 확인.

- [ ] **Step 3: Commit**

```bash
git add godomall_register/ownerclan_client.py
git commit -m "feat(ownerclan): add get_item() with metadata auto-parsing"
```

---

### Task 5: Pagination Helper + search_items()

**Files:**
- Modify: `godomall_register/ownerclan_client.py`

- [ ] **Step 1: Add _paginate() and search_items()**

```python
    def _paginate(self, query_template: str, connection_name: str,
                  first: int = 100,
                  max_pages: int = None,
                  timeout: int = 30) -> list:
        """
        cursor-based pagination 공통 처리.
        query_template: {after_clause}와 {first} 플레이스홀더 포함.
        connection_name: 응답 root key (예: "allItems").
        전체 node 리스트 반환.
        """
        all_nodes = []
        cursor = None
        page = 0

        while True:
            after_clause = f', after: "{cursor}"' if cursor else ""
            query = query_template.format(
                first=first, after_clause=after_clause
            )

            # _graphql이 data의 첫 번째 키를 반환하므로
            # connection 객체가 바로 옴
            result = self._graphql(
                f"{{ {query} }}", timeout=timeout
            )

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

    def search_items(self, *, search: str = None, status: str = None,
                     vendor: str = None, grade: str = None,
                     category: str = None, min_price: int = None,
                     max_price: int = None, sort_by: str = None,
                     date_from: int = None, date_to: int = None,
                     first: int = 100, max_pages: int = None,
                     fields: str = None,
                     timeout: int = 30) -> list[dict]:
        """
        allItems — 필터 조합 검색 + cursor pagination.
        반환: 상품 dict 리스트.
        """
        if fields is None:
            fields = ITEM_FIELDS_LIGHT

        # 필터 파라미터 조립
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
        query_template = (
            f"allItems({param_str}) {{ "
            f"pageInfo {{ hasNextPage endCursor }} "
            f"edges {{ cursor node {{ {fields} }} }} }}"
        )

        items = self._paginate(
            query_template, "allItems",
            first=first, max_pages=max_pages, timeout=timeout
        )

        # metadata 파싱 (fields에 metadata 포함 시)
        if "metadata" in (fields or ""):
            items = [self._parse_metadata(item) for item in items]

        return items
```

- [ ] **Step 2: Test search_items()**

Run: `cd "C:/Users/kohaz/Desktop/Python/파이썬자동화파일/상품가공프로그램" && python -c "
from godomall_register.ownerclan_client import OwnerclanClient
c = OwnerclanClient()
items = c.search_items(search='텀블러', first=5, max_pages=1)
print(f'Found {len(items)} items')
for item in items[:3]:
    print(f'  {item[\"key\"]} - {item[\"name\"][:30]} - {item[\"price\"]}원')
"`

Expected: 최대 5건, 상품코드/이름/가격 출력

- [ ] **Step 3: Test with vendor filter**

Run: `cd "C:/Users/kohaz/Desktop/Python/파이썬자동화파일/상품가공프로그램" && python -c "
from godomall_register.ownerclan_client import OwnerclanClient
c = OwnerclanClient()
items = c.search_items(vendor='2010022149', status='available', first=10, max_pages=1)
print(f'Vendor 2010022149: {len(items)} items')
for item in items[:3]:
    print(f'  {item[\"key\"]} - {item[\"name\"][:30]}')
"`

Expected: 어나더엠(2010022149) 공급사 상품만 반환

- [ ] **Step 4: Commit**

```bash
git add godomall_register/ownerclan_client.py
git commit -m "feat(ownerclan): add search_items() with pagination and filters"
```

---

### Task 6: get_items_by_keys() — Batch Query with Auto-Split

**Files:**
- Modify: `godomall_register/ownerclan_client.py`

- [ ] **Step 1: Add get_items_by_keys()**

```python
    def get_items_by_keys(self, keys: list[str],
                          fields: str = None,
                          timeout: int = 60) -> list[dict]:
        """
        itemsByKeys — 상품코드 배열로 일괄 조회.
        5000개 초과 시 자동 분할.
        metadata 자동 파싱.
        """
        if fields is None:
            fields = ITEM_FIELDS_MONITOR
        if not keys:
            return []

        all_items = []

        # 5000개씩 분할
        for i in range(0, len(keys), self.MAX_KEYS_PER_CALL):
            chunk = keys[i:i + self.MAX_KEYS_PER_CALL]
            keys_str = ", ".join(f'"{k}"' for k in chunk)
            query = (
                f'{{ itemsByKeys(keys: [{keys_str}]) {{ {fields} }} }}'
            )

            result = self._graphql(query, timeout=timeout)
            if isinstance(result, list):
                all_items.extend(result)
            elif result:
                all_items.append(result)

            if len(keys) > self.MAX_KEYS_PER_CALL:
                logger.info(
                    "Batch %d/%d: %d keys",
                    i // self.MAX_KEYS_PER_CALL + 1,
                    (len(keys) - 1) // self.MAX_KEYS_PER_CALL + 1,
                    len(chunk),
                )

        # metadata 파싱
        if "metadata" in (fields or ""):
            all_items = [self._parse_metadata(item) for item in all_items]

        return all_items
```

- [ ] **Step 2: Test with known keys**

Run: `cd "C:/Users/kohaz/Desktop/Python/파이썬자동화파일/상품가공프로그램" && python -c "
from godomall_register.ownerclan_client import OwnerclanClient
c = OwnerclanClient()
items = c.get_items_by_keys(['WFNJKLQ'])
print(f'Got {len(items)} items')
for item in items:
    print(f'  {item[\"key\"]} - {item[\"name\"][:30]}')
    print(f'  status: {item[\"status\"]}')
    print(f'  metadata type: {type(item.get(\"metadata\"))}')
    print(f'  grade: {item.get(\"metadata\", {}).get(\"grade\")}')
"`

Expected: 1건 반환, metadata는 dict 타입, grade 값 확인

- [ ] **Step 3: Commit**

```bash
git add godomall_register/ownerclan_client.py
git commit -m "feat(ownerclan): add get_items_by_keys() with auto-split at 5000"
```

---

### Task 7: get_item_histories()

**Files:**
- Modify: `godomall_register/ownerclan_client.py`

- [ ] **Step 1: Add get_item_histories()**

```python
    def get_item_histories(self, date_from: int, date_to: int = None,
                           kind: str = None, item_key: str = None,
                           first: int = 100, max_pages: int = None,
                           fields: str = None,
                           timeout: int = 30) -> list[dict]:
        """
        itemHistories — 변경 이력 조회.
        date_from/date_to: 밀리초 Unix 타임스탬프.
        date_to 생략 시 현재 시각.
        최대 7일 간격 제한.
        kind: ItemHistoryKind (priceIncreased, soldout 등)
        """
        if fields is None:
            fields = HISTORY_FIELDS

        if date_to is None:
            date_to = int(time.time() * 1000)

        # 7일 제한 검증
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
        query_template = (
            f"itemHistories({param_str}) {{ "
            f"pageInfo {{ hasNextPage endCursor }} "
            f"edges {{ node {{ {fields} }} }} }}"
        )

        return self._paginate(
            query_template, "itemHistories",
            first=first, max_pages=max_pages, timeout=timeout
        )
```

- [ ] **Step 2: Test get_item_histories()**

Run: `cd "C:/Users/kohaz/Desktop/Python/파이썬자동화파일/상품가공프로그램" && python -c "
import time
from godomall_register.ownerclan_client import OwnerclanClient
c = OwnerclanClient()
now = int(time.time() * 1000)
three_days_ago = now - (3 * 24 * 60 * 60 * 1000)
histories = c.get_item_histories(
    date_from=three_days_ago,
    kind='priceDecreased',
    max_pages=1
)
print(f'Found {len(histories)} price decrease events')
for h in histories[:3]:
    print(f'  {h[\"itemKey\"]}: {h[\"valueBefore\"]} -> {h[\"valueAfter\"]}')
"`

Expected: 최근 3일 가격 인하 이력 출력

- [ ] **Step 3: Commit**

```bash
git add godomall_register/ownerclan_client.py
git commit -m "feat(ownerclan): add get_item_histories() with 7-day validation"
```

---

### Task 8: Order + Category Methods

**Files:**
- Modify: `godomall_register/ownerclan_client.py`

- [ ] **Step 1: Add get_orders(), get_order(), get_categories()**

```python
    def get_orders(self, *, status: str = None,
                   date_from: int = None, date_to: int = None,
                   first: int = 100, max_pages: int = None,
                   fields: str = None,
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
        query_template = (
            f"allOrders({param_str}) {{ "
            f"pageInfo {{ hasNextPage endCursor }} "
            f"edges {{ node {{ {fields} }} }} }}"
        )

        return self._paginate(
            query_template, "allOrders",
            first=first, max_pages=max_pages, timeout=timeout
        )

    def get_order(self, key: str, fields: str = None) -> dict:
        """order(key) — 주문 상세."""
        if fields is None:
            fields = ORDER_FIELDS_DETAIL
        query = f'{{ order(key: "{key}") {{ {fields} }} }}'
        return self._graphql(query)

    def get_categories(self, first: int = 100,
                       fields: str = None) -> list[dict]:
        """allCategories — 전체 카테고리 수집 후 리스트 반환."""
        if fields is None:
            fields = CATEGORY_FIELDS

        query_template = (
            f"allCategories(first: {{first}}{{after_clause}}) {{ "
            f"pageInfo {{ hasNextPage endCursor }} "
            f"edges {{ node {{ {fields} }} }} }}"
        )

        return self._paginate(
            query_template, "allCategories", first=first
        )
```

- [ ] **Step 2: Test get_categories()**

Run: `cd "C:/Users/kohaz/Desktop/Python/파이썬자동화파일/상품가공프로그램" && python -c "
from godomall_register.ownerclan_client import OwnerclanClient
c = OwnerclanClient()
cats = c.get_categories()
print(f'Total categories: {len(cats)}')
for cat in cats[:5]:
    print(f'  {cat[\"key\"]} - {cat[\"fullName\"]}')
"`

Expected: 카테고리 리스트 (20+개)

- [ ] **Step 3: Test get_orders()**

Run: `cd "C:/Users/kohaz/Desktop/Python/파이썬자동화파일/상품가공프로그램" && python -c "
from godomall_register.ownerclan_client import OwnerclanClient
c = OwnerclanClient()
orders = c.get_orders(first=5, max_pages=1)
print(f'Orders: {len(orders)}')
for o in orders[:3]:
    print(f'  {o[\"key\"]} - {o[\"status\"]}')
"`

Expected: 최근 주문 목록 (있으면 출력, 없으면 빈 리스트)

- [ ] **Step 4: Commit**

```bash
git add godomall_register/ownerclan_client.py
git commit -m "feat(ownerclan): add order and category query methods"
```

---

### Task 9: Smoke Test (__main__ block)

**Files:**
- Modify: `godomall_register/ownerclan_client.py`

- [ ] **Step 1: Add __main__ smoke test**

파일 맨 끝에 추가:

```python
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
```

- [ ] **Step 2: Run smoke test**

Run: `cd "C:/Users/kohaz/Desktop/Python/파이썬자동화파일/상품가공프로그램" && python -m godomall_register.ownerclan_client`

Expected: 7개 섹션 모두 에러 없이 출력

- [ ] **Step 3: Commit**

```bash
git add godomall_register/ownerclan_client.py
git commit -m "feat(ownerclan): add smoke test for all API methods"
```

---

### Task 10: .gitignore Update

**Files:**
- Modify: `.gitignore`

- [ ] **Step 1: Add config to gitignore**

`ownerclan_config.json`에 비밀번호가 포함되므로 gitignore에 추가:

```
# 오너클랜 인증 정보 (비밀번호 포함)
godomall_register/ownerclan_config.json
```

- [ ] **Step 2: Commit**

```bash
git add .gitignore
git commit -m "chore: gitignore ownerclan config (contains credentials)"
```

---

## Summary

| Task | 내용 | 의존 |
|------|------|------|
| 1 | Exceptions + Config 로딩 | — |
| 2 | Authentication + Token caching | 1 |
| 3 | GraphQL wrapper + Retry | 2 |
| 4 | Metadata parsing + get_item() | 3 |
| 5 | Pagination + search_items() | 3 |
| 6 | get_items_by_keys() (5000 auto-split) | 3 |
| 7 | get_item_histories() (7-day validation) | 3 |
| 8 | get_orders() + get_order() + get_categories() | 3 |
| 9 | Smoke test (__main__) | 4-8 |
| 10 | .gitignore update | 1 |

Tasks 4-8은 Task 3에만 의존하므로 병렬 실행 가능.
