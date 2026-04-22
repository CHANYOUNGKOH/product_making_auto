# Design: 오너클랜 API 클라이언트 (`ownerclan_client.py`)

**Date**: 2026-04-05
**Status**: Draft
**Scope**: Step 1 only — 순수 API 래퍼 (비즈니스 로직 없음)

---

## 목적

오너클랜 Seller GraphQL API를 래핑하는 단일 클라이언트 클래스.
모든 후속 기능(모니터링, 검색, 출고검증)의 기반 계층.

비즈니스 로직은 이 파일에 포함하지 않는다.
DB 접근, 가격 비교, 필터링 등은 별도 모듈(monitor, search)에서 담당.

---

## 파일 구조

```
godomall_register/
├── ownerclan_client.py      # 이 설계의 대상
├── ownerclan_config.json    # 인증 정보 + 토큰 캐시 (신규)
├── api_client.py            # 기존 고도몰 API (변경 없음)
├── config.py                # 기존 고도몰 설정 (변경 없음)
└── docs/
    └── ownerclan_api_spec.md  # API 레퍼런스 (이미 작성)
```

---

## 인증 및 토큰 관리

### Config 파일: `ownerclan_config.json`

```json
{
    "username": "shiningmall",
    "password": "shine140917!",
    "token": null,
    "token_expires_at": null
}
```

- 파일 없으면 `authenticate()` 호출 시 자동 생성 (username/password 필수 파라미터)
- 토큰 발급 후 `token`과 `token_expires_at` (ISO 8601) 업데이트
- 다음 세션 시작 시 파일에서 토큰 로드 → 만료 체크 → 유효하면 재사용

### 인증 흐름

```
OwnerclanClient(config_path="godomall_register/ownerclan_config.json")
  ↓
__init__: config.json 로드 → 토큰 있으면 만료 체크
  ↓
_ensure_token(): 토큰 없거나 만료 1일 이내 → authenticate() 자동 호출
  ↓
authenticate(): POST https://auth.ownerclan.com/auth → JWT 반환 → config.json 저장
```

### 인증 API 상세

```
POST https://auth.ownerclan.com/auth
Content-Type: application/json

{"service": "ownerclan", "userType": "seller", "username": "...", "password": "..."}
```

- 응답: JWT 토큰 문자열
- 유효기간: 30일 (JWT의 `exp - iat` 기준)
- Sandbox: `https://auth-sandbox.ownerclan.com/auth`

---

## 클래스 설계

```python
class OwnerclanClient:
    """오너클랜 GraphQL API 클라이언트 (READ 전용)"""

    def __init__(self, config_path: str = None, sandbox: bool = False):
        """
        config_path: ownerclan_config.json 경로.
                     None이면 이 파일과 같은 디렉터리의 ownerclan_config.json
        sandbox: True면 sandbox 엔드포인트 사용
        """

    # ── 인증 ──

    def authenticate(self) -> str:
        """JWT 토큰 발급 + config.json 저장. 반환: 토큰 문자열"""

    def _ensure_token(self):
        """토큰 유효성 체크 → 필요 시 자동 갱신"""

    # ── GraphQL 공통 ──

    def _graphql(self, query: str, variables: dict = None,
                 timeout: int = 30) -> dict:
        """
        GraphQL 요청 실행.
        - GET ?query=URL_ENCODED_QUERY (READ 전용)
        - Authorization: Bearer {token}
        - 응답 data 추출, errors 있으면 OwnerclanApiError 발생
        - 타임아웃 설정 가능 (기본 30초)
        """

    # ── 상품 조회 ──

    def get_item(self, key: str, fields: str = None) -> dict:
        """
        item(key) — 단건 상세 조회.
        기본 fields: key, name, model, price, fixedPrice, pricePolicy,
                     status, images, options, category, content,
                     searchKeywords, shippingFee, shippingType,
                     metadata, createdAt, updatedAt
        metadata는 JSON 스칼라 → 자동 파싱하여 dict로 반환.
        """

    def search_items(self, *, search: str = None, status: str = None,
                     vendor: str = None, grade: str = None,
                     category: str = None, min_price: int = None,
                     max_price: int = None, sort_by: str = None,
                     date_from: int = None, date_to: int = None,
                     first: int = 100,
                     max_pages: int = None) -> Generator[dict, None, None]:
        """
        allItems — 필터 조합 검색 + cursor pagination 자동 처리.
        제너레이터로 반환 → for item in client.search_items(...): ...
        기본 fields: key, name, price, status, updatedAt
        max_pages: None=전체, 숫자=해당 페이지까지
        """

    def get_items_by_keys(self, keys: list[str],
                          fields: str = None) -> list[dict]:
        """
        itemsByKeys — 상품코드 배열로 일괄 조회.
        5000개 초과 시 자동 분할 호출 후 결과 병합.
        기본 fields: key, name, price, status, metadata
        metadata는 JSON 스칼라 → 자동 파싱.
        """

    def get_item_histories(self, date_from: int, date_to: int = None,
                           kind: str = None, item_key: str = None,
                           first: int = 100) -> Generator[dict, None, None]:
        """
        itemHistories — 변경 이력 조회.
        date_from/date_to: 밀리초 Unix 타임스탬프 (필수).
        date_to 생략 시 현재 시각.
        최대 7일 간격 제한 → 초과 시 OwnerclanApiError.
        kind: ItemHistoryKind enum 문자열 (priceIncreased, soldout 등)
        """

    # ── 주문 조회 ──

    def get_orders(self, *, status: str = None,
                   date_from: int = None, date_to: int = None,
                   first: int = 100) -> Generator[dict, None, None]:
        """
        allOrders — 주문 목록 조회 + pagination.
        status: OrderStatus (paid, preparing, shipped 등)
        date_from/date_to: 밀리초 타임스탬프
        """

    def get_order(self, key: str) -> dict:
        """
        order(key) — 주문 상세.
        products, shippingInfo, ordererNote 등 포함.
        """

    # ── 카테고리 ──

    def get_categories(self, first: int = 100) -> list[dict]:
        """
        allCategories — 전체 카테고리 목록 수집.
        pagination 완료 후 리스트로 반환 (제너레이터 아님).
        """
```

---

## GraphQL 요청 방식

**READ 전용이므로 GET 방식 사용:**

```
GET https://api.ownerclan.com/v1/graphql?query={URL_ENCODED_QUERY}
Authorization: Bearer {JWT_TOKEN}
```

- 쿼리 URL 인코딩 후 길이가 4000자 초과 시 POST 방식으로 자동 전환
  (itemsByKeys 5000개 등 대량 파라미터 포함 쿼리)
- POST: `Content-Type: application/json`, body `{"query": "...", "variables": {...}}`

**응답 처리:**

```python
response = {
    "data": { "item": { ... } },       # 성공
    "errors": [{"message": "..."}]      # 에러 (data와 공존 가능)
}
```

- `errors` 필드 존재 시 `OwnerclanApiError` 발생
- `data` 에서 첫 번째 키의 값을 반환

---

## 에러 처리

```python
class OwnerclanApiError(Exception):
    """GraphQL 에러 또는 HTTP 에러"""
    def __init__(self, message: str, errors: list = None):
        super().__init__(message)
        self.errors = errors or []

class OwnerclanAuthError(OwnerclanApiError):
    """인증 실패 (잘못된 계정, 토큰 만료 등)"""
```

**재시도 정책:**
- HTTP 429 (rate limit): 3초 대기 후 재시도 (최대 3회)
- HTTP 5xx: 2초 대기 후 재시도 (최대 2회)
- 타임아웃: 재시도 없이 즉시 예외
- 인증 에러: 토큰 갱신 1회 시도 후 실패 시 예외

---

## Pagination 처리

`search_items`, `get_item_histories`, `get_orders`에 공통 적용:

```python
def _paginate(self, query_template: str, connection_name: str,
              variables: dict, first: int,
              max_pages: int = None) -> Generator[dict, None, None]:
    """
    cursor-based pagination 공통 처리.
    query_template: {after} 플레이스홀더 포함 쿼리
    connection_name: 응답에서 edges를 꺼낼 키 (예: "allItems")
    """
    cursor = None
    page = 0
    while True:
        # after 파라미터 세팅
        # 쿼리 실행
        # edges 순회 → node yield
        # hasNextPage 체크 → False면 break
        # max_pages 체크
        page += 1
        if max_pages and page >= max_pages:
            break
```

---

## metadata 자동 파싱

오너클랜 API에서 `metadata`는 JSON 스칼라 타입으로 반환됨.
클라이언트에서 자동으로 `json.loads()` 처리:

```python
def _parse_metadata(self, item: dict) -> dict:
    """metadata 문자열 → dict 파싱. 실패 시 빈 dict."""
    if "metadata" in item and isinstance(item["metadata"], str):
        try:
            item["metadata"] = json.loads(item["metadata"])
        except (json.JSONDecodeError, TypeError):
            item["metadata"] = {}
    return item
```

모든 상품 조회 메서드에서 반환 전 자동 적용.

---

## 필드 기본값

| 메서드 | 기본 fields | 이유 |
|--------|------------|------|
| `get_item` | 전체 (key~updatedAt) | 상세 조회 목적 |
| `search_items` | key, name, price, status, updatedAt | 목록 탐색 — 경량 |
| `get_items_by_keys` | key, name, price, status, metadata | 모니터링용 — metadata 필수 |
| `get_item_histories` | itemKey, kind, title, valueBefore, valueAfter, createdAt | 이력 전체 |

`fields` 파라미터(str)로 오버라이드 가능. GraphQL 필드 문자열을 직접 전달.
`price` 필드는 항상 `price(currency: KRW)` 형태로 요청.

---

## 성능 기준 (API 실측 기반)

| 작업 | 예상 소요 | 비고 |
|------|----------|------|
| 인증 | ~1초 | 30일 캐싱 |
| get_item | ~0.5초 | 단건 |
| search_items 1000건 | ~3초 | 1페이지 |
| search_items 10000건 | ~30초 | 10페이지 순차 |
| get_items_by_keys 50건 | ~1초 | metadata 포함 |
| get_items_by_keys 5000건 | ~20초 (추정) | 1회 호출 |
| get_item_histories 7일 | ~3초 | kind 필터 시 |

---

## 테스트 전략

**실제 API 호출 테스트** (sandbox 또는 production):

1. `authenticate()` → JWT 토큰 반환 확인
2. `get_item("WFNJKLQ")` → 상품명, 가격, metadata.grade 확인
3. `search_items(search="텀블러", first=10)` → 10건 이내 반환
4. `get_items_by_keys(["W000312", "W000313"])` → 2건 반환, metadata 파싱 확인
5. `get_item_histories(date_from=..., kind="priceDecreased")` → 이력 반환
6. `get_categories()` → 카테고리 리스트 반환
7. 토큰 캐싱: 두 번째 인스턴스 생성 시 재인증 없이 캐시 토큰 사용 확인
8. 5000개 초과 분할: `get_items_by_keys(["K"]*5001)` → 2회 분할 호출

**단위 테스트는 작성하지 않음** — 외부 API 의존이라 mock 대비 실제 호출 검증이 더 가치 있음.
대신 `if __name__ == "__main__"` 블록에 간단한 smoke test 포함.

---

## 제외 사항 (Step 1 범위 밖)

- Mutation (createItem, updateItem 등) — Seller 권한 미확인
- DB 연동 — Step 2 (monitor)에서 담당
- 가격 캐시 — Step 2에서 담당
- GUI 통합 — Step 4에서 담당
- 비동기(async) — 현재 프로젝트가 동기 기반, 불필요

---

## 의존성

- `requests` (2.31.0, 이미 설치)
- 표준 라이브러리: `json`, `time`, `urllib.parse`, `logging`, `pathlib`
- 추가 설치 없음
