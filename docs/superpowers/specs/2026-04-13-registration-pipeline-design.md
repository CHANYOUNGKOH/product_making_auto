# 등록 파이프라인 아키텍처 설계 (Registration Pipeline Architecture)

## 배경 및 목적

가공 파이프라인(ST1~ST4, IMG)으로 생성된 65k 상품 DB를 각 오픈마켓에 체계적으로 등록하기 위한 아키텍처.

핵심 원칙:
- 오너클랜 카테고리를 마스터 분류 단위로 사용
- OC API로 각 마켓의 카테고리 코드를 수집·저장
- 스토어별 카테고리 배정 + 가격 전략으로 상품 분배
- 같은 사업자 내 동일 마켓카테고리 중복 등록 사전 방지

---

## 1. OC 카테고리 + 마켓카테고리 수집

### 1-1. OC API 검증 필요 사항

현재 `allCategories` 쿼리는 `key name fullName`만 수집. 마켓카테고리 필드 존재 여부 테스트 필요:
```graphql
# 테스트할 필드 후보
allCategories {
  key name fullName
  marketCategories { market code name }   # 존재하면 최선
}
```

- **API에 있는 경우**: `allCategories` 한 번 호출로 전체 OC 카테고리 + 마켓카테고리 일괄 수집
- **API에 없는 경우**: OC Excel 양식 다운로드 → "마켓카테고리" 컬럼 파싱 (기존 `extract_market_categories()` 활용)

OC Excel "마켓카테고리" 컬럼 포맷 (이미 확인):
```
auction,71281100,생활/이용가구>사무용품>스테이플러
gmarket,300010585,생활/이용가구>사무용품>스테이플러
st11,1010651,문구/사무용품>사무용품>스테이플러
storefarm,50003757,생활/건강>문구/사무용품>사무용품>스테이플러
coupang,80087,문구/사무실>사무용품>스테이플러
interpark,001850107001004,...
tmon,50080031,...
esellers,283916000,...
wmp,4103974,...
playauto,44091100,...
```

### 1-2. oc_catalog.db 스키마 확장

```sql
-- 기존 oc_items에 추가
ALTER TABLE oc_items ADD COLUMN market_categories_json TEXT;
-- JSON 구조:
-- {
--   "auction":    {"code": "71281100",      "name": "생활>사무용품>스테이플러"},
--   "gmarket":    {"code": "300010585",     "name": "생활>사무용품>스테이플러"},
--   "st11":       {"code": "1010651",       "name": "문구/사무용품>사무용품>..."},
--   "storefarm":  {"code": "50003757",      "name": "생활/건강>문구/사무용품>..."},
--   "coupang":    {"code": "80087",         "name": "문구/사무실>사무용품>..."},
--   "interpark":  {"code": "...",           "name": "..."},
--   "tmon":       {"code": "...",           "name": "..."},
--   "esellers":   {"code": "...",           "name": "..."},
--   "wmp":        {"code": "...",           "name": "..."},
--   "playauto":   {"code": "...",           "name": "..."}
-- }

-- OC 카테고리별 마켓카테고리 캐시 (카테고리 단위 관리용)
CREATE TABLE IF NOT EXISTS oc_category_markets (
    oc_category_key   TEXT NOT NULL,
    oc_category_name  TEXT,
    market            TEXT NOT NULL,   -- auction, gmarket, st11, storefarm, coupang, ...
    market_cat_code   TEXT,
    market_cat_name   TEXT,
    is_manual         INTEGER DEFAULT 0,  -- 1: OC 미지정, 수동 입력
    updated_at        TEXT,
    PRIMARY KEY (oc_category_key, market)
);
```

### 1-3. 미지정 마켓카테고리 처리

OC에서 특정 마켓카테고리가 미지정인 경우:
- `market_cat_code = NULL`, `is_manual = 0` → 스킵 처리
- 수동 입력 시 `is_manual = 1` 로 저장
- 스킵된 경우 해당 마켓으로는 해당 OC 카테고리 상품 미등록

---

## 2. 카테고리-스토어 배정 전략

### 2-1. 개념

- 배정 단위: **OC 카테고리**
- 한 OC 카테고리는 같은 마켓 내에서 **스토어 1개에만** 배정 가능
- 예: "문구>사무용품" OC 카테고리 → 고도몰A 배정 시 → 스마트스토어B 배정 불가 (둘 다 storefarm 마켓)
- 옥션/지마켓 (ESM) → auction + gmarket 코드가 모두 동일 마켓 그룹

### 2-2. 마켓 그룹 정의

```python
MARKET_GROUPS = {
    # 고도몰과 스마트스토어는 모두 네이버쇼핑 storefarm 카테고리를 사용
    # → 같은 사업자의 두 스토어에 동일 storefarm 카테고리 등록 시 중복 패널티
    "naver":     ["storefarm"],            # 고도몰, 스마트스토어 공용 (네이버카테고리)
    "esm":       ["auction", "gmarket"],   # ESM 통합 (옥션+지마켓 동일 카테고리 = 중복)
    "st11":      ["st11"],
    "coupang":   ["coupang"],
    "interpark": ["interpark"],
    "tmon":      ["tmon"],
    "esellers":  ["esellers"],
}
```

같은 그룹 내 스토어들은 해당 마켓카테고리 코드 기준으로 중복 체크.
(중복 체크 범위: 이 Hub에서 관리하는 스토어들 — 같은 사업자 명의)

### 2-3. DB 스키마

```sql
-- products.db (또는 별도 strategy.db)
CREATE TABLE IF NOT EXISTS store_category_assignments (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    store_alias     TEXT NOT NULL,     -- 고도몰_메인, 스마트스토어_A
    oc_category_key TEXT NOT NULL,
    oc_category_name TEXT,
    market_group    TEXT NOT NULL,     -- naver, esm, st11, coupang, ...
    assigned_at     TEXT,
    UNIQUE(store_alias, oc_category_key)
);

-- 중복 방지 뷰: 같은 market_group 내 동일 OC 카테고리 배정 검사
CREATE VIEW IF NOT EXISTS v_category_conflicts AS
SELECT
    a1.oc_category_key,
    a1.store_alias AS store1,
    a2.store_alias AS store2,
    a1.market_group
FROM store_category_assignments a1
JOIN store_category_assignments a2
  ON a1.oc_category_key = a2.oc_category_key
 AND a1.market_group = a2.market_group
 AND a1.store_alias < a2.store_alias;
```

### 2-4. Hub UI (스토어 관리 페이지 확장)

- 스토어 선택 → 배정할 OC 카테고리 목록 선택
- 배정 전 경고: 같은 마켓그룹 내 다른 스토어에 이미 배정된 카테고리 강조
- 배정 후: `store_category_assignments` 저장

---

## 3. 등록 파이프라인

### 3-1. 파이프라인 흐름

```
[Hub UI - 등록 파이프라인 페이지]
   ↓ 스토어 선택 + 가격전략 선택
   ↓
Step 1. 출고 대상 필터
   → store_category_assignments에서 해당 스토어의 OC 카테고리 목록 조회
   → products WHERE 카테고리명 IN (배정된 OC 카테고리) AND ACTIVE AND oc_price > 0
   ↓
Step 2. 가격 계산
   → price_engine.batch_solve(policy=get_pricing_strategy(market, strategy))
   ↓
Step 3. 포맷 변환
   → 고도몰: convert_ownerclan_to_godomall() → 고도몰 업로드 Excel
   → 스마트스토어: (추후) 스마트스토어 포맷
   → 옥션/지마켓: (추후) ESM 포맷
   ↓
Step 4a. Excel 다운로드 (Phase 1 - 현재)
   → 생성된 Excel 다운로드 → 셀러센터 수동 업로드
   ↓
Step 4b. API 직접 등록 (Phase 2 - 추후)
   → 고도몰 API, 스마트스토어 API 직접 호출
   ↓
Step 5. 등록 기록
   → market_registrations 테이블에 상품코드, 스토어, 마켓, 등록일 기록
   ↓
Step 6. Naver 연동 스토어 재검증 (Phase 2)
   → 고도몰/스마트스토어 업로드 후
   → 네이버쇼핑파트너센터 크롤링으로 실제 카테고리 판정 확인
   → 중복 판정 시 알림
```

### 3-2. 새 테이블: market_registrations

```sql
CREATE TABLE IF NOT EXISTS market_registrations (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    상품코드            TEXT NOT NULL,
    store_alias         TEXT NOT NULL,
    market              TEXT NOT NULL,          -- 고도몰, 스마트스토어, 옥션, ...
    market_product_id   TEXT,                   -- 마켓 부여 상품번호 (API 등록 후)
    oc_category_key     TEXT,
    market_cat_code     TEXT,                   -- 해당 마켓에서의 카테고리 코드
    sell_price          INTEGER,
    strategy            TEXT,                   -- lowest_price, normal_sale, cpc_ad
    registered_at       TEXT,
    status              TEXT DEFAULT 'ACTIVE',  -- ACTIVE, INACTIVE, DUPLICATE_WARNING
    pipeline_run_id     TEXT,                   -- 어떤 파이프라인 실행에서 등록됐는지
    -- UNIQUE 없음: 동일 상품이 재등록될 수 있음 (삭제 후 재등록)
    -- 최신 등록 상태는 registered_at DESC 기준으로 조회
    PRIMARY KEY(상품코드, store_alias, registered_at)
);
```

### 3-3. Hub 파이프라인 페이지 UI 구성

```
[등록 파이프라인] 페이지
  ┌─────────────────────────────────────┐
  │ 스토어 선택      [고도몰_메인 ▼]      │
  │ 가격 전략        ● 최저가  ○ 일반  ○ 광고 │
  │                                     │
  │ [📋 미리보기]  → 출고 예정 N개       │
  │   카테고리별 상품수 요약              │
  │   상품 목록 (코드, 상품명, 원가, 판매가) │
  │                                     │
  │ [▶ Excel 생성]  → 다운로드 링크      │
  │ [🚀 API 등록]   → (Phase 2)         │
  └─────────────────────────────────────┘
```

---

## 4. 대시보드 마켓별 현황

### 4-1. Excel Import (Phase 1)

- 각 마켓 셀러센터에서 상품 목록 내보내기 → Hub에 업로드
- 파싱하여 `market_registrations` 갱신
- 지원 포맷:
  - 고도몰: 상품 목록 Excel
  - 스마트스토어: 상품 현황 Excel
  - 옥션/지마켓: ESM 상품 목록
  - (각 마켓별 파서 구현)

### 4-2. 대시보드 현황 카드

```
대시보드 상단 마켓별 현황 그리드:
┌──────────┬──────────┬──────────┬──────────┐
│ 고도몰   │스마트스토어│  옥션    │ 지마켓   │
│  3,241개 │  1,892개 │  4,102개 │  3,988개 │
│마지막 import│         │          │          │
│ 2026-04-10│2026-04-09│2026-04-08│2026-04-08│
└──────────┴──────────┴──────────┴──────────┘
```

### 4-3. 네이버쇼핑파트너센터 연동 (Phase 2)

- 고도몰/스마트스토어 업로드 후 트리거
- 파트너센터 크롤링: 실제 네이버카테고리 판정 확인
- 중복 판정 발생 시 → `market_registrations.status = 'DUPLICATE_WARNING'`
- 대시보드에 경고 표시

---

## 5. 구현 순서 (Phase)

### Phase 1 (현재 스프린트)
1. **OC API 마켓카테고리 테스트** → `allCategories` 필드 확인
2. **`oc_category_markets` 테이블 생성 + 수집** (API or Excel 파싱)
3. **`store_category_assignments` UI** (스토어 관리 페이지 확장)
4. **대시보드 마켓별 현황 카드** + Excel import
5. **등록 파이프라인 Hub UI** (pipeline_run.py 래핑, Excel 출력)
6. **`market_registrations` 기록**

### Phase 2 (다음 스프린트)
7. 고도몰 API 직접 등록
8. 스마트스토어 API 등록
9. 네이버쇼핑파트너센터 크롤링 검증

---

## 6. 파일 구조 (신규/변경)

```
hub/
  routers/
    pipeline_register.py      # NEW: 등록 파이프라인 API
    market_status.py          # NEW: 마켓 현황 import API
  services/
    register_service.py       # NEW: 파이프라인 실행 로직
    market_status_service.py  # NEW: Excel import + 현황 조회
  static/pages/
    pipeline_register.js      # NEW: 등록 파이프라인 UI
    dashboard.js              # MOD: 마켓별 현황 카드 추가

DB_save/
  database/
    oc_category_markets.py    # NEW: oc_category_markets 테이블 관리
    market_registrations.py   # NEW: market_registrations 테이블 관리

godomall_register/
  pipeline_run.py             # MOD: progress_callback + run_id 반환 추가
  market_category_fetch.py    # NEW: OC API 마켓카테고리 수집

oc_catalog.db                 # MOD: market_categories_json, oc_category_markets
products.db                   # MOD: store_category_assignments, market_registrations
```

---

## 7. OC API 첫 번째 작업: 마켓카테고리 필드 확인

```python
# 실행 테스트 코드 (market_category_fetch.py)
from godomall_register.ownerclan_client import OwnerclanClient

client = OwnerclanClient()

# 테스트 1: allCategories에 마켓카테고리 필드 있는지 확인
try:
    cats = client._graphql("""
    {
      allCategories(first: 1) {
        edges {
          node {
            key name fullName
            marketCategories { market code name }
          }
        }
      }
    }
    """)
    print("마켓카테고리 API 지원:", cats)
except Exception as e:
    print("미지원:", e)
    # → Excel 파싱 경로로 fallback

# 테스트 2: 단건 item에 마켓카테고리 있는지
item = client.get_item("W0B71FF", fields="key category { key name fullName } metadata")
print("item.metadata:", item.get("metadata"))
```
