# Design: 가격 정책 (3-전략 시스템)

**Date**: 2026-04-07
**Status**: Approved (사용자 brainstorming 승인 완료)
**Scope**: 11번가 기준 3-전략 (최저가/일반판매/광고) 데이터 + 로더 + price_engine 통합
**Depends on**: `price_engine.py`, `sales_channel_policy.py`, `sales_channels.json`

---

## 목적

상품의 마켓 등록가를 결정하는 **가격 전략 정책**을 데이터로 관리한다. 가격대별 마진/손실 한도를 전략별 밴드로 정의하고, 같은 코드 한 번 호출로 어느 마켓 어느 전략이든 가격 계산이 가능해야 한다.

**핵심 결정:**
- 1개 마켓 (11번가) 에서 3-전략 모두 검증
- 새 마켓 추가 시 **데이터만 추가** (코드 수정 없음)
- 카테고리별 수수료, 옵션 마진 전략은 후속 작업으로 분리

---

## 배경

### 현재 상태

- `price_engine.py` 가 `solve_h` / `batch_solve` 로 H 역산 가능
- `solve_h(target_s=...)` 형태로 절대값 손실/이익 한 가지만 받음
- 최저가전략 1종만 v3 plan 에서 절대값 밴드 (-100~-1,000원) 로 검증
- 일반판매전략, 광고전략은 미정의

### 문제

1. 한 가지 metric (절대값 S) 만으로는 가격대별로 U(원가대비), V(매출대비) 가 크게 벌어짐
   - 예: 490원 상품이 -100원 손실 = U=-20%, 234,620원 상품이 -1,000원 손실 = U=-0.32%
2. 일반판매/광고 전략은 마진률(%) 기반이라 절대값 metric 으로 표현 불가
3. 마켓별로 검증된 밴드가 코드에 흩어져 데이터 관리 어려움

### 이 spec 의 해결

- 3-전략을 metric (`absolute_s` 또는 `u_percent`) 별로 분리 정의
- 가격대별 밴드를 데이터(JSON) 로 분리
- `batch_solve` 에 `metric` 인자 추가하여 두 metric 모두 처리
- 마켓별 × 전략별로 별도 밴드/C/max_h/round_h 보관

---

## 3-전략 정의

| ID | 한글명 | 목적 | 입력 metric | 11번가 C | max_h | round_h |
|---|---|---|---|---|---|---|
| `lowest_price` | 최저가전략 | 등급/판매건수 확보 | `absolute_s` (음수 원) | 1.12 | 3.5 | 2 |
| `normal_sale` | 일반판매전략 | 마진 확보 | `u_percent` (양수 %) | 1.15 | 3.5 | 2 |
| `cpc_ad` | 광고전략 | 광고 ROI / 원가만큼 회수 | `u_percent` (양수 %) | 2.00 | 3.5 | 2 |

### 11번가 검증 밴드

**최저가전략 (`lowest_price`):** 절대값 (원) — 음수 = 손실
```
≤   500원 →   -100
≤ 1,000원 →   -150
≤ 3,000원 →   -200
≤ 5,000원 →   -300
≤10,000원 →   -500
≤20,000원 →   -700
> 20,000원 → -1,000  (균일)
```

**일반판매전략 (`normal_sale`):** U% (원가대비 마진률)
```
≤   500원 → 30%
≤ 1,000원 → 25%
≤ 3,000원 → 20%
≤ 5,000원 → 18%
≤10,000원 → 15%
≤30,000원 → 12%
≤50,000원 → 10%
≤100,000원→  8%
≤200,000원→  6%
>200,000원→  5%
```

**광고전략 (`cpc_ad`):** U% (원가대비 마진률, 5~10만원 이상 평탄)
```
≤   100원 → 100%   (원가만큼 이익)
≤   200원 → 100%
≤   500원 →  80%
≤ 1,000원 →  65%
≤ 3,000원 →  50%
≤ 5,000원 →  40%
≤10,000원 →  30%
≤30,000원 →  22%
≤50,000원 →  18%
≤100,000원→  15%
>100,000원→  15%   (잘 안 팔리는 가격대, 평탄)
```

### 검증 출처

- `DB_save/광고전략_시뮬_v2.xlsx` (35개 상품: 가상 100~400원 5개 + 실측 30개)
- `DB_save/일반판매전략_시뮬_v1.xlsx` (29개 상품)
- `DB_save/최저가전략_시뮬_v4.xlsx` (29개 상품 + v3 비교)

---

## 데이터 형식

**파일**: `DB_save/pricing_strategies.json`

```json
{
  "strategies": {
    "lowest_price": {
      "label_ko": "최저가전략",
      "metric": "absolute_s",
      "purpose": "등급/판매건수 확보 - 절대값 손실 한도"
    },
    "normal_sale": {
      "label_ko": "일반판매전략",
      "metric": "u_percent",
      "purpose": "마진 확보 - 원가대비 마진률"
    },
    "cpc_ad": {
      "label_ko": "광고전략",
      "metric": "u_percent",
      "purpose": "광고 ROI / 원가만큼 회수"
    }
  },
  "market_strategies": {
    "11번가": {
      "lowest_price": {
        "margin_c": 1.12,
        "max_h": 3.5,
        "round_h": 2,
        "bands": [
          [500, -100], [1000, -150], [3000, -200], [5000, -300],
          [10000, -500], [20000, -700], [999999999, -1000]
        ]
      },
      "normal_sale": {
        "margin_c": 1.15,
        "max_h": 3.5,
        "round_h": 2,
        "bands": [
          [500, 30], [1000, 25], [3000, 20], [5000, 18],
          [10000, 15], [30000, 12], [50000, 10], [100000, 8],
          [200000, 6], [999999999, 5]
        ]
      },
      "cpc_ad": {
        "margin_c": 2.00,
        "max_h": 3.5,
        "round_h": 2,
        "bands": [
          [100, 100], [200, 100], [500, 80], [1000, 65],
          [3000, 50], [5000, 40], [10000, 30], [30000, 22],
          [50000, 18], [100000, 15], [999999999, 15]
        ]
      }
    }
  }
}
```

### 구조 설명

- `strategies` — 전략 메타데이터 (label, metric, purpose). 마켓 무관, 공통
- `market_strategies` — 마켓별 × 전략별 밴드/파라미터 dict. 새 마켓 추가 시 키만 추가
- `bands` — `[가격상한, 값]` 배열. 오름차순. 마지막 항목이 catch-all (예: `[999999999, X]`)
- **밴드 매칭 룰**: `cost_a <= 가격상한` 인 첫 번째 band 의 값 사용 (기존 `price_engine._resolve_target_s` 와 동일 의미)
- `metric` 값 두 종류:
  - `"absolute_s"` — band 값이 target_S (원 단위, 음수=손실/양수=이익)
  - `"u_percent"` — band 값이 U% (원가대비). target_S = band값/100 × cost_a 로 변환

### 파일 인코딩 / 에러 처리

- `pricing_strategies.json` 은 **UTF-8** 로 저장 (한글 키 `"11번가"` 등 포함)
- 파일 누락 시 `FileNotFoundError` 발생, 호출자가 처리
- JSON 파싱 실패 시 `json.JSONDecodeError`, 호출자가 처리
- `get_pricing_strategy` 에서 알 수 없는 마켓/전략 호출 시 `KeyError` 와 함께 어느 키가 누락인지 명시한 메시지 반환
  - 예: `KeyError: "market '스마트스토어' not found in pricing_strategies.json (available: ['11번가'])"`

---

## 호출 인터페이스

**신규 모듈**: `DB_save/pricing_strategies.py` (sales_channel_policy.py 와 같은 패턴)

```python
def get_strategy_meta(strategy_id: str) -> dict:
    """전략 메타 반환.

    Returns:
        {"label_ko": "최저가전략", "metric": "absolute_s", "purpose": "..."}
    """

def get_pricing_strategy(market_name: str, strategy_id: str) -> dict:
    """마켓 + 전략 머지 반환.

    Returns:
        {
            "strategy_id": "lowest_price",
            "label_ko": "최저가전략",
            "metric": "absolute_s",
            "purpose": "...",
            "margin_c": 1.12,
            "max_h": 3.5,
            "round_h": 2,
            "bands": [(500, -100), ...],
        }
    """

def list_strategies() -> list[dict]:
    """모든 전략 메타 리스트. UI 드롭다운용."""

def list_market_strategies(market_name: str) -> list[str]:
    """특정 마켓이 지원하는 전략 ID 리스트."""
```

### price_engine.batch_solve 시그니처 확장

```python
def batch_solve(
    products: list[dict],
    policy: dict,
    target_bands: list[tuple[float, float]],
    metric: str = "absolute_s",   # 신규 인자, 기본값 = 기존 동작
    margin_c: float = 1.12,
    shipping_absorbed: float = 0,
    min_h: float = 1.0,
    max_h: float = 3.5,
    round_h: int = 2,
) -> list[dict]:
    """일괄 H 역산.

    metric:
        "absolute_s": band 값을 target_S 로 사용 (기존 동작)
        "u_percent" : band 값을 U% 로 해석. target_S = band/100 * cost_a
    """
    sorted_bands = sorted(target_bands, key=lambda x: x[0])
    results = []
    for product in products:
        cost_a = product["cost_a"]
        band_value = _resolve_band(cost_a, sorted_bands)
        if metric == "u_percent":
            target_s = round(band_value / 100 * cost_a)
        else:
            target_s = band_value
        # ... 기존 solve_h 호출
```

기존 호출 (`metric` 미지정) 은 `"absolute_s"` 로 동작 → 기존 코드 회귀 없음.

### 사용 예시

```python
from DB_save.sales_channel_policy import get_store_policy
from DB_save.pricing_strategies import get_pricing_strategy
from DB_save.price_engine import batch_solve

policy = get_store_policy("11번가A2-1")
strategy = get_pricing_strategy("11번가", "cpc_ad")

results = batch_solve(
    products=[{"code": "W001", "cost_a": 500}, ...],
    policy=policy,
    target_bands=strategy["bands"],
    metric=strategy["metric"],          # "u_percent"
    margin_c=strategy["margin_c"],      # 2.00
    max_h=strategy["max_h"],            # 3.5
    round_h=strategy["round_h"],        # 2
)
```

---

## 검증 계획

### `pricing_strategies.py` smoke test (`__main__`)

1. `get_strategy_meta("lowest_price"/"normal_sale"/"cpc_ad")` → 3개 메타 로드 성공
2. `get_pricing_strategy("11번가", strategy_id)` → 3개 머지 결과 반환, 필수 키 (`margin_c`, `max_h`, `round_h`, `bands`, `metric`, `label_ko`) 모두 존재
3. `list_strategies()` → 3개 항목 리스트
4. `list_market_strategies("11번가")` → `["lowest_price", "normal_sale", "cpc_ad"]`
5. 에러 케이스: `get_pricing_strategy("존재안함", "lowest_price")` → 명확한 에러 메시지
6. 에러 케이스: `get_pricing_strategy("11번가", "잘못된전략")` → 명확한 에러 메시지

### `price_engine.batch_solve` 회귀 + 신규 metric 검증

1. **회귀**: `batch_solve(...)` (metric 미지정) → 기존 v3 동작과 동일 결과
2. **신규 absolute_s**: `batch_solve(metric="absolute_s")` → (1)과 동일 결과
3. **신규 u_percent**: 11번가 normal_sale 밴드로 batch_solve 실행 → 모든 상품 status="ok", 결과 U% 가 target U% 와 ±0.5% 이내 일치
4. **통합**: `get_pricing_strategy("11번가", "cpc_ad")` → batch_solve 호출 → 35개 (가상 5 + 실측 30) 모두 ok

### 데이터 검증

1. `pricing_strategies.json` 의 `market_strategies["11번가"]` 에 `lowest_price`/`normal_sale`/`cpc_ad` 3개 키 모두 존재
2. 각 전략의 `metric` 값이 `strategies` dict 의 정의와 일치
3. 각 전략의 `bands` 가 가격 오름차순 정렬, 마지막 항목 `999999999` 로 끝남

---

## 파일 구조

```
DB_save/
├── pricing_strategies.json       # 신규: 3-전략 × 마켓별 데이터
├── pricing_strategies.py         # 신규: loader/getter + smoke test
├── price_engine.py               # 수정: batch_solve 에 metric 인자
├── sales_channels.json           # 변경 없음
└── sales_channel_policy.py       # 변경 없음

docs/superpowers/specs/
└── 2026-04-07-pricing-strategy-design.md  # 본 spec
```

---

## 마켓 확장 가이드

새 마켓 (예: 스마트스토어) 추가 시:

1. `sales_channels.json` 에 마켓 정의 (이미 존재하는 경우 skip)
2. `pricing_strategies.json` 의 `market_strategies` 에 키 추가:
   ```json
   "스마트스토어": {
     "lowest_price": { "margin_c": ..., "max_h": ..., "bands": [...] },
     "normal_sale":  { ... },
     "cpc_ad":       { ... }
   }
   ```
3. 시뮬 도구 (`price_compare_excel.py` 또는 별도) 로 검증
4. 사용자 검증 후 데이터 확정
5. **코드 수정 없음**

스마트스토어 특수성 (commission_base=post_discount, 옵션가격 연동) 은 본 spec 범위 외. price_engine.solve_h 는 이미 두 commission_base 모두 지원하므로 데이터만 다르면 됨.

---

## 이 spec 에서 하지 않는 것

- 11번가 외 마켓 데이터 (스마트스토어 등 — 후속 작업)
- 카테고리별 수수료 분기 (사용자가 "추후" 명시)
- 옵션 마진 전략 (스마트스토어 옵션가격 연동)
- `price_compare_excel.py` 마이그레이션 (현재 untracked, 별도 작업)
- UI 변경, 한글 표시명 UI 적용
- 자동 최적화 도구 (사용자가 엑셀 보면서 수동 조정 선호)
- 오너클랜→ES, 고도몰 연동 변경
- DB 읽기/쓰기 (호출자 책임, 기존 패턴 그대로)
- 가격 정책 변경 이력 관리

---

## 의존성

- `price_engine.py` (forward_calc, solve_h, _resolve_target_s)
- `sales_channel_policy.py` (get_store_policy, get_market_policy)
- 표준 라이브러리 (`json`, `pathlib`)
- 추가 설치 없음

---

## 향후 작업 (별도 spec 필요)

1. **스마트스토어 추가** — 데이터 추가 + 검증 시뮬
2. **카테고리별 수수료 분기** — 같은 마켓 내 카테고리에 따라 다른 수수료. 데이터 구조 확장 필요
3. **옵션 마진 전략** — 스마트스토어 옵션가격 판매가 연동, 옵션으로 마진 확보
4. **price_compare_excel.py 마이그레이션** — 신규 pricing_strategies 사용하도록 수정 또는 신규 도구로 대체
5. **UI 통합** — 가격가공 화면에서 전략 선택 → 자동 적용
6. **활성화/비활성화 전략 관리** — 마켓별로 일부 전략만 활성화하는 정책
