# 가격 정책 (3-전략) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 11번가 기준 3-전략 (lowest_price/normal_sale/cpc_ad) 가격 정책 데이터 + 로더 모듈 + price_engine 통합. V기반 metric 신규 지원.

**Architecture:** `pricing_strategies.json` 에 메타+마켓별 밴드 저장 → `pricing_strategies.py` 가 로드 + 머지 → `price_engine.batch_solve` 가 `metric` 인자로 분기 (`absolute_s` / `v_percent`). 신규 `solve_h_by_v` 함수가 V→H 역산 처리.

**Tech Stack:** Python 3.12, 표준 라이브러리(`json`, `math`, `pathlib`), 추가 설치 없음

---

## File Structure

| File | Action | Responsibility |
|------|--------|----------------|
| `DB_save/price_engine.py` | Modify | `solve_h_by_v` 신규 함수 (line ~221 다음), `batch_solve` 에 `metric` 인자 추가 (line 224) |
| `DB_save/pricing_strategies.json` | Create | 3-전략 메타 + 11번가 마켓별 밴드 데이터 (UTF-8) |
| `DB_save/pricing_strategies.py` | Create | JSON 로더, getter 함수, list 함수, 에러 처리, smoke test |

**기존 호출자 영향:**
- `DB_save/price_compare_excel.py:23` 에서 `batch_solve` import 함. 호출은 안 함 (line 167 에서 `solve_h` 직접 호출). `metric` 인자 default 추가라 import 깨짐 없음.
- 기존 `solve_h`, `forward_calc`, `_resolve_target_s` 시그니처 변경 없음.

---

### Task 1: price_engine.py — `solve_h_by_v` 신규 함수 추가

**Files:**
- Modify: `DB_save/price_engine.py` (insert after line 221, 즉 `solve_h` 함수 끝 직후)
- Create (temporary): `_tmp_test_solve_h_by_v.py` (검증 후 삭제)

**수학 derivation (구현 전 확인):**
```
target_V → H 역산:
  V = S/L = 매출대비%
  S = (V/100) × L
  
  L = G × H × (1-J)
  S = G × H × coef_S - M - A - R   [pre_discount, coef_S = (1-J)(1-P) - fee]
  S = G × H × (1-J)(1-fee-P) - M - A - R   [post_discount]

pre_discount:
  G × H × coef_S - M - A - R = (V/100) × G × H × (1-J)
  G × H × [coef_S - (V/100)(1-J)] = M + A + R
  H = (M+A+R) / (G × [(1-J)(1-P) - fee - (V/100)(1-J)])

post_discount:
  G × H × (1-J)(1-fee-P) - M - A - R = (V/100) × G × H × (1-J)
  G × H × (1-J) × [(1-fee-P) - V/100] = M + A + R
  H = (M+A+R) / (G × (1-J) × [1 - fee - P - V/100])
```

이론적 V max 검증:
- 11번가 (J=0.5, fee=0.175, P=0): V max = 100×(1 - 0.175/0.5 - 0) = 65%
- 스마트스토어 (J=0.5, fee=0.065, P=0, post): V max = 100×(1 - 0.065 - 0) = 93.5%

- [ ] **Step 1: 검증 스크립트 작성 (TDD - test 먼저)**

`_tmp_test_solve_h_by_v.py` 작성:

```python
# -*- coding: utf-8 -*-
"""solve_h_by_v 단위 검증."""
import sys
sys.path.insert(0, '.')
from DB_save.price_engine import solve_h_by_v

policy_11st = {
    'commission_rate': 17.5, 'commission_base': 'pre_discount',
    'discount_rate': 50, 'coupon_amount': 120, 'reward_rate': 0,
}
policy_smart = {
    'commission_rate': 6.5, 'commission_base': 'post_discount',
    'discount_rate': 50, 'coupon_amount': 0, 'reward_rate': 0,
}

# Test 1: pre_discount 정상 - target_V=18 → 결과 V≈18
sol = solve_h_by_v(5000, 2.5, policy_11st, 18.0)
fwd = sol['forward']
assert sol['status'] == 'ok', f"T1 status: {sol['status']}"
assert abs(fwd['revenue_ratio'] - 18.0) < 0.5, f"T1 V mismatch: {fwd['revenue_ratio']}"
print(f"T1 PASS pre_discount: H={sol['h']}, V={fwd['revenue_ratio']}")

# Test 2: post_discount 정상 - 스마트스토어 V=18%
sol = solve_h_by_v(5000, 1.5, policy_smart, 18.0)
fwd = sol['forward']
assert sol['status'] == 'ok', f"T2 status: {sol['status']}"
assert abs(fwd['revenue_ratio'] - 18.0) < 0.5, f"T2 V mismatch: {fwd['revenue_ratio']}"
print(f"T2 PASS post_discount: H={sol['h']}, V={fwd['revenue_ratio']}")

# Test 3: error_zero_cost
sol = solve_h_by_v(0, 2.5, policy_11st, 18.0)
assert sol['status'] == 'error_zero_cost', f"T3 status: {sol['status']}"
assert sol['h'] is None
print("T3 PASS: error_zero_cost")

# Test 4: error_v_unreachable - 11번가 V max ≈ 65%, V=70 시도
sol = solve_h_by_v(5000, 2.5, policy_11st, 70.0)
assert sol['status'] == 'error_v_unreachable', f"T4 status: {sol['status']}"
print("T4 PASS: error_v_unreachable (V=70 > 65)")

# Test 5: clamped_max - 작은 cost + 큰 V 시도 (max_h=4.0 초과)
sol = solve_h_by_v(10, 4.0, policy_11st, 30.0, max_h=4.0)
assert sol['status'] == 'clamped_max', f"T5 status: {sol['status']}"
assert sol['h'] == 4.0
print("T5 PASS: clamped_max")

# Test 6: clamped_min - 큰 cost + 작은 V (min_h=1.0 미만)
sol = solve_h_by_v(200000, 4.0, policy_11st, 5.0, min_h=1.0, max_h=5.0)
assert sol['status'] == 'clamped_min', f"T6 status: {sol['status']}"
assert sol['h'] == 1.0
print("T6 PASS: clamped_min")

# Test 7: 11번가 normal_sale 가격대별 검증 (spec 의 13단계)
test_cases = [
    (100, 30, 'ok'),
    (500, 22, 'ok'),
    (5000, 15, 'ok'),
    (50000, 8, 'ok'),
    (200000, 5, 'ok'),
]
for cost, V_target, expected_status in test_cases:
    sol = solve_h_by_v(cost, 2.5, policy_11st, V_target, min_h=1.0, max_h=4.0)
    assert sol['status'] == expected_status, f"cost={cost}: {sol['status']}"
    fwd = sol['forward']
    assert abs(fwd['revenue_ratio'] - V_target) < 0.5, f"cost={cost}: V={fwd['revenue_ratio']}"
print("T7 PASS: 11번가 normal_sale 5개 case")

print("\nALL PASS")
```

- [ ] **Step 2: 검증 스크립트 실행 - 실패 확인 (함수 없음)**

Run: `PYTHONIOENCODING=utf-8 python _tmp_test_solve_h_by_v.py`

Expected:
```
ImportError: cannot import name 'solve_h_by_v' from 'DB_save.price_engine'
```

- [ ] **Step 3: `solve_h_by_v` 함수 구현**

`DB_save/price_engine.py` 에서 line 221 (`solve_h` 함수 끝, return 문 다음) 직후, `batch_solve` 함수 (line 224) 직전에 새 함수 추가:

```python
def solve_h_by_v(
    cost_a: float,
    margin_c: float,
    policy: dict[str, Any],
    target_v_pct: float,
    shipping_absorbed: float = 0,
    min_h: float = 1.0,
    max_h: float = 4.0,
    round_h: int = 2,
) -> dict[str, Any]:
    """역방향 계산: 목표 V (매출대비%) → H 역산.

    매출대비 영업이익률 (V = S/L * 100) 을 target 으로 H 를 역산한다.
    pre_discount 와 post_discount 두 commission_base 모두 지원.

    Args:
        cost_a: 원가 (A열).
        margin_c: 마진배수 (C열).
        policy: get_store_policy() 반환 dict (commission_rate, commission_base,
            discount_rate, coupon_amount, reward_rate).
        target_v_pct: 목표 매출대비% (예: 18 → V=18%).
        shipping_absorbed: 배송비 흡수금 (F=R).
        min_h: H 최소값 (이하면 clamped).
        max_h: H 최대값 (이상이면 clamped).
        round_h: H 소수점 반올림 자리수.

    Returns:
        dict with keys:
            h_raw: 역산된 H (반올림 전)
            h: 반올림 + clamp 적용된 H
            status: "ok" | "clamped_min" | "clamped_max"
                  | "error_zero_cost" | "error_v_unreachable"
            forward: forward_calc 결과 (h 적용 후)

    수학:
        pre_discount:
            coef_v = (1-J)(1-P) - fee_rate - (V/100)(1-J)
            H = (M+A+R) / (G × coef_v)
        post_discount:
            coef_v = (1-J)(1 - fee_rate - P - V/100)
            H = (M+A+R) / (G × coef_v)

    error_v_unreachable: target_V 가 이론적 V max 초과 (coef_v ≤ 0).
        11번가 V max ≈ 65%.
    """
    if cost_a <= 0:
        return {
            "h_raw": None,
            "h": None,
            "status": "error_zero_cost",
            "forward": None,
        }

    fee_rate = policy["commission_rate"] / 100
    J = (policy.get("discount_rate") or 0) / 100
    E = policy.get("coupon_amount", 0)
    P = (policy.get("reward_rate") or 0) / 100
    commission_base = policy.get("commission_base", "pre_discount")
    M = E
    R = shipping_absorbed
    G = cost_a * margin_c + E + shipping_absorbed
    V = target_v_pct / 100

    if commission_base == "pre_discount":
        coef = (1 - J) * (1 - P) - fee_rate - V * (1 - J)
    else:
        coef = (1 - J) * (1 - fee_rate - P - V)

    if coef <= 1e-9:
        return {
            "h_raw": None,
            "h": None,
            "status": "error_v_unreachable",
            "forward": None,
        }

    h_raw = (M + cost_a + R) / (G * coef)

    status = "ok"
    h_clamped = h_raw
    if h_raw < min_h:
        h_clamped = min_h
        status = "clamped_min"
    elif h_raw > max_h:
        h_clamped = max_h
        status = "clamped_max"

    h_final = round(h_clamped, round_h)
    fwd = forward_calc(cost_a, margin_c, h_final, policy, shipping_absorbed)

    return {
        "h_raw": round(h_raw, round_h + 2),
        "h": h_final,
        "status": status,
        "forward": fwd,
    }
```

- [ ] **Step 4: 검증 스크립트 재실행 - 성공 확인**

Run: `PYTHONIOENCODING=utf-8 python _tmp_test_solve_h_by_v.py`

Expected:
```
T1 PASS pre_discount: H=1.74, V=18.1
T2 PASS post_discount: H=..., V=...
T3 PASS: error_zero_cost
T4 PASS: error_v_unreachable (V=70 > 65)
T5 PASS: clamped_max
T6 PASS: clamped_min
T7 PASS: 11번가 normal_sale 5개 case

ALL PASS
```

- [ ] **Step 5: 임시 검증 파일 삭제 + Commit**

```bash
rm _tmp_test_solve_h_by_v.py
git add DB_save/price_engine.py
git commit -m "feat(price): add solve_h_by_v - reverse H from target V%"
```

---

### Task 2: price_engine.py — `batch_solve` 에 `metric` 인자 추가

**Files:**
- Modify: `DB_save/price_engine.py` (line 224 `batch_solve` 시그니처 + 본체)
- Create (temporary): `_tmp_test_batch_solve_metric.py`

- [ ] **Step 1: 회귀 + 신규 metric 검증 스크립트 작성**

`_tmp_test_batch_solve_metric.py`:

```python
# -*- coding: utf-8 -*-
"""batch_solve metric 인자 회귀 + 신규 v_percent 검증."""
import sys
sys.path.insert(0, '.')
from DB_save.price_engine import batch_solve

policy = {
    'commission_rate': 17.5, 'commission_base': 'pre_discount',
    'discount_rate': 50, 'coupon_amount': 120, 'reward_rate': 0,
}

products = [
    {'code': 'A001', 'cost_a': 500},
    {'code': 'A002', 'cost_a': 5000},
    {'code': 'A003', 'cost_a': 50000},
]

# Test 1: 회귀 - metric 미지정 (= absolute_s 기존 동작)
abs_bands = [(500, -100), (5000, -300), (999999999, -1000)]
results = batch_solve(products, policy, abs_bands, margin_c=1.12)
for r in results:
    assert r['status'] == 'ok', f"T1 {r['code']}: {r['status']}"
print(f"T1 PASS: 회귀 - metric 미지정 동작")

# Test 2: 명시적 absolute_s
results2 = batch_solve(products, policy, abs_bands, metric='absolute_s', margin_c=1.12)
assert all(r1['h'] == r2['h'] for r1, r2 in zip(results, results2)), "T2 mismatch"
print(f"T2 PASS: metric='absolute_s' 동일 결과")

# Test 3: v_percent 신규
v_bands = [(500, 22), (5000, 15), (999999999, 5)]
results3 = batch_solve(products, policy, v_bands,
                       metric='v_percent', margin_c=2.5, max_h=4.0)
for r in results3:
    assert r['status'] == 'ok', f"T3 {r['code']}: {r['status']}"
    assert 'revenue_ratio' in r
    target_v = next(v for lim, v in v_bands if r['cost_a'] <= lim)
    assert abs(r['revenue_ratio'] - target_v) < 0.5, \
        f"T3 {r['code']}: V={r['revenue_ratio']}, target={target_v}"
print(f"T3 PASS: metric='v_percent' 신규 동작")

# Test 4: 잘못된 metric 값
try:
    batch_solve(products, policy, v_bands, metric='wrong_metric')
    assert False, "T4: should raise"
except ValueError as e:
    assert 'metric' in str(e).lower()
    print(f"T4 PASS: 잘못된 metric → ValueError")

print("\nALL PASS")
```

- [ ] **Step 2: 실행 - 실패 확인 (metric 인자 없음)**

Run: `PYTHONIOENCODING=utf-8 python _tmp_test_batch_solve_metric.py`

Expected:
```
TypeError: batch_solve() got an unexpected keyword argument 'metric'
```

- [ ] **Step 3: `batch_solve` 함수 수정**

`DB_save/price_engine.py` 의 line 224 부터 `batch_solve` 함수를 다음으로 교체:

```python
def batch_solve(
    products: list[dict[str, Any]],
    policy: dict[str, Any],
    target_bands: list[tuple[float, float]],
    metric: str = "absolute_s",
    margin_c: float = 1.12,
    shipping_absorbed: float = 0,
    min_h: float = 1.0,
    max_h: float = 3.5,
    round_h: int = 2,
) -> list[dict[str, Any]]:
    """Batch processing: solve H for each product using cost-based target bands.

    Args:
        products: List of dicts with at least "code" and "cost_a" keys.
        policy: Store policy dict.
        target_bands: Sorted list of (limit, value) tuples.
        metric: "absolute_s" (band 값=target_S) or "v_percent" (band 값=V%).
        margin_c: Margin coefficient (default 1.12).
        shipping_absorbed: Shipping cost absorbed.
        min_h: Minimum allowed H.
        max_h: Maximum allowed H.
        round_h: Decimal places to round H.

    Returns:
        List of flat dicts with: code, cost_a, target_value, h, h_raw, status,
        plus all forward_calc fields.

    Raises:
        ValueError: metric 이 "absolute_s" 또는 "v_percent" 가 아닌 경우.
    """
    if metric not in ("absolute_s", "v_percent"):
        raise ValueError(
            f"Invalid metric '{metric}'. "
            f"Must be 'absolute_s' or 'v_percent'."
        )

    target_bands = sorted(target_bands, key=lambda x: x[0])
    results = []
    for product in products:
        code = product["code"]
        cost_a = product["cost_a"]
        band_value = _resolve_target_s(cost_a, target_bands)

        if metric == "v_percent":
            result = solve_h_by_v(
                cost_a, margin_c, policy, band_value,
                shipping_absorbed=shipping_absorbed,
                min_h=min_h, max_h=max_h, round_h=round_h,
            )
        else:  # absolute_s
            result = solve_h(
                cost_a, margin_c, policy, band_value,
                shipping_absorbed=shipping_absorbed,
                min_h=min_h, max_h=max_h, round_h=round_h,
            )

        row = {
            "code": code,
            "cost_a": cost_a,
            "target_value": band_value,
            "h": result["h"],
            "h_raw": result["h_raw"],
            "status": result["status"],
        }
        if result["forward"]:
            row.update(result["forward"])

        results.append(row)

    return results
```

**주의:** 기존 결과 dict 의 `target_s` 키가 `target_value` 로 이름 변경됨 (metric 에 따라 절대값/V% 일 수 있어 의미 일반화). 호출자가 `target_s` 키를 사용한다면 깨질 수 있음.

`price_compare_excel.py:164` 에서 `_resolve_target_s` 직접 호출, batch_solve 안 씀 → 영향 없음.

기존 `__main__` smoke test (`price_engine.py` line 282 이하) 에서 `target_s` 키 사용 가능성 확인:

- [ ] **Step 4: 기존 smoke test 영향 검증**

```bash
grep -n 'target_s' DB_save/price_engine.py
```

발견된 사용 위치를 확인하고 batch_solve 결과 dict 의 키 사용이면 `target_value` 로 변경. 아니면 그대로 둠.

- [ ] **Step 5: 검증 스크립트 재실행 - 성공 확인**

Run: `PYTHONIOENCODING=utf-8 python _tmp_test_batch_solve_metric.py`

Expected: `ALL PASS`

- [ ] **Step 6: price_engine.py 자체 smoke test 회귀 확인**

Run: `PYTHONIOENCODING=utf-8 python DB_save/price_engine.py`

Expected: `ALL CHECKS PASSED` (기존 smoke test 모두 통과)

만약 `target_s` 키 변경으로 깨지면 fix 후 재실행.

- [ ] **Step 7: 임시 파일 삭제 + Commit**

```bash
rm _tmp_test_batch_solve_metric.py
git add DB_save/price_engine.py
git commit -m "feat(price): add metric arg to batch_solve (absolute_s/v_percent)"
```

---

### Task 3: pricing_strategies.json 데이터 작성

**Files:**
- Create: `DB_save/pricing_strategies.json`

- [ ] **Step 1: JSON 파일 작성**

`DB_save/pricing_strategies.json` 작성 (UTF-8 인코딩):

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
      "metric": "v_percent",
      "purpose": "마진 확보 - 매출대비 V%"
    },
    "cpc_ad": {
      "label_ko": "광고전략",
      "metric": "v_percent",
      "purpose": "광고 ROI / 원가만큼 회수"
    }
  },
  "market_strategies": {
    "11번가": {
      "lowest_price": {
        "margin_c": 1.12,
        "min_h": 1.0,
        "max_h": 3.5,
        "round_h": 2,
        "bands": [
          [500, -100], [1000, -150], [3000, -200], [5000, -300],
          [10000, -500], [20000, -700], [999999999, -1000]
        ]
      },
      "normal_sale": {
        "margin_c": 2.5,
        "min_h": 1.0,
        "max_h": 4.0,
        "round_h": 2,
        "bands": [
          [100, 30], [200, 28], [300, 25], [500, 22], [1000, 20],
          [3000, 18], [5000, 15], [10000, 12], [30000, 10], [50000, 8],
          [100000, 6], [200000, 5], [999999999, 5]
        ]
      },
      "cpc_ad": {
        "margin_c": 2.5,
        "min_h": 1.0,
        "max_h": 4.0,
        "round_h": 2,
        "bands": [
          [100, 30], [200, 30], [300, 28], [500, 25], [1000, 22],
          [3000, 20], [5000, 18], [10000, 15], [30000, 13], [50000, 11],
          [100000, 10], [200000, 10], [999999999, 10]
        ]
      }
    }
  }
}
```

- [ ] **Step 2: JSON 파싱 검증**

Run:
```bash
python -c "
import json
with open('DB_save/pricing_strategies.json', encoding='utf-8') as f:
    data = json.load(f)

# 구조 검증
assert 'strategies' in data
assert 'market_strategies' in data
assert set(data['strategies'].keys()) == {'lowest_price', 'normal_sale', 'cpc_ad'}
assert '11번가' in data['market_strategies']
assert set(data['market_strategies']['11번가'].keys()) == {'lowest_price', 'normal_sale', 'cpc_ad'}

# 각 전략 메타 키 검증
for sid, meta in data['strategies'].items():
    assert set(meta.keys()) >= {'label_ko', 'metric', 'purpose'}
    assert meta['metric'] in ('absolute_s', 'v_percent')

# 11번가 각 전략 파라미터 검증
for sid, params in data['market_strategies']['11번가'].items():
    assert set(params.keys()) >= {'margin_c', 'min_h', 'max_h', 'round_h', 'bands'}
    bands = params['bands']
    # 오름차순 정렬 확인
    limits = [b[0] for b in bands]
    assert limits == sorted(limits), f'{sid}: bands not sorted'
    # catch-all 확인
    assert bands[-1][0] >= 999999999, f'{sid}: missing catch-all'

print('JSON OK')
"
```

Expected: `JSON OK`

- [ ] **Step 3: Commit**

```bash
git add DB_save/pricing_strategies.json
git commit -m "feat(price): add pricing_strategies.json - 11번가 3-전략 데이터"
```

---

### Task 4: pricing_strategies.py — 모듈 + getter 함수

**Files:**
- Create: `DB_save/pricing_strategies.py`
- Create (temporary): `_tmp_test_pricing_loader.py`

- [ ] **Step 1: 검증 스크립트 작성**

`_tmp_test_pricing_loader.py`:

```python
# -*- coding: utf-8 -*-
"""pricing_strategies.py 로더 검증."""
import sys
sys.path.insert(0, '.')
from DB_save.pricing_strategies import (
    get_strategy_meta, get_pricing_strategy,
    list_strategies, list_market_strategies,
)

# Test 1: get_strategy_meta - 3개 전략 메타 로드
for sid, expected_metric in [
    ('lowest_price', 'absolute_s'),
    ('normal_sale', 'v_percent'),
    ('cpc_ad', 'v_percent'),
]:
    meta = get_strategy_meta(sid)
    assert meta['metric'] == expected_metric, f"{sid}: {meta['metric']}"
    assert 'label_ko' in meta
    assert 'purpose' in meta
print("T1 PASS: get_strategy_meta 3개")

# Test 2: get_pricing_strategy - 11번가 3개 머지
for sid in ['lowest_price', 'normal_sale', 'cpc_ad']:
    s = get_pricing_strategy('11번가', sid)
    assert s['strategy_id'] == sid
    assert s['metric'] in ('absolute_s', 'v_percent')
    assert 'label_ko' in s
    assert 'margin_c' in s
    assert 'min_h' in s
    assert 'max_h' in s
    assert 'round_h' in s
    assert 'bands' in s
    assert len(s['bands']) > 0
print("T2 PASS: get_pricing_strategy 11번가 3개 머지")

# Test 3: 11번가 lowest_price 의 구체값
s = get_pricing_strategy('11번가', 'lowest_price')
assert s['margin_c'] == 1.12
assert s['min_h'] == 1.0
assert s['max_h'] == 3.5
assert s['round_h'] == 2
# 첫 band [500, -100]
assert s['bands'][0] == (500, -100) or s['bands'][0] == [500, -100]
print("T3 PASS: 11번가 lowest_price 구체값")

# Test 4: 11번가 normal_sale - V 30~5
s = get_pricing_strategy('11번가', 'normal_sale')
assert s['margin_c'] == 2.5
assert s['max_h'] == 4.0
# 첫 band [100, 30]
first = s['bands'][0]
assert first[0] == 100 and first[1] == 30
# 마지막 band [999999999, 5]
last = s['bands'][-1]
assert last[0] == 999999999 and last[1] == 5
print("T4 PASS: 11번가 normal_sale V 30~5")

# Test 5: list_strategies
strategies = list_strategies()
assert len(strategies) == 3
ids = [s['id'] for s in strategies]
assert set(ids) == {'lowest_price', 'normal_sale', 'cpc_ad'}
print("T5 PASS: list_strategies 3개")

# Test 6: list_market_strategies('11번가')
ids = list_market_strategies('11번가')
assert set(ids) == {'lowest_price', 'normal_sale', 'cpc_ad'}
print("T6 PASS: list_market_strategies 11번가 3개")

# Test 7: 에러 - 없는 마켓
try:
    get_pricing_strategy('존재안함', 'lowest_price')
    assert False, "T7: should raise"
except KeyError as e:
    assert 'market' in str(e).lower() or '존재안함' in str(e)
    print("T7 PASS: KeyError on unknown market")

# Test 8: 에러 - 없는 전략
try:
    get_pricing_strategy('11번가', 'wrong_strategy')
    assert False, "T8: should raise"
except KeyError as e:
    assert 'strategy' in str(e).lower() or 'wrong_strategy' in str(e)
    print("T8 PASS: KeyError on unknown strategy")

# Test 9: 에러 - get_strategy_meta 없는 전략
try:
    get_strategy_meta('wrong')
    assert False, "T9: should raise"
except KeyError:
    print("T9 PASS: get_strategy_meta KeyError")

# Test 10: 에러 - list_market_strategies 없는 마켓
try:
    list_market_strategies('존재안함')
    assert False, "T10: should raise"
except KeyError:
    print("T10 PASS: list_market_strategies KeyError")

print("\nALL PASS")
```

- [ ] **Step 2: 실행 - 실패 확인 (모듈 없음)**

Run: `PYTHONIOENCODING=utf-8 python _tmp_test_pricing_loader.py`

Expected:
```
ModuleNotFoundError: No module named 'DB_save.pricing_strategies'
```

- [ ] **Step 3: `pricing_strategies.py` 모듈 작성**

`DB_save/pricing_strategies.py`:

```python
"""가격 전략 정책 로더 - 3-전략 (lowest_price/normal_sale/cpc_ad).

JSON 파일 (DB_save/pricing_strategies.json) 에서 전략 메타와 마켓별 밴드를
로드하여 price_engine.batch_solve 가 사용할 수 있는 dict 형태로 반환한다.

Public API:
    get_strategy_meta(strategy_id) -> dict
    get_pricing_strategy(market_name, strategy_id) -> dict
    list_strategies() -> list[dict]
    list_market_strategies(market_name) -> list[str]
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

# ── 데이터 파일 경로 ────────────────────────────────────────────────────────

_DATA_PATH = Path(__file__).resolve().parent / "pricing_strategies.json"

# ── 캐시 ────────────────────────────────────────────────────────────────────

_cache: dict[str, Any] | None = None


def _load_data() -> dict[str, Any]:
    """pricing_strategies.json 을 로드 (캐시).

    Raises:
        FileNotFoundError: 파일 누락 시.
        json.JSONDecodeError: JSON 파싱 실패 시.
    """
    global _cache
    if _cache is None:
        if not _DATA_PATH.exists():
            raise FileNotFoundError(
                f"pricing_strategies.json not found at {_DATA_PATH}"
            )
        with open(_DATA_PATH, encoding="utf-8") as f:
            _cache = json.load(f)
    return _cache


def _reset_cache() -> None:
    """테스트용: 캐시 초기화."""
    global _cache
    _cache = None


# ── Public API ──────────────────────────────────────────────────────────────


def get_strategy_meta(strategy_id: str) -> dict[str, Any]:
    """전략 메타데이터 반환 (마켓 무관).

    Args:
        strategy_id: 'lowest_price', 'normal_sale', 'cpc_ad' 중 하나.

    Returns:
        {"label_ko": str, "metric": str, "purpose": str}

    Raises:
        KeyError: 알 수 없는 strategy_id 인 경우.
    """
    data = _load_data()
    strategies = data["strategies"]
    if strategy_id not in strategies:
        available = sorted(strategies.keys())
        raise KeyError(
            f"strategy '{strategy_id}' not found in pricing_strategies.json "
            f"(available: {available})"
        )
    return dict(strategies[strategy_id])


def get_pricing_strategy(market_name: str, strategy_id: str) -> dict[str, Any]:
    """마켓 + 전략 머지 반환.

    전략 메타 (label_ko, metric, purpose) + 마켓별 파라미터
    (margin_c, min_h, max_h, round_h, bands) 를 합쳐 단일 dict 로 반환.

    Args:
        market_name: 마켓명 (예: '11번가').
        strategy_id: 'lowest_price', 'normal_sale', 'cpc_ad' 중 하나.

    Returns:
        {
            "strategy_id": str,
            "label_ko": str,
            "metric": str,
            "purpose": str,
            "margin_c": float,
            "min_h": float,
            "max_h": float,
            "round_h": int,
            "bands": list[tuple[float, float]],
        }

    Raises:
        KeyError: 알 수 없는 마켓 또는 전략인 경우.
    """
    data = _load_data()

    market_strategies = data["market_strategies"]
    if market_name not in market_strategies:
        available = sorted(market_strategies.keys())
        raise KeyError(
            f"market '{market_name}' not found in pricing_strategies.json "
            f"(available: {available})"
        )

    market_data = market_strategies[market_name]
    if strategy_id not in market_data:
        available = sorted(market_data.keys())
        raise KeyError(
            f"strategy '{strategy_id}' not found for market '{market_name}' "
            f"(available: {available})"
        )

    meta = get_strategy_meta(strategy_id)
    params = market_data[strategy_id]

    return {
        "strategy_id": strategy_id,
        "label_ko": meta["label_ko"],
        "metric": meta["metric"],
        "purpose": meta["purpose"],
        "margin_c": params["margin_c"],
        "min_h": params["min_h"],
        "max_h": params["max_h"],
        "round_h": params["round_h"],
        "bands": [tuple(b) for b in params["bands"]],
    }


def list_strategies() -> list[dict[str, Any]]:
    """모든 전략 메타 리스트 반환 (UI 드롭다운용).

    Returns:
        [{"id": str, "label_ko": str, "metric": str, "purpose": str}, ...]
    """
    data = _load_data()
    strategies = data["strategies"]
    return [
        {
            "id": sid,
            "label_ko": meta["label_ko"],
            "metric": meta["metric"],
            "purpose": meta["purpose"],
        }
        for sid, meta in strategies.items()
    ]


def list_market_strategies(market_name: str) -> list[str]:
    """특정 마켓이 지원하는 전략 ID 리스트 반환.

    Args:
        market_name: 마켓명.

    Returns:
        ['lowest_price', 'normal_sale', 'cpc_ad'] 같은 strategy_id 리스트.

    Raises:
        KeyError: 알 수 없는 마켓.
    """
    data = _load_data()
    market_strategies = data["market_strategies"]
    if market_name not in market_strategies:
        available = sorted(market_strategies.keys())
        raise KeyError(
            f"market '{market_name}' not found in pricing_strategies.json "
            f"(available: {available})"
        )
    return list(market_strategies[market_name].keys())
```

- [ ] **Step 4: 검증 스크립트 재실행 - 성공 확인**

Run: `PYTHONIOENCODING=utf-8 python _tmp_test_pricing_loader.py`

Expected:
```
T1 PASS: get_strategy_meta 3개
T2 PASS: get_pricing_strategy 11번가 3개 머지
T3 PASS: 11번가 lowest_price 구체값
T4 PASS: 11번가 normal_sale V 30~5
T5 PASS: list_strategies 3개
T6 PASS: list_market_strategies 11번가 3개
T7 PASS: KeyError on unknown market
T8 PASS: KeyError on unknown strategy
T9 PASS: get_strategy_meta KeyError
T10 PASS: list_market_strategies KeyError

ALL PASS
```

- [ ] **Step 5: 임시 파일 삭제 + Commit**

```bash
rm _tmp_test_pricing_loader.py
git add DB_save/pricing_strategies.py
git commit -m "feat(price): add pricing_strategies.py - JSON loader + getters"
```

---

### Task 5: pricing_strategies.py — smoke test 추가

**Files:**
- Modify: `DB_save/pricing_strategies.py` (append `__main__` block)

- [ ] **Step 1: smoke test 블록 추가**

`DB_save/pricing_strategies.py` 파일 끝에 추가:

```python


# ── Smoke Test ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys

    print("=" * 70)
    print("pricing_strategies.py — Smoke Test")
    print("=" * 70)

    all_ok = True

    # ── [1] get_strategy_meta ─────────────────────────────────────────────
    print("\n[1] get_strategy_meta - 3개 전략 메타")
    for sid in ["lowest_price", "normal_sale", "cpc_ad"]:
        try:
            meta = get_strategy_meta(sid)
            print(f"  {sid}: {meta['label_ko']} / metric={meta['metric']}")
        except Exception as exc:
            print(f"  {sid}: FAIL - {exc}")
            all_ok = False

    # ── [2] get_pricing_strategy ──────────────────────────────────────────
    print("\n[2] get_pricing_strategy - 11번가 3개 머지")
    for sid in ["lowest_price", "normal_sale", "cpc_ad"]:
        try:
            s = get_pricing_strategy("11번가", sid)
            print(
                f"  {sid}: C={s['margin_c']}, "
                f"max_h={s['max_h']}, bands={len(s['bands'])}단계, "
                f"metric={s['metric']}"
            )
        except Exception as exc:
            print(f"  {sid}: FAIL - {exc}")
            all_ok = False

    # ── [3] list_strategies / list_market_strategies ──────────────────────
    print("\n[3] list_strategies / list_market_strategies")
    try:
        all_strategies = list_strategies()
        print(f"  list_strategies: {len(all_strategies)}개 - "
              f"{[s['id'] for s in all_strategies]}")

        ids = list_market_strategies("11번가")
        print(f"  list_market_strategies('11번가'): {ids}")
    except Exception as exc:
        print(f"  FAIL - {exc}")
        all_ok = False

    # ── [4] 에러 케이스 ──────────────────────────────────────────────────
    print("\n[4] 에러 케이스")

    try:
        get_pricing_strategy("존재안함", "lowest_price")
        print("  unknown market: FAIL (no exception)")
        all_ok = False
    except KeyError as e:
        print(f"  unknown market → KeyError: {str(e)[:80]}")

    try:
        get_pricing_strategy("11번가", "wrong_strategy")
        print("  unknown strategy: FAIL (no exception)")
        all_ok = False
    except KeyError as e:
        print(f"  unknown strategy → KeyError: {str(e)[:80]}")

    # ── [5] 통합: price_engine.batch_solve 와 연동 ────────────────────────
    print("\n[5] 통합 - price_engine.batch_solve 연동")
    try:
        from DB_save.price_engine import batch_solve

        # 11번가 normal_sale 로 5개 상품 batch_solve
        strategy = get_pricing_strategy("11번가", "normal_sale")
        policy = {
            "commission_rate": 17.5,
            "commission_base": "pre_discount",
            "discount_rate": 50,
            "coupon_amount": 120,
            "reward_rate": 0,
        }
        products = [
            {"code": "S001", "cost_a": 500},
            {"code": "S002", "cost_a": 5000},
            {"code": "S003", "cost_a": 50000},
        ]
        results = batch_solve(
            products, policy, strategy["bands"],
            metric=strategy["metric"],
            margin_c=strategy["margin_c"],
            min_h=strategy["min_h"],
            max_h=strategy["max_h"],
            round_h=strategy["round_h"],
        )
        for r in results:
            print(
                f"  {r['code']}: A={r['cost_a']}, target_V={r['target_value']}%, "
                f"H={r['h']}, V={r.get('revenue_ratio', 'N/A')}%, "
                f"status={r['status']}"
            )
            if r['status'] != 'ok':
                all_ok = False
    except Exception as exc:
        print(f"  통합 FAIL: {exc}")
        all_ok = False

    # ── Summary ──────────────────────────────────────────────────────────
    print("\n" + "=" * 70)
    if all_ok:
        print("ALL CHECKS PASSED")
    else:
        print("SOME CHECKS FAILED - see above")
        sys.exit(1)
    print("=" * 70)
```

- [ ] **Step 2: smoke test 실행**

Run: `PYTHONIOENCODING=utf-8 python DB_save/pricing_strategies.py`

Expected:
```
======================================================================
pricing_strategies.py — Smoke Test
======================================================================

[1] get_strategy_meta - 3개 전략 메타
  lowest_price: 최저가전략 / metric=absolute_s
  normal_sale: 일반판매전략 / metric=v_percent
  cpc_ad: 광고전략 / metric=v_percent

[2] get_pricing_strategy - 11번가 3개 머지
  lowest_price: C=1.12, max_h=3.5, bands=7단계, metric=absolute_s
  normal_sale: C=2.5, max_h=4.0, bands=13단계, metric=v_percent
  cpc_ad: C=2.5, max_h=4.0, bands=13단계, metric=v_percent

[3] list_strategies / list_market_strategies
  list_strategies: 3개 - ['lowest_price', 'normal_sale', 'cpc_ad']
  list_market_strategies('11번가'): ['lowest_price', 'normal_sale', 'cpc_ad']

[4] 에러 케이스
  unknown market → KeyError: ...
  unknown strategy → KeyError: ...

[5] 통합 - price_engine.batch_solve 연동
  S001: A=500, target_V=22%, H=2.10, V=22.0%, status=ok
  S002: A=5000, target_V=15%, H=1.62, V=15.1%, status=ok
  S003: A=50000, target_V=8%, H=1.40, V=8.2%, status=ok

======================================================================
ALL CHECKS PASSED
======================================================================
```

- [ ] **Step 3: Commit**

```bash
git add DB_save/pricing_strategies.py
git commit -m "feat(price): add pricing_strategies.py smoke test + integration check"
```

---

### Task 6: 통합 검증 - 35개 상품 × 11번가 3-전략 batch_solve

**Files:**
- Create (temporary): `_tmp_integration_v.py`

35개 상품(가상 100~400원 4개 + 실측 30개) 에 대해 11번가의 3-전략 모두를 batch_solve 호출 → 모두 status="ok" 검증.

- [ ] **Step 1: 통합 검증 스크립트 작성**

`_tmp_integration_v.py`:

```python
# -*- coding: utf-8 -*-
"""11번가 3-전략 35개 상품 통합 검증."""
import sys
sys.path.insert(0, '.')
import openpyxl
from DB_save.pricing_strategies import get_pricing_strategy
from DB_save.price_engine import batch_solve

# 입력 로드: 가상 4 + 실측 30
virtual = [
    {'code': 'VIRT100', 'cost_a': 100},
    {'code': 'VIRT200', 'cost_a': 200},
    {'code': 'VIRT300', 'cost_a': 300},
    {'code': 'VIRT400', 'cost_a': 400},
]
wb = openpyxl.load_workbook('DB_save/price_compare_input_v2.xlsx', data_only=True)
ws = wb.active
real = []
for row in ws.iter_rows(min_row=2, values_only=True):
    if row[0] is None or row[1] is None:
        continue
    real.append({'code': str(row[0]), 'cost_a': float(row[1])})
wb.close()
products = virtual + real
print(f"Total products: {len(products)}")

policy = {
    'commission_rate': 17.5, 'commission_base': 'pre_discount',
    'discount_rate': 50, 'coupon_amount': 120, 'reward_rate': 0,
}

# 3-전략 모두 검증
all_ok = True
for sid in ['lowest_price', 'normal_sale', 'cpc_ad']:
    strategy = get_pricing_strategy('11번가', sid)
    print(f"\n=== {sid} ({strategy['label_ko']}) ===")
    print(f"metric={strategy['metric']}, C={strategy['margin_c']}, "
          f"max_h={strategy['max_h']}, bands={len(strategy['bands'])}단계")

    results = batch_solve(
        products, policy, strategy['bands'],
        metric=strategy['metric'],
        margin_c=strategy['margin_c'],
        min_h=strategy['min_h'],
        max_h=strategy['max_h'],
        round_h=strategy['round_h'],
    )

    not_ok = [r for r in results if r['status'] != 'ok']
    if not_ok:
        print(f"  FAIL: {len(not_ok)}개 not ok")
        for r in not_ok[:5]:
            print(f"    {r['code']}: status={r['status']}")
        all_ok = False
    else:
        print(f"  ALL {len(results)}개 status=ok")

    # V 또는 S 매칭 검증
    if strategy['metric'] == 'v_percent':
        mismatches = []
        for r in results:
            if r['status'] != 'ok':
                continue
            target_v = r['target_value']
            actual_v = r.get('revenue_ratio', 0)
            if abs(actual_v - target_v) > 0.5:
                mismatches.append((r['code'], target_v, actual_v))
        if mismatches:
            print(f"  V 매칭 FAIL: {len(mismatches)}개")
            for code, t, a in mismatches[:3]:
                print(f"    {code}: target={t}, actual={a}")
            all_ok = False
        else:
            print(f"  V 매칭 OK (±0.5% 이내)")

print()
if all_ok:
    print("=" * 60)
    print("INTEGRATION ALL PASS")
    print("=" * 60)
else:
    print("=" * 60)
    print("INTEGRATION FAILED")
    print("=" * 60)
    sys.exit(1)
```

- [ ] **Step 2: 통합 검증 실행**

Run: `PYTHONIOENCODING=utf-8 python _tmp_integration_v.py`

Expected:
```
Total products: 34

=== lowest_price (최저가전략) ===
metric=absolute_s, C=1.12, max_h=3.5, bands=7단계
  ALL 34개 status=ok

=== normal_sale (일반판매전략) ===
metric=v_percent, C=2.5, max_h=4.0, bands=13단계
  ALL 34개 status=ok
  V 매칭 OK (±0.5% 이내)

=== cpc_ad (광고전략) ===
metric=v_percent, C=2.5, max_h=4.0, bands=13단계
  ALL 34개 status=ok
  V 매칭 OK (±0.5% 이내)

============================================================
INTEGRATION ALL PASS
============================================================
```

만약 일부 행이 not ok 라면:
- `clamped_min` / `clamped_max` 발생 시 → 가격대별 H 한계 점검 후 spec 의 max_h 또는 min_h 조정 필요
- `error_v_unreachable` → V 밴드가 11번가 V max(65%) 초과 → spec 의 bands 수정 필요

- [ ] **Step 3: 임시 파일 삭제 + Commit**

```bash
rm _tmp_integration_v.py
git commit --allow-empty -m "test(price): integration verify - 34 products × 11번가 3 strategies"
```

(또는 검증만 하고 commit 안 함 — 검증은 plan 의 일부이지 코드 추가 아님)

---

## Self-Review Checklist

**Spec coverage (각 spec 요구사항 → 매핑된 task):**
- [x] `pricing_strategies.json` 파일 형식 → Task 3
- [x] `pricing_strategies.py` 모듈 (get_strategy_meta, get_pricing_strategy, list_*) → Task 4
- [x] 에러 처리 (FileNotFoundError, KeyError) → Task 4 (Step 3 의 함수 본체)
- [x] `solve_h_by_v` 신규 함수 → Task 1
- [x] `batch_solve` metric 인자 → Task 2
- [x] 11번가 3-전략 데이터 (lowest_price/normal_sale/cpc_ad) → Task 3
- [x] smoke test → Task 5
- [x] price_engine 통합 검증 → Task 5 (smoke test [5]) + Task 6 (전체 35개)
- [x] 회귀: 기존 batch_solve 호출 깨지지 않음 → Task 2 (Step 1 T1, T2)
- [x] 수학 derivation: pre/post_discount 모두 → Task 1 (Step 1 derivation + 함수 구현)

**Placeholder scan:** 없음. 모든 step 에 실제 코드/명령 포함.

**Type consistency:**
- `solve_h_by_v` 시그니처: spec 과 동일 (`cost_a, margin_c, policy, target_v_pct, ...`)
- `batch_solve` 신규 인자 `metric`: 모든 task 에서 동일 명명
- `get_pricing_strategy` 반환 키: `strategy_id, label_ko, metric, purpose, margin_c, min_h, max_h, round_h, bands` 일관
- 결과 dict 키: `target_value` 로 일반화 (이전 `target_s` 에서 변경, Task 2 에서 spec)

**Frequent commits:** 6 task 모두 commit 단계 포함.

**TDD:** 각 task 에 test 먼저 → 실패 확인 → 구현 → 성공 확인 → commit 사이클.
