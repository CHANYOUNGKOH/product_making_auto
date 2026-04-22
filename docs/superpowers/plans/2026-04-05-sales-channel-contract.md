# 판매처 계약 정규화 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 흩어진 판매채널 정책 데이터를 단일 JSON + 읽기 함수로 중앙 관리

**Architecture:** `sales_channels.json`에 3계층(마켓→명의자→스토어) 정책 데이터를 저장하고, `sales_channel_policy.py`가 상속 규칙을 적용하여 병합된 정책을 반환하는 읽기 전용 인터페이스 제공. 기존 `store_memos.json`과 `config.py`에서 초기 스토어 목록을 자동 추출.

**Tech Stack:** Python 3, json (표준 라이브러리), logging, pathlib

**Spec:** `docs/superpowers/specs/2026-04-05-sales-channel-contract-design.md`

---

## File Structure

| 파일 | 역할 | 작업 |
|------|------|------|
| `DB_save/sales_channels.json` | 3계층 정책 데이터 (markets, owners, stores, templates) | **신규 생성** |
| `DB_save/sales_channel_policy.py` | 읽기 함수 인터페이스 (계약) | **신규 생성** |

기존 파일 (`config.py`, `store_memos.json`) 은 수정하지 않음 — 다른 모듈이 의존 중.

---

### Task 1: sales_channels.json 생성 — markets + owners + templates

`store_memos.json`에서 스토어를 추출하기 전에, 먼저 확정된 마켓 수수료 데이터와 명의자/템플릿을 작성한다.

**Files:**
- Create: `DB_save/sales_channels.json`

- [ ] **Step 1: JSON 파일 생성 — markets 섹션**

```json
{
  "markets": {
    "쿠팡": {
      "commission_rate": 11.5,
      "commission_base": "pre_discount",
      "default_discount_rate": 2,
      "status": "pending"
    },
    "11번가": {
      "commission_rate": 17.5,
      "commission_base": "pre_discount",
      "default_discount_rate": 2,
      "status": "registering"
    },
    "옥션": {
      "commission_rate": 16.5,
      "commission_base": "pre_discount",
      "default_discount_rate": 2,
      "status": "registering",
      "note": "스토어별 2% 또는 61% 선택 가능"
    },
    "지마켓": {
      "commission_rate": 16.5,
      "commission_base": "pre_discount",
      "default_discount_rate": 2,
      "status": "registering",
      "note": "스토어별 2% 또는 61% 선택 가능"
    },
    "스마트스토어": {
      "commission_rate": 6.5,
      "commission_base": "post_discount",
      "default_discount_rate": 50,
      "status": "registering",
      "note": "할인율 70% 미만 유지 필요 (노출 제한 위험). 옵션가격이 판매가 연동 → 옵션으로 마진 확보 전략"
    },
    "롯데온": {
      "commission_rate": 16.5,
      "commission_base": "pre_discount",
      "default_discount_rate": 2,
      "status": "planned"
    },
    "카카오톡스토어": {
      "commission_rate": 10.0,
      "commission_base": "pre_discount",
      "default_discount_rate": 2,
      "status": "planned"
    },
    "올웨이즈": {
      "commission_rate": 12.0,
      "commission_base": "pre_discount",
      "default_discount_rate": 2,
      "status": "planned"
    },
    "에이블리": {
      "commission_rate": 18.0,
      "commission_base": "pre_discount",
      "default_discount_rate": 2,
      "status": "planned"
    },
    "토스쇼핑": {
      "commission_rate": 15.0,
      "commission_base": "pre_discount",
      "default_discount_rate": 2,
      "status": "planned"
    },
    "멸치쇼핑": {
      "commission_rate": 16.5,
      "commission_base": "pre_discount",
      "default_discount_rate": 2,
      "status": "planned"
    },
    "오늘의집": {
      "commission_rate": 20.0,
      "commission_base": "pre_discount",
      "default_discount_rate": 2,
      "status": "planned"
    },
    "고도몰": {
      "commission_rate": 4.0,
      "commission_base": "post_discount",
      "default_discount_rate": null,
      "status": "pending",
      "note": "할인율 개념 없음. 실결제금액 기준 수수료. 자체쿠폰정책: 가격대별 4단계, 위탁주문가 이상만 쿠폰 발행"
    }
  },

  "owners": {
    "A": {
      "name": "고찬영",
      "default_shipping_strategy": "paid",
      "default_shipping_fee": 3000
    },
    "B": {
      "name": "최빛나",
      "default_shipping_strategy": "free",
      "default_shipping_fee": 0
    }
  },

  "stores": {},

  "templates": {
    "owner_A_default": {
      "shipping_strategy": "paid",
      "shipping_fee": 3000
    },
    "owner_B_default": {
      "shipping_strategy": "free",
      "shipping_fee": 0
    }
  }
}
```

- [ ] **Step 2: JSON 유효성 확인**

Run: `python -c "import json; json.load(open('DB_save/sales_channels.json', encoding='utf-8')); print('OK')"`
Expected: `OK`

- [ ] **Step 3: Commit**

```bash
git add DB_save/sales_channels.json
git commit -m "feat: sales_channels.json 생성 — markets/owners/templates 초기 데이터"
```

---

### Task 2: sales_channel_policy.py — JSON 로드 + get_market_policy()

첫 번째 함수: JSON을 로드하고 마켓 정책을 반환한다.

**Files:**
- Create: `DB_save/sales_channel_policy.py`

- [ ] **Step 1: 모듈 생성 — 상수, 로드, get_market_policy**

```python
"""판매처 계약 정규화 — 읽기 전용 정책 인터페이스.

sales_channels.json에서 3계층(마켓→명의자→스토어) 정책을 로드하고,
상속 규칙을 적용하여 병합된 정책을 반환한다.
"""

import json
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

# ── Constants ────────────────────────────────────────────────────────────────

DEFAULT_JSON_PATH = Path(__file__).resolve().parent / "sales_channels.json"


# ── Internal: Load ───────────────────────────────────────────────────────────

def _load_data(json_path: str | Path | None = None) -> dict:
    """Load and return the raw sales_channels.json data.

    Raises FileNotFoundError if the file doesn't exist.
    Raises json.JSONDecodeError if the file is malformed.
    """
    path = Path(json_path) if json_path else DEFAULT_JSON_PATH
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _validate_store_refs(data: dict) -> None:
    """Warn if any store references a non-existent market or owner."""
    markets = data.get("markets", {})
    owners = data.get("owners", {})
    for alias, store in data.get("stores", {}).items():
        if store.get("market") and store["market"] not in markets:
            logger.warning(
                "Store %s references unknown market: %s", alias, store["market"]
            )
        if store.get("owner") and store["owner"] not in owners:
            logger.warning(
                "Store %s references unknown owner: %s", alias, store["owner"]
            )


# ── Public API ───────────────────────────────────────────────────────────────


def get_market_policy(market_name: str, json_path: str | Path | None = None) -> dict:
    """Return policy for a single market.

    Args:
        market_name: Market name (e.g. "쿠팡", "스마트스토어")
        json_path: Path to sales_channels.json. None → default.

    Returns:
        dict with keys: commission_rate, commission_base, default_discount_rate, status
        Plus optional: note

    Raises:
        KeyError: if market_name not found
    """
    data = _load_data(json_path)
    markets = data.get("markets", {})
    if market_name not in markets:
        raise KeyError(f"Unknown market: {market_name!r}")
    return dict(markets[market_name])
```

- [ ] **Step 2: 스모크 테스트 실행**

Run: `python -c "from DB_save.sales_channel_policy import get_market_policy; p = get_market_policy('쿠팡'); print(p); assert p['commission_rate'] == 11.5; print('OK')"`
Expected: `{'commission_rate': 11.5, 'commission_base': 'pre_discount', ...}` then `OK`

- [ ] **Step 3: Commit**

```bash
git add DB_save/sales_channel_policy.py
git commit -m "feat: sales_channel_policy.py — JSON 로드 + get_market_policy()"
```

---

### Task 3: get_store_policy() — 3계층 병합

핵심 함수: 마켓 정책 + 명의자 기본값 + 스토어 오버라이드를 병합하여 반환.

**Files:**
- Modify: `DB_save/sales_channel_policy.py`

- [ ] **Step 1: get_store_policy 함수 추가**

`sales_channel_policy.py`의 Public API 섹션에 추가:

```python
def get_store_policy(store_alias: str, json_path: str | Path | None = None) -> dict:
    """Return merged policy for a store (market + owner defaults + store overrides).

    Inheritance rules:
    - commission_rate, commission_base: always from market (no store override)
    - discount_rate: store value if not null, else market default_discount_rate
    - shipping_strategy, shipping_fee: store value if not null, else owner defaults

    Args:
        store_alias: Store alias (e.g. "쿠팡A2-1", "옥션B1-0")
        json_path: Path to sales_channels.json. None → default.

    Returns:
        dict with all merged fields:
        store_alias, market, owner, business_code, business_name, store_num,
        status, commission_rate, commission_base, discount_rate,
        shipping_strategy, shipping_fee

    Raises:
        KeyError: if store_alias not found
    """
    data = _load_data(json_path)
    stores = data.get("stores", {})
    if store_alias not in stores:
        raise KeyError(f"Unknown store: {store_alias!r}")

    store = stores[store_alias]
    market_name = store.get("market", "")
    owner_key = store.get("owner", "")

    # Market policy
    markets = data.get("markets", {})
    market = markets.get(market_name, {})

    # Owner defaults
    owners = data.get("owners", {})
    owner = owners.get(owner_key, {})

    # Merge: market fields (no override)
    result = {
        "store_alias": store_alias,
        "market": market_name,
        "owner": owner_key,
        "business_code": store.get("business_code", ""),
        "business_name": store.get("business_name", ""),
        "store_num": store.get("store_num", ""),
        "status": store.get("status", "active"),
        # From market (no store override)
        "commission_rate": market.get("commission_rate", 0),
        "commission_base": market.get("commission_base", "pre_discount"),
        # Discount: store override → market default
        "discount_rate": (
            store["discount_rate"]
            if store.get("discount_rate") is not None
            else market.get("default_discount_rate")
        ),
        # Shipping: store override → owner default
        "shipping_strategy": (
            store["shipping_strategy"]
            if store.get("shipping_strategy") is not None
            else owner.get("default_shipping_strategy", "paid")
        ),
        "shipping_fee": (
            store["shipping_fee"]
            if store.get("shipping_fee") is not None
            else owner.get("default_shipping_fee", 0)
        ),
    }

    return result
```

- [ ] **Step 2: 테스트를 위한 임시 스토어 추가**

`sales_channels.json`의 `"stores": {}` 에 테스트용 스토어 1개 추가:

```json
"stores": {
    "쿠팡A2-1": {
        "market": "쿠팡",
        "owner": "A",
        "business_code": "A2",
        "business_name": "마이유통",
        "store_num": "1",
        "status": "active",
        "shipping_strategy": null,
        "shipping_fee": null,
        "discount_rate": null
    }
}
```

- [ ] **Step 3: 스모크 테스트 — 병합 확인**

Run: `python -c "from DB_save.sales_channel_policy import get_store_policy; p = get_store_policy('쿠팡A2-1'); print(p); assert p['commission_rate'] == 11.5; assert p['shipping_strategy'] == 'paid'; assert p['shipping_fee'] == 3000; assert p['discount_rate'] == 2; print('OK')"`

Expected: 마켓(수수료 11.5) + 명의자A(유료배송 3000) + 마켓(할인율 2) 병합 확인 후 `OK`

- [ ] **Step 4: Commit**

```bash
git add DB_save/sales_channel_policy.py DB_save/sales_channels.json
git commit -m "feat: get_store_policy() — 3계층 정책 병합 (마켓+명의자+스토어)"
```

---

### Task 4: list_active_stores() + get_template()

나머지 공개 함수 2개.

**Files:**
- Modify: `DB_save/sales_channel_policy.py`

- [ ] **Step 1: list_active_stores 함수 추가**

`sales_channel_policy.py`의 Public API 섹션에 추가:

```python
def list_active_stores(
    market: str | None = None, json_path: str | Path | None = None
) -> list[dict]:
    """Return list of merged policies for all active stores.

    Args:
        market: Filter by market name. None → all markets.
        json_path: Path to sales_channels.json. None → default.

    Returns:
        List of dicts (same structure as get_store_policy output),
        filtered to status == "active", sorted by store_alias.
    """
    data = _load_data(json_path)
    stores = data.get("stores", {})

    result = []
    for alias, store in sorted(stores.items()):
        if store.get("status") != "active":
            continue
        if market and store.get("market") != market:
            continue
        try:
            result.append(get_store_policy(alias, json_path))
        except KeyError:
            logger.warning("Skipping store with bad references: %s", alias)
    return result
```

- [ ] **Step 2: get_template 함수 추가**

```python
def get_template(template_name: str, json_path: str | Path | None = None) -> dict:
    """Return a template for creating new stores.

    Args:
        template_name: Template name (e.g. "owner_A_default")
        json_path: Path to sales_channels.json. None → default.

    Returns:
        dict with template fields (shipping_strategy, shipping_fee)

    Raises:
        KeyError: if template_name not found
    """
    data = _load_data(json_path)
    templates = data.get("templates", {})
    if template_name not in templates:
        raise KeyError(f"Unknown template: {template_name!r}")
    return dict(templates[template_name])
```

- [ ] **Step 3: 스모크 테스트**

Run:
```bash
python -c "
from DB_save.sales_channel_policy import list_active_stores, get_template
stores = list_active_stores()
print(f'Active stores: {len(stores)}')
for s in stores:
    print(f'  {s[\"store_alias\"]}: {s[\"market\"]} / {s[\"shipping_strategy\"]} / discount={s[\"discount_rate\"]}')
t = get_template('owner_A_default')
print(f'Template: {t}')
assert t['shipping_strategy'] == 'paid'
assert t['shipping_fee'] == 3000
print('OK')
"
```

Expected: Active stores 목록 + Template 확인 + `OK`

- [ ] **Step 4: Commit**

```bash
git add DB_save/sales_channel_policy.py
git commit -m "feat: list_active_stores() + get_template() 추가"
```

---

### Task 5: 초기 스토어 데이터 추출 스크립트

`store_memos.json` + `config.py`에서 기존 스토어 목록을 자동 추출하여 `sales_channels.json`의 stores 섹션을 채운다. 일회성 스크립트로 실행 후 결과를 JSON에 반영.

**Files:**
- Create: `DB_save/_populate_stores.py` (일회성 스크립트)
- Modify: `DB_save/sales_channels.json` (stores 섹션 채움)

- [ ] **Step 1: 추출 스크립트 작성**

```python
"""일회성 스크립트: store_memos.json에서 스토어 목록 추출 → sales_channels.json 반영.

store_memos.json 키 형식: "시트명::명의자::사업자번호::별칭"
예: "ESM_Auction_info::A::2::옥션A2-1"

시트명 → 마켓명 매핑은 config.py의 SHEET_DISPLAY_NAMES 사용.
사업자코드 = 명의자 + 사업자번호 (예: "A2")
상호명 = config.py의 BUSINESS_NAMES[사업자코드]
"""

import json
import re
import sys
from pathlib import Path

# Paths
BASE = Path(__file__).resolve().parent
MEMOS_PATH = BASE / "store_memos.json"
CHANNELS_PATH = BASE / "sales_channels.json"

# 시트명 → 마켓명 매핑 (config.py의 SHEET_DISPLAY_NAMES에서 소매처만)
SHEET_TO_MARKET = {
    "ESM_Auction_info": "옥션",
    "ESM_Gmarket_info": "지마켓",
    "smartstore_info": "스마트스토어",
    "elevenst_info": "11번가",
    "godomall_info": "고도몰",
    "coupang_info": "쿠팡",
    "allways_info": "올웨이즈",
    "kakao_info": "카카오톡스토어",
    "lotteon_info": "롯데온",
    "ably_info": "에이블리",
    "toss_info": "토스쇼핑",
    "smelchi_info": "멸치쇼핑",
    "ohous_info": "오늘의집",
}

# 사업자코드 → 상호명 (config.py에서 복사)
BUSINESS_NAMES = {
    "A1": "굿투굿", "A2": "마이유통", "A3": "마이비타민",
    "A4": "러블리몰", "A5": "럽미몰",
    "B1": "마이짐", "B2": "샤이닝몰", "B3": "샤인몰",
    "B4": "순수한노을", "B5": "포근한정원", "B6": "푸르른초원",
    "B7": "행복한저택",
}


def parse_memo_key(key: str) -> dict | None:
    """Parse 'sheet::owner::biz_num::alias' → dict or None."""
    parts = key.split("::")
    if len(parts) != 4:
        return None
    sheet, owner, biz_num, alias = parts
    market = SHEET_TO_MARKET.get(sheet)
    if not market:
        return None

    # alias에서 store_num 추출: "옥션A2-1" → "1", "쿠팡A2" → "0"
    match = re.search(r'-(\d+)$', alias)
    store_num = match.group(1) if match else "0"

    biz_code = f"{owner}{biz_num}"

    return {
        "alias": alias,
        "market": market,
        "owner": owner,
        "business_code": biz_code,
        "business_name": BUSINESS_NAMES.get(biz_code, ""),
        "store_num": store_num,
    }


def extract_discount_from_memo(memo: str) -> float | None:
    """Extract discount rate from memo string like '할2_3000_스0.5' → 2.0."""
    match = re.match(r'할(\d+)', memo)
    if match:
        return float(match.group(1))
    return None


def extract_shipping_from_memo(memo: str) -> tuple[str | None, int | None]:
    """Extract shipping strategy + fee from memo.

    '할2_3000_스0.5'    → ('paid', 3000)
    '할61_무배_스0.5'   → ('free', 0)
    '할2_배3000_스0.5'  → ('paid', 3000)
    ''                  → (None, None)
    """
    if not memo:
        return None, None
    if '무배' in memo:
        return 'free', 0
    # 배N or just a number
    match = re.search(r'(?:배)?(\d{3,})', memo)
    if match:
        fee = int(match.group(1))
        return 'paid', fee
    return None, None


def main():
    # Load existing data
    with open(MEMOS_PATH, "r", encoding="utf-8") as f:
        memos = json.load(f)

    with open(CHANNELS_PATH, "r", encoding="utf-8") as f:
        channels = json.load(f)

    stores = {}
    for key, val in sorted(memos.items()):
        parsed = parse_memo_key(key)
        if not parsed:
            continue

        alias = parsed["alias"]
        memo = val.get("memo", "")

        # Extract policy from memo (auction/gmarket have policy memos)
        discount = extract_discount_from_memo(memo)
        ship_strategy, ship_fee = extract_shipping_from_memo(memo)

        stores[alias] = {
            "market": parsed["market"],
            "owner": parsed["owner"],
            "business_code": parsed["business_code"],
            "business_name": parsed["business_name"],
            "store_num": parsed["store_num"],
            "status": "active",
            "shipping_strategy": ship_strategy,
            "shipping_fee": ship_fee,
            "discount_rate": discount,
        }

    # Update channels
    channels["stores"] = stores

    with open(CHANNELS_PATH, "w", encoding="utf-8") as f:
        json.dump(channels, f, ensure_ascii=False, indent=2)

    print(f"Populated {len(stores)} stores into sales_channels.json")
    # Summary per market
    by_market = {}
    for s in stores.values():
        m = s["market"]
        by_market[m] = by_market.get(m, 0) + 1
    for m, c in sorted(by_market.items()):
        print(f"  {m}: {c}")


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: 스크립트 실행**

Run: `cd DB_save && python _populate_stores.py`

Expected output:
```
Populated 70 stores into sales_channels.json
  11번가: 5
  고도몰: 8
  스마트스토어: 21
  옥션: 19
  지마켓: 17
```

- [ ] **Step 3: 결과 확인 — JSON이 유효하고 스토어가 채워졌는지**

Run: `python -c "import json; d = json.load(open('DB_save/sales_channels.json', encoding='utf-8')); print(f'stores: {len(d[\"stores\"])}'); print(list(d['stores'].keys())[:5])"`

Expected: `stores: 70` + 처음 5개 스토어 별칭 출력

- [ ] **Step 4: get_store_policy로 병합 결과 검증**

Run:
```bash
python -c "
from DB_save.sales_channel_policy import get_store_policy
# 옥션 A 스토어: 할인 2%, 유료배송 3000
p = get_store_policy('옥션A2-1')
print(f'옥션A2-1: discount={p[\"discount_rate\"]}, ship={p[\"shipping_strategy\"]}({p[\"shipping_fee\"]}), commission={p[\"commission_rate\"]}%')
assert p['discount_rate'] == 2
assert p['shipping_fee'] == 3000

# 옥션 B 스토어: 할인 61%, 무배
p2 = get_store_policy('옥션B1-0')
print(f'옥션B1-0: discount={p2[\"discount_rate\"]}, ship={p2[\"shipping_strategy\"]}({p2[\"shipping_fee\"]})')
assert p2['discount_rate'] == 61
assert p2['shipping_strategy'] == 'free'

# 스마트스토어: memo에 정책 없음 → 마켓 기본 할인 50%, 명의자 기본 배송
p3 = get_store_policy('스스A1-0')
print(f'스스A1-0: discount={p3[\"discount_rate\"]}, ship={p3[\"shipping_strategy\"]}({p3[\"shipping_fee\"]})')
assert p3['discount_rate'] == 50
assert p3['shipping_strategy'] == 'paid'

print('OK')
"
```

Expected: 3가지 시나리오 병합 결과 + `OK`

- [ ] **Step 5: Commit**

```bash
git add DB_save/sales_channels.json DB_save/_populate_stores.py
git commit -m "feat: store_memos.json에서 70개 스토어 초기 데이터 자동 추출"
```

---

### Task 6: __main__ 스모크 테스트 블록

`sales_channel_policy.py`에 종합 스모크 테스트 블록 추가.

**Files:**
- Modify: `DB_save/sales_channel_policy.py`

- [ ] **Step 1: __main__ 블록 추가**

`sales_channel_policy.py` 맨 아래에 추가:

```python
if __name__ == "__main__":
    """Smoke test — sales_channels.json 로드 및 전체 함수 검증"""
    import sys

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    print("=" * 60)
    print("판매처 계약 정규화 Smoke Test")
    print("=" * 60)

    # 1. Load + validate
    print("\n[1] JSON 로드")
    try:
        data = _load_data()
        _validate_store_refs(data)
    except Exception as e:
        print(f"  FAIL: {e}")
        sys.exit(1)
    print(f"  Markets: {len(data['markets'])}")
    print(f"  Owners: {len(data['owners'])}")
    print(f"  Stores: {len(data['stores'])}")
    print(f"  Templates: {len(data['templates'])}")

    # 2. get_market_policy
    print("\n[2] get_market_policy")
    for name in ["쿠팡", "스마트스토어", "고도몰"]:
        p = get_market_policy(name)
        print(f"  {name}: {p['commission_rate']}% ({p['commission_base']}), discount={p['default_discount_rate']}")

    # 3. get_store_policy — 병합 확인
    print("\n[3] get_store_policy (병합 확인)")
    test_aliases = list(data["stores"].keys())[:5]
    for alias in test_aliases:
        p = get_store_policy(alias)
        print(
            f"  {alias}: commission={p['commission_rate']}%, "
            f"discount={p['discount_rate']}, "
            f"ship={p['shipping_strategy']}({p['shipping_fee']})"
        )

    # 4. list_active_stores
    print("\n[4] list_active_stores")
    all_active = list_active_stores()
    print(f"  전체 active: {len(all_active)}")
    # Per-market count
    market_counts = {}
    for s in all_active:
        m = s["market"]
        market_counts[m] = market_counts.get(m, 0) + 1
    for m, c in sorted(market_counts.items()):
        print(f"    {m}: {c}")

    # 5. get_template
    print("\n[5] get_template")
    for tname in data.get("templates", {}):
        t = get_template(tname)
        print(f"  {tname}: {t}")

    # 6. Error handling
    print("\n[6] 에러 처리 확인")
    try:
        get_market_policy("존재하지않는마켓")
        print("  FAIL: should have raised KeyError")
    except KeyError as e:
        print(f"  OK: get_market_policy 미존재 → {e}")

    try:
        get_store_policy("존재하지않는스토어")
        print("  FAIL: should have raised KeyError")
    except KeyError as e:
        print(f"  OK: get_store_policy 미존재 → {e}")

    try:
        get_template("존재하지않는템플릿")
        print("  FAIL: should have raised KeyError")
    except KeyError as e:
        print(f"  OK: get_template 미존재 → {e}")

    print("\n" + "=" * 60)
    print("Smoke test complete!")
    print("=" * 60)
```

- [ ] **Step 2: 스모크 테스트 실행**

Run: `python -m DB_save.sales_channel_policy`

Expected:
```
============================================================
판매처 계약 정규화 Smoke Test
============================================================

[1] JSON 로드
  Markets: 13
  Owners: 2
  Stores: 70
  Templates: 2

[2] get_market_policy
  쿠팡: 11.5% (pre_discount), discount=2
  스마트스토어: 6.5% (post_discount), discount=50
  고도몰: 4.0% (post_discount), discount=None

[3] get_store_policy (병합 확인)
  ...5개 스토어의 병합된 정책...

[4] list_active_stores
  전체 active: 70
  ...마켓별 카운트...

[5] get_template
  owner_A_default: {'shipping_strategy': 'paid', 'shipping_fee': 3000}
  owner_B_default: {'shipping_strategy': 'free', 'shipping_fee': 0}

[6] 에러 처리 확인
  OK: get_market_policy 미존재 → ...
  OK: get_store_policy 미존재 → ...
  OK: get_template 미존재 → ...

============================================================
Smoke test complete!
============================================================
```

- [ ] **Step 3: Commit**

```bash
git add DB_save/sales_channel_policy.py
git commit -m "feat: __main__ 스모크 테스트 — 전체 함수 종합 검증"
```

---

## Self-Review Checklist

**1. Spec coverage:**
- [x] markets 데이터 (13개 마켓, 수수료율/기준/할인율/상태) → Task 1
- [x] owners 데이터 (A/B 명의자, 배송전략/배송비) → Task 1
- [x] stores 데이터 (store_memos.json에서 추출) → Task 5
- [x] templates (명의자별 기본값) → Task 1
- [x] get_market_policy() → Task 2
- [x] get_store_policy() 병합 로직 → Task 3
- [x] list_active_stores() → Task 4
- [x] get_template() → Task 4
- [x] 상속 규칙 (할인율→마켓, 배송→명의자) → Task 3
- [x] 에러 처리 (KeyError, FileNotFoundError) → Task 2, 6
- [x] 참조 검증 (_validate_store_refs) → Task 2
- [x] 스모크 테스트 → Task 6

**2. Placeholder scan:** No TBD/TODO found. All code blocks complete.

**3. Type consistency:**
- `get_market_policy` → returns dict ✓
- `get_store_policy` → returns dict with consistent field names ✓
- `list_active_stores` → returns list[dict] ✓
- `get_template` → returns dict ✓
- `_load_data` used consistently across all functions ✓
- `json_path` parameter consistent across all functions ✓
