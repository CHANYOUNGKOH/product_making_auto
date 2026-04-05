"""판매처 계약 정규화 - read-only policy interface.

sales_channels.json 을 읽어 마켓·스토어·오너 3계층 정책을 병합하여 반환한다.
데이터를 수정하지 않으며, 조회 전용(read-only) 인터페이스만 제공한다.

Public API:
    get_market_policy(market_name, json_path=None)
    get_store_policy(store_alias, json_path=None)
    list_active_stores(market=None, json_path=None)
    get_template(template_name, json_path=None)
"""

import json
import logging
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

# ── Constants ────────────────────────────────────────────────────────────────

DEFAULT_JSON_PATH = Path(__file__).resolve().parent / "sales_channels.json"


# ── Internal Helpers ─────────────────────────────────────────────────────────


def _load_data(json_path: str | Path | None = None) -> dict:
    """Load and parse sales_channels.json.

    Args:
        json_path: Path to JSON file. None -> DEFAULT_JSON_PATH.

    Returns:
        Parsed dict with keys: markets, owners, stores, templates.

    Raises:
        FileNotFoundError: JSON file does not exist.
        json.JSONDecodeError: JSON is malformed.
        ValueError: Required top-level keys are missing.
    """
    if json_path is None:
        json_path = DEFAULT_JSON_PATH
    json_path = Path(json_path)

    if not json_path.exists():
        raise FileNotFoundError(f"sales_channels.json not found: {json_path}")

    with open(json_path, encoding="utf-8") as f:
        data = json.load(f)

    required_keys = {"markets", "owners", "stores", "templates"}
    missing = required_keys - set(data.keys())
    if missing:
        raise ValueError(
            f"sales_channels.json missing required keys: {missing}"
        )

    _validate_store_refs(data)
    return data


def _validate_store_refs(data: dict) -> None:
    """Warn if any store references an unknown market or owner.

    Does not raise -- only emits warnings so callers can still proceed.
    """
    markets = data.get("markets", {})
    owners = data.get("owners", {})
    stores = data.get("stores", {})

    for alias, store in stores.items():
        market_ref = store.get("market")
        if market_ref and market_ref not in markets:
            logger.warning(
                "Store '%s' references unknown market '%s'", alias, market_ref
            )

        owner_ref = store.get("owner")
        if owner_ref and owner_ref not in owners:
            logger.warning(
                "Store '%s' references unknown owner '%s'", alias, owner_ref
            )


# ── Public API ───────────────────────────────────────────────────────────────


def get_market_policy(
    market_name: str, json_path: str | Path | None = None
) -> dict[str, Any]:
    """Return market-level policy dict.

    Args:
        market_name: Market key (e.g. '쿠팡', '11번가').
        json_path: Path to JSON file. None -> DEFAULT_JSON_PATH.

    Returns:
        dict with keys: commission_rate, commission_base,
        default_discount_rate, status.  May also include 'note'.

    Raises:
        KeyError: market_name not found in markets.
    """
    data = _load_data(json_path)
    markets = data["markets"]

    if market_name not in markets:
        available = ", ".join(sorted(markets.keys()))
        raise KeyError(
            f"Market '{market_name}' not found. "
            f"Available: {available}"
        )

    return dict(markets[market_name])


def _merge_store_policy(data: dict, store_alias: str) -> dict[str, Any]:
    """Merge 3-layer policy from pre-loaded data (internal helper).

    Used by both get_store_policy() and list_active_stores() to avoid
    redundant JSON re-reads.
    """
    stores = data["stores"]
    markets = data["markets"]
    owners = data["owners"]

    if store_alias not in stores:
        available = ", ".join(sorted(stores.keys()))
        raise KeyError(
            f"Store '{store_alias}' not found. "
            f"Available: {available}"
        )

    store = stores[store_alias]
    market_name = store["market"]
    owner_key = store["owner"]

    if market_name not in markets:
        raise KeyError(
            f"Store '{store_alias}' references unknown market '{market_name}'"
        )
    if owner_key not in owners:
        raise KeyError(
            f"Store '{store_alias}' references unknown owner '{owner_key}'"
        )

    market = markets[market_name]
    owner = owners[owner_key]

    # Merge: discount_rate
    discount_rate = store.get("discount_rate")
    if discount_rate is None:
        discount_rate = market.get("default_discount_rate")

    # Merge: shipping_strategy, shipping_fee
    shipping_strategy = store.get("shipping_strategy")
    if shipping_strategy is None:
        shipping_strategy = owner.get("default_shipping_strategy")

    shipping_fee = store.get("shipping_fee")
    if shipping_fee is None:
        shipping_fee = owner.get("default_shipping_fee")

    return {
        "store_alias": store_alias,
        "market": market_name,
        "owner": owner_key,
        "business_code": store.get("business_code"),
        "business_name": store.get("business_name"),
        "store_num": store.get("store_num"),
        "status": store.get("status"),
        "commission_rate": market["commission_rate"],
        "commission_base": market["commission_base"],
        "discount_rate": discount_rate,
        "shipping_strategy": shipping_strategy,
        "shipping_fee": shipping_fee,
    }


def get_store_policy(
    store_alias: str, json_path: str | Path | None = None
) -> dict[str, Any]:
    """Return merged 3-layer policy for a specific store.

    Merge rules:
        - commission_rate, commission_base: always from market (no store override)
        - discount_rate: store value if not null, else market's default_discount_rate
        - shipping_strategy, shipping_fee: store value if not null, else owner defaults

    Args:
        store_alias: Store key (e.g. '쿠팡A2-1').
        json_path: Path to JSON file. None -> DEFAULT_JSON_PATH.

    Returns:
        dict with keys: store_alias, market, owner, business_code,
        business_name, store_num, status, commission_rate, commission_base,
        discount_rate, shipping_strategy, shipping_fee.

    Raises:
        KeyError: store_alias not found, or referenced market/owner missing.
    """
    data = _load_data(json_path)
    return _merge_store_policy(data, store_alias)


def list_active_stores(
    market: str | None = None, json_path: str | Path | None = None
) -> list[dict[str, Any]]:
    """Return merged policy dicts for all active stores.

    Args:
        market: If provided, filter to only this market's stores.
        json_path: Path to JSON file. None -> DEFAULT_JSON_PATH.

    Returns:
        List of merged policy dicts (same shape as get_store_policy),
        sorted by store_alias. Only stores with status == 'active'.
    """
    data = _load_data(json_path)
    stores = data["stores"]

    results = []
    for alias, store in stores.items():
        if store.get("status") != "active":
            continue
        if market is not None and store.get("market") != market:
            continue
        try:
            results.append(_merge_store_policy(data, alias))
        except KeyError as exc:
            logger.warning("Skipping store '%s': %s", alias, exc)

    results.sort(key=lambda p: p["store_alias"])
    return results


def get_template(
    template_name: str, json_path: str | Path | None = None
) -> dict[str, Any]:
    """Return a named template dict.

    Args:
        template_name: Template key (e.g. 'owner_A_default').
        json_path: Path to JSON file. None -> DEFAULT_JSON_PATH.

    Returns:
        dict with template fields (e.g. shipping_strategy, shipping_fee).

    Raises:
        KeyError: template_name not found in templates.
    """
    data = _load_data(json_path)
    templates = data["templates"]

    if template_name not in templates:
        available = ", ".join(sorted(templates.keys()))
        raise KeyError(
            f"Template '{template_name}' not found. "
            f"Available: {available}"
        )

    return dict(templates[template_name])


# ── Smoke Test ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    """Smoke test - sales_channels.json 로드 및 정책 조회 검증."""
    import sys

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    print("=" * 60)
    print("판매처 계약 정규화 - Smoke Test")
    print(f"JSON: {DEFAULT_JSON_PATH}")
    print("=" * 60)

    # 1. Load data
    try:
        data = _load_data()
        print(f"\n[1] JSON 로드 성공")
        print(f"  Markets: {len(data['markets'])}개")
        print(f"  Owners:  {len(data['owners'])}개")
        print(f"  Stores:  {len(data['stores'])}개")
        print(f"  Templates: {len(data['templates'])}개")
    except Exception as exc:
        print(f"\n[1] JSON 로드 실패: {exc}")
        sys.exit(1)

    # 2. get_market_policy
    print(f"\n[2] get_market_policy 테스트")
    for name in ["쿠팡", "스마트스토어", "고도몰"]:
        policy = get_market_policy(name)
        print(f"  {name}: 수수료={policy['commission_rate']}%, "
              f"기준={policy['commission_base']}, "
              f"기본할인={policy['default_discount_rate']}")

    # 3. get_store_policy
    print(f"\n[3] get_store_policy 테스트")
    for alias in data["stores"]:
        policy = get_store_policy(alias)
        print(f"  {alias}: 수수료={policy['commission_rate']}%, "
              f"할인={policy['discount_rate']}%, "
              f"배송={policy['shipping_strategy']}({policy['shipping_fee']}원)")

    # 4. list_active_stores
    print(f"\n[4] list_active_stores 테스트")
    active = list_active_stores()
    print(f"  Active stores: {len(active)}개")
    for p in active:
        print(f"    {p['store_alias']} ({p['market']}, {p['owner']})")

    # 5. get_template
    print(f"\n[5] get_template 테스트")
    for name in data["templates"]:
        tmpl = get_template(name)
        print(f"  {name}: {tmpl}")

    # 6. Error handling
    print(f"\n[6] 에러 처리 테스트")
    try:
        get_market_policy("없는마켓")
        print("  FAIL: KeyError expected")
    except KeyError as exc:
        print(f"  OK: {exc}")

    try:
        get_store_policy("없는스토어")
        print("  FAIL: KeyError expected")
    except KeyError as exc:
        print(f"  OK: {exc}")

    try:
        get_template("없는템플릿")
        print("  FAIL: KeyError expected")
    except KeyError as exc:
        print(f"  OK: {exc}")

    print("\n" + "=" * 60)
    print("Smoke test complete!")
    print("=" * 60)
