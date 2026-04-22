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

    result = {
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
    if "discount_bands" in params:
        result["discount_bands"] = [tuple(b) for b in params["discount_bands"]]
    if "round_mode" in params:
        result["round_mode"] = params["round_mode"]
    return result


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


# ── Smoke Test ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys

    print("=" * 70)
    print("pricing_strategies.py - Smoke Test")
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
        print(f"  unknown market -> KeyError: {str(e)[:80]}")

    try:
        get_pricing_strategy("11번가", "wrong_strategy")
        print("  unknown strategy: FAIL (no exception)")
        all_ok = False
    except KeyError as e:
        print(f"  unknown strategy -> KeyError: {str(e)[:80]}")

    try:
        get_strategy_meta("wrong")
        print("  get_strategy_meta wrong: FAIL")
        all_ok = False
    except KeyError:
        print("  get_strategy_meta('wrong') -> KeyError OK")

    try:
        list_market_strategies("존재안함")
        print("  list_market_strategies wrong: FAIL")
        all_ok = False
    except KeyError:
        print("  list_market_strategies('존재안함') -> KeyError OK")

    # ── [5] 통합: price_engine.batch_solve 와 연동 ────────────────────────
    print("\n[5] 통합 - price_engine.batch_solve 연동")
    try:
        try:
            from DB_save.price_engine import batch_solve
        except ImportError:
            from price_engine import batch_solve

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
