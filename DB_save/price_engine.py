"""가격계산 엔진 -- 순수 Python 대수 계산.

엑셀/xlwings GoalSeek 없이 마켓별 정책 기반 가격 계산.
현재 지원: 11번가 (pre_discount + post_discount 공식 모두 구현)

Public API:
    forward_calc(cost_a, margin_c, h, policy, shipping_absorbed=0)
    solve_h(cost_a, margin_c, policy, target_s, shipping_absorbed=0, ...)
    batch_solve(products, policy, target_bands, metric="absolute_s", margin_c=1.12, ...)
"""

from __future__ import annotations

import math
from typing import Any

# ── Internal Helpers ─────────────────────────────────────────────────────────


def _extract_policy_params(policy: dict[str, Any]) -> tuple[float, float, float, float, str]:
    """Extract and normalise policy parameters.

    Returns:
        (fee_rate, J, E, P, commission_base)
        where fee_rate / J / P are already divided by 100.
    """
    fee_rate = policy["commission_rate"] / 100
    J = (policy.get("discount_rate") or 0) / 100
    E = policy.get("coupon_amount", 0)
    P = (policy.get("reward_rate") or 0) / 100
    commission_base = policy["commission_base"]
    return fee_rate, J, E, P, commission_base


def _resolve_target_s(cost_a: float, target_bands: list[tuple[float, float]]) -> float:
    """Return the target_s for the first band where cost_a <= limit.

    Args:
        cost_a: Product cost.
        target_bands: Sorted list of (limit, target_s) tuples.

    Returns:
        The target_s from the matching band, or the last band's target_s
        as fallback.
    """
    if not target_bands:
        return 0.0
    for limit, target_s in target_bands:
        if cost_a <= limit:
            return target_s
    # Fallback: last band
    return target_bands[-1][1]


# ── Public API ───────────────────────────────────────────────────────────────


def forward_calc(
    cost_a: float,
    margin_c: float,
    h: float,
    policy: dict[str, Any],
    shipping_absorbed: float = 0,
) -> dict[str, Any]:
    """Forward price calculation: given H (selling price rate), compute all derived values.

    Args:
        cost_a: Product cost (원가).
        margin_c: Margin coefficient (e.g. 1.12 = 12% margin).
        h: Selling price rate multiplier.
        policy: Store policy dict from get_store_policy().
        shipping_absorbed: Shipping cost absorbed by seller.

    Returns:
        dict with keys: G, I, L, N, Q, O, S, cost_ratio, revenue_ratio, market_price.
    """
    fee_rate, J, E, P, commission_base = _extract_policy_params(policy)
    M = E          # coupon deduction at settlement
    F = shipping_absorbed
    R = shipping_absorbed

    G = cost_a * margin_c + E + F
    I = G * h
    L = I * (1 - J)

    if commission_base == "pre_discount":
        N = I * fee_rate
    else:
        N = L * fee_rate

    Q = L * P
    O = L - M - N - Q
    S = O - cost_a - R

    # Revenue ratio: avoid division by zero
    if L != 0:
        revenue_ratio = round(S / L * 100, 2)
    else:
        revenue_ratio = 0.0

    # Cost ratio: avoid division by zero
    if cost_a != 0:
        cost_ratio = round(S / cost_a, 4)
    else:
        cost_ratio = 0.0

    market_price = int(math.ceil(I / 10) * 10)

    return {
        "G": round(G, 2),
        "I": round(I, 2),
        "L": round(L, 2),
        "N": round(N, 2),
        "Q": round(Q, 2),
        "O": round(O, 2),
        "S": round(S, 2),
        "cost_ratio": cost_ratio,
        "revenue_ratio": revenue_ratio,
        "market_price": market_price,
    }


def solve_h(
    cost_a: float,
    margin_c: float,
    policy: dict[str, Any],
    target_s: float,
    shipping_absorbed: float = 0,
    min_h: float = 1.0,
    max_h: float = 3.5,
    round_h: int = 2,
    round_mode: str = "nearest",
) -> dict[str, Any]:
    """Reverse calculation: given target S (operating profit), solve for H.

    Algebraically derives H from the forward equations, then validates
    by running forward_calc.

    Args:
        cost_a: Product cost.
        margin_c: Margin coefficient.
        policy: Store policy dict.
        target_s: Desired operating profit (usually negative = acceptable loss).
        shipping_absorbed: Shipping cost absorbed.
        min_h: Minimum allowed H value.
        max_h: Maximum allowed H value.
        round_h: Decimal places to round H.

    Returns:
        dict with keys: h_raw, h, status, forward.
    """
    # Edge case: zero cost
    if cost_a <= 0:
        return {
            "h_raw": None,
            "h": None,
            "status": "error_zero_cost",
            "forward": None,
        }

    fee_rate, J, E, P, commission_base = _extract_policy_params(policy)
    M = E
    R = shipping_absorbed

    G = cost_a * margin_c + E + shipping_absorbed

    # Derive coefficient: L*coef = O  (before subtracting M)
    # O = L - M - N - Q
    # S = O - cost_a - R  =>  O = S + M + cost_a + R  (wait, O = L - M - N - Q)
    # Actually: S = O - cost_a - R  =>  O = target_S + cost_a + R
    # O = L - M - N - Q
    # target_S + cost_a + R = L - M - N - Q
    # target_S + cost_a + R + M = L - N - Q
    #
    # pre_discount:  N = I * fee_rate = G*H * fee_rate
    #   L = I*(1-J) = G*H*(1-J)
    #   Q = L*P = G*H*(1-J)*P
    #   L - N - Q = G*H*(1-J) - G*H*fee_rate - G*H*(1-J)*P
    #             = G*H * [(1-J) - fee_rate - (1-J)*P]
    #             = G*H * [(1-J)*(1-P) - fee_rate]
    #
    # post_discount: N = L * fee_rate = G*H*(1-J)*fee_rate
    #   L - N - Q = G*H*(1-J) - G*H*(1-J)*fee_rate - G*H*(1-J)*P
    #             = G*H*(1-J) * [1 - fee_rate - P]

    if commission_base == "pre_discount":
        coef = (1 - J) * (1 - P) - fee_rate
    else:
        coef = (1 - J) * (1 - fee_rate - P)

    denom = G * coef

    if abs(denom) < 1e-9:
        return {
            "h_raw": None,
            "h": None,
            "status": "error_zero_denom",
            "forward": None,
        }

    h_raw = (target_s + M + cost_a + R) / denom

    # Clamp and round
    status = "ok"
    h_clamped = h_raw
    if h_raw < min_h:
        h_clamped = min_h
        status = "clamped_min"
    elif h_raw > max_h:
        h_clamped = max_h
        status = "clamped_max"

    if round_mode == "ceil":
        h_final = math.ceil(h_clamped * 10 ** round_h) / 10 ** round_h
    else:
        h_final = round(h_clamped, round_h)

    fwd = forward_calc(cost_a, margin_c, h_final, policy, shipping_absorbed)

    return {
        "h_raw": round(h_raw, round_h + 2),
        "h": h_final,
        "status": status,
        "forward": fwd,
    }


def solve_h_by_v(
    cost_a: float,
    margin_c: float,
    policy: dict[str, Any],
    target_v_pct: float,
    shipping_absorbed: float = 0,
    min_h: float = 1.0,
    max_h: float = 4.0,
    round_h: int = 2,
    round_mode: str = "nearest",
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
        max_h: H 최대값 (이상이면 clamped). Default 4.0 (V 기반 전략은
            큰 margin_c 와 함께 사용되어 H 헤드룸이 더 필요. solve_h 는 3.5).
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

    fee_rate, J, E, P, commission_base = _extract_policy_params(policy)
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

    if round_mode == "ceil":
        h_final = math.ceil(h_clamped * 10 ** round_h) / 10 ** round_h
    else:
        h_final = round(h_clamped, round_h)
    fwd = forward_calc(cost_a, margin_c, h_final, policy, shipping_absorbed)

    return {
        "h_raw": round(h_raw, round_h + 2),
        "h": h_final,
        "status": status,
        "forward": fwd,
    }


def solve_auto_c(
    cost_a: float,
    margin_c: float,
    policy: dict[str, Any],
    target: float,
    shipping_absorbed: float = 0,
    min_h: float = 1.0,
    max_h: float = 3.5,
    round_h: int = 2,
    round_mode: str = "nearest",
    metric: str = "absolute_s",
    c_min: float = 0.5,
    c_max: float = 30.0,
) -> tuple[dict[str, Any], float]:
    """solve_h / solve_h_by_v 래퍼. clamped 시 margin_c를 이진탐색으로 자동 조정.

    Returns:
        (sol dict, used_margin_c)
    """
    def _try(c: float) -> dict:
        if metric == "absolute_s":
            return solve_h(
                cost_a=cost_a, margin_c=c, policy=policy,
                target_s=target, shipping_absorbed=shipping_absorbed,
                min_h=min_h, max_h=max_h, round_h=round_h, round_mode=round_mode,
            )
        else:
            return solve_h_by_v(
                cost_a=cost_a, margin_c=c, policy=policy,
                target_v_pct=target, shipping_absorbed=shipping_absorbed,
                min_h=min_h, max_h=max_h, round_h=round_h,
            )

    sol = _try(margin_c)
    if sol["status"] == "ok":
        return sol, margin_c

    if sol["status"] == "clamped_max":
        # H_raw 너무 큼 → C를 키워서 G를 높이면 H_raw 감소
        c_lo, c_hi = margin_c, c_max
    elif sol["status"] == "clamped_min":
        # H_raw 너무 작음 → C를 줄여서 G를 낮추면 H_raw 증가
        c_lo, c_hi = c_min, margin_c
    else:
        return sol, margin_c  # error_* 계열은 C 조정으로 해결 불가

    best_sol, best_c = sol, margin_c
    for _ in range(50):
        c_mid = (c_lo + c_hi) / 2
        if c_hi - c_lo < 1e-6:
            break
        s = _try(c_mid)
        if s["status"] == "ok":
            best_sol, best_c = s, c_mid
            # ok 구간의 경계를 좁혀나감
            if sol["status"] == "clamped_max":
                c_hi = c_mid   # ok 유지하면서 C를 최소화
            else:
                c_lo = c_mid   # ok 유지하면서 C를 최대화
        elif s["status"] == "clamped_max":
            c_lo = c_mid       # C가 아직 작음 → 키워야
        else:
            # clamped_min 또는 error → C가 너무 큼
            c_hi = c_mid

    return best_sol, round(best_c, 4)


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
    round_mode: str = "nearest",
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
                round_mode=round_mode,
            )
        else:  # absolute_s
            result = solve_h(
                cost_a, margin_c, policy, band_value,
                shipping_absorbed=shipping_absorbed,
                min_h=min_h, max_h=max_h, round_h=round_h,
                round_mode=round_mode,
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


# ── Smoke Test ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys

    print("=" * 70)
    print("가격계산 엔진 - Smoke Test")
    print("=" * 70)

    all_ok = True

    # ── [1] forward_calc Excel verification ──────────────────────────────
    print("\n[1] forward_calc - Excel 검증값 비교")

    test_policy = {
        "commission_rate": 17.5,
        "commission_base": "pre_discount",
        "discount_rate": 50,
        "coupon_amount": 120,
        "reward_rate": 0.5,
    }

    # Test case 1: cost_a = 5000
    r1 = forward_calc(5000, 1.12, 1.9, test_policy)
    expected1 = {"G": 5720, "I": 10868, "L": 5434, "N": 1901.9, "Q": 27.17, "O": 3384.93, "S": -1615.07}

    print(f"  cost_a=5000, margin_c=1.12, h=1.9")
    for key, exp_val in expected1.items():
        actual = r1[key]
        ok = abs(actual - exp_val) < 0.01
        mark = "OK" if ok else "FAIL"
        if not ok:
            all_ok = False
        print(f"    {key}: expected={exp_val}, actual={actual} [{mark}]")

    # Test case 2: cost_a = 10000
    r2 = forward_calc(10000, 1.12, 1.9, test_policy)
    expected_s2 = -3183.67
    # Verify: G=10000*1.12+120+0=11320, I=11320*1.9=21508, L=21508*0.5=10754
    # N=21508*0.175=3763.9, Q=10754*0.005=53.77, O=10754-120-3763.9-53.77=6816.33
    # S=6816.33-10000=-3183.67
    ok2 = abs(r2["S"] - expected_s2) < 0.01
    mark2 = "OK" if ok2 else "FAIL"
    if not ok2:
        all_ok = False
    print(f"\n  cost_a=10000, margin_c=1.12, h=1.9")
    print(f"    G: expected=11320, actual={r2['G']} [{'OK' if abs(r2['G']-11320)<0.01 else 'FAIL'}]")
    print(f"    I: expected=21508, actual={r2['I']} [{'OK' if abs(r2['I']-21508)<0.01 else 'FAIL'}]")
    print(f"    L: expected=10754, actual={r2['L']} [{'OK' if abs(r2['L']-10754)<0.01 else 'FAIL'}]")
    print(f"    N: expected=3763.9, actual={r2['N']} [{'OK' if abs(r2['N']-3763.9)<0.01 else 'FAIL'}]")
    print(f"    Q: expected=53.77, actual={r2['Q']} [{'OK' if abs(r2['Q']-53.77)<0.01 else 'FAIL'}]")
    print(f"    O: expected=6816.33, actual={r2['O']} [{'OK' if abs(r2['O']-6816.33)<0.01 else 'FAIL'}]")
    print(f"    S: expected={expected_s2}, actual={r2['S']} [{mark2}]")
    print(f"    market_price: {r2['market_price']}원")

    # ── [2] solve_h roundtrip ────────────────────────────────────────────
    print("\n[2] solve_h - 역산 라운드트립 검증")

    for target_s in [-100, -200, -500, -1000, -2000]:
        res = solve_h(5000, 1.12, test_policy, target_s)
        actual_s = res["forward"]["S"] if res["forward"] else None
        dev = abs(actual_s - target_s) if actual_s is not None else float("inf")
        ok_rt = dev < 50
        mark_rt = "OK" if ok_rt else "FAIL"
        if not ok_rt:
            all_ok = False
        print(f"  target_S={target_s:>7} → h={res['h']}, "
              f"actual_S={actual_s:>10}, dev={dev:>6.2f} [{mark_rt}]")

    # ── [3] batch_solve with 11번가 bands ────────────────────────────────
    print("\n[3] batch_solve - 11번가 밴드 테스트")

    bands_11st = [
        (500, -100), (1000, -150), (3000, -200), (5000, -250),
        (10000, -300), (20000, -350), (30000, -400), (50000, -450),
        (100000, -500), (999999999, -1000),
    ]

    # 11번가 actual policy (reward_rate=0 in JSON but test uses 0.5)
    policy_11st = {
        "commission_rate": 17.5,
        "commission_base": "pre_discount",
        "discount_rate": 50,
        "coupon_amount": 120,
        "reward_rate": 0,
    }

    test_products = [
        {"code": "P001", "cost_a": 300},
        {"code": "P002", "cost_a": 800},
        {"code": "P003", "cost_a": 1500},
        {"code": "P004", "cost_a": 3500},
        {"code": "P005", "cost_a": 7000},
        {"code": "P006", "cost_a": 15000},
        {"code": "P007", "cost_a": 25000},
        {"code": "P008", "cost_a": 40000},
        {"code": "P009", "cost_a": 80000},
    ]

    batch_results = batch_solve(test_products, policy_11st, bands_11st)

    # Print formatted table
    hdr = f"  {'code':<6} {'cost_a':>8} {'target_value':>12} {'h':>5} {'market':>8} {'S':>10} {'status':<12}"
    print(hdr)
    print("  " + "-" * (len(hdr) - 2))
    for row in batch_results:
        mp = row.get("market_price", "")
        s_val = row.get("S", "")
        print(f"  {row['code']:<6} {row['cost_a']:>8} {row['target_value']:>12} "
              f"{row['h']:>5} {mp:>8} {s_val:>10} {row['status']:<12}")

    # ── [4] Integration with sales_channel_policy ────────────────────────
    print("\n[4] Integration - sales_channel_policy 연동 테스트")

    try:
        from DB_save.sales_channel_policy import get_store_policy

        store_alias = "11번가A2-1"
        live_policy = get_store_policy(store_alias)
        print(f"  {store_alias} 정책 로드 성공:")
        print(f"    commission_rate={live_policy['commission_rate']}%, "
              f"commission_base={live_policy['commission_base']}")
        print(f"    discount_rate={live_policy['discount_rate']}, "
              f"coupon_amount={live_policy.get('coupon_amount', 0)}, "
              f"reward_rate={live_policy.get('reward_rate', 0)}")

        res_live = solve_h(5000, 1.12, live_policy, target_s=-200)
        if res_live["forward"]:
            fwd = res_live["forward"]
            print(f"    solve_h(cost=5000, target=-200) → h={res_live['h']}, "
                  f"market_price={fwd['market_price']}원, S={fwd['S']}원 [OK]")
        else:
            print(f"    solve_h 실패: status={res_live['status']}")
            all_ok = False

    except ImportError:
        # Running as standalone script -try relative import
        try:
            from sales_channel_policy import get_store_policy

            store_alias = "11번가A2-1"
            live_policy = get_store_policy(store_alias)
            print(f"  {store_alias} 정책 로드 성공 (relative import):")
            print(f"    commission_rate={live_policy['commission_rate']}%, "
                  f"commission_base={live_policy['commission_base']}")
            print(f"    discount_rate={live_policy['discount_rate']}, "
                  f"coupon_amount={live_policy.get('coupon_amount', 0)}, "
                  f"reward_rate={live_policy.get('reward_rate', 0)}")

            res_live = solve_h(5000, 1.12, live_policy, target_s=-200)
            if res_live["forward"]:
                fwd = res_live["forward"]
                print(f"    solve_h(cost=5000, target=-200) → h={res_live['h']}, "
                      f"market_price={fwd['market_price']}원, S={fwd['S']}원 [OK]")
            else:
                print(f"    solve_h 실패: status={res_live['status']}")
                all_ok = False

        except Exception as exc:
            print(f"  SKIP: sales_channel_policy 로드 실패 -{exc}")

    # ── Summary ──────────────────────────────────────────────────────────
    print("\n" + "=" * 70)
    if all_ok:
        print("ALL CHECKS PASSED")
    else:
        print("SOME CHECKS FAILED -see above")
    print("=" * 70)
