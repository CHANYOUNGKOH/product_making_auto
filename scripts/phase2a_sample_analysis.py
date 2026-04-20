# -*- coding: utf-8 -*-
"""Phase 2A sample analysis - 고도몰A1-1 × 10건 read-only.
신 lowest_price J=35% 정책 적용 시 가격/brand_code 변화 분석.
"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from godomall_register.config import STORES
from godomall_register.api_client import GodomallApiClient
from DB_save.price_engine import solve_auto_c, _resolve_target_s
from DB_save.pricing_strategies import get_pricing_strategy


def calc_new_price(cost_a):
    strategy = get_pricing_strategy("고도몰", "lowest_price")
    discount_bands = strategy.get("discount_bands", [[999999999, 10]])
    J = float(_resolve_target_s(cost_a, discount_bands))
    policy = {
        "commission_rate": 4.0,
        "commission_base": "post_discount",
        "discount_rate": J,
        "coupon_amount": 0,
        "reward_rate": 0,
    }
    sol, _ = solve_auto_c(
        cost_a=cost_a,
        margin_c=strategy["margin_c"],
        policy=policy,
        target=_resolve_target_s(cost_a, strategy["bands"]),
        shipping_absorbed=0,
        min_h=strategy["min_h"],
        max_h=strategy["max_h"],
        round_h=strategy["round_h"],
        round_mode=strategy.get("round_mode", "nearest"),
        metric=strategy.get("metric", "absolute_s"),
    )
    fwd = sol.get("forward") or {}
    return int(fwd.get("market_price") or fwd.get("I") or cost_a), J


def main():
    cred = STORES["고도몰A1-1"]
    print(f"[store] {cred.get('shop_name', '고도몰A1-1')}  admin={cred.get('admin_url','')}")
    client = GodomallApiClient(cred["partner_key"], cred["key"])

    print("[fetch] page=1 size=10...")
    res = client.search_products(page=1, size=10)
    print(f"[result] code={res['code']} total={res['total']} max_page={res['max_page']} returned={len(res['products'])}")
    if res["code"] != "000":
        print("[error]", res.get("msg"))
        return

    admin_base = cred.get("admin_url", "").rstrip("/")
    print()
    print("| 상품번호 | 상품코드 | 원가 | 현재가 | 신가 | ΔI | 현brand | 신brand | 관리자 링크 |")
    print("|---|---|---:|---:|---:|---:|---|---|---|")

    deltas = []
    brand_changes = 0
    for p in res["products"]:
        goods_no = p.get("goodsNo") or ""
        goods_cd = p.get("goodsCd") or p.get("goods_cd") or ""
        cur_price = int(float(p.get("goodsPrice") or p.get("goods_price") or 0))
        cost = int(float(p.get("costPrice") or p.get("cost_price") or 0))
        cur_brand = p.get("brandCd") or p.get("brand_code") or ""

        admin_link = f"{admin_base}/goods/goods_list.php?key=goodsNo&keyword={goods_no}&searchFl=y" if admin_base and goods_no else ""

        if cost <= 0:
            print(f"| {goods_no} | {goods_cd} | - | {cur_price:,} | n/a | n/a | {cur_brand} | - | {admin_link} | (원가 누락)")
            continue

        new_price, J = calc_new_price(cost)
        new_brand = "001"
        delta = new_price - cur_price
        deltas.append(delta)
        if str(cur_brand).zfill(3) != new_brand:
            brand_changes += 1

        print(f"| {goods_no} | {goods_cd} | {cost:,} | {cur_price:,} | {new_price:,} | {delta:+,} | {cur_brand} | {new_brand} | {admin_link} |")

    print()
    if deltas:
        print(f"[summary] 건수={len(deltas)}  평균ΔI={sum(deltas)/len(deltas):.0f}  최대ΔI={max(deltas)}  최소ΔI={min(deltas)}  brand변경건수={brand_changes}")
    else:
        print("[summary] 분석 가능한 건수 0 (원가 필드 없음)")

    print()
    print("[raw sample keys] first product keys:", list(res["products"][0].keys()) if res["products"] else [])


if __name__ == "__main__":
    main()
