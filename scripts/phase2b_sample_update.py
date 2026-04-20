# -*- coding: utf-8 -*-
"""Phase 2B sample update - 고도몰A1-1 × 10건 실제 업데이트.
goodsPrice + brandCd 만 변경 (나머지 필드 건드리지 않음).
"""
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from godomall_register.config import STORES
from godomall_register.api_client import GodomallApiClient
from godomall_register.xml_builder import build_product_xml
from godomall_register.xml_host import XmlHost
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
        cost_a=cost_a, margin_c=strategy["margin_c"], policy=policy,
        target=_resolve_target_s(cost_a, strategy["bands"]),
        shipping_absorbed=0,
        min_h=strategy["min_h"], max_h=strategy["max_h"],
        round_h=strategy["round_h"],
        round_mode=strategy.get("round_mode", "nearest"),
        metric=strategy.get("metric", "absolute_s"),
    )
    fwd = sol.get("forward") or {}
    return int(fwd.get("market_price") or fwd.get("I") or cost_a)


def main():
    cred = STORES["고도몰A1-1"]
    print(f"[store] {cred.get('shop_name')}  admin={cred.get('admin_url')}")
    client = GodomallApiClient(cred["partner_key"], cred["key"])
    xml_host = XmlHost()

    # 샘플 10건 다시 조회
    res = client.search_products(page=1, size=10)
    if res["code"] != "000":
        print("[error] search fail:", res.get("msg"))
        return

    print(f"[phase2b] target={len(res['products'])}건 업데이트 시작\n")

    uploaded = []
    results = {"updated": [], "errors": [], "skipped": []}

    for p in res["products"]:
        goods_no = p.get("goodsNo") or ""
        goods_cd = p.get("goodsCd") or ""
        cost = int(float(p.get("costPrice") or 0))
        cur_price = int(float(p.get("goodsPrice") or 0))
        cur_brand = p.get("brandCd") or ""

        if cost <= 0:
            print(f"[skip] {goods_cd} (원가 0) ")
            results["skipped"].append(goods_cd)
            continue

        new_price = calc_new_price(cost)
        new_brand = "001"

        # 이미 정답이면 skip
        if cur_price == new_price and str(cur_brand).zfill(3) == new_brand:
            print(f"[skip-same] {goods_cd} 이미 동일")
            results["skipped"].append(goods_cd)
            continue

        # 기존 상품 데이터를 베이스로 복사 + 가격/brand 만 덮어쓰기
        from godomall_register.xml_builder import SIMPLE_FIELDS
        update_payload = {"goodsNo": goods_no}
        for f in SIMPLE_FIELDS:
            v = p.get(f)
            if v not in (None, ""):
                update_payload[f] = v
        update_payload["goodsPrice"] = str(new_price)
        update_payload["brandCd"] = new_brand

        try:
            xml_content = build_product_xml(update_payload, mode="update")
            fn = f"phase2b_{goods_no}_{int(time.time()*1000)}.xml"
            data_url = xml_host.upload(xml_content, fn)
            uploaded.append(fn)

            r = client.update_product(data_url)
            if r["code"] == "000":
                print(f"[ok]  {goods_cd}  {cur_price:,}→{new_price:,}  brand {cur_brand}→{new_brand}")
                results["updated"].append({
                    "goodsNo": goods_no, "goodsCd": goods_cd,
                    "oldPrice": cur_price, "newPrice": new_price,
                    "oldBrand": cur_brand, "newBrand": new_brand,
                })
            else:
                print(f"[err] {goods_cd}  code={r['code']} msg={r['msg']}")
                results["errors"].append({"goodsCd": goods_cd, "err": f"{r['code']} {r['msg']}"})
        except Exception as e:
            print(f"[exc] {goods_cd}  {e}")
            results["errors"].append({"goodsCd": goods_cd, "err": str(e)})

    # R2 cleanup
    try:
        xml_host.cleanup(uploaded)
    except Exception as e:
        print(f"[cleanup warn] {e}")

    print()
    print(f"[result] updated={len(results['updated'])}  skipped={len(results['skipped'])}  errors={len(results['errors'])}")

    # 재검증: 업데이트된 건만 다시 조회
    if results["updated"]:
        print("\n[verify] 변경 후 재조회...")
        time.sleep(2)
        res2 = client.search_products(page=1, size=10)
        by_no = {p["goodsNo"]: p for p in res2.get("products", [])}
        print("| 상품코드 | 예상가 | 실제가 | 예상brand | 실제brand | 일치 |")
        print("|---|---:|---:|---|---|---|")
        for u in results["updated"]:
            back = by_no.get(u["goodsNo"], {})
            act_price = int(float(back.get("goodsPrice") or 0))
            act_brand = back.get("brandCd") or ""
            ok = (act_price == u["newPrice"]) and (str(act_brand).zfill(3) == u["newBrand"])
            print(f"| {u['goodsCd']} | {u['newPrice']:,} | {act_price:,} | {u['newBrand']} | {act_brand} | {'✅' if ok else '❌'} |")


if __name__ == "__main__":
    main()
