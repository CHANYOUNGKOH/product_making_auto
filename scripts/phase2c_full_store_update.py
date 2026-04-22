# -*- coding: utf-8 -*-
"""Phase 2C full store update - 병렬 버전 (fetch 4워커, update 8워커)."""
import sys
import time
import json
import threading
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from godomall_register.config import STORES
from godomall_register.api_client import GodomallApiClient
from godomall_register.xml_builder import build_product_xml, SIMPLE_FIELDS
from godomall_register.xml_host import XmlHost
from DB_save.price_engine import solve_auto_c, _resolve_target_s
from DB_save.pricing_strategies import get_pricing_strategy


def calc_new_price(cost_a):
    strategy = get_pricing_strategy("고도몰", "lowest_price")
    J = float(_resolve_target_s(cost_a, strategy.get("discount_bands", [[999999999, 10]])))
    policy = {"commission_rate": 4.0, "commission_base": "post_discount",
              "discount_rate": J, "coupon_amount": 0, "reward_rate": 0}
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


def process_one(p, client, xml_host, uploaded_lock, uploaded):
    """한 상품 업데이트 — thread-safe"""
    goods_no = p.get("goodsNo") or ""
    goods_cd = p.get("goodsCd") or ""
    cost = int(float(p.get("costPrice") or 0))
    cur_price = int(float(p.get("goodsPrice") or 0))
    cur_brand = str(p.get("brandCd") or "").zfill(3)

    if cost <= 0:
        return {"goodsCd": goods_cd, "status": "skip_no_cost"}

    new_price = calc_new_price(cost)
    new_brand = "001"

    if cur_price == new_price and cur_brand == new_brand:
        return {"goodsCd": goods_cd, "status": "skip_same"}

    payload = {"goodsNo": goods_no}
    for f in SIMPLE_FIELDS:
        v = p.get(f)
        if v not in (None, ""):
            payload[f] = v
    payload["goodsPrice"] = str(new_price)
    payload["brandCd"] = new_brand

    try:
        xml = build_product_xml(payload, mode="update")
        fn = f"p2c_{goods_no}_{int(time.time()*1000)}_{threading.get_ident()}.xml"
        data_url = xml_host.upload(xml, fn)
        with uploaded_lock:
            uploaded.append(fn)
        r = client.update_product(data_url)
        if r["code"] == "000":
            return {"goodsCd": goods_cd, "goodsNo": goods_no, "status": "ok",
                    "oldPrice": cur_price, "newPrice": new_price,
                    "oldBrand": cur_brand, "newBrand": new_brand}
        return {"goodsCd": goods_cd, "goodsNo": goods_no, "status": "err",
                "code": r["code"], "msg": r["msg"]}
    except Exception as e:
        return {"goodsCd": goods_cd, "status": "exc", "err": str(e)}


def main(store_key, fetch_workers=4, update_workers=8):
    cred = STORES[store_key]
    print(f"[store] {store_key}  {cred.get('shop_name')}  fetch_workers={fetch_workers} update_workers={update_workers}", flush=True)
    client = GodomallApiClient(cred["partner_key"], cred["key"])
    xml_host = XmlHost()

    log_dir = ROOT / "godomall_register" / "logs" / "phase2c"
    log_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    snapshot_path = log_dir / f"{store_key}_{ts}_snapshot.jsonl"
    result_path = log_dir / f"{store_key}_{ts}_result.jsonl"

    # Step 1: 병렬 fetch
    print("[fetch] 시작 (병렬)...", flush=True)
    t0 = time.time()
    all_products = client.get_all_products(max_workers=fetch_workers)
    print(f"[fetch] done  {len(all_products)}건  {time.time()-t0:.1f}s", flush=True)

    # Snapshot
    with open(snapshot_path, "w", encoding="utf-8") as f:
        for p in all_products:
            f.write(json.dumps({
                "goodsNo": p.get("goodsNo"), "goodsCd": p.get("goodsCd"),
                "goodsPrice": p.get("goodsPrice"), "brandCd": p.get("brandCd"),
                "costPrice": p.get("costPrice"),
            }, ensure_ascii=False) + "\n")
    print(f"[snapshot] {snapshot_path}", flush=True)

    # Step 2: 병렬 update
    print(f"[update] 시작 (병렬 {update_workers}워커)...", flush=True)
    t1 = time.time()
    uploaded = []
    uploaded_lock = threading.Lock()
    stats = {"ok": 0, "skip_same": 0, "skip_no_cost": 0, "err": 0}

    result_fp = open(result_path, "w", encoding="utf-8")
    result_lock = threading.Lock()

    completed = 0
    completed_lock = threading.Lock()

    def worker(p):
        nonlocal completed
        r = process_one(p, client, xml_host, uploaded_lock, uploaded)
        with result_lock:
            result_fp.write(json.dumps(r, ensure_ascii=False) + "\n")
        with completed_lock:
            completed += 1
            c = completed
            if r["status"] == "ok": stats["ok"] += 1
            elif r["status"] == "skip_same": stats["skip_same"] += 1
            elif r["status"] == "skip_no_cost": stats["skip_no_cost"] += 1
            else: stats["err"] += 1
        if c % 200 == 0:
            elapsed = time.time() - t1
            rate = c / elapsed if elapsed > 0 else 0
            remain = (len(all_products) - c) / rate if rate > 0 else 0
            print(f"[update] {c}/{len(all_products)}  ok={stats['ok']} err={stats['err']} skip={stats['skip_same']+stats['skip_no_cost']}  rate={rate:.1f}/s  eta={remain:.0f}s", flush=True)
            result_fp.flush()
            # R2 cleanup intermediate
            with uploaded_lock:
                if len(uploaded) >= 500:
                    try:
                        xml_host.cleanup(uploaded[:])
                        uploaded.clear()
                    except Exception:
                        pass
        return r

    with ThreadPoolExecutor(max_workers=update_workers) as pool:
        list(pool.map(worker, all_products))

    result_fp.close()
    with uploaded_lock:
        if uploaded:
            try:
                xml_host.cleanup(uploaded)
            except Exception:
                pass

    elapsed = time.time() - t1
    print(f"\n[done] {store_key}  ok={stats['ok']}  skip_same={stats['skip_same']}  skip_no_cost={stats['skip_no_cost']}  err={stats['err']}  elapsed={elapsed:.0f}s", flush=True)
    print(f"[files] snapshot={snapshot_path}  result={result_path}", flush=True)


if __name__ == "__main__":
    store_key = sys.argv[1] if len(sys.argv) > 1 else "고도몰A1-1"
    main(store_key)
