# -*- coding: utf-8 -*-
"""Phase 2D — Goods_Update 배치 XML 테스트.
multi-goods_data 한 XML 로 여러 상품 동시 업데이트 가능한지 검증.
Phase 2C 완료 후 실행. 이미 업데이트된 상품들 중 5건을 다시 같은 값으로 업데이트 (변화 없음).
"""
import sys
import time
import xml.etree.ElementTree as ET
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from godomall_register.config import STORES
from godomall_register.api_client import GodomallApiClient
from godomall_register.xml_builder import build_product_xml, SIMPLE_FIELDS
from godomall_register.xml_host import XmlHost


def main():
    cred = STORES["고도몰A1-1"]
    client = GodomallApiClient(cred["partner_key"], cred["key"])
    xml_host = XmlHost()

    res = client.search_products(page=1, size=5)
    if res["code"] != "000":
        print("[err]", res.get("msg")); return

    products = res["products"]
    print(f"[batch test] 5 상품, 현재값 그대로 재업로드")

    # 5건을 하나의 XML 로 묶기 (기존 값 그대로 → no-op 업데이트)
    payloads = []
    for p in products:
        payload = {"goodsNo": p["goodsNo"]}
        for f in SIMPLE_FIELDS:
            v = p.get(f)
            if v not in (None, ""):
                payload[f] = v
        payloads.append(payload)

    xml_content = build_product_xml(payloads, mode="update")
    print(f"[xml] size={len(xml_content):,}바이트  goods_data 태그 수={xml_content.count('<goods_data>')}")

    fn = f"phase2d_batch_{int(time.time()*1000)}.xml"
    data_url = xml_host.upload(xml_content, fn)
    print(f"[upload] {data_url}")

    t0 = time.time()
    result = client._request("goods/Goods_Update.php", {"data_url": data_url})
    elapsed = time.time() - t0
    print(f"[api] {elapsed:.2f}s")

    header = result.get("header", {})
    return_data = result.get("return", {})
    goods_data = return_data.get("goods_data") if isinstance(return_data, dict) else None

    print(f"[header] code={header.get('code')} msg={header.get('msg')}")
    print(f"[return type] {type(return_data).__name__}  goods_data type={type(goods_data).__name__ if goods_data is not None else 'None'}")

    if isinstance(goods_data, list):
        print(f"[return count] {len(goods_data)}건 per-item 결과")
        for i, g in enumerate(goods_data, 1):
            if isinstance(g, dict):
                print(f"  [{i}] code={g.get('code')} goodsNo={g.get('goodsNo', '?')} msg={g.get('msg') or g.get('message', '')}")
            else:
                print(f"  [{i}] {g}")
    elif isinstance(goods_data, dict):
        print(f"[return] dict (1건) — 배치 지원 안 하거나 마지막 건만 반환")
        print(f"  {goods_data}")
    else:
        print(f"[return] {goods_data}")

    # cleanup
    try:
        xml_host.cleanup([fn])
    except Exception as e:
        print(f"[cleanup warn] {e}")

    print()
    print("[raw XML (first 500)]")
    raw = client._last_raw_response[:500] if hasattr(client, '_last_raw_response') else ''
    print(raw)


if __name__ == "__main__":
    main()
