"""One-time script: store_memos.json -> sales_channels.json stores section.

Reads store entries from store_memos.json, parses key/memo, builds store dicts,
and writes them into the 'stores' section of sales_channels.json.

Usage:
    python DB_save/_populate_stores.py
"""

import json
import re
import sys
from collections import Counter
from pathlib import Path

# ── Mappings ─────────────────────────────────────────────────────────────────

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

BUSINESS_NAMES = {
    "A1": "굿투굿",
    "A2": "마이유통",
    "A3": "마이비타민",
    "A4": "러블리몰",
    "A5": "럽미몰",
    "B1": "마이짐",
    "B2": "샤이닝몰",
    "B3": "샤인몰",
    "B4": "순수한노을",
    "B5": "포근한정원",
    "B6": "푸르른초원",
    "B7": "행복한저택",
}

# Only these sheets contain policy data in memo strings
POLICY_SHEETS = {"ESM_Auction_info", "ESM_Gmarket_info"}

# ── Paths ────────────────────────────────────────────────────────────────────

BASE_DIR = Path(__file__).resolve().parent
STORE_MEMOS_PATH = BASE_DIR / "store_memos.json"
SALES_CHANNELS_PATH = BASE_DIR / "sales_channels.json"

# ── Parsers ──────────────────────────────────────────────────────────────────


def parse_key(key: str) -> dict:
    """Parse store_memos key into components.

    Key format: "시트명::명의자::사업자번호::별칭"
    Example: "ESM_Auction_info::A::2::옥션A2-1"

    Returns:
        dict with: sheet, owner, biz_num, alias, market, business_code, business_name, store_num
    """
    parts = key.split("::")
    if len(parts) != 4:
        raise ValueError(f"Invalid key format (expected 4 parts): {key!r}")

    sheet, owner, biz_num, alias = parts

    market = SHEET_TO_MARKET.get(sheet)
    if market is None:
        raise ValueError(f"Unknown sheet name: {sheet!r} in key {key!r}")

    business_code = f"{owner}{biz_num}"
    business_name = BUSINESS_NAMES.get(business_code)
    if business_name is None:
        raise ValueError(
            f"Unknown business_code: {business_code!r} in key {key!r}"
        )

    # Extract store_num from alias: "옥션A2-1" -> "1", "쿠팡A2" -> "0"
    dash_idx = alias.rfind("-")
    if dash_idx != -1:
        store_num = alias[dash_idx + 1 :]
    else:
        store_num = "0"

    return {
        "sheet": sheet,
        "owner": owner,
        "biz_num": biz_num,
        "alias": alias,
        "market": market,
        "business_code": business_code,
        "business_name": business_name,
        "store_num": store_num,
    }


def parse_memo_policy(memo: str, sheet: str) -> dict:
    """Extract shipping/discount policy from memo string.

    Only auction/gmarket memos contain policy data. Examples:
        "할2_3000_스0.5"    -> discount 2%, shipping paid 3000
        "할61_무배_스0.5"   -> discount 61%, shipping free 0
        "할2_배3000_스0.5"  -> discount 2%, shipping paid 3000
        ""                  -> no policy (all null)

    Returns:
        dict with: shipping_strategy, shipping_fee, discount_rate (all nullable)
    """
    no_policy = {
        "shipping_strategy": None,
        "shipping_fee": None,
        "discount_rate": None,
    }

    # Only ESM sheets have policy in memo
    if sheet not in POLICY_SHEETS:
        return no_policy

    if not memo or not memo.strip():
        return no_policy

    # Parse discount: "할2_..." or "할61_..."
    discount_match = re.match(r"할(\d+)", memo)
    discount_rate = float(discount_match.group(1)) if discount_match else None

    # Parse shipping: "무배" -> free/0, "3000" or "배3000" -> paid/3000
    if "무배" in memo:
        shipping_strategy = "free"
        shipping_fee = 0
    else:
        # Match "배3000" or standalone "3000" (between underscores)
        ship_match = re.search(r"배?(\d{3,})", memo)
        if ship_match:
            shipping_strategy = "paid"
            shipping_fee = int(ship_match.group(1))
        else:
            shipping_strategy = None
            shipping_fee = None

    return {
        "shipping_strategy": shipping_strategy,
        "shipping_fee": shipping_fee,
        "discount_rate": discount_rate,
    }


# ── Main ─────────────────────────────────────────────────────────────────────


def main():
    sys.stdout.reconfigure(encoding="utf-8")

    # 1. Load store_memos.json
    print(f"[1] Loading {STORE_MEMOS_PATH.name} ...")
    with open(STORE_MEMOS_PATH, encoding="utf-8") as f:
        store_memos = json.load(f)
    print(f"    {len(store_memos)} entries found")

    # 2. Load existing sales_channels.json
    print(f"[2] Loading {SALES_CHANNELS_PATH.name} ...")
    with open(SALES_CHANNELS_PATH, encoding="utf-8") as f:
        sales_channels = json.load(f)
    existing_count = len(sales_channels.get("stores", {}))
    print(f"    Existing stores: {existing_count}")

    # 3. Build store dicts
    print("[3] Parsing store entries ...")
    stores = {}
    market_counter = Counter()
    errors = []

    for key, value in store_memos.items():
        try:
            parsed = parse_key(key)
        except ValueError as exc:
            errors.append(str(exc))
            continue

        memo = value.get("memo", "")
        policy = parse_memo_policy(memo, parsed["sheet"])

        alias = parsed["alias"]
        store_dict = {
            "market": parsed["market"],
            "owner": parsed["owner"],
            "business_code": parsed["business_code"],
            "business_name": parsed["business_name"],
            "store_num": parsed["store_num"],
            "status": "active",
            "shipping_strategy": policy["shipping_strategy"],
            "shipping_fee": policy["shipping_fee"],
            "discount_rate": policy["discount_rate"],
        }

        if alias in stores:
            print(f"    WARNING: Duplicate alias {alias!r}, overwriting")

        stores[alias] = store_dict
        market_counter[parsed["market"]] += 1

    # 4. Write into sales_channels.json
    print(f"[4] Writing {len(stores)} stores to {SALES_CHANNELS_PATH.name} ...")
    sales_channels["stores"] = stores

    with open(SALES_CHANNELS_PATH, "w", encoding="utf-8") as f:
        json.dump(sales_channels, f, ensure_ascii=False, indent=2)

    # 5. Print summary
    print()
    print("=" * 50)
    print(f"Total stores: {len(stores)}")
    print("-" * 50)
    for market, count in sorted(market_counter.items(), key=lambda x: -x[1]):
        print(f"  {market:12s}: {count}개")
    print("=" * 50)

    if errors:
        print(f"\nErrors ({len(errors)}):")
        for e in errors:
            print(f"  - {e}")

    print("\nDone!")


if __name__ == "__main__":
    main()
