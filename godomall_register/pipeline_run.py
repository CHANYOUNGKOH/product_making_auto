"""고도몰 최저가전략 파이프라인.

흐름:
  1. OC API → DB 입고 (oc_import.import_all)
  2. DB에서 출고 대상 조회 (oc_price IS NOT NULL + ACTIVE)
  3. convert_ownerclan_to_godomall(strategy_id="lowest_price") 적용
  4. 출고 엑셀 저장

실행:
    cd .worktrees/feat-pricing
    python godomall_register/pipeline_run.py [--skip-import] [--output PATH]

옵션:
    --skip-import   OC API 동기화를 건너뜀 (DB 현재 데이터 그대로 사용)
    --output PATH   출고 파일 경로 (기본: DB_save/고도몰_출고_YYYYMMDD.xlsx)
    --strategy ID   가격 전략 (기본: lowest_price)
    --dry-run       DB 조회 + 가격 계산만, 파일 저장 안 함
"""
from __future__ import annotations

import argparse
import json
import logging
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)

# 루트 경로
_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))
sys.path.insert(0, str(_ROOT / "OC_ES_converter" / "scripts"))

DEFAULT_DB_PATH = _ROOT / "DB_save" / "products.db"
DEFAULT_TEMPLATE = str(Path.home() / "Downloads" / "상품샘플파일.xlsx")


# ── DB 조회 ──────────────────────────────────────────────────────────────────

def _options_json_to_combo(options_json: str | None) -> str:
    """oc_options_json → '옵션명,추가금,재고' 텍스트."""
    if not options_json:
        return ""
    try:
        options = json.loads(options_json)
    except (ValueError, TypeError):
        return ""
    lines = []
    for opt in options:
        attrs = opt.get("optionAttributes") or []
        name = attrs[0].get("value", "기본") if attrs else "기본"
        price = opt.get("price") or 0
        qty = opt.get("quantity") or 999
        lines.append(f"{name},{price},{qty}")
    return "\n".join(lines)


def _first_image(images_json: str | None, fallback_mix: str = "",
                 fallback_nukki: str = "") -> str:
    """oc_images_json 첫 번째 URL 반환. 없으면 믹스url/누끼url 순으로 폴백."""
    if images_json:
        try:
            imgs = json.loads(images_json)
            if isinstance(imgs, list) and imgs:
                first = imgs[0]
                if isinstance(first, str):
                    return first
                if isinstance(first, dict):
                    return first.get("url", "") or first.get("src", "")
        except (ValueError, TypeError):
            pass
    return fallback_mix or fallback_nukki


def _first_product_name(names_json: str | None, fallback: str = "") -> str:
    """product_names_json 첫 번째 이름. 없으면 원본상품명 폴백."""
    if names_json:
        try:
            names = json.loads(names_json)
            if isinstance(names, list) and names:
                return names[0]
        except (ValueError, TypeError):
            pass
    return fallback


def fetch_export_products(db_path: Path) -> list[dict]:
    """출고 대상 상품 조회.

    조건: ACTIVE + oc_price IS NOT NULL + oc_price > 0
    존재하는 컬럼만 선택 (oc_* 컬럼이 없는 구버전 DB 호환).
    """
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        cursor = conn.cursor()

        # 실제 존재하는 컬럼 파악
        cursor.execute("PRAGMA table_info(products)")
        existing_cols = {row[1] for row in cursor.fetchall()}

        # 필수 + 선택 컬럼 목록
        required = ["상품코드", "원본상품명", "oc_price"]
        optional = [
            "product_names_json", "oc_options_json", "oc_images_json",
            "oc_content", "oc_search_keywords", "oc_shipping_fee",
            "oc_shipping_type", "믹스url", "누끼url",
        ]
        select_cols = required + [c for c in optional if c in existing_cols]

        col_str = ", ".join(f'"{c}"' for c in select_cols)
        cursor.execute(
            f"SELECT {col_str} FROM products "
            "WHERE product_status = 'ACTIVE' "
            "AND oc_price IS NOT NULL AND oc_price > 0"
        )
        rows = [dict(r) for r in cursor.fetchall()]
        logger.info("출고 대상: %d개 상품", len(rows))
        return rows
    finally:
        conn.close()


def build_oc_dataframe(rows: list[dict]):
    """DB 조회 결과 → convert_ownerclan_to_godomall 입력 DataFrame."""
    import pandas as pd

    records = []
    for r in rows:
        records.append({
            "상품코드":     r.get("상품코드", ""),
            "오너클랜판매가": r.get("oc_price") or 0,
            "마켓상품명":    _first_product_name(
                r.get("product_names_json"), r.get("원본상품명", "")
            ),
            "이미지대":      _first_image(
                r.get("oc_images_json"),
                r.get("mix_url", ""),
                r.get("nukki_url", ""),
            ),
            "본문상세설명":  r.get("oc_content", ""),
            "조합형옵션":    _options_json_to_combo(r.get("oc_options_json")),
            "키워드":        r.get("oc_search_keywords", ""),
            "배송비":        r.get("oc_shipping_fee") or 0,
            "배송유형":      r.get("oc_shipping_type", ""),
        })
    return pd.DataFrame(records)


# ── 파이프라인 ────────────────────────────────────────────────────────────────

def run(
    db_path: Path = DEFAULT_DB_PATH,
    output_path: str | None = None,
    strategy_id: str = "lowest_price",
    skip_import: bool = False,
    dry_run: bool = False,
    template_path: str = DEFAULT_TEMPLATE,
    progress_callback=None,
) -> dict:
    """파이프라인 실행.

    Returns:
        dict: steps 결과 요약
    """
    report = {}
    now_str = datetime.now().strftime("%Y%m%d_%H%M")

    def _log(msg):
        logger.info(msg)
        if progress_callback:
            progress_callback(msg)

    # ── Step 1: OC API → DB 입고 ─────────────────────────────────────────────
    if not skip_import:
        _log("Step 1: OC API → DB 입고")
        from godomall_register.oc_import import import_all
        import_result = import_all(
            db_path=db_path,
            progress_callback=lambda c, t, m: _log(f"  {m}"),
        )
        report["import"] = {
            "updated":   import_result.get("updated", 0),
            "not_found": import_result.get("not_found", 0),
            "errors":    len(import_result.get("errors", [])),
        }
        _log(
            f"  완료: 갱신 {import_result.get('updated', 0)}개 / "
            f"미발견 {import_result.get('not_found', 0)}개 / "
            f"오류 {len(import_result.get('errors', []))}개"
        )
    else:
        _log("Step 1: OC 입고 건너뜀 (--skip-import)")
        report["import"] = "skipped"

    # ── Step 2: DB → 출고 대상 조회 ──────────────────────────────────────────
    _log("Step 2: DB 출고 대상 조회")
    products = fetch_export_products(db_path)
    report["fetched"] = len(products)
    if not products:
        _log("  출고 대상 없음 — 종료")
        return report

    # ── Step 3: convert_ownerclan_to_godomall ────────────────────────────────
    _log(f"Step 3: 가격 계산 + 고도몰 변환 (전략: {strategy_id})")
    sys.path.insert(0, str(_ROOT / "OC_ES_converter" / "scripts"))
    from convert_godomall import convert_ownerclan_to_godomall

    oc_df = build_oc_dataframe(products)
    godomall_df, stats = convert_ownerclan_to_godomall(
        oc_df, strategy_id=strategy_id
    )
    report["converted"] = stats["converted_count"]
    _log(f"  변환 완료: {stats['converted_count']}개")

    # ── Step 4: 출고 파일 저장 ────────────────────────────────────────────────
    if dry_run:
        _log("Step 4: dry-run -파일 저장 건너뜀")
        report["output"] = None

        # 콘솔 미리보기 (5행)
        preview_cols = ["goods_price", "goods_discount", "goods_discount_fl",
                        "goods_name", "goods_cd"]
        preview_cols = [c for c in preview_cols if c in godomall_df.columns]
        print("\n=== 미리보기 (상위 5행) ===")
        print(godomall_df[preview_cols].head(5).to_string(index=False))
        return report

    if output_path is None:
        out_dir = _ROOT / "DB_save"
        output_path = str(out_dir / f"고도몰_출고_{now_str}.xlsx")

    _log(f"Step 4: 출고 파일 저장 → {output_path}")
    from convert_godomall import save_godomall
    save_result = save_godomall(godomall_df, template_path, output_path)
    report["output"] = save_result["path"]
    _log(f"  저장 완료: {save_result['path']} ({save_result['count']}행)")

    return report


# ── CLI ───────────────────────────────────────────────────────────────────────

def _parse_args():
    p = argparse.ArgumentParser(description="고도몰 최저가전략 파이프라인")
    p.add_argument("--skip-import", action="store_true",
                   help="OC API 동기화 건너뜀")
    p.add_argument("--output", default=None, help="출고 파일 경로")
    p.add_argument("--strategy", default="lowest_price",
                   choices=["lowest_price", "normal_sale", "cpc_ad"],
                   help="가격 전략 (기본: lowest_price)")
    p.add_argument("--dry-run", action="store_true",
                   help="파일 저장 없이 미리보기만")
    p.add_argument("--db", default=None, help="products.db 경로")
    return p.parse_args()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    args = _parse_args()

    db_path = Path(args.db) if args.db else DEFAULT_DB_PATH
    if not db_path.exists():
        print(f"[오류] DB 파일 없음: {db_path}")
        sys.exit(1)

    print("=" * 60)
    print("고도몰 출고 파이프라인")
    print(f"DB:       {db_path}")
    print(f"전략:     {args.strategy}")
    print(f"dry-run:  {args.dry_run}")
    print("=" * 60)

    result = run(
        db_path=db_path,
        output_path=args.output,
        strategy_id=args.strategy,
        skip_import=args.skip_import,
        dry_run=args.dry_run,
        progress_callback=print,
    )

    print("\n=== 결과 요약 ===")
    for k, v in result.items():
        print(f"  {k}: {v}")
