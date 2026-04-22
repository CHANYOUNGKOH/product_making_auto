# hub/services/market_status_service.py
"""마켓별 현황 조회 + 셀러센터 Excel import."""
from __future__ import annotations

from hub.services.db_service import _conn


def get_market_status() -> list[dict]:
    """마켓별 등록 현황 집계 (store_alias, market, 상태별 수)."""
    with _conn() as con:
        rows = con.execute(
            """SELECT store_alias, market,
                      SUM(CASE WHEN status='READY'        THEN 1 ELSE 0 END) as ready_count,
                      SUM(CASE WHEN status='UPLOADED'     THEN 1 ELSE 0 END) as uploaded_count,
                      SUM(CASE WHEN status='UPLOAD_FAILED' THEN 1 ELSE 0 END) as failed_count,
                      MAX(confirmed_at) as last_import_at
               FROM market_registrations
               GROUP BY store_alias, market
               ORDER BY market, store_alias"""
        ).fetchall()
    return [dict(r) for r in rows]


def import_from_excel(store_alias: str, market: str, excel_path: str) -> dict:
    """셀러센터 Excel import → market_registrations 갱신.

    Excel 최소 요구사항: '상품코드' 열 존재.
    import에 있는 상품코드 → UPLOADED, 기존 READY인데 import에 없으면 → UPLOAD_FAILED.
    """
    import openpyxl
    wb = openpyxl.load_workbook(excel_path, read_only=True, data_only=True)
    try:
        ws = wb.active
        rows = list(ws.iter_rows(values_only=True))
    finally:
        wb.close()

    if not rows:
        return {"updated": 0, "failed": 0, "error": "빈 파일"}

    headers = [str(h or "").strip() for h in rows[0]]
    try:
        code_idx = headers.index("상품코드")
    except ValueError:
        return {"updated": 0, "failed": 0, "error": f"'상품코드' 열 없음. 헤더: {headers[:10]}"}

    imported_codes = {
        str(row[code_idx]).strip()
        for row in rows[1:]
        if row[code_idx]
    }

    with _conn() as con:
        updated = 0
        failed = 0
        ready_rows = con.execute(
            """SELECT 상품코드, created_at FROM market_registrations
               WHERE store_alias=? AND market=? AND status='READY'""",
            [store_alias, market],
        ).fetchall()

        for row in ready_rows:
            code = row["상품코드"]
            if code in imported_codes:
                con.execute(
                    """UPDATE market_registrations SET status='UPLOADED',
                       confirmed_at=datetime('now','localtime')
                       WHERE 상품코드=? AND store_alias=? AND created_at=?""",
                    [code, store_alias, row["created_at"]],
                )
                updated += 1
            else:
                con.execute(
                    """UPDATE market_registrations SET status='UPLOAD_FAILED'
                       WHERE 상품코드=? AND store_alias=? AND created_at=?""",
                    [code, store_alias, row["created_at"]],
                )
                failed += 1

    return {"updated": updated, "failed": failed}
