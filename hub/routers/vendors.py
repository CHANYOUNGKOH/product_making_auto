"""공급사 관리 API."""
from __future__ import annotations

from fastapi import APIRouter, HTTPException, UploadFile, File
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from hub.services import db_service
from hub.services.db_service import get_db_path

router = APIRouter()


class VendorIn(BaseModel):
    vendor_code: str
    vendor_name: str = ""
    source: str = "oc"


@router.get("/api/vendors")
async def list_vendors():
    return db_service.get_vendors()


@router.post("/api/vendors")
async def add_vendor(body: VendorIn):
    code = body.vendor_code.strip()
    if not code:
        raise HTTPException(status_code=400, detail="vendor_code 필수")
    db_service.upsert_vendor(vendor_code=code, vendor_name=body.vendor_name.strip(), source=body.source)
    return {"ok": True}


@router.post("/api/vendors/sync-from-products")
async def sync_from_products():
    """products 테이블의 vendor_code를 vendors 테이블에 일괄 반영."""
    return db_service.sync_vendors_from_products()


@router.post("/api/vendors/import-excel")
async def import_vendors_excel(file: UploadFile = File(...)):
    data = await file.read()
    try:
        records = db_service.parse_vendors_from_excel(data)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"엑셀 파싱 실패: {e}")
    for rec in records:
        rec.pop("oc_grade_num", None)  # DB에 저장하지 않는 참고 필드 제거
        db_service.upsert_vendor(**rec)
    return {"imported": len(records)}


@router.post("/api/vendors/import-all")
async def import_all_vendors(skip_over: int = 50000):
    """등록된 공급사 전체에서 신규 상품 일괄 수집 (SSE 없이 동기 실행).

    skip_over: 상품수가 이 값 초과인 공급사는 원청추정으로 건너뜀.
    """
    vendors = db_service.get_vendors()
    total_inserted = 0
    total_updated = 0
    skipped_vendors = []
    errors = []

    for v in vendors:
        if v.get("source") != "oc":
            continue
        cnt = v.get("product_count") or 0
        if cnt > skip_over:
            skipped_vendors.append({"vendor_code": v["vendor_code"], "reason": f"상품수 {cnt} > {skip_over}"})
            continue
        try:
            from godomall_register.oc_import import import_new_from_vendor
            result = import_new_from_vendor(vendor=v["vendor_code"], db_path=get_db_path())
            total_inserted += result.get("inserted", 0)
            total_updated  += result.get("updated", 0)
            if result.get("errors"):
                errors.extend(result["errors"][:3])
            db_service.mark_vendor_imported(v["vendor_code"])
        except Exception as e:
            errors.append(f"{v['vendor_code']}: {e}")

    return {
        "inserted": total_inserted,
        "updated":  total_updated,
        "skipped_vendors": skipped_vendors,
        "errors": errors[:10],
    }


@router.post("/api/vendors/{vendor_code}/import")
async def import_from_vendor(vendor_code: str):
    """특정 공급사 신규 상품 DB 수집."""
    try:
        from godomall_register.oc_import import import_new_from_vendor
        result = import_new_from_vendor(
            vendor=vendor_code,
            db_path=get_db_path(),
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    db_service.mark_vendor_imported(vendor_code)
    return result


@router.get("/api/vendors/scan-all")
async def scan_all_vendors():
    """전체 OC 동기화 SSE — 대시보드 /api/pipeline/sync/stream 과 동일 함수.

    Pass 1(전체 키) → Pass 2(기존 갱신) → Pass 3(신규 INSERT + 공급사 발굴).
    """
    from hub.services.oc_service import sync_stream
    return StreamingResponse(
        sync_stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@router.get("/api/vendors/discover")
async def discover_vendors(grade: str = "DIAMOND1", max_pages: int = 20):
    """OC API allItems → vendorKey 그룹핑 → DB 미등록 공급사 후보 반환.

    gradeDetail.releaseRate = GOOD 우선 정렬.
    """
    try:
        import sys
        from pathlib import Path
        sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
        from godomall_register.ownerclan_client import OwnerclanClient
        client = OwnerclanClient()
        items = client.search_items(
            grade=grade,
            status="available",
            fields="key metadata",
            first=100,
            max_pages=max_pages,
            timeout=60,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"OC API 오류: {e}")

    # vendorKey 기준 그룹핑
    vendor_map: dict[str, dict] = {}
    for item in items:
        meta = item.get("metadata") or {}
        if isinstance(meta, str):
            import json
            try:
                meta = json.loads(meta)
            except Exception:
                meta = {}
        vkey = meta.get("vendorKey") or meta.get("vendor_key", "")
        if not vkey:
            continue
        if vkey not in vendor_map:
            grade_detail = meta.get("gradeDetail") or {}
            vendor_map[vkey] = {
                "vendor_code": vkey,
                "vendor_grade": meta.get("grade", ""),
                "release_rate": grade_detail.get("releaseRate", ""),
                "average_ship": grade_detail.get("averageShip", ""),
                "qna_answer": grade_detail.get("qnaAnswer", ""),
                "item_count": 0,
            }
        vendor_map[vkey]["item_count"] += 1

    # DB 등록된 코드 제외
    registered = {v["vendor_code"] for v in db_service.get_vendors()}
    candidates = [v for v in vendor_map.values() if v["vendor_code"] not in registered]

    # GOOD 출고율 우선 정렬, 그 다음 상품 수
    def _sort_key(v):
        release_score = {"GOOD": 0, "NORMAL": 1, "BAD": 2}.get(v["release_rate"], 3)
        return (release_score, -v["item_count"])

    candidates.sort(key=_sort_key)
    return candidates
