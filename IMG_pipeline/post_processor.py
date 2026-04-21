"""후처리: 리사이즈 + R2 업로드 + DB 저장.

입력: 로컬 PNG 경로 + 상품코드 + lane (codex/web)
출력: 공개 URL + DB 업데이트
"""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from pathlib import Path

from PIL import Image
import boto3
from botocore.exceptions import ClientError


HERE = Path(__file__).resolve().parent
WORKTREE_ROOT = HERE.parent
PROJECT_ROOT = WORKTREE_ROOT.parent.parent.parent
DB_PATH = PROJECT_ROOT / "DB_save" / "products.db"


# Cloudflare R2 설정 (기존 IMG_stage5/cloudflare_upload_gui.py 와 동일)
import os as _os
R2_ACCOUNT_ID = _os.environ.get("R2_ACCOUNT_ID", "400d9de3ac7d9e34b7bb1b88b8d915e5")
R2_ACCESS_KEY_ID = _os.environ.get("R2_ACCESS_KEY_ID", "a895886eefaafb6f809ee29f9b6b1b7a")
R2_SECRET_ACCESS_KEY = _os.environ.get("R2_SECRET_ACCESS_KEY", "5f889aa58a3b454deb3928963d9e11686264e995bcd61e67e005ad69355abd0c")
R2_ENDPOINT_URL = f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com"
R2_BUCKET = "images001"  # 기존 버킷 (cloudflare_upload_gui.py 참조)
R2_PUBLIC_BASE = "https://pub-c3e2ead4e8884b79a78e3ea2eb6d23bf.r2.dev"
R2_KEY_PREFIX = "genai"  # 최상위 폴더


_s3 = None

def _client():
    global _s3
    if _s3 is None:
        from botocore.config import Config
        _s3 = boto3.client(
            "s3",
            endpoint_url=R2_ENDPOINT_URL,
            aws_access_key_id=R2_ACCESS_KEY_ID,
            aws_secret_access_key=R2_SECRET_ACCESS_KEY,
            region_name="auto",
            config=Config(connect_timeout=15, read_timeout=60, retries={"max_attempts": 3}),
        )
    return _s3


# ── 리사이즈 ────────────────────────────────────────────────────────────────

def resize_1000(src: Path, dest: Path, crop_bottom_right_px: int = 0) -> Path:
    """중앙 크롭 + 1000×1000 리사이즈 → JPG 저장 (오픈마켓 규정).

    dest는 .jpg 확장자 권장. quality=95, 원본 EXIF 제거.
    """
    img = Image.open(src).convert("RGB")
    w, h = img.size
    if crop_bottom_right_px > 0:
        img = img.crop((0, 0, w - crop_bottom_right_px, h - crop_bottom_right_px))
        w, h = img.size
    side = min(w, h)
    left = (w - side) // 2
    top = (h - side) // 2
    img = img.crop((left, top, left + side, top + side))
    img = img.resize((1000, 1000), Image.LANCZOS)
    dest.parent.mkdir(parents=True, exist_ok=True)
    img.save(dest, "JPEG", quality=95, optimize=True, progressive=True)
    return dest


# ── R2 업로드 ───────────────────────────────────────────────────────────────

def upload_to_r2(local_path: Path, product_code: str, lane: str,
                  max_attempts: int = 3) -> str:
    """R2 업로드 + 재시도 (지수 backoff)."""
    import time as _t
    key = f"{R2_KEY_PREFIX}/{lane}/{product_code}.jpg"
    last_err = None
    for attempt in range(1, max_attempts + 1):
        try:
            c = _client()
            c.upload_file(
                Filename=str(local_path), Bucket=R2_BUCKET, Key=key,
                ExtraArgs={"ContentType": "image/jpeg"},
            )
            return f"{R2_PUBLIC_BASE}/{key}"
        except Exception as e:
            last_err = e
            if attempt < max_attempts:
                _t.sleep(2 ** attempt)  # 2, 4, 8초
    raise last_err or RuntimeError("upload failed")


# ── DB 저장 ────────────────────────────────────────────────────────────────

_MIGRATED = False

def _ensure_columns(con: sqlite3.Connection):
    global _MIGRATED
    if _MIGRATED:
        return
    con.execute("PRAGMA journal_mode=WAL")
    cols = {r[1] for r in con.execute("PRAGMA table_info(products)").fetchall()}
    if "image_slots" not in cols:
        con.execute("ALTER TABLE products ADD COLUMN image_slots TEXT")
    if "genai_updated_at" not in cols:
        con.execute("ALTER TABLE products ADD COLUMN genai_updated_at TEXT")
    if "genai_status" not in cols:
        con.execute("ALTER TABLE products ADD COLUMN genai_status TEXT")
    if "genai_error" not in cols:
        con.execute("ALTER TABLE products ADD COLUMN genai_error TEXT")
    if "genai_locked_at" not in cols:
        con.execute("ALTER TABLE products ADD COLUMN genai_locked_at TEXT")
    if "genai_issue_memo" not in cols:
        con.execute("ALTER TABLE products ADD COLUMN genai_issue_memo TEXT")
    con.commit()
    _MIGRATED = True


def _code_col(con: sqlite3.Connection) -> str:
    cols = [r[1] for r in con.execute("PRAGMA table_info(products)").fetchall()]
    return next(c for c in cols if "상품코드" in c)


def save_to_db(product_code: str, lane: str, public_url: str,
               db_path: Path = DB_PATH) -> dict:
    con = sqlite3.connect(str(db_path), timeout=30)
    try:
        _ensure_columns(con)
        code_col = _code_col(con)
        # 원자적 read-modify-write — BEGIN IMMEDIATE로 락 선점
        con.execute("BEGIN IMMEDIATE")
        row = con.execute(
            f'SELECT image_slots FROM products WHERE "{code_col}"=?',
            (product_code,)
        ).fetchone()
        slots: dict = {}
        if row and row[0]:
            try: slots = json.loads(row[0])
            except Exception: slots = {}
        now = datetime.now().isoformat(timespec="seconds")
        slots[lane] = {"url": public_url, "generated_at": now}
        con.execute(
            f'UPDATE products SET image_slots=?, genai_updated_at=?, '
            f'genai_status=?, genai_error=NULL, genai_locked_at=NULL WHERE "{code_col}"=?',
            (json.dumps(slots, ensure_ascii=False), now, "done", product_code),
        )
        con.commit()
        return slots
    except Exception:
        try: con.rollback()
        except Exception: pass
        raise
    finally:
        con.close()


def mark_status(product_code: str, status: str, *, error: str | None = None,
                lock: bool = False, db_path: Path = DB_PATH):
    """상태 전이 — running/failed/pending/done."""
    con = sqlite3.connect(str(db_path), timeout=30)
    try:
        _ensure_columns(con)
        code_col = _code_col(con)
        now = datetime.now().isoformat(timespec="seconds")
        locked_at = now if lock else None
        con.execute(
            f'UPDATE products SET genai_status=?, genai_error=?, genai_locked_at=?, genai_updated_at=? '
            f'WHERE "{code_col}"=?',
            (status, error, locked_at, now, product_code),
        )
        con.commit()
    finally:
        con.close()


def save_issue_memo(product_code: str, memo: str, db_path: Path = DB_PATH):
    """이슈 메모 저장 + image_slots NULL 로 pending 복귀."""
    con = sqlite3.connect(str(db_path), timeout=30)
    try:
        _ensure_columns(con)
        code_col = _code_col(con)
        now = datetime.now().isoformat(timespec="seconds")
        con.execute(
            f'UPDATE products SET genai_issue_memo=?, image_slots=NULL, '
            f'genai_status=?, genai_error=?, genai_locked_at=NULL, genai_updated_at=? '
            f'WHERE "{code_col}"=?',
            (memo, "pending", None, now, product_code),
        )
        con.commit()
    finally:
        con.close()


def get_issue_memo(product_code: str, db_path: Path = DB_PATH) -> str | None:
    con = sqlite3.connect(str(db_path), timeout=30)
    try:
        _ensure_columns(con)
        code_col = _code_col(con)
        r = con.execute(
            f'SELECT genai_issue_memo FROM products WHERE "{code_col}"=?',
            (product_code,)).fetchone()
        return r[0] if r else None
    finally:
        con.close()


def reset_stale_locks(stale_minutes: int = 30, db_path: Path = DB_PATH) -> int:
    """30분+ 된 'running' 상태를 'pending'으로 복귀. GUI 시작 시 호출."""
    from datetime import timedelta
    cutoff = (datetime.now() - timedelta(minutes=stale_minutes)).isoformat(timespec="seconds")
    con = sqlite3.connect(str(db_path), timeout=30)
    try:
        _ensure_columns(con)
        cur = con.execute(
            "UPDATE products SET genai_status='pending', genai_locked_at=NULL "
            "WHERE genai_status='running' AND (genai_locked_at IS NULL OR genai_locked_at < ?)",
            (cutoff,)
        )
        con.commit()
        return cur.rowcount
    finally:
        con.close()


# ── 통합 파이프라인 ──────────────────────────────────────────────────────────

def process_one(local_png: Path, product_code: str, lane: str,
                crop_watermark: bool = False, logger=print) -> dict:
    """로컬 PNG → 리사이즈 → R2 업로드 → DB 저장.

    lane: "web" | "codex" | "gemini" etc.
    crop_watermark: Gemini 출력 등 우하단 워터마크 자르기 (80px 여유)
    """
    resized = local_png.parent / f"{local_png.stem}_1000.jpg"
    resize_1000(local_png, resized, crop_bottom_right_px=80 if crop_watermark else 0)
    logger(f"resized: {resized} ({resized.stat().st_size}B)")

    try:
        url = upload_to_r2(resized, product_code, lane)
        logger(f"uploaded: {url}")
    except ClientError as e:
        logger(f"R2 upload failed: {e}")
        raise

    slots = save_to_db(product_code, lane, url)
    logger(f"DB saved: {product_code}.image_slots = {slots}")
    return {"url": url, "slots": slots, "resized": str(resized)}


if __name__ == "__main__":
    import sys
    try: sys.stdout.reconfigure(encoding="utf-8")
    except Exception: pass
    # 자체 테스트: 기존 web_chatgpt.png 1건
    src = HERE / "codex_output" / "W701C91" / "web_chatgpt.png"
    if src.exists():
        print(process_one(src, "W701C91", "web"))
    else:
        print(f"not found: {src}")
