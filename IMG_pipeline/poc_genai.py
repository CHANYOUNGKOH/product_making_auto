"""GenAI 1:1 썸네일 PoC — 5개 샘플로 Gemini/GPT 품질 비교.

사용:
    python -m IMG_pipeline.poc_genai --provider gemini --n 5
    python -m IMG_pipeline.poc_genai --provider openai --n 5
    python -m IMG_pipeline.poc_genai --provider both --n 5

환경변수/키파일:
    GEMINI_API_KEY  또는  stage3_product_name/.gemini_api_key_stage3_batch
    OPENAI_API_KEY  또는  .openai_api_key

출력:
    IMG_pipeline/poc_output/{상품코드}/
        nukki.jpg              (원본 누끼)
        prompt.txt             (사용 프롬프트)
        gemini_nb2_001.png     (Nano Banana 2 결과)
        openai_gpt_image_001.png (GPT-Image-1 결과)
        meta.json              (생성 메타)
"""
from __future__ import annotations

import argparse
import base64
import io
import json
import os
import sqlite3
import sys
import time
from pathlib import Path

# Windows 콘솔 한글
try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

from IMG_pipeline.prompt_builder import build_image_prompt


HERE = Path(__file__).resolve().parent
WORKTREE_ROOT = HERE.parent
PROJECT_ROOT = WORKTREE_ROOT.parent.parent.parent  # .claude/worktrees/<wt>/ → 프로젝트 루트
DB_PATH = PROJECT_ROOT / "DB_save" / "products.db"
OUTPUT_DIR = HERE / "poc_output"


# ── 키 로딩 ────────────────────────────────────────────────────────────────────

def _load_key_file(rel_path: str) -> str | None:
    p = PROJECT_ROOT / rel_path
    if p.exists() and p.stat().st_size > 0:
        return p.read_text(encoding="utf-8").strip()
    return None


def get_gemini_key() -> str | None:
    return (
        os.environ.get("GEMINI_API_KEY")
        or os.environ.get("GOOGLE_API_KEY")
        or _load_key_file("stage3_product_name/.gemini_api_key_stage3_batch")
        or _load_key_file(".gemini_api_key")
    )


def get_openai_key() -> str | None:
    return (
        os.environ.get("OPENAI_API_KEY")
        or _load_key_file(".openai_api_key")
        or _load_key_file("stage3_product_name/.openai_api_key")
    )


# ── 샘플 추출 ──────────────────────────────────────────────────────────────────

def fetch_samples(n: int = 5) -> list[dict]:
    con = sqlite3.connect(str(DB_PATH))
    con.row_factory = sqlite3.Row
    cols = [r[1] for r in con.execute("PRAGMA table_info(products)").fetchall()]
    nukki_col = [c for c in cols if c.endswith("url")][0]
    code_col = [c for c in cols if "상품코드" in c][0]
    q = (
        f'SELECT "{code_col}" AS code, "{nukki_col}" AS nukki_url, ST2_JSON '
        f'FROM products '
        f'WHERE "{nukki_col}" IS NOT NULL AND "{nukki_col}" != "" '
        f'  AND ST2_JSON IS NOT NULL AND ST2_JSON != "" '
        f'LIMIT ?'
    )
    rows = con.execute(q, (n,)).fetchall()
    samples = []
    for r in rows:
        try:
            st2 = json.loads(r["ST2_JSON"])
        except Exception:
            continue
        samples.append({"code": r["code"], "nukki_url": r["nukki_url"], "st2": st2})
    return samples


def download_nukki(url: str, dest: Path) -> bytes:
    import urllib.request
    if dest.exists() and dest.stat().st_size > 0:
        return dest.read_bytes()
    req = urllib.request.Request(url, headers={"User-Agent": "image-genai-poc/1.0"})
    with urllib.request.urlopen(req, timeout=30) as r:
        data = r.read()
    dest.write_bytes(data)
    return data


# ── Gemini Nano Banana 2 ──────────────────────────────────────────────────────

def gen_gemini(api_key: str, nukki_bytes: bytes, prompt: str, out_path: Path) -> dict:
    """gemini-3.1-flash-image-preview (Nano Banana 2) 호출.

    REST 직접 호출로 의존성 최소화.
    """
    import urllib.request

    model = "gemini-3.1-flash-image-preview"
    url = (
        f"https://generativelanguage.googleapis.com/v1beta/models/"
        f"{model}:generateContent?key={api_key}"
    )

    body = {
        "contents": [{
            "parts": [
                {"text": prompt},
                {"inline_data": {
                    "mime_type": "image/jpeg",
                    "data": base64.b64encode(nukki_bytes).decode("ascii"),
                }},
            ]
        }],
        "generationConfig": {
            "responseModalities": ["IMAGE"],
        },
    }

    req = urllib.request.Request(
        url,
        data=json.dumps(body).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    t0 = time.time()
    try:
        with urllib.request.urlopen(req, timeout=120) as r:
            resp = json.loads(r.read().decode("utf-8"))
    except Exception as e:
        return {"ok": False, "error": str(e), "elapsed": time.time() - t0}

    # parts에서 inline_data 찾아 저장
    cand = (resp.get("candidates") or [{}])[0]
    parts = (cand.get("content") or {}).get("parts") or []
    for p in parts:
        if "inlineData" in p or "inline_data" in p:
            inline = p.get("inlineData") or p.get("inline_data")
            img_b64 = inline.get("data")
            out_path.write_bytes(base64.b64decode(img_b64))
            return {"ok": True, "model": model, "elapsed": time.time() - t0}

    return {"ok": False, "error": "no image in response", "raw": resp,
            "elapsed": time.time() - t0}


# ── GPT-Image-1 ────────────────────────────────────────────────────────────────

def gen_openai(api_key: str, nukki_bytes: bytes, prompt: str, out_path: Path) -> dict:
    """gpt-image-1 (image edit) 호출."""
    import urllib.request

    boundary = "----imgpoc" + str(int(time.time() * 1000))
    body_parts = []

    def add_field(name: str, value: str):
        body_parts.append(
            f"--{boundary}\r\n"
            f'Content-Disposition: form-data; name="{name}"\r\n\r\n'
            f"{value}\r\n".encode("utf-8")
        )

    def add_file(name: str, filename: str, data: bytes, ctype: str):
        body_parts.append(
            f"--{boundary}\r\n"
            f'Content-Disposition: form-data; name="{name}"; filename="{filename}"\r\n'
            f"Content-Type: {ctype}\r\n\r\n".encode("utf-8")
        )
        body_parts.append(data)
        body_parts.append(b"\r\n")

    add_field("model", "gpt-image-1")
    add_field("prompt", prompt)
    add_field("size", "1024x1024")
    add_field("n", "1")
    add_file("image", "nukki.png", nukki_bytes, "image/png")
    body_parts.append(f"--{boundary}--\r\n".encode("utf-8"))
    body = b"".join(body_parts)

    req = urllib.request.Request(
        "https://api.openai.com/v1/images/edits",
        data=body,
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": f"multipart/form-data; boundary={boundary}",
        },
        method="POST",
    )
    t0 = time.time()
    try:
        with urllib.request.urlopen(req, timeout=180) as r:
            resp = json.loads(r.read().decode("utf-8"))
    except Exception as e:
        return {"ok": False, "error": str(e), "elapsed": time.time() - t0}

    img_b64 = (resp.get("data") or [{}])[0].get("b64_json")
    if not img_b64:
        return {"ok": False, "error": "no b64_json", "raw": resp,
                "elapsed": time.time() - t0}
    out_path.write_bytes(base64.b64decode(img_b64))
    return {"ok": True, "model": "gpt-image-1", "elapsed": time.time() - t0}


# ── 메인 ───────────────────────────────────────────────────────────────────────

def run(provider: str, n: int):
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    samples = fetch_samples(n)
    print(f"[INFO] DB: {DB_PATH}")
    print(f"[INFO] Output: {OUTPUT_DIR}")
    print(f"[INFO] Samples: {len(samples)}")

    gem_key = get_gemini_key() if provider in ("gemini", "both") else None
    oai_key = get_openai_key() if provider in ("openai", "both") else None

    if provider in ("gemini", "both") and not gem_key:
        print("[WARN] Gemini key 없음 — Gemini 스킵")
    if provider in ("openai", "both") and not oai_key:
        print("[WARN] OpenAI key 없음 — OpenAI 스킵")

    results = []
    for s in samples:
        code = s["code"]
        out_dir = OUTPUT_DIR / code
        out_dir.mkdir(exist_ok=True)
        print(f"\n=== {code} ===")

        # 누끼 다운로드
        nukki_path = out_dir / "nukki.jpg"
        try:
            nukki_bytes = download_nukki(s["nukki_url"], nukki_path)
            print(f"  nukki: {len(nukki_bytes)} bytes → {nukki_path.name}")
        except Exception as e:
            print(f"  [ERR] nukki download: {e}")
            continue

        # 프롬프트
        prom = build_image_prompt(s["st2"])
        prompt_text = prom["scene_prompt"]
        (out_dir / "prompt.txt").write_text(prompt_text, encoding="utf-8")
        (out_dir / "alt.txt").write_text(prom["alt_caption"], encoding="utf-8")
        print(f"  prompt: {prompt_text[:100]}...")

        meta = {"code": code, "nukki_url": s["nukki_url"], "prompt": prompt_text,
                "alt": prom["alt_caption"], "results": {}}

        if gem_key:
            print("  → Gemini Nano Banana 2 호출...")
            r = gen_gemini(gem_key, nukki_bytes, prompt_text,
                           out_dir / "gemini_nb2_001.png")
            print(f"    {'OK' if r['ok'] else 'FAIL'} ({r.get('elapsed', 0):.1f}s)"
                  f"{' - ' + str(r.get('error', ''))[:120] if not r['ok'] else ''}")
            meta["results"]["gemini"] = r

        if oai_key:
            print("  → GPT-Image-1 호출...")
            r = gen_openai(oai_key, nukki_bytes, prompt_text,
                           out_dir / "openai_gpt_image_001.png")
            print(f"    {'OK' if r['ok'] else 'FAIL'} ({r.get('elapsed', 0):.1f}s)"
                  f"{' - ' + str(r.get('error', ''))[:120] if not r['ok'] else ''}")
            meta["results"]["openai"] = r

        (out_dir / "meta.json").write_text(
            json.dumps(meta, ensure_ascii=False, indent=2, default=str),
            encoding="utf-8",
        )
        results.append(meta)

    # 요약
    print("\n" + "=" * 60)
    print("[SUMMARY]")
    for r in results:
        line = f"  {r['code']}: "
        for k, v in r["results"].items():
            line += f"{k}={'OK' if v.get('ok') else 'FAIL'} "
        print(line)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--provider", choices=["gemini", "openai", "both"],
                    default="both")
    ap.add_argument("--n", type=int, default=5)
    args = ap.parse_args()
    run(args.provider, args.n)
