"""5개 샘플 입력 묶음 추출 (API 호출 없음, 카테고리 다양화).

출력:
    IMG_pipeline/poc_input/{상품코드}/
        nukki.jpg            — R2 누끼 원본
        prompt_gemini.txt    — Gemini Nano Banana용 (narrative)
        prompt_gpt.txt       — GPT-Image용 (labeled sections)
        prompt_ko.txt        — 한글 상품 정보 요약 (사람 확인)
        st2_excerpt.json     — ST2 핵심 발췌
    IMG_pipeline/poc_input/README.md
"""
from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

from IMG_pipeline.prompt_builder import build_image_prompts


HERE = Path(__file__).resolve().parent
WORKTREE_ROOT = HERE.parent
PROJECT_ROOT = WORKTREE_ROOT.parent.parent.parent
DB_PATH = PROJECT_ROOT / "DB_save" / "products.db"
OUTPUT_DIR = HERE / "poc_input"


def _top_category(path: str) -> str:
    """'생활/건강>문구/사무용품>테이프' → '생활/건강'"""
    if not path:
        return ""
    return path.split(">", 1)[0].strip()


def fetch_diverse_samples(n: int = 5) -> list[dict]:
    """카테고리 최상위 기준으로 다양화해서 n개 추출."""
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
        f'LIMIT 500'
    )
    rows = con.execute(q).fetchall()

    seen_tops: set[str] = set()
    picked: list[dict] = []
    leftover: list[dict] = []

    for r in rows:
        try:
            st2 = json.loads(r["ST2_JSON"])
        except Exception:
            continue
        cat_path = (st2.get("meta") or {}).get("카테고리_경로", "")
        top = _top_category(cat_path)
        entry = {"code": r["code"], "nukki_url": r["nukki_url"],
                 "st2": st2, "top_cat": top, "cat_path": cat_path}
        if top and top not in seen_tops:
            seen_tops.add(top)
            picked.append(entry)
            if len(picked) >= n:
                return picked
        else:
            leftover.append(entry)

    # 다양화 못 채우면 leftover로 보충
    for e in leftover:
        if len(picked) >= n:
            break
        picked.append(e)
    return picked[:n]


def download(url: str, dest: Path) -> int:
    import urllib.request
    if dest.exists() and dest.stat().st_size > 0:
        return dest.stat().st_size
    req = urllib.request.Request(url, headers={"User-Agent": "img-poc/1.0"})
    with urllib.request.urlopen(req, timeout=30) as r:
        data = r.read()
    dest.write_bytes(data)
    return len(data)


def excerpt_st2(st2: dict) -> dict:
    meta = st2.get("meta", {}) or {}
    core = st2.get("core_attributes", {}) or {}
    naming = st2.get("naming_seeds", {}) or {}
    return {
        "기본상품명": meta.get("기본상품명"),
        "카테고리_경로": meta.get("카테고리_경로"),
        "상품타입": core.get("상품타입"),
        "주요용도": core.get("주요용도"),
        "스타일/특징": core.get("스타일/특징"),
        "사용대상": core.get("사용대상"),
        "사이즈": core.get("사이즈"),
        "사용시나리오": (st2.get("usage_scenarios") or [])[:3],
        "상황/장소/계절": naming.get("상황/장소/계절"),
        "차별화포인트": naming.get("차별화포인트"),
    }


def korean_brief(st2: dict, prompts: dict) -> str:
    e = excerpt_st2(st2)
    lines = [
        f"# {e['기본상품명']}",
        f"- 카테고리: {e['카테고리_경로']}",
        f"- 상품타입: {e['상품타입']}",
        f"- 주요용도: {', '.join(e['주요용도'] or [])}",
        f"- 사용대상: {', '.join(e['사용대상'] or [])}",
        f"- 장면: {', '.join(e['상황/장소/계절'] or [])}",
        f"- 차별화: {', '.join(e['차별화포인트'] or [])}",
        "",
        "## 사용 시나리오",
    ]
    for s in e["사용시나리오"]:
        lines.append(f"- {s}")
    lines += ["", "## ALT", f"- {prompts['alt_caption']}"]
    return "\n".join(lines)


README = """# Image GenAI 품질 비교 PoC (v4 — 스킬 기반)

각 상품 폴더 내용:
- `nukki.jpg` — 누끼 원본 (R2 다운로드)
- `prompt_gemini.txt` — Gemini Nano Banana용 (서술형 한 문단)
- `prompt_gpt.txt` — GPT-Image용 (BACKGROUND/SUBJECT/DETAILS/CONSTRAINTS)
- `prompt_ko.txt` — 한글 상품 정보 요약
- `st2_excerpt.json` — ST2 핵심 발췌

## 테스트 방법

### Gemini (gemini.google.com 또는 aistudio.google.com)
1. 새 채팅, 모델 = Nano Banana / Flash Image
2. `nukki.jpg` 첨부
3. `prompt_gemini.txt` 내용 붙여넣기 → 전송
4. 결과 다운로드

### ChatGPT (chatgpt.com) — Pro 구독
1. 새 채팅
2. `nukki.jpg` 첨부
3. `prompt_gpt.txt` 내용 붙여넣기 → 전송
4. 결과 다운로드

## 후처리 (파이프라인 담당, 이 단계에선 수동)
- Gemini 결과: 우하단 ✨ 워터마크 크롭 → 1:1 중앙 크롭 → 1000×1000 리사이즈
- GPT 결과: 1000×1000 리사이즈만
- R2 업로드는 기존 `IMG_stage5/cloudflare_upload_gui.py`

## 스킬 참조
`.claude/skills/image-genai-prompt-writer/SKILL.md`
"""


def main(n: int = 5):
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    (OUTPUT_DIR / "README.md").write_text(README, encoding="utf-8")
    samples = fetch_diverse_samples(n)
    print(f"[INFO] DB: {DB_PATH}")
    print(f"[INFO] Output: {OUTPUT_DIR}")
    print(f"[INFO] Samples: {len(samples)}")

    for s in samples:
        code = s["code"]
        d = OUTPUT_DIR / code
        d.mkdir(exist_ok=True)
        print(f"\n=== {code} [{s['top_cat']}] ===")
        print(f"  cat: {s['cat_path']}")
        try:
            sz = download(s["nukki_url"], d / "nukki.jpg")
            print(f"  nukki: {sz} bytes")
        except Exception as e:
            print(f"  [ERR] nukki: {e}")

        prompts = build_image_prompts(s["st2"])
        (d / "prompt_gemini.txt").write_text(prompts["gemini_prompt"], encoding="utf-8")
        (d / "prompt_gpt.txt").write_text(prompts["gpt_prompt"], encoding="utf-8")
        (d / "prompt_ko.txt").write_text(korean_brief(s["st2"], prompts), encoding="utf-8")
        (d / "st2_excerpt.json").write_text(
            json.dumps(excerpt_st2(s["st2"]), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        print(f"  alt: {prompts['alt_caption']}")

    print(f"\n[DONE] {OUTPUT_DIR.relative_to(WORKTREE_ROOT)}")


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=5)
    main(ap.parse_args().n)
