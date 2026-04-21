"""Codex CLI 기반 이미지 생성 배치 러너.

ChatGPT Pro 구독 쿼터 사용 (별도 API 결제 없음).
참조: codex exec --enable image_generation -i <nukki> --sandbox workspace-write "<prompt>"

전제:
  - `codex` CLI 설치 + `codex login` (ChatGPT) 완료
  - feature flag `image_generation` 활성화 필요 (--enable)

사용:
    python -m IMG_pipeline.codex_runner --codes W1A0F97,WFKDYA0,WFL8OVH
    python -m IMG_pipeline.codex_runner --all-pending --limit 20
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass


HERE = Path(__file__).resolve().parent
WORKTREE_ROOT = HERE.parent
POC_INPUT = HERE / "poc_input"
CODEX_OUTPUT = HERE / "codex_output"
USAGE_LOG = HERE / "codex_usage.json"


def _load_usage() -> dict:
    if USAGE_LOG.exists():
        try:
            return json.loads(USAGE_LOG.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {"runs": []}


def _save_usage(data: dict):
    USAGE_LOG.write_text(json.dumps(data, ensure_ascii=False, indent=2),
                          encoding="utf-8")


def _run_codex(code: str, nukki_path: Path, prompt_path: Path,
               out_path: Path, timeout: int = 300) -> dict:
    """단일 상품에 대해 codex exec 호출 → result.png 반환."""
    prompt = prompt_path.read_text(encoding="utf-8")
    # 상대 경로로 지정 (codex 워킹 디렉토리가 worktree 루트)
    rel_nukki = nukki_path.relative_to(WORKTREE_ROOT)
    rel_out = out_path.relative_to(WORKTREE_ROOT).as_posix()
    meta = (
        "=== INSTRUCTIONS FOR CODEX ===\n"
        "Do NOT rewrite, summarize, paraphrase, or shorten the prompt above in any way.\n"
        "Pass the ENTIRE prompt verbatim to the built-in image_gen tool in EDIT MODE "
        "(the reference image has been provided via -i).\n"
        "Set quality=high.\n"
        "Set size=1024x1024.\n"
        "The reference image is the primary source of truth for product color, label, "
        "logo, and shape — preserve them faithfully in edit mode.\n"
        f"After generation, save the PNG to `{rel_out}` (relative to current working directory). "
        "Do not post-process, crop, or modify the generated image."
    )
    full_prompt = f"{prompt}\n\n{meta}"

    # Windows: codex는 npm shim (.cmd) — shell=True 로 확장자 자동 해결
    import shlex
    nukki_rel = str(rel_nukki).replace("\\", "/")
    cmd_str = (
        f'codex exec --enable image_generation '
        f'-i "{nukki_rel}" --sandbox workspace-write -'
    )

    t0 = time.time()
    try:
        proc = subprocess.run(
            cmd_str,
            shell=True,
            cwd=str(WORKTREE_ROOT),
            input=full_prompt,
            capture_output=True,
            text=True,
            timeout=timeout,
            encoding="utf-8",
            errors="replace",
        )
        elapsed = time.time() - t0
    except subprocess.TimeoutExpired:
        return {"ok": False, "error": "timeout", "elapsed": time.time() - t0}
    except Exception as e:
        return {"ok": False, "error": f"{type(e).__name__}: {e}",
                "elapsed": time.time() - t0}

    # 토큰 사용량 파싱
    tokens = None
    for line in proc.stdout.splitlines():
        if "tokens used" in line.lower():
            try:
                tokens = int(proc.stdout.split("tokens used")[-1].split()[0].replace(",", ""))
            except Exception:
                pass
            break

    success = out_path.exists() and out_path.stat().st_size > 0
    return {
        "ok": success,
        "elapsed": elapsed,
        "tokens": tokens,
        "file_size": out_path.stat().st_size if success else 0,
        "stdout_tail": "\n".join(proc.stdout.splitlines()[-8:]),
        "stderr_tail": "\n".join(proc.stderr.splitlines()[-5:]) if proc.stderr else "",
        "returncode": proc.returncode,
    }


def run(codes: list[str], timeout: int = 300, delay_between: int = 5):
    CODEX_OUTPUT.mkdir(parents=True, exist_ok=True)
    usage = _load_usage()
    session = {
        "started_at": datetime.now().isoformat(),
        "codes": codes,
        "results": [],
    }

    for i, code in enumerate(codes, 1):
        in_dir = POC_INPUT / code
        nukki = in_dir / "nukki.jpg"
        prompt = in_dir / "prompt_gpt.txt"
        out_dir = CODEX_OUTPUT / code
        out_dir.mkdir(parents=True, exist_ok=True)
        out_file = out_dir / "result.png"

        if not nukki.exists():
            print(f"[{i}/{len(codes)}] {code}: [SKIP] nukki 없음")
            continue
        if not prompt.exists():
            print(f"[{i}/{len(codes)}] {code}: [SKIP] prompt_gpt.txt 없음")
            continue

        print(f"[{i}/{len(codes)}] {code}: 생성 중...", flush=True)
        r = _run_codex(code, nukki, prompt, out_file, timeout=timeout)
        status = "OK" if r["ok"] else "FAIL"
        tok = r.get("tokens")
        sz = r.get("file_size")
        print(f"  [{status}] {r.get('elapsed', 0):.1f}s"
              f"{', tokens=' + str(tok) if tok else ''}"
              f"{', size=' + str(sz) if sz else ''}")
        if not r["ok"]:
            print(f"  stderr: {r.get('stderr_tail', '')}")
            print(f"  stdout tail: {r.get('stdout_tail', '')}")

        session["results"].append({"code": code, **r})
        usage["runs"].append({"code": code, "at": datetime.now().isoformat(),
                              **{k: v for k, v in r.items() if k != "stdout_tail"}})
        _save_usage(usage)

        if i < len(codes) and delay_between > 0:
            time.sleep(delay_between)

    # 요약
    ok = sum(1 for r in session["results"] if r["ok"])
    total = len(session["results"])
    tot_tokens = sum(r.get("tokens") or 0 for r in session["results"])
    tot_time = sum(r.get("elapsed") or 0 for r in session["results"])
    print(f"\n=== Summary: {ok}/{total} OK ===")
    print(f"Total tokens: {tot_tokens:,}")
    print(f"Total time: {tot_time:.1f}s (avg {tot_time/max(1,total):.1f}s/ea)")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--codes", type=str,
                    help="쉼표로 구분된 상품코드 리스트 (예: W1A0F97,WFKDYA0)")
    ap.add_argument("--timeout", type=int, default=300)
    ap.add_argument("--delay", type=int, default=5,
                    help="연속 호출 사이 딜레이(초)")
    args = ap.parse_args()

    if not args.codes:
        # 기본: poc_input의 모든 폴더
        codes = sorted(d.name for d in POC_INPUT.iterdir()
                       if d.is_dir() and (d / "prompt_gpt.txt").exists())
    else:
        codes = [c.strip() for c in args.codes.split(",") if c.strip()]

    print(f"Target: {len(codes)} codes — {codes}")
    run(codes, timeout=args.timeout, delay_between=args.delay)
