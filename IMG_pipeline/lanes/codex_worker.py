"""Codex 레인 워커 프로세스.

multiprocessing.Process 타겟. GUI가 task_q에 Task 넣으면 순차 처리 후
event_q 로 Event 푸시. None 받으면 종료.
"""
from __future__ import annotations

import multiprocessing as mp
import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

from IMG_pipeline.post_processor import mark_status


HERE = Path(__file__).resolve().parent
IMG_PIPELINE = HERE.parent
WORKTREE_ROOT = IMG_PIPELINE.parent


def _build_prompt(prompt_path: Path, rel_out: str) -> str:
    prompt = prompt_path.read_text(encoding="utf-8")
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
    return f"{prompt}\n\n{meta}"


def _run_one(task: dict) -> dict:
    nukki = Path(task["nukki_path"])
    prompt = Path(task["prompt_path"])
    out = Path(task["out_path"])
    out.parent.mkdir(parents=True, exist_ok=True)

    rel_nukki = nukki.relative_to(WORKTREE_ROOT).as_posix()
    rel_out = out.relative_to(WORKTREE_ROOT).as_posix()
    full_prompt = _build_prompt(prompt, rel_out)

    model = task.get("model")  # None → codex 기본, "gpt-5.3-codex-spark" → Spark
    model_flag = f'-m "{model}" ' if model else ''
    cmd = (
        f'codex exec {model_flag}--enable image_generation '
        f'-i "{rel_nukki}" --sandbox workspace-write -'
    )
    t0 = time.time()
    try:
        proc = subprocess.run(
            cmd, shell=True, cwd=str(WORKTREE_ROOT),
            input=full_prompt, capture_output=True, text=True,
            timeout=task.get("timeout", 360),
            encoding="utf-8", errors="replace",
        )
        elapsed = time.time() - t0
    except subprocess.TimeoutExpired:
        return {"ok": False, "error": "timeout", "elapsed": time.time() - t0}
    except Exception as e:
        return {"ok": False, "error": f"{type(e).__name__}: {e}",
                "elapsed": time.time() - t0}

    ok = out.exists() and out.stat().st_size > 0
    import re as _re
    tokens = None
    m = _re.search(r"tokens used[\s:]*([0-9,]+)", proc.stdout or "", _re.I)
    if m:
        try: tokens = int(m.group(1).replace(",", ""))
        except Exception: pass

    out_all = (proc.stdout or "") + "\n" + (proc.stderr or "")

    # 쿼터 소진
    quota_hit = False
    for pat in ("rate limit", "quota", "usage cap", "try again later",
                "429", "usage limit", "요금제 한도", "사용량 초과"):
        if pat.lower() in out_all.lower():
            quota_hit = True
            break

    # Spark: image_gen 도구 비노출 감지 (세션별 비일관 동작)
    tool_unavailable = False
    for pat in ("노출되어 있지 않", "not exposed", "not available in this session",
                "활성화되어 있지 않", "실행 경로가 활성화"):
        if pat.lower() in out_all.lower():
            tool_unavailable = True
            break
    return {
        "ok": ok,
        "elapsed": elapsed,
        "tokens": tokens,
        "quota_hit": quota_hit,
        "tool_unavailable": tool_unavailable,
        "file_size": out.stat().st_size if ok else 0,
        "returncode": proc.returncode,
        "stderr_tail": "\n".join((proc.stderr or "").splitlines()[-3:]),
    }


def worker_loop(task_q: "mp.Queue", event_q: "mp.Queue",
                lane_name: str = "codex"):
    """메인 루프. GUI 프로세스에서 spawn됨.

    lane_name: 'codex' (기본) | 'codex_spark' (Spark 별도 쿼터)
    """
    try: sys.stdout.reconfigure(encoding="utf-8")
    except Exception: pass

    event_q.put({"kind": "log", "lane": lane_name,
                 "message": f"Codex worker started (pid={os.getpid()}, lane={lane_name})"})
    consecutive_fails = 0
    MAX_FAIL = 5
    import datetime as _dt
    last_heartbeat = _dt.datetime.now()
    while True:
        # 유휴 상태에서도 heartbeat
        try:
            task = task_q.get(timeout=30)
        except Exception:
            now = _dt.datetime.now()
            if (now - last_heartbeat).total_seconds() > 120:
                event_q.put({"kind": "log", "lane": lane_name,
                             "message": f"💓 heartbeat (idle)"})
                last_heartbeat = now
            continue
        if task is None:
            event_q.put({"kind": "done", "lane": lane_name,
                         "message": "worker exit"})
            return
        code = task["code"]
        last_heartbeat = _dt.datetime.now()
        mark_status(code, "running", error=None, lock=True)
        event_q.put({"kind": "status", "lane": lane_name,
                     "code": code, "status": "running"})
        event_q.put({"kind": "log", "lane": lane_name,
                     "message": f"[{code}] codex exec 시작"})
        retries = task.get("max_retries", 1)
        attempt = 0
        r = None
        orig_model = task.get("model")
        while attempt <= retries:
            attempt += 1
            r = _run_one(task)
            if r.get("ok"):
                break
            # Spark에서 image_gen 비노출 → 기본 모델로 자동 폴백
            if r.get("tool_unavailable") and task.get("model"):
                event_q.put({"kind": "log", "lane": lane_name,
                             "message": f"[{code}] Spark image_gen 비노출 → 기본 모델로 폴백"})
                task["model"] = None  # 기본 codex로 재실행
                r2 = _run_one(task)
                if r2.get("ok"):
                    r = r2
                    r["fallback_used"] = True
                    break
                r = r2
                # 폴백도 실패하면 model 복구 후 다음 외부 재시도로
                task["model"] = orig_model
            if attempt <= retries:
                event_q.put({"kind": "log", "lane": lane_name,
                             "message": f"[{code}] attempt {attempt} 실패 재시도: {r.get('error') or r.get('stderr_tail','')[:60]}"})
                time.sleep(3)
        if r["ok"]:
            # 후처리 (예외/타임아웃 안전)
            url = None
            event_q.put({"kind": "log", "lane": lane_name,
                         "message": f"[{code}] 후처리 시작 (리사이즈→R2→DB)"})
            try:
                from IMG_pipeline.post_processor import process_one
                result = process_one(Path(task["out_path"]), code, lane_name,
                                      logger=lambda m: event_q.put(
                                          {"kind": "log", "lane": lane_name,
                                           "message": m}))
                url = result["url"]
                event_q.put({"kind": "log", "lane": lane_name,
                             "message": f"[{code}] 후처리 완료"})
            except Exception as pe:
                mark_status(code, "failed", error=f"post-process failed: {pe}", lock=False)
                event_q.put({"kind": "log", "lane": lane_name,
                             "message": f"[{code}] post-process failed: {pe}"})
                event_q.put({
                    "kind": "status", "lane": lane_name, "code": code,
                    "status": "failed",
                    "message": f"POST-PROCESS FAIL: {pe}",
                    "meta": {**r, "url": None},
                })
                consecutive_fails += 1
                continue
            consecutive_fails = 0
            event_q.put({
                "kind": "status", "lane": lane_name, "code": code,
                "status": "done",
                "message": f"OK {r['elapsed']:.1f}s{' → ' + url if url else ''}",
                "meta": {**r, "url": url},
            })
        else:
            # 쿼터 소진 감지 시 장기 대기
            if r and r.get("quota_hit"):
                event_q.put({"kind": "log", "lane": lane_name,
                             "message": "[!] 쿼터 소진 감지 — 1시간 sleep 후 재개"})
                event_q.put({"kind": "status", "lane": lane_name, "code": code,
                             "status": "failed", "message": "RATE LIMITED"})
                mark_status(code, "failed", error="rate limited", lock=False)
                time.sleep(3600)
                continue
            consecutive_fails += 1
            mark_status(code, "failed", error=r.get("error") or r.get("stderr_tail"), lock=False)
            event_q.put({
                "kind": "status", "lane": lane_name, "code": code,
                "status": "failed",
                "message": f"FAIL: {r.get('error') or r.get('stderr_tail')}",
                "meta": r,
            })
            if consecutive_fails >= MAX_FAIL:
                event_q.put({"kind": "log", "lane": lane_name,
                             "message": f"[!] 연속 {MAX_FAIL}회 실패 — 10분 대기 후 재개"})
                event_q.put({"kind": "done", "lane": lane_name,
                             "message": f"worker pausing (consecutive fails); auto-resume in 10min"})
                time.sleep(600)
                consecutive_fails = 0
                event_q.put({"kind": "log", "lane": lane_name,
                             "message": "워커 재개"})
