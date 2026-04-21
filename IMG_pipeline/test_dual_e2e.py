"""Codex 2 + Web 2 동시 실행 E2E.

Codex 워커 프로세스와 Web 워커 프로세스 각각 구동 → 결과 수집.
"""
from __future__ import annotations

import json
import multiprocessing as mp
import sqlite3
import sys
import time
import urllib.request
from pathlib import Path
from queue import Empty


def _prepare(code, DB, cd_col, nu_col, HERE):
    con = sqlite3.connect(str(DB)); con.row_factory = sqlite3.Row
    r = con.execute(f'SELECT "{nu_col}" nu, ST2_JSON FROM products WHERE "{cd_col}"=?', (code,)).fetchone()
    con.close()
    st2 = json.loads(r["ST2_JSON"])
    from IMG_pipeline.prompt_builder import build_image_prompts
    d = HERE / "poc_input" / code
    d.mkdir(parents=True, exist_ok=True)
    nukki = d / "nukki.jpg"
    if not nukki.exists():
        req = urllib.request.Request(r["nu"], headers={"User-Agent": "p/1"})
        with urllib.request.urlopen(req, timeout=30) as rr: nukki.write_bytes(rr.read())
    p = build_image_prompts(st2)
    (d/"prompt_gpt.txt").write_text(p["gpt_prompt"], encoding="utf-8")
    return {"code": code, "nukki": nukki, "prompt": d/"prompt_gpt.txt", "name": st2["meta"]["기본상품명"]}


def _codex_entry(task_q, event_q):
    from IMG_pipeline.lanes.codex_worker import worker_loop
    worker_loop(task_q, event_q)


def _web_entry(task_q, event_q):
    from IMG_pipeline.lanes.web_worker import worker_loop
    worker_loop(task_q, event_q, account_name="chatgpt_acc1", headless=False)


def main():
    try: sys.stdout.reconfigure(encoding="utf-8")
    except Exception: pass

    HERE = Path(__file__).resolve().parent
    WT = HERE.parent
    DB = (WT.parent.parent.parent / "DB_save/products.db").resolve()

    cols_con = sqlite3.connect(str(DB))
    cols = [r[1] for r in cols_con.execute("PRAGMA table_info(products)").fetchall()]
    cols_con.close()
    cd_col = next(c for c in cols if "상품코드" in c)
    nu_col = next(c for c in cols if c.endswith("url"))

    # 새 상품 4건 (아직 안 돌린 것 고름)
    CODEX_CODES = ["W96D5A2", "WDB4D9C"]
    WEB_CODES = ["W10B7FF", "WBF29E5"]

    codex_tasks = []
    web_tasks = []
    for c in CODEX_CODES:
        p = _prepare(c, DB, cd_col, nu_col, HERE)
        out = HERE / "codex_output" / c / "codex.png"
        out.parent.mkdir(parents=True, exist_ok=True)
        if out.exists(): out.unlink()
        codex_tasks.append({"code": c, "nukki_path": str(p["nukki"]),
                            "prompt_path": str(p["prompt"]),
                            "out_path": str(out), "timeout": 360})
        print(f"CODEX prep: {c} | {p['name'][:40]}")

    for c in WEB_CODES:
        p = _prepare(c, DB, cd_col, nu_col, HERE)
        out = HERE / "codex_output" / c / "web_chatgpt.png"
        out.parent.mkdir(parents=True, exist_ok=True)
        if out.exists(): out.unlink()
        web_tasks.append({"code": c, "nukki_path": str(p["nukki"]),
                          "prompt_path": str(p["prompt"]),
                          "out_path": str(out), "timeout": 300})
        print(f"WEB   prep: {c} | {p['name'][:40]}")

    codex_q = mp.Queue()
    web_q = mp.Queue()
    event_q = mp.Queue()

    codex_p = mp.Process(target=_codex_entry, args=(codex_q, event_q), daemon=True)
    web_p = mp.Process(target=_web_entry, args=(web_q, event_q), daemon=True)
    codex_p.start(); web_p.start()
    print(f"workers started: codex={codex_p.pid}, web={web_p.pid}")

    for t in codex_tasks: codex_q.put(t)
    for t in web_tasks: web_q.put(t)
    codex_q.put(None)
    web_q.put(None)

    expected_done = len(codex_tasks) + len(web_tasks)
    done_count = 0
    t0 = time.time()
    TIMEOUT = 900

    while done_count < expected_done and (time.time() - t0) < TIMEOUT:
        try:
            ev = event_q.get(timeout=5)
        except Empty:
            continue
        k = ev.get("kind")
        lane = ev.get("lane", "?")
        if k == "log":
            print(f"[{lane}] {ev.get('message', '')}")
        elif k == "status":
            print(f"[{lane}/{ev['code']}] {ev['status']} — {ev.get('message', '')}")
            if ev["status"] in ("done", "failed"):
                done_count += 1
        elif k == "done":
            print(f"[{lane}] worker exit")

    print(f"\n=== 완료 {done_count}/{expected_done} (elapsed {time.time()-t0:.1f}s) ===")

    # DB 확인
    con = sqlite3.connect(str(DB)); con.row_factory = sqlite3.Row
    for c in CODEX_CODES + WEB_CODES:
        r = con.execute(f'SELECT image_slots FROM products WHERE "{cd_col}"=?', (c,)).fetchone()
        print(f"  {c}: image_slots = {r['image_slots']}")
    con.close()


if __name__ == "__main__":
    mp.freeze_support()
    main()
