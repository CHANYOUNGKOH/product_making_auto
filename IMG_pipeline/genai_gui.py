"""이미지 GenAI 파이프라인 GUI — 운영 최적화 버전.

기능:
 (1) 완료된 항목 숨기기
 (2) 상단 카운터 바 (조회/완료/실패/대기/진행)
 (3) 전체 선택 / 해제 / 미완료만 선택
 (4) 실패 자동 재시도 (워커 내장)
 (5) Resume — 시작 시 pending DB 자동 로드 옵션
 (6) 썸네일 미리보기 (hover)
 (8) R2 URL 복사 (우클릭)
 (9) 진행률 프로그레스바
 (11) 채팅 자동 삭제 (워커 내장)
 (12) 시간당 쿼터 카운터
 (13) 스케줄 일시정지 (실행 시간 창 지정)
"""
from __future__ import annotations

import json
import multiprocessing as mp
import sqlite3
import sys
import threading
import tkinter as tk
from collections import deque
from datetime import datetime, time as dt_time
from pathlib import Path
from queue import Empty
from tkinter import ttk, messagebox

try: sys.stdout.reconfigure(encoding="utf-8")
except Exception: pass

from IMG_pipeline.db_query import fetch_targets, count_targets, _resolve_columns
from IMG_pipeline.prompt_builder import build_image_prompts
from IMG_pipeline.prompt_feedback import get_memory_snapshot, record_issue, record_success

try:
    from PIL import Image, ImageTk
    _PIL_OK = True
except Exception:
    _PIL_OK = False


HERE = Path(__file__).resolve().parent
POC_INPUT = HERE / "poc_input"
CODEX_OUTPUT = HERE / "codex_output"
OK_REASON_DEFAULT = "OK_reason"
LANE_LABELS = {
    "codex": "Codex",
    "codex_spark": "Codex Spark",
    "web": "웹",
}
STATUS_LABELS = {
    "queued": "⏳ 대기열",
    "running": "🔄 실행 중",
    "done": "✅ 완료",
    "failed": "❌ 실패",
    "asset_failed": "❌ 자산 실패",
}


def format_lane_status(product: dict, runtime_state: dict | None = None) -> str:
    runtime_state = runtime_state or {}
    lane = runtime_state.get("lane")
    status = runtime_state.get("status")
    if lane and status:
        lane_label = LANE_LABELS.get(lane, lane)
        status_label = STATUS_LABELS.get(status, status)
        return f"{lane_label} · {status_label}"

    slots = product.get("image_slots") or {}
    if slots:
        ui_lanes = [LANE_LABELS.get(lane, lane) for lane in sorted(slots.keys())]
        return f"{','.join(ui_lanes)} · {STATUS_LABELS['done']}"

    genai_status = (product.get("genai_status") or "").strip().lower()
    if genai_status == "running":
        return STATUS_LABELS["running"]
    if genai_status == "failed":
        return STATUS_LABELS["failed"]
    return "⏳ 대기"


def _codex_entry(task_q, event_q):
    from IMG_pipeline.lanes.codex_worker import worker_loop
    worker_loop(task_q, event_q, lane_name="codex")


def _spark_entry(task_q, event_q):
    from IMG_pipeline.lanes.codex_worker import worker_loop
    worker_loop(task_q, event_q, lane_name="codex_spark")


def _web_entry(task_q, event_q, account_name):
    from IMG_pipeline.lanes.web_worker import worker_loop
    worker_loop(task_q, event_q, account_name=account_name, headless=False)


class GenAIGui:
    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title("Image GenAI Pipeline")
        self.root.geometry("1400x900")
        self.products: list[dict] = []
        self._selected: set[str] = set()
        self._codex_queue: list[str] = []
        self._spark_queue: list[str] = []
        self._web_queue: list[str] = []
        self._running = False
        self._auto_retry = True
        self._inflight: set[str] = set()  # 중복 큐잉 방지
        self._session_started_at: datetime | None = None
        self._session_tokens_total = 0
        self._current_prompt_bundle: dict[str, dict] = {}
        self._runtime_lane_state: dict[str, dict[str, str]] = {}
        self._ok_reason_tags = ["[연출좋음]", "[실사용감]", "[손자연]", "[크기정확]", "[브랜드보존]", "[구도좋음]"]

        # 쿼터 카운터 — 최근 60분 윈도우
        self._hourly_events: deque[tuple[datetime, str]] = deque()

        # multiprocessing — 3 lanes
        self.codex_task_q: mp.Queue = mp.Queue()
        self.spark_task_q: mp.Queue = mp.Queue()
        self.web_task_q: mp.Queue = mp.Queue()
        self.event_q: mp.Queue = mp.Queue()
        self.codex_proc: mp.Process | None = None
        self.spark_proc: mp.Process | None = None
        self.web_proc: mp.Process | None = None

        self._build_ui()
        self._recover_stale_genai_locks()
        self._refresh_stats()
        self.root.after(200, self._poll_events)
        self.root.after(5000, self._tick_hourly)
        self.root.protocol("WM_DELETE_WINDOW", self._on_close)

    # ── UI ─────────────────────────────────────────────────────────────────
    def _recover_stale_genai_locks(self):
        try:
            from IMG_pipeline.post_processor import reset_stale_locks
            reset_count = reset_stale_locks(stale_minutes=30)
            if reset_count:
                self._log(f"[startup] stale running -> pending reset: {reset_count}")
        except Exception as exc:
            self._log(f"[startup] stale lock reset failed: {exc}")

    def _build_ui(self):
        # 상단 필터 바
        top = ttk.Frame(self.root, padding=6)
        top.pack(fill="x")

        ttk.Label(top, text="카테고리:").pack(side="left")
        self.cat_var = tk.StringVar(value="")
        ttk.Combobox(top, textvariable=self.cat_var, width=18,
                     values=["", "생활/건강", "패션잡화", "디지털/가전",
                             "가구/인테리어", "패션의류", "출산/육아",
                             "스포츠/레저", "식품", "화장품/미용"]).pack(side="left", padx=4)

        ttk.Label(top, text="LIMIT:").pack(side="left", padx=(8, 2))
        self.limit_var = tk.StringVar(value="200")
        ttk.Entry(top, textvariable=self.limit_var, width=8).pack(side="left")

        # 뷰 모드: 대기/완료/전체
        self.view_mode = tk.StringVar(value="대기")
        for lbl, val in (("대기(실행)", "대기"), ("완료(리뷰)", "완료"), ("전체", "전체")):
            ttk.Radiobutton(top, text=lbl, variable=self.view_mode, value=val,
                             command=self._on_view_change).pack(side="left", padx=2)

        # hide_done = view_mode='대기'과 동일 의미로 유지 (기존 호출부 호환)
        self.hide_done = tk.BooleanVar(value=True)

        ttk.Button(top, text="조회", command=self._on_query).pack(side="left", padx=6)
        ttk.Button(top, text="pending 자동 로드", command=self._on_load_pending).pack(side="left", padx=4)

        # 통계 카운터
        self.stat_lbl = ttk.Label(top, text="조회 0 | 완료 0 | 실패 0 | 대기 0 | 진행 0",
                                   font=("Segoe UI", 9, "bold"))
        self.stat_lbl.pack(side="right")

        # 실행 제어 바
        mid = ttk.Frame(self.root, padding=6)
        mid.pack(fill="x")

        ttk.Label(mid, text="분배% (codex/spark/web):").pack(side="left")
        self.codex_pct = tk.IntVar(value=20)
        self.spark_pct = tk.IntVar(value=10)
        self.web_pct = tk.IntVar(value=70)
        ttk.Spinbox(mid, from_=0, to=100, increment=5,
                    textvariable=self.codex_pct, width=4).pack(side="left", padx=2)
        ttk.Spinbox(mid, from_=0, to=100, increment=5,
                    textvariable=self.spark_pct, width=4).pack(side="left", padx=2)
        ttk.Spinbox(mid, from_=0, to=100, increment=5,
                    textvariable=self.web_pct, width=4).pack(side="left", padx=2)

        self.retry_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(mid, text="실패 1회 재시도", variable=self.retry_var).pack(side="left", padx=6)

        self.autodelete_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(mid, text="채팅 자동 삭제", variable=self.autodelete_var).pack(side="left", padx=6)

        # 스케줄 시간 창
        ttk.Label(mid, text="실행 시간창:").pack(side="left", padx=(8, 2))
        self.sched_start = tk.StringVar(value="00:00")
        self.sched_end = tk.StringVar(value="23:59")
        ttk.Entry(mid, textvariable=self.sched_start, width=6).pack(side="left")
        ttk.Label(mid, text="~").pack(side="left")
        ttk.Entry(mid, textvariable=self.sched_end, width=6).pack(side="left")

        self.start_btn = ttk.Button(mid, text="▶ 시작", command=self._on_start)
        self.start_btn.pack(side="left", padx=10)
        self.stop_btn = ttk.Button(mid, text="■ 중단", command=self._on_stop, state="disabled")
        self.stop_btn.pack(side="left")

        # 시간당 카운터 + 프로그레스 + 세션 통계
        self.hourly_lbl = ttk.Label(mid, text="최근1h: codex 0 / web 0")
        self.hourly_lbl.pack(side="right", padx=6)
        self.progress = ttk.Progressbar(mid, length=200, mode="determinate")
        self.progress.pack(side="right", padx=6)
        self.session_lbl = ttk.Label(self.root, text="세션: 아직 시작 안 함",
                                       anchor="w", font=("Segoe UI", 9), foreground="#555")
        self.session_lbl.pack(fill="x", padx=8)

        # 선택 버튼들
        sel_fr = ttk.Frame(self.root, padding=(6, 0))
        sel_fr.pack(fill="x")
        ttk.Button(sel_fr, text="전체 선택", command=lambda: self._select_all(True)).pack(side="left", padx=2)
        ttk.Button(sel_fr, text="선택 해제", command=lambda: self._select_all(False)).pack(side="left", padx=2)
        ttk.Button(sel_fr, text="미완료만 선택", command=self._select_pending_only).pack(side="left", padx=2)
        self.selected_lbl = ttk.Label(sel_fr, text="선택 0")
        self.selected_lbl.pack(side="right")

        # 테이블
        tf = ttk.Frame(self.root, padding=6)
        tf.pack(fill="both", expand=True)
        cols = ("sel", "code", "category", "name", "lane_status", "r2_url")
        self.tree = ttk.Treeview(tf, columns=cols, show="headings", selectmode="extended", height=8)
        self.tree.heading("sel", text="☐")
        self.tree.heading("code", text="상품코드")
        self.tree.heading("category", text="카테고리")
        self.tree.heading("name", text="상품명")
        self.tree.heading("lane_status", text="레인 / 상태")
        self.tree.heading("r2_url", text="R2 URL")
        self.tree.column("sel", width=30, anchor="center")
        self.tree.column("code", width=90)
        self.tree.column("category", width=180)
        self.tree.column("name", width=320)
        self.tree.column("lane_status", width=170)
        self.tree.column("r2_url", width=300)
        self.tree.pack(side="left", fill="both", expand=True)
        sb = ttk.Scrollbar(tf, orient="vertical", command=self.tree.yview)
        sb.pack(side="right", fill="y")
        self.tree.configure(yscrollcommand=sb.set)
        self.tree.bind("<Button-1>", self._on_click)
        self.tree.bind("<Double-1>", self._on_double_click)
        self.tree.bind("<Button-3>", self._on_right_click)
        # 툴팁 (hover 프리뷰는 이미지 간단 표시는 복잡하니 경로 팝업)
        self.tree.bind("<Motion>", self._on_hover)
        self._hover_row = None
        self._tooltip = None

        # 우클릭 메뉴
        self._ctx_menu = tk.Menu(self.root, tearoff=0)
        self._ctx_menu.add_command(label="결과 열기 (Codex)", command=lambda: self._ctx_open("codex"))
        self._ctx_menu.add_command(label="결과 열기 (Web)", command=lambda: self._ctx_open("web"))
        self._ctx_menu.add_command(label="결과 폴더 열기", command=self._ctx_open_folder)
        self._ctx_menu.add_separator()
        self._ctx_menu.add_command(label="R2 URL 복사 (web)", command=lambda: self._ctx_copy_url("web"))
        self._ctx_menu.add_command(label="R2 URL 복사 (codex)", command=lambda: self._ctx_copy_url("codex"))
        self._ctx_menu.add_separator()
        self._ctx_menu.add_command(label="누끼 이미지 열기", command=self._ctx_open_nukki)
        self._ctx_menu.add_separator()
        self._ctx_menu.add_command(label="⚠ 이슈 보고 + 재생성 큐", command=self._ctx_report_issue)
        self._ctx_target: str | None = None

        # 리뷰 패널 — 단순 2뷰 (누끼 + 결과)
        prev_outer = ttk.LabelFrame(self.root, text="리뷰 (← / →  또는 휠로 이동)")
        prev_outer.pack(fill="x", padx=6, pady=(0, 4))

        nav = ttk.Frame(prev_outer); nav.pack(fill="x", padx=4, pady=2)
        ttk.Button(nav, text="◀ (←)", width=5,
                    command=lambda: self._review_nav(-1)).pack(side="left")
        ttk.Button(nav, text="(→) ▶", width=5,
                    command=lambda: self._review_nav(1)).pack(side="left", padx=4)
        self.review_lbl = ttk.Label(nav, text="", font=("Segoe UI", 10, "bold"))
        self.review_lbl.pack(side="left", padx=10)

        # 정렬 토글 (기본: 완료순 최신)
        self.sort_mode = tk.StringVar(value="완료순")
        ttk.Radiobutton(nav, text="완료순(최신)", variable=self.sort_mode,
                         value="완료순", command=self._redraw_table).pack(side="right", padx=2)
        ttk.Radiobutton(nav, text="대기순", variable=self.sort_mode,
                         value="대기순", command=self._redraw_table).pack(side="right", padx=2)

        self.thumb_size_var = tk.IntVar(value=500)
        ttk.Label(nav, text="크기").pack(side="right", padx=(10, 2))
        ttk.Spinbox(nav, from_=200, to=700, increment=40, width=5,
                     textvariable=self.thumb_size_var,
                     command=lambda: self._on_row_select()).pack(side="right")

        # 2컬럼 (누끼 + 결과)
        row_fr = ttk.Frame(prev_outer); row_fr.pack(fill="x", padx=4, pady=2)
        self.preview_nukki = ttk.Label(row_fr, text="누끼")
        self.preview_result = ttk.Label(row_fr, text="결과")
        self.preview_nukki.pack(side="left", padx=6, pady=4, anchor="n")
        self.preview_result.pack(side="left", padx=6, pady=4, anchor="n")

        # 메타 라인 (어떤 레인에서 나왔는지 작게)
        self.result_meta = ttk.Label(prev_outer, text="", font=("Segoe UI", 9),
                                       foreground="#555")
        self.result_meta.pack(fill="x", padx=8)
        prompt_box = ttk.LabelFrame(prev_outer, text="Prompt Loop")
        prompt_box.pack(fill="x", padx=8, pady=(4, 2))
        self.prompt_meta = ttk.Label(prompt_box, text="", font=("Segoe UI", 9), foreground="#666")
        self.prompt_meta.pack(fill="x", padx=6, pady=(4, 2))
        self.prompt_view = tk.Text(prompt_box, height=8, wrap="word")
        self.prompt_view.pack(fill="x", padx=6, pady=(0, 6))

        # 판정 버튼 1세트 통합
        self._verdict_tags = ["[4분할]","[사이즈오인]","[연출없음]","[효과만]",
                              "[라벨깨짐]","[인종]","[얼굴]","[과묘사]","[품질낮음]","[기타]"]
        verdict_fr = ttk.Frame(prev_outer); verdict_fr.pack(fill="x", padx=8, pady=4)
        ttk.Button(verdict_fr, text="👍 OK (→)", width=12,
                    command=self._verdict_ok_current).pack(side="left", padx=2)
        self.tag_var = tk.StringVar(value="태그 선택")
        ttk.Combobox(verdict_fr, textvariable=self.tag_var, values=self._verdict_tags,
                      width=14, state="readonly").pack(side="left", padx=2)
        self.feedback_note_var = tk.StringVar()
        ttk.Entry(verdict_fr, textvariable=self.feedback_note_var, width=42).pack(side="left", padx=4)
        ttk.Button(verdict_fr, text="⚠ 이슈 저장 + 재생성",
                    command=self._verdict_issue_current).pack(side="left", padx=2)
        ttk.Label(verdict_fr, text="(단축키: Enter=OK, i=이슈)",
                   foreground="#888").pack(side="right")

        self.ok_tag_var = tk.StringVar(value=OK_REASON_DEFAULT)
        ttk.Combobox(verdict_fr, textvariable=self.ok_tag_var, values=self._ok_reason_tags,
                      width=14, state="readonly").pack(side="left", padx=2)
        self._preview_imgs: dict = {}
        self.tree.bind("<<TreeviewSelect>>", self._on_row_select)
        self.root.bind("<Left>", lambda e: self._review_nav(-1))
        self.root.bind("<Right>", lambda e: self._review_nav(1))
        self.root.bind("<Return>", lambda e: self._verdict_ok_current())
        self.root.bind("i", lambda e: self._verdict_issue_current() if self.tag_var.get() != "태그 선택" else None)

        # 로그
        bot = ttk.Frame(self.root, padding=6)
        bot.pack(fill="x")
        lf = ttk.LabelFrame(bot, text="로그")
        lf.pack(fill="x", expand=True)
        self.log = tk.Text(lf, height=4, wrap="none")
        self.log.pack(side="left", fill="both", expand=True)
        lsb = ttk.Scrollbar(lf, orient="vertical", command=self.log.yview)
        lsb.pack(side="right", fill="y")
        self.log.configure(yscrollcommand=lsb.set)

    # ── 썸네일 미리보기 ───────────────────────────────────────────────────
    _thumb_cache: dict = {}

    def _thumb(self, path: Path, size: int | None = None):
        size = size or getattr(self, "thumb_size_var", None)
        size = size.get() if hasattr(size, "get") else (size or 400)
        if not _PIL_OK or not path.exists(): return None
        key = (str(path), path.stat().st_mtime_ns, size)
        if key in self._thumb_cache: return self._thumb_cache[key]
        try:
            with Image.open(path) as im:
                im = im.copy()
                im.thumbnail((size, size))
                ph = ImageTk.PhotoImage(im)
            self._thumb_cache[key] = ph
            if len(self._thumb_cache) > 100:
                # LRU 간단 — 임의 하나 제거
                self._thumb_cache.pop(next(iter(self._thumb_cache)))
            return ph
        except Exception:
            return None

    def _set_label_image(self, label, path: Path, title: str):
        img = self._thumb(path)
        if img:
            label.config(image=img, text="", compound="top")
            self._preview_imgs[title] = img
        else:
            label.config(image="", text=f"{title}\n(없음)")

    def _review_nav(self, direction: int):
        """이전/다음 행으로 이동."""
        kids = self.tree.get_children()
        if not kids: return
        cur_sel = self.tree.selection()
        if not cur_sel:
            self.tree.selection_set(kids[0])
            self.tree.focus(kids[0])
            self.tree.see(kids[0])
            return
        try:
            idx = kids.index(cur_sel[0])
        except ValueError:
            idx = 0
        new_idx = (idx + direction) % len(kids)
        new_code = kids[new_idx]
        self.tree.selection_set(new_code)
        self.tree.focus(new_code)
        self.tree.see(new_code)

    # ── 통합 판정 ────────────────────────────────────────────────────────
    def _current_result_lane(self, code: str) -> str | None:
        slots = self._product_slots(code)
        # 우선순위: web > codex > codex_spark (실제 어떤 게 보이는지 기준)
        for k in ("web", "codex", "codex_spark"):
            if k in slots: return k
        return None

    def _verdict_ok_current(self):
        sel = self.tree.selection()
        if not sel: return
        code = sel[0]
        lane = self._current_result_lane(code) or "?"
        self._log(f"[판정 OK] {code} / {lane}")
        reason = self.ok_tag_var.get() if hasattr(self, "ok_tag_var") else ""
        note = self.feedback_note_var.get().strip() if hasattr(self, "feedback_note_var") else ""
        product = self._product_by_code(code) or {}
        prompt_meta = self._current_prompt_bundle.get(code) or {}
        product_class = prompt_meta.get("_product_class") or product.get("_product_class") or "generic"
        if reason.lower().startswith("ok"):
            reason = ""
        if reason and reason != OK_REASON_DEFAULT:
            record_success(reason, product_class=product_class, lane=lane, code=code, note=note)
        elif note:
            record_success("[실사용감]", product_class=product_class, lane=lane, code=code, note=note)
        if hasattr(self, "feedback_note_var"):
            self.feedback_note_var.set("")
        self._refresh_prompt_panel(code)
        self._review_nav(1)

    def _verdict_issue_current(self):
        sel = self.tree.selection()
        if not sel: return
        code = sel[0]
        lane = self._current_result_lane(code) or "unknown"
        tag = self.tag_var.get()
        if not tag or tag == "태그 선택":
            messagebox.showwarning("태그 선택", "먼저 이슈 태그 선택")
            return
        self._verdict_issue(lane, tag)
        self.tag_var.set("태그 선택")

    def _verdict_ok(self, lane: str):
        sel = self.tree.selection()
        if not sel: return
        code = sel[0]
        self._log(f"[판정 OK] {code} / {lane}")
        self._review_nav(1)

    def _requeue_task(self, code: str, lane: str):
        """이슈 저장 후 즉시 해당 레인에 재큐잉. 워커가 살아있으면 바로, 없으면 자동 시작."""
        task = self._prepare_assets(code, lane)
        if not task:
            self._log(f"[재큐잉 실패] {code} 자산 준비 실패")
            return False
        if lane == "codex_spark":
            task["model"] = "gpt-5.3-codex-spark"
        task["max_retries"] = 1
        task["auto_delete_chat"] = self.autodelete_var.get() if hasattr(self,"autodelete_var") else True
        q_map = {"codex": self.codex_task_q,
                 "codex_spark": self.spark_task_q,
                 "web": self.web_task_q}
        proc_map = {"codex": (self.codex_proc, _codex_entry, (self.codex_task_q, self.event_q)),
                    "codex_spark": (self.spark_proc, _spark_entry, (self.spark_task_q, self.event_q)),
                    "web": (self.web_proc, _web_entry, (self.web_task_q, self.event_q, "chatgpt_acc1"))}
        q = q_map.get(lane)
        if q is None: return False
        # 워커 살아있나 확인, 없으면 시작
        proc, entry, args = proc_map[lane]
        if proc is None or not proc.is_alive():
            new_proc = mp.Process(target=entry, args=args, daemon=True)
            new_proc.start()
            if lane == "codex": self.codex_proc = new_proc
            elif lane == "codex_spark": self.spark_proc = new_proc
            else: self.web_proc = new_proc
            self._log(f"워커 재시작: {lane} pid={new_proc.pid}")
        self._inflight.add(code)
        q.put(task)
        self._ui_update_status(code, lane, "queued")
        self._log(f"[재큐잉] {code} → {lane}")
        return True

    def _verdict_issue(self, lane: str, tag: str):
        sel = self.tree.selection()
        if not sel: return
        code = sel[0]
        if not tag or tag == "태그 선택":
            messagebox.showwarning("태그 선택", "이슈 태그 먼저 선택하세요")
            return
        note = self.feedback_note_var.get().strip() if hasattr(self, "feedback_note_var") else ""
        memo = f"{tag} (lane={lane})"
        if note:
            memo += f" | {note}"
        try:
            from IMG_pipeline.post_processor import save_issue_memo, _client, R2_BUCKET
            save_issue_memo(code, memo)
            prompt_meta = self._current_prompt_bundle.get(code) or {}
            product_class = prompt_meta.get("_product_class") or "generic"
            record_issue(tag, product_class=product_class, lane=lane, code=code, note=note)
            # R2 + 로컬 정리 (해당 lane만)
            key_lane = lane
            try:
                _client().delete_object(Bucket=R2_BUCKET, Key=f"genai/{key_lane}/{code}.jpg")
            except Exception: pass
            # 해당 lane 파일만 삭제
            suffix_map = {"codex": "codex", "codex_spark": "codex_spark",
                          "web": "web_chatgpt"}
            suffix = suffix_map.get(lane, lane)
            for fn in [f"{suffix}.png", f"{suffix}_1000.jpg"]:
                p = CODEX_OUTPUT / code / fn
                if p.exists():
                    try: p.unlink()
                    except Exception: pass
            # products 캐시 갱신
            for p in self.products:
                if p["code"] == code:
                    slots = p.get("image_slots") or {}
                    slots.pop(lane, None)
                    p["image_slots"] = slots
                    p["genai_status"] = "pending"
                    p["genai_error"] = ""
                    break
            try:
                self.tree.set(code, "r2_url", "")
            except Exception: pass
            self._clear_runtime_lane_state(code)
            self._refresh_tree_lane_status(code)
            self._log(f"[판정 이슈] {code} / {lane} / {memo}")
            # 즉시 재큐잉
            self._requeue_task(code, lane)
        except Exception as e:
            messagebox.showerror("저장 실패", str(e))
        if hasattr(self, "feedback_note_var"):
            self.feedback_note_var.set("")
        self._refresh_prompt_panel(code)
        self._review_nav(1)

    def _on_row_select(self, event=None):
        sel = self.tree.selection()
        if not sel: return
        code = sel[0]
        if not self.tree.exists(code): return
        kids = self.tree.get_children()
        idx = kids.index(code) if code in kids else -1
        name = ""
        for p in self.products:
            if p["code"] == code:
                name = p["name"][:60]
                break
        self.review_lbl.config(text=f"[{idx+1}/{len(kids)}] {code}  {name}")
        if hasattr(self, "tag_var"): self.tag_var.set("태그 선택")
        if hasattr(self, "feedback_note_var"): self.feedback_note_var.set("")

        # 누끼
        if hasattr(self, "ok_tag_var"): self.ok_tag_var.set(OK_REASON_DEFAULT)
        self._refresh_prompt_panel(code)
        nk = self._nukki_path(code)
        if not nk.exists() or nk.stat().st_size == 0:
            p = self._product_by_code(code)
            url = p.get("nukki_url") if p else None
            if url:
                threading.Thread(target=self._fetch_remote_to,
                                  args=(url, nk, code, "nukki"),
                                  daemon=True).start()
            self.preview_nukki.config(image="", text="누끼\n(로딩...)")
        else:
            self._set_label_image(self.preview_nukki, nk, "누끼")

        # 결과 — 우선순위: web > codex > codex_spark
        slots = self._product_slots(code)
        lane = None
        for k in ("web", "codex", "codex_spark"):
            if k in slots: lane = k; break
        if lane:
            lane_file_map = {
                "web": ("web_chatgpt_1000.jpg", "web_chatgpt.png"),
                "codex": ("codex_1000.jpg", "codex.png"),
                "codex_spark": ("codex_spark_1000.jpg", "codex_spark.png"),
            }
            jpg_name, png_name = lane_file_map[lane]
            local_jpg = CODEX_OUTPUT / code / jpg_name
            local_png = CODEX_OUTPUT / code / png_name
            if local_jpg.exists():
                self._set_label_image(self.preview_result, local_jpg, "결과")
            elif local_png.exists():
                self._set_label_image(self.preview_result, local_png, "결과")
            else:
                url = (slots.get(lane) or {}).get("url")
                if url:
                    threading.Thread(target=self._fetch_remote_to,
                                      args=(url, local_jpg, code, lane),
                                      daemon=True).start()
                    self.preview_result.config(image="", text="결과\n(로딩...)")
                else:
                    self.preview_result.config(image="", text="결과\n(없음)")
            ts = (slots.get(lane) or {}).get("generated_at", "")
            lane_label = LANE_LABELS.get(lane, lane)
            self.result_meta.config(text=f"레인: {lane_label}  |  생성: {ts}")
        else:
            self.preview_result.config(image="", text="결과\n(아직 생성 전)")
            self.result_meta.config(text="")

    def _render_prompt_panel(self, code: str, prompts: dict | None = None):
        if not hasattr(self, "prompt_view"):
            return
        prompts = prompts or self._current_prompt_bundle.get(code) or {}
        product = self._product_by_code(code) or {}
        product_class = prompts.get("_product_class") or product.get("_product_class") or "generic"
        why = prompts.get("_why_this_image") or ""
        memory_lines = prompts.get("_memory_lines") or []
        snapshot = get_memory_snapshot(product_class)
        top_issue_counts = sorted(snapshot.get("issue_counts", {}).items(), key=lambda item: (-item[1], item[0]))[:3]
        top_success_counts = sorted(snapshot.get("success_counts", {}).items(), key=lambda item: (-item[1], item[0]))[:3]
        reinforce_rules = snapshot.get("reinforce_rules", [])
        avoid_rules = snapshot.get("avoid_rules", [])
        candidate_reinforce_rules = snapshot.get("candidate_reinforce_rules", [])
        candidate_avoid_rules = snapshot.get("candidate_avoid_rules", [])
        recent_notes = snapshot.get("recent_notes", [])

        meta_parts = [f"분류: {product_class}"]
        if prompts.get("_memory_updated_at"):
            meta_parts.append(f"메모리 갱신: {prompts['_memory_updated_at']}")
        self.prompt_meta.config(text=" | ".join(meta_parts))

        lines = []
        lines.append("학습 루프")
        lines.append(
            f"승격 규칙: 강화 {len(reinforce_rules)} / 회피 {len(avoid_rules)} | "
            f"후보 규칙: 강화 {len(candidate_reinforce_rules)} / 회피 {len(candidate_avoid_rules)}"
        )
        if snapshot.get("recent_events"):
            last = snapshot["recent_events"][-1]
            last_lane = LANE_LABELS.get(last.get("lane", "?"), last.get("lane", "?"))
            lines.append(
                f"마지막 피드백: {last.get('kind', '?')} {last.get('tag', '')} "
                f"({last.get('product_class', 'generic')}, {last_lane})"
            )
        lines.append("")
        if why:
            lines.append("왜 이 이미지를 만드는가")
            lines.append(why)
            lines.append("")
        if memory_lines:
            lines.append("압축된 프롬프트 메모리")
            lines.extend(f"- {line}" for line in memory_lines)
            lines.append("")
        if candidate_reinforce_rules or candidate_avoid_rules:
            lines.append("검토 중인 후보 규칙")
            lines.extend(f"- 강화 후보: {line}" for line in candidate_reinforce_rules)
            lines.extend(f"- 회피 후보: {line}" for line in candidate_avoid_rules)
            lines.append("")
        if top_issue_counts:
            lines.append("상위 이슈 태그")
            lines.extend(f"- {tag}: {count}" for tag, count in top_issue_counts)
            lines.append("")
        if top_success_counts:
            lines.append("상위 성공 태그")
            lines.extend(f"- {tag}: {count}" for tag, count in top_success_counts)
            lines.append("")
        if recent_notes:
            lines.append("최근 메모 신호")
            for item in reversed(recent_notes[-3:]):
                lines.append(
                    f"- {item.get('kind', '?')} {item.get('tag', '')}: {item.get('note', '')[:90]}"
                )
            lines.append("")
        gpt_prompt = prompts.get("gpt_prompt", "")
        if gpt_prompt:
            lines.append("현재 GPT 프롬프트 미리보기")
            lines.append(gpt_prompt[:900] + ("..." if len(gpt_prompt) > 900 else ""))
        self.prompt_view.delete("1.0", "end")
        self.prompt_view.insert("1.0", "\n".join(lines).strip())

    def _refresh_prompt_panel(self, code: str):
        product = self._product_by_code(code)
        if not product:
            return
        prompts = build_image_prompts(product["st2"])
        self._current_prompt_bundle[code] = prompts
        self._render_prompt_panel(code, prompts)

    def _product_by_code(self, code):
        for p in self.products:
            if p["code"] == code: return p
        return None

    def _fetch_remote_to(self, url: str, dest: Path, code: str, slot: str):
        import urllib.request
        try:
            dest.parent.mkdir(parents=True, exist_ok=True)
            req = urllib.request.Request(url, headers={"User-Agent": "img-gui/1.0"})
            with urllib.request.urlopen(req, timeout=20) as r:
                dest.write_bytes(r.read())
            def _refresh():
                cur = self.tree.selection()
                if cur and cur[0] == code:
                    label_map = {
                        "nukki": self.preview_nukki,
                        "codex": self.preview_result,
                        "codex_spark": self.preview_result,
                        "web": self.preview_result,
                    }
                    label = label_map.get(slot)
                    if label is not None:
                        self._set_label_image(label, dest, slot.capitalize())
            self.root.after(0, _refresh)
        except Exception as e:
            self.root.after(0, lambda: self._log(f"preview fetch 실패 {slot}/{code}: {e}"))

    # ── 경로 헬퍼 ──────────────────────────────────────────────────────────
    def _codex_out(self, code): return CODEX_OUTPUT / code / "codex.png"
    def _web_out(self, code): return CODEX_OUTPUT / code / "web_chatgpt.png"
    def _nukki_path(self, code): return POC_INPUT / code / "nukki.jpg"

    def _product_slots(self, code: str) -> dict:
        p = next((x for x in self.products if x["code"] == code), None)
        return p.get("image_slots") or {} if p else {}

    def _product_runtime_state(self, code: str) -> dict:
        return self._runtime_lane_state.get(code, {})

    def _set_runtime_lane_state(self, code: str, *, lane: str | None = None,
                                status: str | None = None, message: str = ""):
        state = dict(self._runtime_lane_state.get(code, {}))
        if lane is not None:
            state["lane"] = lane
        if status is not None:
            state["status"] = status
        if message:
            state["message"] = message
        self._runtime_lane_state[code] = state

    def _clear_runtime_lane_state(self, code: str):
        self._runtime_lane_state.pop(code, None)

    def _refresh_tree_lane_status(self, code: str):
        if not self.tree.exists(code):
            return
        product = self._product_by_code(code) or {"image_slots": {}}
        try:
            self.tree.set(code, "lane_status",
                          format_lane_status(product, self._product_runtime_state(code)))
        except Exception:
            pass

    # ── 조회 ─────────────────────────────────────────────────────────────
    def _refresh_stats(self):
        try:
            total = count_targets(category_contains=self.cat_var.get() or None)
        except Exception:
            total = 0
        self._base_total = total

    def _on_view_change(self):
        # 대기 모드면 hide_done=True, 아니면 False
        self.hide_done.set(self.view_mode.get() == "대기")
        # 로컬 재필터 (재쿼리 불필요)
        if self.products:
            self._redraw_table()

    def _filter_by_view(self, rows):
        mode = self.view_mode.get()
        if mode == "대기":
            return [r for r in rows if not (r.get("image_slots") or {})]
        if mode == "완료":
            return [r for r in rows if (r.get("image_slots") or {})]
        return rows

    def _on_query(self):
        try: limit = int(self.limit_var.get() or 200)
        except ValueError: limit = 200
        cat = self.cat_var.get() or None
        mode = self.view_mode.get()
        # 대기만 보기면 SQL에서도 exclude_done 적용 (성능)
        exclude = (mode == "대기")
        rows = fetch_targets(limit=limit, category_contains=cat,
                              exclude_done=exclude)
        # image_slots 컬럼 읽어서 state 계산
        cols = _resolve_columns()
        con = sqlite3.connect(str(self._db_path()))
        codes = [r["code"] for r in rows]
        placeholders = ",".join("?" for _ in codes)
        slot_map = {}
        genai_map = {}
        if codes:
            q = (
                f'SELECT "{cols["code"]}", image_slots, genai_status, genai_error '
                f'FROM products WHERE "{cols["code"]}" IN ({placeholders})'
            )
            for r in con.execute(q, codes).fetchall():
                try:
                    slot_map[r[0]] = json.loads(r[1]) if r[1] else {}
                except Exception:
                    slot_map[r[0]] = {}
                genai_map[r[0]] = {
                    "genai_status": r[2] or "",
                    "genai_error": r[3] or "",
                }
        con.close()

        for r in rows:
            r["image_slots"] = slot_map.get(r["code"], {})
            r.update(genai_map.get(r["code"], {}))

        self.products = rows
        self._selected.clear()
        self._redraw_table()
        self._update_stats_label()
        self._log(f"조회 {len(rows)}건 (cat={cat}, limit={limit}, 완료숨김={self.hide_done.get()})")

    def _on_load_pending(self):
        """모든 pending(=image_slots 없음) 자동 로드 — limit 무시."""
        self.limit_var.set("9999")
        self.hide_done.set(True)
        self._on_query()

    def _db_path(self):
        from IMG_pipeline.db_query import DB_PATH
        return DB_PATH

    def _redraw_table(self):
        self.tree.delete(*self.tree.get_children())
        # view_mode 필터 우선 적용
        filtered = self._filter_by_view(self.products) if hasattr(self, "view_mode") else self.products
        # 정렬: 완료순(최근 완료 상단) vs 대기순(미완료 상단)
        mode = getattr(self, "sort_mode", None)
        mode = mode.get() if mode else "대기순"
        # view가 "완료"면 자동으로 "완료순" 정렬 강제
        if hasattr(self, "view_mode") and self.view_mode.get() == "완료":
            mode = "완료순"
        def _sort_key(p):
            slots = p.get("image_slots") or {}
            done = len(slots) > 0
            # 최신 generated_at 추출
            ts = ""
            for v in slots.values():
                t = (v or {}).get("generated_at", "")
                if t > ts: ts = t
            if mode == "완료순":
                # 완료된 것 먼저, 그 안에서 ts 내림차순
                return (0 if done else 1, -ord(ts[0]) if ts else 0, ts)
            else:
                return (0 if not done else 1, p["code"])
        ordered = sorted(self.products, key=_sort_key) if mode == "대기순" else \
                  sorted(self.products,
                         key=lambda p: (0 if (p.get("image_slots") or {}) else 1,
                                         ""),
                         reverse=False)
        if mode == "완료순":
            done_list = [p for p in filtered if (p.get("image_slots") or {})]
            pend_list = [p for p in filtered if not (p.get("image_slots") or {})]
            def _latest_ts(p):
                slots = p.get("image_slots") or {}
                return max(((v or {}).get("generated_at","") for v in slots.values()),
                            default="")
            done_list.sort(key=_latest_ts, reverse=True)
            ordered = done_list + pend_list
        else:
            ordered = filtered

        for p in ordered:
            slots = p.get("image_slots") or {}
            status = format_lane_status(p, self._product_runtime_state(p["code"]))
            url = (slots.get("web", {}).get("url")
                   or slots.get("codex", {}).get("url")
                   or slots.get("codex_spark", {}).get("url") or "")
            self.tree.insert("", "end", iid=p["code"],
                             values=("☐", p["code"], p["category"][:40],
                                     p["name"][:80], status, url[:90]))

    def _update_stats_label(self):
        total_loaded = len(self.products)
        done_codex = sum(1 for p in self.products if "codex" in (p.get("image_slots") or {}))
        done_spark = sum(1 for p in self.products if "codex_spark" in (p.get("image_slots") or {}))
        done_web = sum(1 for p in self.products if "web" in (p.get("image_slots") or {}))
        done = sum(1 for p in self.products
                   if any(lane in (p.get("image_slots") or {}) for lane in ("codex", "codex_spark", "web")))
        running = sum(1 for p in self.products
                      if self._product_runtime_state(p["code"]).get("status") == "running"
                      or (p.get("genai_status") or "").lower() == "running")
        failed = sum(1 for p in self.products
                     if self._product_runtime_state(p["code"]).get("status") in ("failed", "asset_failed")
                     or (p.get("genai_status") or "").lower() == "failed")
        pending = max(0, total_loaded - done - running - failed)
        self.stat_lbl.config(
            text=f"조회 {total_loaded} | 완료 {done} (codex {done_codex}, spark {done_spark}, web {done_web}) | "
                 f"대기 {pending} | 진행 {running} | 실패 {failed} | DB전체 {self._base_total:,}")
        self.selected_lbl.config(text=f"선택 {len(self._selected)}")

    # ── 선택 ─────────────────────────────────────────────────────────────
    def _on_click(self, event):
        region = self.tree.identify_region(event.x, event.y)
        col = self.tree.identify_column(event.x)
        if region != "cell" or col != "#1": return
        row_id = self.tree.identify_row(event.y)
        if not row_id: return
        self._toggle_sel(row_id)

    def _toggle_sel(self, code):
        if code in self._selected:
            self._selected.remove(code)
            self.tree.set(code, "sel", "☐")
        else:
            self._selected.add(code)
            self.tree.set(code, "sel", "☑")
        self._update_stats_label()

    def _select_all(self, on: bool):
        for p in self.products:
            if on:
                self._selected.add(p["code"])
                self.tree.set(p["code"], "sel", "☑")
            else:
                self._selected.discard(p["code"])
                self.tree.set(p["code"], "sel", "☐")
        self._update_stats_label()

    def _select_pending_only(self):
        self._selected.clear()
        for p in self.products:
            slots = p.get("image_slots") or {}
            if not slots:
                self._selected.add(p["code"])
                self.tree.set(p["code"], "sel", "☑")
            else:
                self.tree.set(p["code"], "sel", "☐")
        self._update_stats_label()

    # ── hover tooltip ──────────────────────────────────────────────────────
    def _on_hover(self, event):
        row = self.tree.identify_row(event.y)
        if row != self._hover_row:
            self._hover_row = row
            self._hide_tooltip()
            if row:
                self._show_tooltip(event, row)

    def _show_tooltip(self, event, code):
        slots = self._product_slots(code)
        lines = [f"{code}"]
        nukki = self._nukki_path(code)
        lines.append(f"누끼: {'있음' if nukki.exists() else '없음'}")
        if slots:
            for k, v in slots.items():
                lines.append(f"{k}: {v.get('url', '')[:60]}")
        else:
            lines.append("연출 이미지: 아직 없음")
        self._tooltip = tk.Toplevel(self.root)
        self._tooltip.wm_overrideredirect(True)
        self._tooltip.geometry(f"+{event.x_root+12}+{event.y_root+12}")
        tk.Label(self._tooltip, text="\n".join(lines), bg="#222", fg="white",
                  padx=6, pady=4, font=("Consolas", 9), justify="left").pack()

    def _hide_tooltip(self):
        if self._tooltip:
            try: self._tooltip.destroy()
            except Exception: pass
            self._tooltip = None

    # ── 컨텍스트 메뉴 ──────────────────────────────────────────────────────
    def _on_double_click(self, event):
        row = self.tree.identify_row(event.y)
        if not row: return
        # 우선순위: web > codex
        for path in [self._web_out(row), self._codex_out(row)]:
            if path.exists():
                self._open_in_system(path)
                return
        messagebox.showinfo("결과 없음", f"{row}: 생성된 이미지 없음")

    def _on_right_click(self, event):
        row = self.tree.identify_row(event.y)
        if not row: return
        self._ctx_target = row
        self._ctx_menu.tk_popup(event.x_root, event.y_root)

    def _ctx_open(self, lane):
        if not self._ctx_target: return
        path = self._web_out(self._ctx_target) if lane == "web" else self._codex_out(self._ctx_target)
        self._open_in_system(path)

    def _ctx_open_folder(self):
        if not self._ctx_target: return
        self._open_in_system(self._codex_out(self._ctx_target).parent)

    def _ctx_copy_url(self, lane):
        if not self._ctx_target: return
        slots = self._product_slots(self._ctx_target)
        url = (slots.get(lane) or {}).get("url", "")
        if url:
            self.root.clipboard_clear()
            self.root.clipboard_append(url)
            self._log(f"URL 복사({lane}): {url}")
        else:
            messagebox.showinfo("없음", f"{self._ctx_target}: {lane} 결과 URL 없음")

    def _ctx_open_nukki(self):
        if self._ctx_target:
            self._open_in_system(self._nukki_path(self._ctx_target))

    def _ctx_report_issue(self):
        if not self._ctx_target: return
        code = self._ctx_target
        # 빠른 입력 다이얼로그
        dlg = tk.Toplevel(self.root)
        dlg.title(f"이슈 메모: {code}")
        dlg.geometry("540x260")
        dlg.transient(self.root)
        dlg.grab_set()
        ttk.Label(dlg, text=f"상품코드: {code}  (저장 후 pending 복귀)",
                   font=("Segoe UI", 10, "bold")).pack(pady=(10, 4), padx=10, anchor="w")
        ttk.Label(dlg, text="이슈 요약 (예: 4분할, 사이즈 오인, 연출 없음, 효과만, 라벨깨짐...)").pack(
            padx=10, anchor="w")
        txt = tk.Text(dlg, height=8, wrap="word")
        txt.pack(fill="both", expand=True, padx=10, pady=6)
        # 기존 메모 불러오기
        try:
            from IMG_pipeline.post_processor import get_issue_memo
            old = get_issue_memo(code) or ""
            if old: txt.insert("1.0", old + "\n---\n")
        except Exception: pass

        btn_fr = ttk.Frame(dlg); btn_fr.pack(fill="x", padx=10, pady=6)
        # 빠른 태그 버튼
        def add_tag(s):
            txt.insert("end", s + " ")
        for tag in ["[4분할]", "[사이즈오인]", "[연출없음]", "[효과만]",
                    "[라벨깨짐]", "[인종]", "[얼굴]", "[기타]"]:
            ttk.Button(btn_fr, text=tag,
                        command=lambda s=tag: add_tag(s)).pack(side="left", padx=1)

        act_fr = ttk.Frame(dlg); act_fr.pack(fill="x", padx=10, pady=6)
        def save_and_close():
            memo = txt.get("1.0", "end").strip()
            try:
                from IMG_pipeline.post_processor import save_issue_memo
                save_issue_memo(code, memo)
                # 로컬 파일 정리 + R2 삭제
                from IMG_pipeline.post_processor import _client, R2_BUCKET
                import os as _os
                for fn in ["codex.png","codex_1000.jpg","codex_spark.png",
                           "codex_spark_1000.jpg","web_chatgpt.png","web_chatgpt_1000.jpg"]:
                    p = CODEX_OUTPUT / code / fn
                    if p.exists():
                        try: p.unlink()
                        except Exception: pass
                try:
                    c = _client()
                    for lane in ("codex", "codex_spark", "web"):
                        try: c.delete_object(Bucket=R2_BUCKET,
                                              Key=f"genai/{lane}/{code}.jpg")
                        except Exception: pass
                except Exception: pass
                # 테이블 상태 반영
                try: self.tree.set(code, "r2_url", "")
                except Exception: pass
                # products 캐시 갱신
                for p in self.products:
                    if p["code"] == code:
                        p["image_slots"] = {}
                        p["genai_status"] = "pending"
                        p["genai_error"] = ""
                        break
                self._clear_runtime_lane_state(code)
                self._refresh_tree_lane_status(code)
                self._log(f"[ISSUE] {code} 메모 저장 + pending 복귀: {memo[:80]}")
                dlg.destroy()
            except Exception as e:
                messagebox.showerror("저장 실패", str(e))

        ttk.Button(act_fr, text="저장 + pending 복귀", command=save_and_close).pack(side="right")
        ttk.Button(act_fr, text="취소", command=dlg.destroy).pack(side="right", padx=4)

    def _open_in_system(self, path: Path):
        import os as _os
        if not path.exists():
            messagebox.showwarning("없음", str(path))
            return
        try: _os.startfile(str(path))
        except Exception as e: messagebox.showerror("열기 실패", str(e))

    # ── 실행 ─────────────────────────────────────────────────────────────
    def _in_schedule_window(self) -> bool:
        try:
            s = dt_time.fromisoformat(self.sched_start.get())
            e = dt_time.fromisoformat(self.sched_end.get())
        except Exception:
            return True
        now = datetime.now().time()
        if s <= e:
            return s <= now <= e
        return now >= s or now <= e

    def _prepare_assets(self, code, lane):
        import urllib.request
        p = next((x for x in self.products if x["code"] == code), None)
        if not p: return None
        d = POC_INPUT / code
        d.mkdir(parents=True, exist_ok=True)
        nukki = d / "nukki.jpg"
        if not nukki.exists() or nukki.stat().st_size == 0:
            try:
                req = urllib.request.Request(p["nukki_url"], headers={"User-Agent": "p/1"})
                with urllib.request.urlopen(req, timeout=30) as r:
                    nukki.write_bytes(r.read())
            except Exception as e:
                self._log(f"[{code}] nukki 다운로드 실패: {e}")
                return None
        prompts = build_image_prompts(p["st2"])
        self._current_prompt_bundle[code] = prompts
        (d / "prompt_gpt.txt").write_text(prompts["gpt_prompt"], encoding="utf-8")
        (d / "prompt_gemini.txt").write_text(prompts["gemini_prompt"], encoding="utf-8")
        # out 파일명 lane별 분리
        if lane == "web":
            out = self._web_out(code)
        elif lane == "codex_spark":
            out = CODEX_OUTPUT / code / "codex_spark.png"
        else:
            out = self._codex_out(code)
        out.parent.mkdir(parents=True, exist_ok=True)
        return {
            "code": code,
            "nukki_path": str(nukki),
            "prompt_path": str(d / "prompt_gpt.txt"),
            "out_path": str(out),
            "timeout": 360,
            "max_retries": 1 if self.retry_var.get() else 0,
            "auto_delete_chat": self.autodelete_var.get(),
        }

    def _on_start(self):
        if not self._selected:
            messagebox.showwarning("선택 없음", "상품을 먼저 선택하세요")
            return
        if not self._in_schedule_window():
            messagebox.showwarning("시간창 밖", f"실행 시간창: {self.sched_start.get()} ~ {self.sched_end.get()}")
            return

        # 3-way 분배 (codex/spark/web)
        sel = sorted(self._selected)
        n = len(sel)
        p1 = max(0, self.codex_pct.get())
        p2 = max(0, self.spark_pct.get())
        p3 = max(0, self.web_pct.get())
        total_p = p1 + p2 + p3
        if total_p <= 0:
            messagebox.showwarning("분배 오류", "세 비율 중 하나 이상을 0보다 크게 설정")
            return
        c_n = round(n * p1 / total_p)
        s_n = round(n * p2 / total_p)
        w_n = n - c_n - s_n
        if w_n < 0:  # rounding
            c_n, s_n, w_n = c_n, s_n, max(0, n - c_n - s_n)
        self._codex_queue = sel[:c_n]
        self._spark_queue = sel[c_n:c_n + s_n]
        self._web_queue = sel[c_n + s_n:]
        self._log(f"분배: Codex {c_n} / Spark {s_n} / Web {w_n} (합 {n})")

        # 워커 프로세스 기동 (3 레인)
        if self._codex_queue and (self.codex_proc is None or not self.codex_proc.is_alive()):
            self.codex_proc = mp.Process(target=_codex_entry,
                                          args=(self.codex_task_q, self.event_q),
                                          daemon=True)
            self.codex_proc.start()
            self._log(f"Codex 워커 시작 pid={self.codex_proc.pid}")
        if self._spark_queue and (self.spark_proc is None or not self.spark_proc.is_alive()):
            self.spark_proc = mp.Process(target=_spark_entry,
                                          args=(self.spark_task_q, self.event_q),
                                          daemon=True)
            self.spark_proc.start()
            self._log(f"Spark 워커 시작 pid={self.spark_proc.pid}")
        if self._web_queue and (self.web_proc is None or not self.web_proc.is_alive()):
            self.web_proc = mp.Process(target=_web_entry,
                                        args=(self.web_task_q, self.event_q, "chatgpt_acc1"),
                                        daemon=True)
            self.web_proc.start()
            self._log(f"Web 워커 시작 pid={self.web_proc.pid}")

        for code in self._codex_queue:
            self._set_runtime_lane_state(code, lane="codex", status="queued")
            self._refresh_tree_lane_status(code)
        for code in self._spark_queue:
            self._set_runtime_lane_state(code, lane="codex_spark", status="queued")
            self._refresh_tree_lane_status(code)
        for code in self._web_queue:
            self._set_runtime_lane_state(code, lane="web", status="queued")
            self._refresh_tree_lane_status(code)

        self._running = True
        total = len(self._codex_queue) + len(self._spark_queue) + len(self._web_queue)
        self._total_in_session = total
        self._done_in_session = 0
        self._session_started_at = datetime.now()
        self._session_tokens_total = 0
        self.progress.config(maximum=max(1, total), value=0)
        self.start_btn.config(state="disabled")
        self.stop_btn.config(state="normal")

        # 3 레인 각 독립 스레드
        if self._codex_queue:
            threading.Thread(target=self._async_prepare_one_lane,
                args=("codex", list(self._codex_queue), self.codex_task_q),
                daemon=True).start()
        if self._spark_queue:
            threading.Thread(target=self._async_prepare_one_lane,
                args=("codex_spark", list(self._spark_queue), self.spark_task_q),
                daemon=True).start()
        if self._web_queue:
            threading.Thread(target=self._async_prepare_one_lane,
                args=("web", list(self._web_queue), self.web_task_q),
                daemon=True).start()

    def _async_prepare_one_lane(self, lane, codes, task_q):
        pushed = 0
        skipped = 0
        for code in codes:
            if code in self._inflight:
                skipped += 1; continue
            t = self._prepare_assets(code, lane)
            if t:
                # Spark면 model 파라미터 주입
                if lane == "codex_spark":
                    t["model"] = "gpt-5.3-codex-spark"
                self._inflight.add(code)
                task_q.put(t)
                self._ui_update_status(code, lane, "queued")
                pushed += 1
            else:
                self._ui_update_status(code, lane, "asset_failed")
        self._ui_log(f"[{lane}] 큐잉 {pushed}건 (중복 {skipped})")

    def _ui_update_status(self, code, lane, status):
        # tk 호출은 메인스레드에서
        self.root.after(0, lambda: self._safe_set_status(code, lane, status))

    def _safe_set_status(self, code, lane, status):
        self._set_runtime_lane_state(code, lane=lane, status=status)
        product = self._product_by_code(code)
        if product is not None:
            product["genai_status"] = "failed" if status == "asset_failed" else status
        self._refresh_tree_lane_status(code)
        self._update_stats_label()

    def _ui_log(self, msg):
        self.root.after(0, lambda: self._log(msg))

    def _on_stop(self):
        drained = 0
        for q in (self.codex_task_q, self.spark_task_q, self.web_task_q):
            try:
                while True:
                    q.get_nowait()
                    drained += 1
            except Empty: pass
        self._log(f"[STOP] 대기 태스크 {drained}건 취소")
        self.stop_btn.config(state="disabled")

    # ── 이벤트 ──────────────────────────────────────────────────────────
    def _poll_events(self):
        # 틱당 최대 30건만 처리 → 이벤트 폭주 시에도 UI 멈추지 않음
        processed = 0
        try:
            while processed < 30:
                ev = self.event_q.get_nowait()
                self._handle_event(ev)
                processed += 1
        except Empty: pass
        # 처리량에 따라 다음 폴링 간격 조정
        next_interval = 80 if processed >= 30 else 200
        self.root.after(next_interval, self._poll_events)

    def _handle_event(self, ev):
        k = ev.get("kind")
        lane = ev.get("lane", "?")
        if k == "log":
            self._log(f"[{lane}] {ev['message']}")
            return
        if k == "done":
            self._log(f"[{lane}] 워커 종료")
            return
        if k == "status":
            code = ev["code"]
            status = ev["status"]
            self._set_runtime_lane_state(code, lane=lane, status=status, message=ev.get("message", ""))
            product = self._product_by_code(code)
            if product is not None:
                product["genai_status"] = status
                if status == "failed":
                    product["genai_error"] = ev.get("message", "")
            if ev.get("message"):
                self._log(f"[{lane}/{code}] {ev['message']}")
            if status == "done":
                url = (ev.get("meta") or {}).get("url") or ""
                if url:
                    try: self.tree.set(code, "r2_url", url[:90])
                    except Exception: pass
                    for p in self.products:
                        if p["code"] == code:
                            slots = p.get("image_slots") or {}
                            slots[lane] = {"url": url, "generated_at": datetime.now().isoformat(timespec="seconds")}
                            p["image_slots"] = slots
                            p["genai_status"] = "done"
                            break
                tok = (ev.get("meta") or {}).get("tokens")
                if tok: self._session_tokens_total += tok
                self._hourly_events.append((datetime.now(), lane))
                self._done_in_session += 1
                self.progress.config(value=self._done_in_session)
                self._inflight.discard(code)
                self._clear_runtime_lane_state(code)
            elif status == "failed":
                self._done_in_session += 1
                self.progress.config(value=self._done_in_session)
                self._inflight.discard(code)
            self._refresh_tree_lane_status(code)
            self._update_stats_label()
            if status in ("done", "failed"):
                self._check_all_done()

    def _check_all_done(self):
        if not self._running: return
        all_done = True
        for q in (self._codex_queue, self._spark_queue, self._web_queue):
            for code in q:
                runtime_status = self._product_runtime_state(code).get("status", "")
                if runtime_status in ("queued", "running"):
                    all_done = False
                    break
            if not all_done: break
        if all_done:
            self._running = False
            self.start_btn.config(state="normal")
            self.stop_btn.config(state="disabled")
            self._log("=== 전체 완료 ===")

    # ── 시간당 쿼터 ───────────────────────────────────────────────────────
    def _tick_hourly(self):
        cutoff = datetime.now().timestamp() - 3600
        while self._hourly_events and self._hourly_events[0][0].timestamp() < cutoff:
            self._hourly_events.popleft()
        cx = sum(1 for _, l in self._hourly_events if l == "codex")
        wb = sum(1 for _, l in self._hourly_events if l == "web")
        self.hourly_lbl.config(text=f"최근1h: codex {cx} / web {wb}")

        # 세션 통계
        if self._session_started_at and self._total_in_session:
            elapsed = (datetime.now() - self._session_started_at).total_seconds()
            done = self._done_in_session
            remaining = max(0, self._total_in_session - done)
            per_item = (elapsed / done) if done > 0 else 0
            eta_sec = int(remaining * per_item) if per_item > 0 else 0
            h, m = divmod(eta_sec // 60, 60)
            tok_txt = f" | tokens ~{self._session_tokens_total:,}" if self._session_tokens_total else ""
            self.session_lbl.config(
                text=f"세션: {done}/{self._total_in_session} done "
                     f"({elapsed:.0f}s 경과, 평균 {per_item:.1f}s/건, ETA {h}시간 {m}분){tok_txt}")
        self.root.after(5000, self._tick_hourly)

    # ── 유틸 ──────────────────────────────────────────────────────────
    def _log(self, msg):
        self.log.insert("end", f"[{datetime.now():%H:%M:%S}] {msg}\n")
        # 로그 줄수 2000 초과 시 상단 500줄 삭제 (메모리 누수 방지)
        try:
            lines = int(self.log.index("end-1c").split(".")[0])
            if lines > 2000:
                self.log.delete("1.0", f"{lines-1500}.0")
        except Exception: pass
        self.log.see("end")

    def _on_close(self):
        for proc, q in ((self.codex_proc, self.codex_task_q),
                        (self.spark_proc, self.spark_task_q),
                        (self.web_proc, self.web_task_q)):
            if proc and proc.is_alive():
                try:
                    q.put(None)
                    proc.join(timeout=3)
                except Exception: pass
                if proc.is_alive():
                    proc.terminate()
        self.root.destroy()


def main():
    mp.freeze_support()
    root = tk.Tk()
    app = GenAIGui(root)
    root.mainloop()


if __name__ == "__main__":
    main()
