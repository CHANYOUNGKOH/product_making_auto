# gui_stage1_batch.py
"""
STAGE1 Batch GUI (Tkinter) - Batch API용
여러 개의 엑셀/배치를 한 화면에서 관리할 수 있도록 개선된 버전.
"""

import threading
import tkinter as tk
from tkinter import ttk, filedialog, messagebox
from tkinter.scrolledtext import ScrolledText
from dataclasses import dataclass
from datetime import datetime
import os
import json
import tkinter.font as tkfont  # ✅ 추가
from openai import OpenAI

from batch_stage1_core import (
    API_KEY_FILE,
    load_api_key_from_file,
    save_api_key_to_file,
    create_batch_input_jsonl,
    submit_batch,
    wait_and_collect_batch,
)

# 배치 작업 목록 저장 파일 (GUI 스크립트와 같은 폴더 기준)
BATCH_JOBS_FILE = os.path.join(os.path.dirname(__file__), "stage1_batch_jobs.json")


# =====================================
# 배치 작업 정보 구조체
# =====================================

@dataclass
class BatchJobInfo:
    batch_id: str
    src_excel: str
    jsonl_path: str
    out_excel: str
    model: str
    effort: str
    created_at: datetime
    status: str = "submitted"        # submitted / completed / error
    completed_at: datetime | None = None  # 완료 시각 (완료 전에는 None)
    result_dir: str | None = None         # 결과 파일이 있는 로컬 디렉터리 경로
    archived: bool = False                # GUI 목록에서만 숨김(보관함)


# =====================================
# 툴팁 유틸
# =====================================

class ToolTip:
    def __init__(self, widget, text: str):
        self.widget = widget
        self.text = text
        self.tipwindow: tk.Toplevel | None = None
        self.widget.bind("<Enter>", self.show_tip)
        self.widget.bind("<Leave>", self.hide_tip)

    def show_tip(self, event=None):
        if self.tipwindow or not self.text:
            return
        x = self.widget.winfo_rootx() + 20
        y = self.widget.winfo_rooty() + self.widget.winfo_height() + 1

        self.tipwindow = tw = tk.Toplevel(self.widget)
        tw.wm_overrideredirect(True)
        tw.wm_geometry(f"+{x}+{y}")

        label = tk.Label(
            tw,
            text=self.text,
            justify="left",
            background="#ffffe0",
            relief="solid",
            borderwidth=1,
            font=("맑은 고딕", 9),
        )
        label.pack(ipadx=3, ipady=2)

    def hide_tip(self, event=None):
        tw = self.tipwindow
        self.tipwindow = None
        if tw:
            tw.destroy()


# =====================================
# GUI 클래스
# =====================================

class Stage1BatchGUI(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("스테이지1-B 대량 상품명생성_ST1 (배치 API)")
        # 윈도우 크기
        self.geometry("1000x950")

        # -------- 색상 / 스타일 설정 --------
        self.colors = {
            "bg": "#f7f8fc",          # 전체 배경
            "panel": "#ffffff",       # 패널/리스트 배경
            "header": "#e5edff",      # LabelFrame 헤더 배경
            "border": "#d0d7e2",      # 연한 테두리
            "accent": "#4f46e5",      # 강조 버튼/선택 색
            "accent_dark": "#3730a3", # 강조 hover 색
        }
        self.configure(bg=self.colors["bg"])

        # ✅ 전역 폰트 설정 방식 수정 (에러 원인 제거)
        try:
            default_font = tkfont.nametofont("TkDefaultFont")
            default_font.configure(family="맑은 고딕", size=10)
        except Exception:
            pass

        self._setup_style()

        # 기본 설정 변수
        self.api_key_var = tk.StringVar(value=load_api_key_from_file())
        self.model_var = tk.StringVar(value="gpt-5-mini")
        self.effort_var = tk.StringVar(value="low")

        self.src_excel_var = tk.StringVar(value="")
        self.jsonl_path_var = tk.StringVar(value="")
        self.out_excel_var = tk.StringVar(value="")

        self.batch_id_var = tk.StringVar(value="")
        self.poll_interval_var = tk.IntVar(value=30)

        self._client: OpenAI | None = None

        # 수집 중단용 이벤트 & 버튼 핸들
        self.collect_stop_event = threading.Event()
        self.btn_stop_collect: ttk.Button | None = None

        # 다중 배치 관리용
        self.batch_jobs: dict[str, BatchJobInfo] = {}
        self.batch_tree: ttk.Treeview | None = None

        # 삭제된 배치(보관함) 창 관련
        self.archived_window: tk.Toplevel | None = None
        self.archived_tree: ttk.Treeview | None = None

        self._build_widgets()

        # 실행 시, 저장된 stage1_batch_jobs.json에서 배치 목록 자동 로드
        self._load_jobs_from_disk()

    # ---------- 스타일 설정 ----------
    def _setup_style(self):
        style = ttk.Style()
        try:
            style.theme_use("clam")
        except Exception:
            pass

        c = self.colors

        # 공통 배경
        style.configure("TFrame", background=c["bg"])
        style.configure("TLabel", background=c["bg"])
        style.configure("TCheckbutton", background=c["bg"])
        style.configure("TRadiobutton", background=c["bg"])

        # LabelFrame
        style.configure(
            "TLabelframe",
            background=c["bg"],
            bordercolor=c["border"],
            relief="groove",
        )
        style.configure(
            "TLabelframe.Label",
            background=c["header"],
            foreground="#111827",
            padding=(6, 2, 6, 2),
        )

        # Treeview
        style.configure(
            "Treeview",
            background=c["panel"],
            fieldbackground=c["panel"],
            bordercolor=c["border"],
            rowheight=22,
        )
        style.map(
            "Treeview",
            background=[("selected", c["accent"])],
            foreground=[("selected", "white")],
        )

        # 버튼 기본
        style.configure("TButton", padding=(8, 3))

        # 강조 버튼
        style.configure(
            "Accent.TButton",
            padding=(10, 4),
            background=c["accent"],
            foreground="white",
            borderwidth=0,
        )
        style.map(
            "Accent.TButton",
            background=[
                ("active", c["accent_dark"]),
                ("disabled", "#a5b4fc"),
            ],
            foreground=[
                ("disabled", "#e5e7eb"),
            ],
        )

    # ---------- UI ----------
    def _build_widgets(self):
        # 0. API 설정
        frame_api = ttk.LabelFrame(self, text="OpenAI API 설정 (배치용)")
        frame_api.pack(fill="x", padx=10, pady=5)

        # API Key
        ttk.Label(frame_api, text="API Key:").grid(row=0, column=0, sticky="w", padx=5, pady=2)
        entry_key = ttk.Entry(frame_api, textvariable=self.api_key_var, width=50, show="*")
        entry_key.grid(row=0, column=1, sticky="we", padx=5, pady=2)

        btn_save = ttk.Button(frame_api, text="키 저장", command=self.on_save_api_key)
        btn_save.grid(row=0, column=2, sticky="w", padx=5, pady=2)

        frame_api.columnconfigure(1, weight=1)

        # 모델 / Effort
        ttk.Label(frame_api, text="모델:").grid(row=1, column=0, sticky="w", padx=5, pady=2)
        combo_model = ttk.Combobox(
            frame_api,
            textvariable=self.model_var,
            values=["gpt-5", "gpt-5-mini", "gpt-5-nano"],
            state="readonly",
            width=20,
        )
        combo_model.grid(row=1, column=1, sticky="w", padx=5, pady=2)

        ttk.Label(frame_api, text="Reasoning Effort:").grid(row=2, column=0, sticky="w", padx=5, pady=2)
        combo_effort = ttk.Combobox(
            frame_api,
            textvariable=self.effort_var,
            values=["low", "medium", "high"],
            state="readonly",
            width=20,
        )
        combo_effort.grid(row=2, column=1, sticky="w", padx=5, pady=2)

        # --- 사용 순서 안내 텍스트 (툴팁용) ---
        usage_text = (
            "0) API Key 입력 후 [키 저장]\n"
            "1) ① 원본 엑셀 선택\n"
            "2) ②/③ [경로 자동 설정]으로 JSONL / 결과 엑셀 경로 생성 (또는 직접 입력)\n"
            "3) [1단계: JSONL 생성] 실행 → Batch API용 JSONL 생성\n"
            "4) [2단계: Batch 제출] 실행 → Batch ID 자동 입력 및 배치 목록에 추가\n"
            "5) 모델 처리 시간이 지난 후, 배치 목록에서 하나 이상 선택 후\n"
            "   [3단계: 결과 수집 + 엑셀 병합] 실행\n"
            "   (목록에서 아무 것도 선택하지 않으면 ④/①~③ 설정값으로 단일 수집)\n"
            "6) [3-A] 버튼은 ④ Batch ID만으로 결과 JSONL만 다운로드할 때 사용\n"
            "7) 필요 시 [수집 중단]으로 폴링 작업만 중단 가능 (Batch는 서버에서 계속 진행)\n"
            "8) 결과 엑셀에서 ST1_정제상품명 / ST1_판매형태 확인"
        )

        help_label = ttk.Label(
            frame_api,
            text="❓ 사용 방법",
            cursor="question_arrow",
            foreground="#2563eb",
        )
        help_label.grid(row=0, column=3, rowspan=3, sticky="ne", padx=5, pady=2)
        ToolTip(help_label, usage_text)

        # 1. 파일 설정
        frame_file = ttk.LabelFrame(self, text="파일 설정")
        frame_file.pack(fill="x", padx=10, pady=5)

        # ① 원본 엑셀
        ttk.Label(frame_file, text="① 원본 엑셀:").grid(row=0, column=0, sticky="w", padx=5, pady=4)
        entry_src = ttk.Entry(frame_file, textvariable=self.src_excel_var, width=70)
        entry_src.grid(row=0, column=1, sticky="we", padx=5, pady=4)
        btn_browse = ttk.Button(frame_file, text="찾기...", command=self.on_browse_excel)
        btn_browse.grid(row=0, column=2, sticky="w", padx=5, pady=4)

        frame_file.columnconfigure(1, weight=1)

        # ② JSONL 입력파일
        ttk.Label(frame_file, text="② JSONL 입력파일:").grid(row=1, column=0, sticky="w", padx=5, pady=4)
        entry_jsonl = ttk.Entry(frame_file, textvariable=self.jsonl_path_var, width=70)
        entry_jsonl.grid(row=1, column=1, sticky="we", padx=5, pady=4)
        btn_jsonl_auto = ttk.Button(
            frame_file,
            text="JSONL 경로 자동 설정",
            command=self.on_auto_jsonl_path,
        )
        btn_jsonl_auto.grid(row=1, column=2, sticky="w", padx=5, pady=4)

        # ③ 결과 엑셀
        ttk.Label(frame_file, text="③ 결과 엑셀:").grid(row=2, column=0, sticky="w", padx=5, pady=4)
        entry_out = ttk.Entry(frame_file, textvariable=self.out_excel_var, width=70)
        entry_out.grid(row=2, column=1, sticky="we", padx=5, pady=4)
        btn_out_auto = ttk.Button(
            frame_file,
            text="결과 엑셀 경로 자동 설정",
            command=self.on_auto_out_excel_path,
        )
        btn_out_auto.grid(row=2, column=2, sticky="w", padx=5, pady=4)

        # 2. 배치 설정
        frame_batch = ttk.LabelFrame(self, text="배치 설정")
        frame_batch.pack(fill="x", padx=10, pady=5)

        ttk.Label(frame_batch, text="④ Batch ID:").grid(row=0, column=0, sticky="w", padx=5, pady=4)
        entry_batch = ttk.Entry(frame_batch, textvariable=self.batch_id_var, width=40)
        entry_batch.grid(row=0, column=1, sticky="w", padx=5, pady=4)

        ttk.Label(frame_batch, text="⑤ 폴링 간격(초):").grid(row=0, column=2, sticky="e", padx=5, pady=4)
        spin_poll = ttk.Spinbox(
            frame_batch,
            from_=5,
            to=600,
            increment=5,
            textvariable=self.poll_interval_var,
            width=7,
        )
        spin_poll.grid(row=0, column=3, sticky="w", padx=5, pady=4)

        # 3. 단계 버튼
        frame_steps = ttk.Frame(self)
        frame_steps.pack(fill="x", padx=10, pady=5)

        btn_step1 = ttk.Button(
            frame_steps,
            text="1단계: JSONL 생성",
            command=self.on_step1_jsonl,
            style="Accent.TButton",
        )
        btn_step1.pack(side="left", padx=5, pady=3)

        btn_step2 = ttk.Button(
            frame_steps,
            text="2단계: Batch 제출",
            command=self.on_step2_submit,
            style="Accent.TButton",
        )
        btn_step2.pack(side="left", padx=5, pady=3)

        btn_step3 = ttk.Button(
            frame_steps,
            text="3단계: 결과 수집 + 엑셀 병합",
            command=self.on_step3_collect,
            style="Accent.TButton",
        )
        btn_step3.pack(side="left", padx=5, pady=3)

        # 3-A: batch_id만으로 결과 JSONL만 받기
        btn_fetch_raw = ttk.Button(
            frame_steps,
            text="3-A: Batch 결과 JSONL만 받기",
            command=self.on_fetch_raw_jsonl,
            style="Accent.TButton",
        )
        btn_fetch_raw.pack(side="left", padx=5, pady=3)

        self.btn_stop_collect = ttk.Button(
            frame_steps,
            text="수집 중단",
            command=self.on_stop_collect,
            state="disabled",
            style="Accent.TButton",
        )
        self.btn_stop_collect.pack(side="left", padx=5, pady=3)

        # 4. 배치 작업 목록 (여러 파일 동시 관리)
        frame_jobs = ttk.LabelFrame(self, text="배치 작업 목록 (여러 파일 동시 관리)")
        frame_jobs.pack(fill="both", expand=False, padx=10, pady=5)

        # 배치 목록 컬럼에 model / effort 포함
        columns = ("batch_id", "status", "model", "effort", "src_excel", "out_excel", "created_at")
        self.batch_tree = ttk.Treeview(
            frame_jobs,
            columns=columns,
            show="headings",
            selectmode="extended",
            height=6,
        )
        self.batch_tree.heading("batch_id", text="Batch ID")
        self.batch_tree.heading("status", text="상태")
        self.batch_tree.heading("model", text="모델")
        self.batch_tree.heading("effort", text="Effort")
        self.batch_tree.heading("src_excel", text="원본 엑셀")
        self.batch_tree.heading("out_excel", text="결과 엑셀")
        self.batch_tree.heading("created_at", text="생성 시각")

        self.batch_tree.column("batch_id", width=140, anchor="w")
        self.batch_tree.column("status", width=70, anchor="center")
        self.batch_tree.column("model", width=90, anchor="center")
        self.batch_tree.column("effort", width=80, anchor="center")
        self.batch_tree.column("src_excel", width=220, anchor="w")
        self.batch_tree.column("out_excel", width=220, anchor="w")
        self.batch_tree.column("created_at", width=150, anchor="center")

        # 줄무늬(스트라이프)용 태그
        self.batch_tree.tag_configure("evenrow", background="#ffffff")
        self.batch_tree.tag_configure("oddrow", background="#f9fafb")

        self.batch_tree.pack(fill="both", expand=True, padx=5, pady=5)

        lbl_jobs = ttk.Label(
            frame_jobs,
            text=(
                "※ 2단계에서 제출된 Batch 들이 stage1_batch_jobs.json에 저장되고,\n"
                "   프로그램 실행 시 자동으로 이 목록에 불러와집니다.\n"
                "   여러 개를 선택한 뒤 3단계를 누르면 선택된 Batch 들에 대해\n"
                "   결과를 한 번에 수집할 수 있습니다."
            ),
            justify="left",
        )
        lbl_jobs.pack(anchor="w", padx=5, pady=(0, 5))

        # --- 배치 목록 관리 버튼 (삭제, 보관함) ---
        manage_frame = ttk.Frame(frame_jobs)
        manage_frame.pack(fill="x", padx=5, pady=(0, 5))

        btn_delete_jobs = ttk.Button(
            manage_frame,
            text="선택 배치 삭제 (보관함으로 이동)",
            command=self.on_delete_selected_jobs,
            style="Accent.TButton",
        )
        btn_delete_jobs.pack(side="left", padx=(0, 5))

        ToolTip(
            btn_delete_jobs,
            "배치 작업 목록에서 선택한 항목을 '삭제된 배치(보관함)'으로 이동합니다.\n"
            "- stage1_batch_jobs.json에는 archived=True 로 표시만 됩니다.\n"
            "- 이미 서버에 제출된 Batch나, 생성된 결과 엑셀/JSONL 파일은 삭제하지 않습니다."
        )

        btn_show_archived = ttk.Button(
            manage_frame,
            text="삭제된 배치 보기 / 복구",
            command=self.on_show_archived_jobs,
            style="Accent.TButton",
        )
        btn_show_archived.pack(side="left", padx=(5, 5))

        ToolTip(
            btn_show_archived,
            "지금까지 '삭제'한 배치들을 보관함에서 모아 보고,\n"
            "다시 활성 목록으로 복구하거나, 완전 삭제할 수 있습니다."
        )

        # 6. 로그 영역
        frame_log = ttk.LabelFrame(self, text="로그")
        frame_log.pack(fill="both", expand=True, padx=10, pady=5)

        # 로그창 크게 (줄 수 25)
        self.log_widget = ScrolledText(frame_log, height=25)
        self.log_widget.configure(
            font=("맑은 고딕", 10),
            bg=self.colors["panel"],
            fg="#111827",
            insertbackground="#111827",
            relief="solid",
            borderwidth=1,
        )
        self.log_widget.pack(fill="both", expand=True, padx=5, pady=5)

        # ---- 툴팁 연결 ----
        ToolTip(btn_jsonl_auto,
                "원본 엑셀 파일과 같은 폴더에\n"
                "'<엑셀파일명>_stage1_batch_input.jsonl' 이름으로\n"
                "JSONL 경로를 자동으로 채웁니다.")
        ToolTip(btn_out_auto,
                "원본 엑셀 파일과 같은 폴더에\n"
                "'<엑셀파일명>_stage1_batch_output.xlsx' 이름으로\n"
                "결과 엑셀 파일 경로를 자동으로 채웁니다.")
        ToolTip(spin_poll,
                "OpenAI Batch 상태를 몇 초 간격으로 다시 조회할지 설정합니다.\n"
                "값이 작을수록 더 자주 조회합니다.")
        ToolTip(btn_step1,
                "원본 엑셀에서 '원본상품명/카테고리명/판매형태' 컬럼을 읽어\n"
                "Batch API용 JSONL 입력파일을 생성합니다.")
        ToolTip(btn_step2,
                "JSONL 입력파일을 OpenAI에 업로드하고\n"
                "Batch 작업을 생성합니다.\n"
                "생성된 Batch ID는 위 ④ 칸에 자동으로 입력되고,\n"
                "배치 작업 목록에도 추가됩니다.")
        ToolTip(btn_step3,
                "배치 작업 목록에서 선택한 Batch 들이 완료될 때까지 상태를 확인한 후,\n"
                "각각의 결과 JSONL을 다운로드하여 원본 엑셀과 병합하고\n"
                "ST1_정제상품명 / ST1_판매형태 컬럼을 채웁니다.\n"
                "※ 목록에서 아무 것도 선택하지 않으면, ④ Batch ID / ①~③ 설정값 기준으로\n"
                "   단일 Batch만 수집합니다.")
        ToolTip(btn_fetch_raw,
                "④ Batch ID만 알고 있는 경우,\n"
                "해당 Batch의 결과 JSONL 원본만 파일로 다운로드합니다.\n"
                "엑셀 없이 결과 구조만 보고 싶을 때 사용하세요.")
        ToolTip(self.btn_stop_collect,
                "3단계(결과 수집)가 진행 중일 때 누르면\n"
                "폴링 작업과 결과 병합을 중단합니다.\n"
                "이미 서버에 제출된 Batch 자체는 계속 실행됩니다.")
        ToolTip(entry_batch,
                "이미 생성된 Batch ID가 있다면 여기 붙여넣고\n"
                "목록 선택 없이 3단계를 눌러 결과만 다시 수집할 수 있습니다.")
        ToolTip(entry_out,
                "3단계 실행 시 결과를 저장할 엑셀 파일 경로입니다.\n"
                "기본값은 원본 엑셀과 같은 폴더의\n"
                "'<엑셀파일명>_stage1_batch_output.xlsx' 입니다.")

    # ---------- 배치 목록 load/save 유틸 ----------
    def _load_jobs_from_disk(self):
        """
        stage1_batch_jobs.json에서 배치 작업 목록을 읽어와
        self.batch_jobs에 복원 후, 메인 목록/보관함 동기화.
        """
        if not os.path.exists(BATCH_JOBS_FILE):
            self.append_log(f"[INFO] 저장된 배치 작업 파일이 없어 새로 시작합니다. ({BATCH_JOBS_FILE})")
            return

        try:
            with open(BATCH_JOBS_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)

            if not isinstance(data, list):
                self.append_log("[WARN] stage1_batch_jobs.json 형식이 예상과 달라 무시합니다.")
                return

            loaded_count = 0
            for item in data:
                try:
                    created_str = item.get("created_at")
                    try:
                        created_at = datetime.fromisoformat(created_str) if created_str else datetime.now()
                    except Exception:
                        created_at = datetime.now()

                    # 완료 시각
                    completed_str = item.get("completed_at")
                    completed_at = None
                    if completed_str:
                        try:
                            completed_at = datetime.fromisoformat(completed_str)
                        except Exception:
                            completed_at = None

                    raw_result_dir = item.get("result_dir")
                    archived = bool(item.get("archived", False))

                    job = BatchJobInfo(
                        batch_id=item["batch_id"],
                        src_excel=item.get("src_excel", ""),
                        jsonl_path=item.get("jsonl_path", ""),
                        out_excel=item.get("out_excel", ""),
                        model=item.get("model", ""),
                        effort=item.get("effort", ""),
                        created_at=created_at,
                        status=item.get("status", "submitted"),
                        completed_at=completed_at,
                        result_dir=raw_result_dir,
                        archived=archived,
                    )

                    # result_dir 보정
                    job.result_dir = self._compute_result_dir(job)

                    # 같은 batch_id가 중복 저장된 경우, 마지막 항목으로 overwrite
                    self.batch_jobs[job.batch_id] = job
                    loaded_count += 1
                except Exception as e:
                    self.append_log(f"[WARN] 저장된 항목 하나를 로드하지 못했습니다: {e}")

            # 메인 목록은 최신 created_at 순으로 재구성
            self._rebuild_main_tree()
            self.append_log(f"[INFO] 저장된 배치 작업 {loaded_count}개를 로드했습니다.")
        except Exception as e:
            self.append_log(f"[ERROR] 배치 작업 목록 로드 중 예외 발생: {e}")

    def _save_jobs_to_disk(self):
        """
        현재 self.batch_jobs 내용을 stage1_batch_jobs.json에 저장.
        result_dir도 한번 더 보정해서 일관성 있게 유지.
        """
        try:
            data = []
            for job in self.batch_jobs.values():
                job.result_dir = self._compute_result_dir(job)
                data.append({
                    "batch_id": job.batch_id,
                    "src_excel": job.src_excel,
                    "jsonl_path": job.jsonl_path,
                    "out_excel": job.out_excel,
                    "model": job.model,
                    "effort": job.effort,
                    "created_at": job.created_at.isoformat(),
                    "status": job.status,
                    "completed_at": job.completed_at.isoformat() if job.completed_at else None,
                    "result_dir": job.result_dir,
                    "archived": job.archived,
                })

            with open(BATCH_JOBS_FILE, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)

            self.append_log(f"[INFO] 배치 작업 목록 저장 완료 ({BATCH_JOBS_FILE})")
        except Exception as e:
            self.append_log(f"[ERROR] 배치 작업 목록 저장 중 예외 발생: {e}")

    # ---------- 내부 유틸 ----------
    def _compute_result_dir(self, job: BatchJobInfo) -> str | None:
        """
        result_dir가 비어 있거나 이상할 때 보정용.
        우선 순위:
        1) job.result_dir
        2) out_excel 이 있는 폴더
        3) src_excel 이 있는 폴더
        4) jsonl_path 가 있는 폴더
        """
        candidates = [
            job.result_dir,
            job.out_excel,
            job.src_excel,
            job.jsonl_path,
        ]
        for p in candidates:
            if not p:
                continue
            d = os.path.dirname(p)
            if d:
                return d
        return None

    def _apply_striped_rows(self, tree: ttk.Treeview | None):
        """Treeview에 짝/홀수 줄마다 다른 배경색 적용."""
        if not tree:
            return
        children = tree.get_children()
        for idx, iid in enumerate(children):
            tag = "evenrow" if idx % 2 == 0 else "oddrow"
            tree.item(iid, tags=(tag,))

    def append_log(self, msg: str):
        # 메인 스레드에서만 위젯 조작하도록 after 사용
        def _do():
            self.log_widget.insert("end", msg + "\n")
            self.log_widget.see("end")
        self.after(0, _do)

    def get_client(self) -> OpenAI:
        if self._client is None:
            key = self.api_key_var.get().strip()
            if not key:
                raise RuntimeError("API Key가 비어 있습니다.")
            self._client = OpenAI(api_key=key)
        return self._client

    # --- 배치 목록 유틸 ---
    def _add_job_to_tree(self, job: BatchJobInfo):
        """
        메인 Treeview에 job 하나 추가.
        - archived=True 인 것은 추가하지 않음
        - 이미 같은 batch_id(iid)가 있으면 새로 삽입하지 않고 업데이트만 수행
        """
        if not self.batch_tree:
            return
        if job.archived:
            return

        created_str = job.created_at.strftime("%Y-%m-%d %H:%M:%S")

        existing_ids = self.batch_tree.get_children()
        values = (
            job.batch_id,
            job.status,
            job.model,
            job.effort,
            job.src_excel,
            job.out_excel,
            created_str,
        )

        if job.batch_id in existing_ids:
            # 이미 존재하면 값만 갱신
            try:
                self.batch_tree.item(
                    job.batch_id,
                    values=values,
                )
            except tk.TclError:
                # 혹시라도 문제가 있으면 삭제 후 다시 삽입
                try:
                    self.batch_tree.delete(job.batch_id)
                except tk.TclError:
                    pass
                self.batch_tree.insert(
                    "",
                    "end",
                    iid=job.batch_id,
                    values=values,
                )
        else:
            self.batch_tree.insert(
                "",
                "end",
                iid=job.batch_id,
                values=values,
            )

    def _rebuild_main_tree(self):
        """
        메인 Treeview를 self.batch_jobs 기준으로
        archived=False 만, created_at 내림차순으로 재구성.
        """
        if not self.batch_tree:
            return

        self.batch_tree.delete(*self.batch_tree.get_children())

        jobs = [job for job in self.batch_jobs.values() if not job.archived]
        jobs.sort(key=lambda j: j.created_at, reverse=True)

        for job in jobs:
            self._add_job_to_tree(job)

        self._apply_striped_rows(self.batch_tree)

    def _update_job_in_tree(self, batch_id: str):
        """
        상태만 바뀔 때 사용하는 Treeview 갱신 함수.
        archived=True 인 경우에는 메인 목록에서 제거.
        """
        if not self.batch_tree:
            return
        job = self.batch_jobs.get(batch_id)
        if not job:
            return

        if job.archived:
            # 보관 중이면 메인 트리에서 제거
            try:
                self.batch_tree.delete(batch_id)
            except tk.TclError:
                pass
            self._apply_striped_rows(self.batch_tree)
            return

        self._add_job_to_tree(job)
        self._apply_striped_rows(self.batch_tree)

    # ---------- 삭제된 배치(보관함) UI ----------
    def on_show_archived_jobs(self):
        """
        삭제된 배치(archived=True)를 보여주는 별도 창.
        여기서 복구/완전삭제 가능.
        """
        if self.archived_window is not None and self.archived_window.winfo_exists():
            # 이미 떠 있다면 앞으로 가져오고 리스트만 갱신
            self.archived_window.deiconify()
            self.archived_window.lift()
            self._refresh_archived_tree()
            return

        win = tk.Toplevel(self)
        win.title("삭제된 배치 목록 (보관함)")
        win.geometry("900x500")
        win.configure(bg=self.colors["bg"])
        self.archived_window = win

        columns = ("batch_id", "status", "model", "effort", "src_excel", "out_excel", "created_at")
        tree = ttk.Treeview(
            win,
            columns=columns,
            show="headings",
            selectmode="extended",
        )
        tree.heading("batch_id", text="Batch ID")
        tree.heading("status", text="상태")
        tree.heading("model", text="모델")
        tree.heading("effort", text="Effort")
        tree.heading("src_excel", text="원본 엑셀")
        tree.heading("out_excel", text="결과 엑셀")
        tree.heading("created_at", text="생성 시각")

        tree.column("batch_id", width=140, anchor="w")
        tree.column("status", width=70, anchor="center")
        tree.column("model", width=90, anchor="center")
        tree.column("effort", width=80, anchor="center")
        tree.column("src_excel", width=220, anchor="w")
        tree.column("out_excel", width=220, anchor="w")
        tree.column("created_at", width=150, anchor="center")

        tree.tag_configure("evenrow", background="#ffffff")
        tree.tag_configure("oddrow", background="#f9fafb")

        tree.pack(fill="both", expand=True, padx=5, pady=5)
        self.archived_tree = tree

        btn_frame = ttk.Frame(win)
        btn_frame.pack(fill="x", padx=5, pady=5)

        btn_restore = ttk.Button(
            btn_frame,
            text="선택 복구",
            command=self.on_restore_archived_jobs,
            style="Accent.TButton",
        )
        btn_restore.pack(side="left", padx=5)

        ToolTip(
            btn_restore,
            "선택한 삭제된 배치를 다시 활성 목록으로 복구합니다.\n"
            "- stage1_batch_jobs.json 에서 archived=False 로 변경됩니다.\n"
            "- 메인 배치 목록 트리에도 다시 표시됩니다."
        )

        btn_delete_perm = ttk.Button(
            btn_frame,
            text="선택 완전 삭제",
            command=self.on_delete_archived_jobs_permanently,
            style="Accent.TButton",
        )
        btn_delete_perm.pack(side="left", padx=5)

        ToolTip(
            btn_delete_perm,
            "선택한 삭제된 배치를 stage1_batch_jobs.json 에서도 완전히 제거합니다.\n"
            "- 실제 서버 Batch 작업이나 로컬 엑셀/JSONL 파일은 삭제하지 않습니다."
        )

        btn_close = ttk.Button(
            btn_frame,
            text="닫기",
            command=win.destroy,
        )
        btn_close.pack(side="right", padx=5)

        self._refresh_archived_tree()

    def _refresh_archived_tree(self):
        """
        보관함 Treeview를 self.batch_jobs 기준으로
        archived=True 만, created_at 내림차순으로 재구성.
        """
        if not self.archived_tree:
            return
        self.archived_tree.delete(*self.archived_tree.get_children())

        jobs = [job for job in self.batch_jobs.values() if job.archived]
        jobs.sort(key=lambda j: j.created_at, reverse=True)

        for job in jobs:
            created_str = job.created_at.strftime("%Y-%m-%d %H:%M:%S")
            self.archived_tree.insert(
                "",
                "end",
                iid=job.batch_id,
                values=(
                    job.batch_id,
                    job.status,
                    job.model,
                    job.effort,
                    job.src_excel,
                    job.out_excel,
                    created_str,
                ),
            )

        self._apply_striped_rows(self.archived_tree)

    def on_restore_archived_jobs(self):
        """
        보관함 창에서 선택된 항목을 다시 활성 목록으로 복구.
        """
        if not self.archived_tree:
            return
        selected_ids = self.archived_tree.selection()
        if not selected_ids:
            messagebox.showinfo("알림", "복구할 배치를 선택하세요.")
            return

        for bid in selected_ids:
            job = self.batch_jobs.get(bid)
            if not job:
                continue
            job.archived = False

        # 데이터 반영 후, 두 목록 모두 재구성
        self._save_jobs_to_disk()
        self._rebuild_main_tree()
        self._refresh_archived_tree()
        self.append_log(f"[INFO] 삭제된 배치 {len(selected_ids)}개를 복구했습니다.")

    def on_delete_archived_jobs_permanently(self):
        """
        보관함 창에서 선택된 항목을 stage1_batch_jobs.json 에서도 완전 삭제.
        실제 서버 Batch나 로컬 파일은 건들지 않는다.
        """
        if not self.archived_tree:
            return
        selected_ids = self.archived_tree.selection()
        if not selected_ids:
            messagebox.showinfo("알림", "완전 삭제할 배치를 선택하세요.")
            return

        msg = (
            f"선택한 {len(selected_ids)}개 배치를 stage1_batch_jobs.json 에서 완전히 제거합니다.\n\n"
            "※ 실제 OpenAI 서버의 Batch 작업, 로컬 엑셀/JSONL 파일은 삭제되지 않습니다.\n"
            "   이력 관리용 GUI 목록(stage1_batch_jobs.json)에서만 제거됩니다.\n\n"
            "계속하시겠습니까?"
        )
        if not messagebox.askyesno("완전 삭제 확인", msg):
            return

        deleted = 0
        for bid in selected_ids:
            # 딕셔너리에서 제거
            if bid in self.batch_jobs:
                del self.batch_jobs[bid]
            deleted += 1

        # 저장 및 두 목록 재구성
        self._save_jobs_to_disk()
        self._rebuild_main_tree()
        self._refresh_archived_tree()
        self.append_log(f"[INFO] 삭제된 배치 {deleted}개를 완전히 제거했습니다.")

    # ---------- 배치 삭제 (soft delete) ----------
    def on_delete_selected_jobs(self):
        """
        배치 작업 목록(Treeview)에서 선택된 항목들을 '삭제된 배치(보관함)'으로 이동.
        - self.batch_jobs 에서는 archived=True 표시만 함
        - 메인 Treeview에서는 숨김
        - stage1_batch_jobs.json 에 다시 저장
        - 실제 서버 Batch나 결과 파일은 건들지 않음
        """
        if not self.batch_tree:
            return

        selected_ids = self.batch_tree.selection()
        if not selected_ids:
            messagebox.showinfo("알림", "삭제(보관)할 배치 작업을 먼저 선택하세요.")
            return

        # 완료되지 않은 배치가 포함되어 있는지 체크
        incomplete = [
            bid for bid in selected_ids
            if (self.batch_jobs.get(bid) and self.batch_jobs[bid].status != "completed")
        ]

        if incomplete:
            msg = (
                f"선택한 {len(selected_ids)}개 중 {len(incomplete)}개는 아직 'completed' 상태가 아닙니다.\n"
                "그래도 목록에서 제거(보관함으로 이동)하시겠습니까?\n\n"
                "※ 서버에 제출된 Batch 작업과, 이미 생성된 결과 파일(엑셀/JSONL)은 그대로 남습니다.\n"
                "   이 화면(Stage1 배치 작업 리스트)에서만 숨겨지고,\n"
                "   '삭제된 배치 보기/복구' 창에서 언제든 복구할 수 있습니다."
            )
        else:
            msg = (
                f"선택한 {len(selected_ids)}개 배치 작업을 목록에서 숨기고,\n"
                "'삭제된 배치(보관함)'으로 이동합니다.\n\n"
                "※ 서버에 제출된 Batch 작업과, 이미 생성된 결과 파일(엑셀/JSONL)은 그대로 남습니다.\n"
                "   이 화면(Stage1 배치 작업 리스트)에서만 숨겨지고,\n"
                "   '삭제된 배치 보기/복구' 창에서 언제든 복구할 수 있습니다."
            )

        if not messagebox.askyesno("배치 삭제(보관) 확인", msg):
            return

        for bid in selected_ids:
            job = self.batch_jobs.get(bid)
            if not job:
                continue
            job.archived = True

        # 저장 및 두 목록 재구성
        self._save_jobs_to_disk()
        self._rebuild_main_tree()
        self._refresh_archived_tree()
        self.append_log(f"[INFO] 배치 작업 {len(selected_ids)}개를 삭제된 배치(보관함)으로 이동했습니다.")

    # ---------- 이벤트 핸들러 ----------
    def on_save_api_key(self):
        key = self.api_key_var.get().strip()
        if not key:
            messagebox.showwarning("경고", "API Key를 입력하세요.")
            return
        save_api_key_to_file(key)
        self._client = None  # 새 키 기준으로 다시 생성되도록
        self.append_log(f"[INFO] API 키 저장 완료 ({API_KEY_FILE}).")

    def on_browse_excel(self):
        path = filedialog.askopenfilename(
            title="원본 엑셀 선택",
            filetypes=[("Excel files", "*.xlsx;*.xls")],
        )
        if path:
            self.src_excel_var.set(path)
            self._auto_paths_from_excel()

    def _auto_paths_from_excel(self):
        excel_path = self.src_excel_var.get().strip()
        if not excel_path:
            return
        base_dir = os.path.dirname(excel_path)
        base_name = os.path.splitext(os.path.basename(excel_path))[0]

        jsonl_default = os.path.join(base_dir, f"{base_name}_stage1_batch_input.jsonl")
        out_default = os.path.join(base_dir, f"{base_name}_stage1_batch_output.xlsx")

        if not self.jsonl_path_var.get().strip():
            self.jsonl_path_var.set(jsonl_default)
        if not self.out_excel_var.get().strip():
            self.out_excel_var.set(out_default)

        self.append_log(f"[INFO] JSONL 기본 경로 설정: {jsonl_default}")
        self.append_log(f"[INFO] 결과 엑셀 기본 경로 설정: {out_default}")

    def on_auto_jsonl_path(self):
        excel_path = self.src_excel_var.get().strip()
        if not excel_path:
            messagebox.showwarning("경고", "먼저 ① 원본 엑셀을 선택하세요.")
            return
        base_dir = os.path.dirname(excel_path)
        base_name = os.path.splitext(os.path.basename(excel_path))[0]
        jsonl_default = os.path.join(base_dir, f"{base_name}_stage1_batch_input.jsonl")
        self.jsonl_path_var.set(jsonl_default)
        self.append_log(f"[INFO] JSONL 경로를 엑셀 기준으로 설정: {jsonl_default}")

    def on_auto_out_excel_path(self):
        excel_path = self.src_excel_var.get().strip()
        if not excel_path:
            messagebox.showwarning("경고", "먼저 ① 원본 엑셀을 선택하세요.")
            return
        base_dir = os.path.dirname(excel_path)
        base_name = os.path.splitext(os.path.basename(excel_path))[0]
        out_default = os.path.join(base_dir, f"{base_name}_stage1_batch_output.xlsx")
        self.out_excel_var.set(out_default)
        self.append_log(f"[INFO] 결과 엑셀 경로를 엑셀 기준으로 설정: {out_default}")

    # --- 단계별 버튼 ---
    def on_step1_jsonl(self):
        excel_path = self.src_excel_var.get().strip()
        jsonl_path = self.jsonl_path_var.get().strip()
        model = self.model_var.get().strip()
        effort = self.effort_var.get().strip()

        if not excel_path:
            messagebox.showwarning("경고", "① 원본 엑셀 경로를 먼저 지정하세요.")
            return
        if not jsonl_path:
            messagebox.showwarning("경고", "② JSONL 입력파일 경로를 먼저 지정하세요.")
            return

        try:
            info = create_batch_input_jsonl(
                excel_path=excel_path,
                jsonl_path=jsonl_path,
                model_name=model,
                reasoning_effort=effort,
            )
            self.append_log(f"[PREPARE] JSONL 생성 완료: {jsonl_path}")
            self.append_log(
                f"  - 전체 행: {info['total_rows']}개 / "
                f"JSONL 포함: {info['written_count']}개 / "
                f"제외: {info['skipped_count']}개"
            )
            if info.get("skipped_path"):
                self.append_log(
                    f"  - 필수 정보 누락으로 제외된 행 요약 파일: {info['skipped_path']}"
                )
        except Exception as e:
            self.append_log(f"[ERROR] JSONL 생성 중 예외 발생: {e}")
            messagebox.showerror("에러", f"JSONL 생성 실패:\n{e}")

    def on_step2_submit(self):
        excel_path = self.src_excel_var.get().strip()
        jsonl_path = self.jsonl_path_var.get().strip()
        out_excel_path = self.out_excel_var.get().strip()

        if not excel_path:
            messagebox.showwarning("경고", "① 원본 엑셀 경로를 먼저 지정하세요.")
            return
        if not jsonl_path:
            messagebox.showwarning("경고", "② JSONL 입력파일 경로를 먼저 지정하세요.")
            return

        if not os.path.exists(jsonl_path):
            messagebox.showwarning("경고", f"JSONL 파일을 찾을 수 없습니다:\n{jsonl_path}")
            return

        # 결과 엑셀 경로가 비어 있으면 자동 설정
        if not out_excel_path:
            base_dir = os.path.dirname(excel_path)
            base_name = os.path.splitext(os.path.basename(excel_path))[0]
            out_excel_path = os.path.join(base_dir, f"{base_name}_stage1_batch_output.xlsx")
            self.out_excel_var.set(out_excel_path)
            self.append_log(f"[INFO] 결과 엑셀 경로를 엑셀 기준으로 자동 설정: {out_excel_path}")

        try:
            client = self.get_client()
        except Exception as e:
            messagebox.showerror("에러", f"API 클라이언트 초기화 실패:\n{e}")
            return

        try:
            batch_id = submit_batch(
                jsonl_path=jsonl_path,
                client=client,
                completion_window="24h",
            )
            self.batch_id_var.set(batch_id)
            self.append_log(f"[SUBMIT] Batch 생성 완료. batch_id={batch_id}")

            job = BatchJobInfo(
                batch_id=batch_id,
                src_excel=excel_path,
                jsonl_path=jsonl_path,
                out_excel=out_excel_path,
                model=self.model_var.get().strip(),
                effort=self.effort_var.get().strip(),
                created_at=datetime.now(),
                status="submitted",
                completed_at=None,
                result_dir=os.path.dirname(out_excel_path) if out_excel_path else None,
                archived=False,
            )
            job.result_dir = self._compute_result_dir(job)

            self.batch_jobs[batch_id] = job
            self._save_jobs_to_disk()
            self._rebuild_main_tree()
            self.append_log(
                f"[INFO] 배치 작업 목록에 추가: batch_id={batch_id}, "
                f"src={excel_path}, out={out_excel_path}"
            )
        except Exception as e:
            self.append_log(f"[ERROR] Batch 제출 중 예외 발생: {e}")
            messagebox.showerror("에러", f"Batch 제출 실패:\n{e}")

    def on_step3_collect(self):
        interval = int(self.poll_interval_var.get())

        try:
            client = self.get_client()
        except Exception as e:
            messagebox.showerror("에러", f"API 클라이언트 초기화 실패:\n{e}")
            return

        selected_ids = ()
        if self.batch_tree is not None:
            selected_ids = self.batch_tree.selection()

        jobs: list[BatchJobInfo] = []

        if selected_ids:
            for iid in selected_ids:
                job = self.batch_jobs.get(iid)
                if job and not job.archived:
                    jobs.append(job)
        else:
            jobs = [
                job for job in self.batch_jobs.values()
                if (not job.archived and job.status != "completed")
            ]

        # --- 다중 배치 순차 수집 모드 ---
        if jobs:
            self.collect_stop_event.clear()
            if self.btn_stop_collect:
                self.btn_stop_collect.config(state="normal")

            self.append_log(
                f"[INFO] {len(jobs)}개 Batch에 대해 결과 수집을 '순차적으로' 시작합니다."
            )

            def worker_multi():
                try:
                    for job in jobs:
                        if self.collect_stop_event.is_set():
                            self.append_log(
                                "[INFO] 수집 중단 요청 감지. 남은 배치 작업은 실행하지 않습니다."
                            )
                            break

                        self.append_log(
                            f"[INFO] Batch {job.batch_id} 결과 수집 시작..."
                        )
                        try:
                            wait_and_collect_batch(
                                batch_id=job.batch_id,
                                excel_path=job.src_excel,
                                output_excel_path=job.out_excel,
                                client=client,
                                poll_interval_sec=interval,
                                log_fn=self.append_log,
                                stop_event=self.collect_stop_event,
                                model_name=job.model,
                                reasoning_effort=job.effort,
                            )

                            job.status = "completed"
                            job.completed_at = datetime.now()
                            job.result_dir = self._compute_result_dir(job)
                            self._save_jobs_to_disk()
                            self.after(0, self._update_job_in_tree, job.batch_id)
                            self.append_log(
                                f"[DONE] Batch {job.batch_id} 결과 수집 및 엑셀 병합 완료 → {job.out_excel}"
                            )

                        except RuntimeError as e:
                            if "중단" in str(e):
                                self.append_log(
                                    f"[INFO] Batch {job.batch_id} 수집이 사용자에 의해 중단되었습니다."
                                )
                            else:
                                self.append_log(
                                    f"[ERROR] Batch {job.batch_id} 결과 수집 중 예외 발생: {e}"
                                )
                            job.status = "error"
                            self._save_jobs_to_disk()
                            self.after(0, self._update_job_in_tree, job.batch_id)
                            break

                        except Exception as e:
                            job.status = "error"
                            self._save_jobs_to_disk()
                            self.after(0, self._update_job_in_tree, job.batch_id)
                            self.append_log(
                                f"[ERROR] Batch {job.batch_id} 결과 수집 중 예외 발생: {e}"
                            )
                finally:
                    if self.btn_stop_collect:
                        self.after(
                            0,
                            lambda: self.btn_stop_collect.config(state="disabled")
                        )

            th = threading.Thread(target=worker_multi, daemon=True)
            th.start()
            return

        # --- 단일 Batch 모드 ---
        batch_id = self.batch_id_var.get().strip()
        excel_path = self.src_excel_var.get().strip()
        out_excel_path = self.out_excel_var.get().strip()

        if not batch_id:
            messagebox.showwarning("경고", "④ Batch ID를 먼저 입력(또는 2단계에서 생성)하세요.")
            return
        if not excel_path:
            messagebox.showwarning("경고", "① 원본 엑셀 경로를 지정하세요.")
            return
        if not out_excel_path:
            messagebox.showwarning("경고", "③ 결과 엑셀 경로를 지정하세요.")
            return

        job = self.batch_jobs.get(batch_id)
        if job and job.archived:
            messagebox.showwarning(
                "경고",
                "해당 Batch ID는 '삭제된 배치(보관함)'에 있습니다.\n"
                "먼저 '삭제된 배치 보기/복구'에서 복구한 뒤 다시 시도해주세요.",
            )
            return

        self.collect_stop_event.clear()
        if self.btn_stop_collect:
            self.btn_stop_collect.config(state="normal")

        def worker_single():
            try:
                wait_and_collect_batch(
                    batch_id=batch_id,
                    excel_path=excel_path,
                    output_excel_path=out_excel_path,
                    client=client,
                    poll_interval_sec=interval,
                    log_fn=self.append_log,
                    stop_event=self.collect_stop_event,
                    model_name=self.model_var.get().strip(),
                    reasoning_effort=self.effort_var.get().strip(),
                )

                job_local = self.batch_jobs.get(batch_id)
                if job_local:
                    job_local.status = "completed"
                    job_local.out_excel = out_excel_path
                    job_local.completed_at = datetime.now()
                    job_local.result_dir = self._compute_result_dir(job_local)
                    self._save_jobs_to_disk()
                    self.after(0, self._update_job_in_tree, batch_id)

                self.after(
                    0,
                    lambda: messagebox.showinfo(
                        "완료",
                        f"결과 수집 및 엑셀 병합 완료.\n\n{out_excel_path}",
                    ),
                )
            except RuntimeError as e:
                job_local = self.batch_jobs.get(batch_id)
                if "중단" in str(e):
                    self.append_log("[INFO] 사용자가 결과 수집을 중단했습니다.")
                else:
                    self.append_log(f"[ERROR] 결과 수집 중 예외 발생: {e}")
                    err_msg = f"결과 수집 실패:\n{e}"
                    self.after(
                        0,
                        lambda msg=err_msg: messagebox.showerror("에러", msg),
                    )
                if job_local:
                    job_local.status = "error"
                    self._save_jobs_to_disk()
                    self.after(0, self._update_job_in_tree, batch_id)
            except Exception as e:
                self.append_log(f"[ERROR] 결과 수집 중 예외 발생: {e}")
                err_msg = f"결과 수집 실패:\n{e}"
                self.after(
                    0,
                    lambda msg=err_msg: messagebox.showerror("에러", msg),
                )
                job_local = self.batch_jobs.get(batch_id)
                if job_local:
                    job_local.status = "error"
                    self._save_jobs_to_disk()
                    self.after(0, self._update_job_in_tree, batch_id)
            finally:
                if self.btn_stop_collect:
                    self.after(0, lambda: self.btn_stop_collect.config(state="disabled"))

        th = threading.Thread(target=worker_single, daemon=True)
        th.start()

    def on_fetch_raw_jsonl(self):
        """
        ④ Batch ID만 알고 있는 경우, 해당 Batch의 결과 JSONL만 파일로 저장하는 기능.
        """
        batch_id = self.batch_id_var.get().strip()
        if not batch_id:
            messagebox.showwarning("경고", "④ Batch ID를 먼저 입력하세요.")
            return

        try:
            client = self.get_client()
        except Exception as e:
            messagebox.showerror("에러", f"API 클라이언트 초기화 실패:\n{e}")
            return

        if self.src_excel_var.get().strip():
            initial_dir = os.path.dirname(self.src_excel_var.get().strip())
        else:
            initial_dir = os.getcwd()

        default_name = f"{batch_id}_output.jsonl"
        save_path = filedialog.asksaveasfilename(
            title="Batch 결과 JSONL 저장 위치 선택",
            defaultextension=".jsonl",
            initialdir=initial_dir,
            initialfile=default_name,
            filetypes=[("JSON Lines 파일", "*.jsonl"), ("모든 파일", "*.*")],
        )
        if not save_path:
            return

        try:
            self.append_log(f"[FETCH] Batch {batch_id} 정보 조회 중...")
            batch = client.batches.retrieve(batch_id)

            output_file_id = getattr(batch, "output_file_id", None)
            if not output_file_id:
                output_file_ids = getattr(batch, "output_file_ids", None)
                if output_file_ids and len(output_file_ids) > 0:
                    output_file_id = output_file_ids[0]

            if not output_file_id:
                raise RuntimeError("해당 Batch에서 output_file_id를 찾을 수 없습니다.")

            self.append_log(f"[FETCH] output_file_id={output_file_id} 다운로드 중...")
            file_content = client.files.content(output_file_id)

            if hasattr(file_content, "read"):
                data_bytes = file_content.read()
            elif hasattr(file_content, "iter_bytes"):
                chunks = []
                for chunk in file_content.iter_bytes():
                    chunks.append(chunk)
                data_bytes = b"".join(chunks)
            else:
                data_bytes = file_content

            with open(save_path, "wb") as f:
                f.write(data_bytes)

            self.append_log(f"[FETCH] Batch {batch_id} 결과 JSONL 저장 완료 → {save_path}")
            messagebox.showinfo(
                "완료",
                f"Batch 결과 JSONL 다운로드 완료.\n\n{save_path}",
            )
        except Exception as e:
            self.append_log(f"[ERROR] Batch 결과 JSONL 다운로드 중 예외 발생: {e}")
            messagebox.showerror("에러", f"Batch 결과 JSONL 다운로드 실패:\n{e}")

    def on_stop_collect(self):
        self.collect_stop_event.set()
        self.append_log("[INFO] 사용자가 수집 중단을 요청했습니다. 다음 폴링 시점에 중단됩니다.")


# ---------- main ----------

if __name__ == "__main__":
    app = Stage1BatchGUI()
    app.mainloop()
