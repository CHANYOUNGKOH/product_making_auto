"""
stage3_batch_api.py

Stage 3 Batch API 실행 스크립트 (GUI)
- 기능: Batch JSONL 생성 -> 업로드 -> 실행 -> 결과 병합 -> 통합 리포트
- Template: Stage 4-2 Batch API Structure
- [Fix] 'NoneType' object is not subscriptable 오류 수정 (날짜 필드 None 처리)
"""

import os
import sys
import json
import threading
import subprocess
from datetime import datetime
from dataclasses import asdict

import pandas as pd
import tkinter as tk
from tkinter import ttk, filedialog, messagebox, Menu
from tkinter.scrolledtext import ScrolledText

from openai import OpenAI

# [필수 의존성] stage3_core.py
try:
    from stage3_core import (
        safe_str,
        Stage3Settings,
        Stage3Request,
        build_stage3_request_from_row,
        MODEL_PRICING_USD_PER_MTOK,
        load_api_key_from_file,
        save_api_key_to_file,
    )
    from stage3_run_history import append_run_history
except ImportError:
    # 의존성 파일이 없을 경우를 대비한 더미 (실행 시 에러 방지용)
    MODEL_PRICING_USD_PER_MTOK = {}
    def safe_str(x): return str(x)
    def load_api_key_from_file(x): return ""
    def save_api_key_to_file(x, y): pass

# === 기본 설정 ===
API_KEY_FILE = ".openai_api_key_stage3_batch"
BATCH_JOBS_FILE = os.path.join(os.path.dirname(__file__), "stage3_batch_jobs.json")

# --- UI 색상 팔레트 ---
COLOR_BG = "#F8F9FA"
COLOR_WHITE = "#FFFFFF"
COLOR_PRIMARY = "#4A90E2"
COLOR_PRIMARY_HOVER = "#357ABD"
COLOR_SUCCESS = "#28A745"
COLOR_SUCCESS_HOVER = "#218838"
COLOR_DANGER = "#DC3545"
COLOR_DANGER_HOVER = "#C82333"
COLOR_TEXT = "#333333"
COLOR_HEADER = "#E9ECEF"

def get_seoul_now():
    try:
        from pytz import timezone
        return datetime.now(timezone("Asia/Seoul"))
    except:
        return datetime.now()

# ========================================================
# 배치 잡 관리 (JSON DB)
# ========================================================
def load_batch_jobs():
    if not os.path.exists(BATCH_JOBS_FILE):
        return []
    try:
        with open(BATCH_JOBS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except:
        return []

def save_batch_jobs(jobs):
    try:
        with open(BATCH_JOBS_FILE, "w", encoding="utf-8") as f:
            json.dump(jobs, f, indent=2, ensure_ascii=False)
    except Exception as e:
        print(f"[Error] 잡 저장 실패: {e}")

def upsert_batch_job(batch_id, **kwargs):
    jobs = load_batch_jobs()
    found = False
    now_str = datetime.now().isoformat()
    
    for j in jobs:
        if j["batch_id"] == batch_id:
            if kwargs.get("status") == "completed" and j.get("status") != "completed":
                if "completed_at" not in kwargs:
                    j["completed_at"] = now_str
            j.update(kwargs)
            j["updated_at"] = now_str
            found = True
            break
            
    if not found:
        new_job = {
            "batch_id": batch_id,
            "created_at": now_str,
            "updated_at": now_str,
            "completed_at": "",
            "archived": False,
            **kwargs
        }
        jobs.insert(0, new_job)
    save_batch_jobs(jobs)

def archive_batch_job(batch_ids, archive=True):
    if isinstance(batch_ids, str): batch_ids = [batch_ids]
    jobs = load_batch_jobs()
    for j in jobs:
        if j["batch_id"] in batch_ids:
            j["archived"] = archive
    save_batch_jobs(jobs)

def hard_delete_batch_job(batch_ids):
    if isinstance(batch_ids, str): batch_ids = [batch_ids]
    jobs = load_batch_jobs()
    jobs = [j for j in jobs if j["batch_id"] not in batch_ids]
    save_batch_jobs(jobs)

# ========================================================
# GUI Class
# ========================================================
class Stage3BatchGUI(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Stage 3: Batch API Manager (Generation)")
        self.geometry("1200x950")
        
        self.api_key_var = tk.StringVar()
        
        # 탭 1 변수 (파일 & 기본옵션)
        self.src_file_var = tk.StringVar()
        self.model_var = tk.StringVar(value="gpt-5-mini")
        self.effort_var = tk.StringVar(value="medium")
        self.skip_exist_var = tk.BooleanVar(value=True)
        
        # Stage 3 전용 옵션 변수
        self.market_var = tk.StringVar(value="네이버 50자")
        self.max_len_var = tk.IntVar(value=50)
        self.num_cand_var = tk.IntVar(value=10)
        self.naming_strategy_var = tk.StringVar(value="통합형")
        
        # 탭 3 변수
        self.batch_id_var = tk.StringVar()
        
        self._configure_styles()
        self._init_ui()
        self._load_key()

    def _configure_styles(self):
        style = ttk.Style()
        try: style.theme_use('clam')
        except: pass
        
        self.configure(background=COLOR_BG)
        
        style.configure("TFrame", background=COLOR_BG)
        style.configure("TLabel", background=COLOR_BG, foreground=COLOR_TEXT, font=("맑은 고딕", 10))
        style.configure("Header.TLabel", font=("맑은 고딕", 11, "bold"), foreground="#444")
        
        style.configure("TLabelframe", background=COLOR_BG, bordercolor="#D0D7DE")
        style.configure("TLabelframe.Label", background=COLOR_BG, foreground="#0056b3", font=("맑은 고딕", 10, "bold"))

        style.configure("TNotebook", background=COLOR_BG, borderwidth=0)
        style.configure("TNotebook.Tab", background="#E1E4E8", padding=[12, 5], font=("맑은 고딕", 10))
        style.map("TNotebook.Tab", background=[("selected", COLOR_WHITE)], foreground=[("selected", COLOR_PRIMARY)])
        
        style.configure("Treeview", background=COLOR_WHITE, fieldbackground=COLOR_WHITE, font=("맑은 고딕", 9), rowheight=28)
        style.configure("Treeview.Heading", background=COLOR_HEADER, foreground="#333", font=("맑은 고딕", 9, "bold"))
        style.map("Treeview", background=[('selected', '#CCE5FF')], foreground=[('selected', 'black')])

        style.configure("TButton", font=("맑은 고딕", 9), padding=5, borderwidth=1)
        style.configure("Primary.TButton", background=COLOR_PRIMARY, foreground="white", bordercolor=COLOR_PRIMARY)
        style.map("Primary.TButton", background=[("active", COLOR_PRIMARY_HOVER)])
        style.configure("Success.TButton", background=COLOR_SUCCESS, foreground="white", bordercolor=COLOR_SUCCESS)
        style.map("Success.TButton", background=[("active", COLOR_SUCCESS_HOVER)])
        style.configure("Danger.TButton", background=COLOR_DANGER, foreground="white", bordercolor=COLOR_DANGER)
        style.map("Danger.TButton", background=[("active", COLOR_DANGER_HOVER)])

    def _init_ui(self):
        main_container = ttk.Frame(self, padding=15)
        main_container.pack(fill='both', expand=True)

        # 1. 상단 API Key
        f_top = ttk.LabelFrame(main_container, text="🔑 API 설정", padding=10)
        f_top.pack(fill='x', pady=(0, 10))
        
        ttk.Label(f_top, text="Batch API Key:", font=("맑은 고딕", 9, "bold")).pack(side='left')
        entry_key = ttk.Entry(f_top, textvariable=self.api_key_var, show="*", width=50, font=("Consolas", 10))
        entry_key.pack(side='left', padx=10)
        ttk.Button(f_top, text="저장", command=self._save_key, style="Primary.TButton").pack(side='left')

        # 2. 메인 탭
        self.main_tabs = ttk.Notebook(main_container)
        self.main_tabs.pack(fill='both', expand=True, pady=5)
        
        self.tab_create = ttk.Frame(self.main_tabs)
        self.tab_manage = ttk.Frame(self.main_tabs) 
        self.tab_merge = ttk.Frame(self.main_tabs)
        
        self.main_tabs.add(self.tab_create, text=" 1. 배치 생성 & 업로드 ")
        self.main_tabs.add(self.tab_manage, text=" 2. 배치 관리 (목록/병합/리포트) ")
        self.main_tabs.add(self.tab_merge, text=" 3. 개별 병합 (수동) ")
        
        self._init_tab_create()
        self._init_tab_manage()
        self._init_tab_merge()
        
        # 3. 로그
        f_log = ttk.LabelFrame(main_container, text="📋 시스템 로그", padding=10)
        f_log.pack(fill='both', expand=True, pady=(10, 0))
        self.log_widget = ScrolledText(f_log, height=15, state='disabled', font=("Consolas", 9), bg="#F1F3F5")
        self.log_widget.pack(fill='both', expand=True)

    def _load_key(self):
        loaded = load_api_key_from_file(API_KEY_FILE)
        if loaded: self.api_key_var.set(loaded)

    def _save_key(self):
        k = self.api_key_var.get().strip()
        if k:
            save_api_key_to_file(k, API_KEY_FILE)
            messagebox.showinfo("저장", "API Key 저장 완료")

    def append_log(self, msg):
        ts = datetime.now().strftime("%H:%M:%S")
        self.log_widget.config(state='normal')
        self.log_widget.insert('end', f"[{ts}] {msg}\n")
        self.log_widget.see('end')
        self.log_widget.config(state='disabled')

    # ----------------------------------------------------
    # Tab 1: Create
    # ----------------------------------------------------
    def _init_tab_create(self):
        container = ttk.Frame(self.tab_create, padding=20)
        container.pack(fill='both', expand=True)
        
        # Step 1: 파일 선택
        f_file = ttk.LabelFrame(container, text="1. 작업 대상 파일 선택", padding=15)
        f_file.pack(fill='x', pady=(0, 15))
        
        ttk.Entry(f_file, textvariable=self.src_file_var, font=("맑은 고딕", 10)).pack(side='left', fill='x', expand=True)
        ttk.Button(f_file, text="📂 파일 찾기", command=self._select_src_file).pack(side='right', padx=5)
        
        # Step 2: Stage 3 상세 옵션
        f_opt = ttk.LabelFrame(container, text="2. Stage 3 생성 옵션", padding=15)
        f_opt.pack(fill='x', pady=5)

        # 모델 & Effort
        fr1 = ttk.Frame(f_opt)
        fr1.pack(fill='x', pady=5)
        ttk.Label(fr1, text="모델 (Model):", width=12).pack(side='left')
        models = list(MODEL_PRICING_USD_PER_MTOK.keys())
        ttk.Combobox(fr1, textvariable=self.model_var, values=models, state="readonly", width=20).pack(side='left', padx=5)
        
        ttk.Label(fr1, text="추론 강도:", width=10).pack(side='left', padx=(20, 5))
        ttk.Combobox(fr1, textvariable=self.effort_var, values=["none", "low", "medium", "high"], state="readonly", width=12).pack(side='left', padx=5)
        
        # 마켓 설정
        fr2 = ttk.Frame(f_opt)
        fr2.pack(fill='x', pady=5)
        ttk.Label(fr2, text="타겟 마켓:", width=12).pack(side='left')
        markets = ["네이버 50자", "쿠팡 100자", "지마켓/옥션 45자", "기타"]
        cb_mk = ttk.Combobox(fr2, textvariable=self.market_var, values=markets, state="readonly", width=15)
        cb_mk.pack(side='left', padx=5)
        cb_mk.bind("<<ComboboxSelected>>", self._on_market_change)

        ttk.Label(fr2, text="최대 글자:", width=10).pack(side='left', padx=(20, 5))
        ttk.Spinbox(fr2, from_=10, to=200, textvariable=self.max_len_var, width=10).pack(side='left', padx=5)

        # 출력 개수 & 전략
        fr3 = ttk.Frame(f_opt)
        fr3.pack(fill='x', pady=5)
        ttk.Label(fr3, text="출력 개수:", width=12).pack(side='left')
        ttk.Spinbox(fr3, from_=1, to=30, textvariable=self.num_cand_var, width=10).pack(side='left', padx=5)

        ttk.Label(fr3, text="명명 전략:", width=10).pack(side='left', padx=(25, 5))
        ttk.Combobox(fr3, textvariable=self.naming_strategy_var, values=["통합형", "옵션포함형"], state="readonly", width=10).pack(side='left', padx=5)

        # 체크박스
        f_row_chk = ttk.Frame(f_opt)
        f_row_chk.pack(fill='x', pady=10)
        ttk.Checkbutton(f_row_chk, text=" 이미 결과(ST3_결과상품명)가 있는 행은 건너뛰기 (Skip)", variable=self.skip_exist_var).pack(side='left')
        
        # Step 3: 실행
        f_step3 = ttk.LabelFrame(container, text="3. 실행", padding=15)
        f_step3.pack(fill='x', pady=15)
        
        btn = ttk.Button(f_step3, text="🚀 JSONL 생성 및 배치 업로드 (Start Batch)", command=self._start_create_batch, style="Success.TButton")
        btn.pack(fill='x', ipady=8)
        ttk.Label(container, text="※ 배치 API는 결과 수신까지 최대 24시간이 소요됩니다. (비용 50% 절감)", foreground="#666").pack()

    def _on_market_change(self, event=None):
        val = self.market_var.get()
        if "네이버" in val: self.max_len_var.set(50)
        elif "쿠팡" in val: self.max_len_var.set(100)
        elif "지마켓" in val: self.max_len_var.set(45)

    def _select_src_file(self):
        p = filedialog.askopenfilename(filetypes=[("Excel", "*.xlsx;*.xls")])
        if p: self.src_file_var.set(p)

    def _start_create_batch(self):
        if not self.api_key_var.get():
            messagebox.showwarning("오류", "API Key가 필요합니다.")
            return
        if not self.src_file_var.get():
            messagebox.showwarning("오류", "파일을 선택해주세요.")
            return
        t = threading.Thread(target=self._run_create_batch)
        t.daemon = True
        t.start()

    def _run_create_batch(self):
        key = self.api_key_var.get().strip()
        src = self.src_file_var.get().strip()
        
        settings = Stage3Settings(
            market=self.market_var.get(),
            max_len=self.max_len_var.get(),
            num_candidates=self.num_cand_var.get(),
            naming_strategy=self.naming_strategy_var.get(),
            model_name=self.model_var.get(),
            reasoning_effort=self.effort_var.get()
        )
        
        try:
            client = OpenAI(api_key=key)
            self.append_log(f"엑셀 로드 중... {os.path.basename(src)}")
            df = pd.read_excel(src)
            
            if "ST2_JSON" not in df.columns:
                raise ValueError("필수 컬럼(ST2_JSON)이 누락되었습니다. Stage 2를 먼저 수행하세요.")

            jsonl_lines = []
            skipped_cnt = 0
            
            for idx, row in df.iterrows():
                # 스킵 로직
                if self.skip_exist_var.get() and "ST3_결과상품명" in df.columns:
                    val = str(row.get("ST3_결과상품명", "")).strip()
                    if val and val != "nan":
                        continue
                
                # 1. Prompt 생성
                req = build_stage3_request_from_row(row, settings)
                prompt = req.prompt

                # 2. Batch Payload 구성
                body = {
                    "model": settings.model_name,
                    "messages": [{"role": "user", "content": prompt}],
                }
                
                is_reasoning = any(x in settings.model_name for x in ["gpt-5", "o1", "o3"])
                if is_reasoning and settings.reasoning_effort != "none":
                    body["reasoning_effort"] = settings.reasoning_effort
                elif not is_reasoning:
                    body["temperature"] = 0.7

                request_obj = {
                    "custom_id": f"row_{idx}",
                    "method": "POST",
                    "url": "/v1/chat/completions",
                    "body": body
                }
                
                jsonl_lines.append(json.dumps(request_obj, ensure_ascii=False))
            
            if not jsonl_lines:
                self.append_log("생성할 요청이 없습니다.")
                return

            base, _ = os.path.splitext(src)
            jsonl_path = f"{base}_stage3_batch_input.jsonl"
            with open(jsonl_path, "w", encoding="utf-8") as f:
                f.write("\n".join(jsonl_lines))
            
            self.append_log(f"JSONL 생성 완료: {len(jsonl_lines)}건 (스킵 {skipped_cnt}건)")
            self.append_log("OpenAI 업로드 중...")
            
            with open(jsonl_path, "rb") as f:
                batch_input_file = client.files.create(file=f, purpose="batch")
            
            file_id = batch_input_file.id
            self.append_log(f"업로드 완료 ID: {file_id}")
            
            batch_job = client.batches.create(
                input_file_id=file_id,
                endpoint="/v1/chat/completions",
                completion_window="24h"
            )
            
            batch_id = batch_job.id
            self.append_log(f"✅ 배치 시작! ID: {batch_id}")
            
            # 로컬 DB 저장
            upsert_batch_job(
                batch_id=batch_id,
                src_excel=src,
                jsonl_path=jsonl_path,
                model=settings.model_name,
                effort=settings.reasoning_effort,
                status=batch_job.status,
                output_file_id=None,
                market=settings.market,
                strategy=settings.naming_strategy
            )
            
            self._load_jobs_all()
            self._load_archive_list()
            messagebox.showinfo("성공", f"배치 작업이 시작되었습니다.\nID: {batch_id}")

        except Exception as e:
            self.append_log(f"에러: {e}")
            messagebox.showerror("에러", str(e))

    # ----------------------------------------------------
    # Tab 2: Manage (List & Trash)
    # ----------------------------------------------------
    def _init_tab_manage(self):
        container = ttk.Frame(self.tab_manage, padding=10)
        container.pack(fill='both', expand=True)

        sub_tabs = ttk.Notebook(container)
        sub_tabs.pack(fill='both', expand=True)
        
        self.sub_active = ttk.Frame(sub_tabs, padding=10)
        self.sub_archive = ttk.Frame(sub_tabs, padding=10)
        
        sub_tabs.add(self.sub_active, text=" ▶ 진행중 / 완료 (Active) ")
        sub_tabs.add(self.sub_archive, text=" 🗑 휴지통 (Archive) ")
        
        # --- Active Tab UI ---
        f_ctrl = ttk.Frame(self.sub_active)
        f_ctrl.pack(fill='x', pady=(0, 10))
        
        ttk.Button(f_ctrl, text="🔄 선택 갱신", command=lambda: self._refresh_selected(self.tree_active)).pack(side='left', padx=2)
        ttk.Button(f_ctrl, text="📥 선택 일괄 병합", command=self._merge_selected, style="Primary.TButton").pack(side='left', padx=10)
        ttk.Button(f_ctrl, text="📊 선택 일괄 리포트", command=self._report_selected_unified, style="Success.TButton").pack(side='left', padx=2)
        ttk.Button(f_ctrl, text="🗑 휴지통 이동", command=self._archive_selected, style="Danger.TButton").pack(side='right', padx=2)
        
        cols = ("batch_id", "status", "created", "completed", "model", "market", "counts")
        self.tree_active = ttk.Treeview(self.sub_active, columns=cols, show='headings', height=18, selectmode='extended')
        
        self.tree_active.tag_configure('odd', background=COLOR_WHITE)
        self.tree_active.tag_configure('even', background='#F2F7FF') 
        
        self.tree_active.heading("batch_id", text="Batch ID")
        self.tree_active.heading("status", text="상태")
        self.tree_active.heading("created", text="생성일시")
        self.tree_active.heading("completed", text="완료일시")
        self.tree_active.heading("model", text="모델")
        self.tree_active.heading("market", text="마켓")
        self.tree_active.heading("counts", text="완료/전체")
        
        self.tree_active.column("batch_id", width=200)
        self.tree_active.column("status", width=90, anchor="center")
        self.tree_active.column("created", width=130, anchor="center")
        self.tree_active.column("completed", width=130, anchor="center")
        self.tree_active.column("model", width=90, anchor="center")
        self.tree_active.column("market", width=100, anchor="center")
        self.tree_active.column("counts", width=90, anchor="center")
        
        # 스크롤바
        sb = ttk.Scrollbar(self.sub_active, orient="vertical", command=self.tree_active.yview)
        self.tree_active.configure(yscroll=sb.set)
        sb.pack(side='right', fill='y')
        self.tree_active.pack(fill='both', expand=True)
        
        # 우클릭 메뉴
        self.menu_active = Menu(self, tearoff=0)
        self.menu_active.add_command(label="상태 갱신", command=lambda: self._refresh_selected(self.tree_active))
        self.menu_active.add_separator()
        self.menu_active.add_command(label="결과 병합", command=self._merge_selected)
        self.menu_active.add_command(label="통합 리포트 생성", command=self._report_selected_unified)
        self.menu_active.add_separator()
        self.menu_active.add_command(label="휴지통으로 이동", command=self._archive_selected)
        
        self.tree_active.bind("<Button-3>", lambda event: self._show_context_menu(event, self.tree_active, self.menu_active))
        self.tree_active.bind("<Double-1>", self._on_tree_double_click)

        # --- Archive Tab UI ---
        f_arch_ctrl = ttk.Frame(self.sub_archive)
        f_arch_ctrl.pack(fill='x', pady=(0, 10))
        ttk.Button(f_arch_ctrl, text="♻️ 선택 복구", command=self._restore_selected, style="Primary.TButton").pack(side='left')
        ttk.Button(f_arch_ctrl, text="🔥 선택 영구 삭제", command=self._hard_delete_selected, style="Danger.TButton").pack(side='right')
        
        self.tree_arch = ttk.Treeview(self.sub_archive, columns=cols, show='headings', height=18, selectmode='extended')
        self.tree_arch.tag_configure('odd', background=COLOR_WHITE)
        self.tree_arch.tag_configure('even', background='#FFF2F2') 

        for col in cols: 
            self.tree_arch.heading(col, text=col.capitalize())
            self.tree_arch.column(col, anchor="center")
        self.tree_arch.column("batch_id", width=200, anchor="w")
        
        self.tree_arch.pack(fill='both', expand=True)
        
        self._load_jobs_all()
        self._load_archive_list()

    def _show_context_menu(self, event, tree, menu):
        item = tree.identify_row(event.y)
        if item:
            if item not in tree.selection():
                tree.selection_set(item)
            menu.post(event.x_root, event.y_root)

    def _get_selected_ids(self, tree):
        selection = tree.selection()
        ids = []
        for item in selection:
            vals = tree.item(item)['values']
            if vals: ids.append(vals[0])
        return ids

    def _load_jobs_all(self):
        if not hasattr(self, 'tree_active'): return
        for i in self.tree_active.get_children(): self.tree_active.delete(i)
        jobs = load_batch_jobs()
        
        idx = 0
        for j in jobs:
            if j.get("archived", False): continue
            
            cnt = "-"
            if "request_counts" in j and j["request_counts"]:
                rc = j["request_counts"]
                cnt = f"{rc.get('completed',0)}/{rc.get('total',0)}"
            
            c_at = (j.get("created_at") or "")[:16].replace("T", " ")
            f_at = (j.get("completed_at") or "")[:16].replace("T", " ")
            
            tag = 'even' if idx % 2 == 0 else 'odd'
            self.tree_active.insert("", "end", values=(
                j["batch_id"], j.get("status"), 
                c_at, f_at, 
                j.get("model"), j.get("market", "-"), cnt
            ), tags=(tag,))
            idx += 1

    def _load_archive_list(self):
        if not hasattr(self, 'tree_arch'): return
        for i in self.tree_arch.get_children(): self.tree_arch.delete(i)
        jobs = load_batch_jobs()
        
        idx = 0
        for j in jobs:
            if not j.get("archived", False): continue
            
            cnt = "-"
            if "request_counts" in j and j["request_counts"]:
                rc = j["request_counts"]
                cnt = f"{rc.get('completed',0)}/{rc.get('total',0)}"

            c_at = (j.get("created_at") or "")[:16].replace("T", " ")
            f_at = (j.get("completed_at") or "")[:16].replace("T", " ")
            
            tag = 'even' if idx % 2 == 0 else 'odd'
            self.tree_arch.insert("", "end", values=(
                j["batch_id"], j.get("status"), 
                c_at, f_at, 
                j.get("model"), j.get("market", "-"), cnt
            ), tags=(tag,))
            idx += 1

    # --- Actions ---
    def _refresh_selected(self, tree):
        ids = self._get_selected_ids(tree)
        if not ids: return
        
        jobs = load_batch_jobs()
        completed = [bid for bid in ids if next((x for x in jobs if x["batch_id"] == bid), {}).get("status") in ["completed", "merged"]]
        
        if completed:
            if messagebox.askyesno("확인", f"{len(completed)}건은 이미 완료되었습니다.\n제외하고 미완료 건만 갱신할까요?"):
                ids = [i for i in ids if i not in completed]
        
        if not ids:
            messagebox.showinfo("취소", "갱신할 대상이 없습니다.")
            return

        t = threading.Thread(target=self._run_refresh_ids, args=(ids,))
        t.daemon = True
        t.start()

    def _run_refresh_ids(self, ids):
        key = self.api_key_var.get().strip()
        client = OpenAI(api_key=key)
        self.append_log(f"선택된 {len(ids)}건 갱신 중...")
        for bid in ids:
            try:
                remote = client.batches.retrieve(bid)
                rc = None
                if remote.request_counts:
                    rc = {"total": remote.request_counts.total, "completed": remote.request_counts.completed, "failed": remote.request_counts.failed}
                upsert_batch_job(bid, status=remote.status, output_file_id=remote.output_file_id, request_counts=rc)
            except Exception as e:
                self.append_log(f"{bid} 갱신 실패: {e}")
        self.after(0, lambda: [self._load_jobs_all(), self._load_archive_list()])
        self.append_log("갱신 완료")

    def _merge_selected(self):
        ids = self._get_selected_ids(self.tree_active)
        if not ids: return
        jobs = load_batch_jobs()
        targets = [bid for bid in ids if next((j for j in jobs if j["batch_id"] == bid), {}).get("status") == "completed"]
        if not targets:
            messagebox.showinfo("알림", "병합할 수 있는 'completed' 상태의 작업이 없습니다.")
            return
        if messagebox.askyesno("병합", f"선택한 {len(targets)}건을 병합하시겠습니까?"):
            t = threading.Thread(target=self._run_merge_multi, args=(targets,))
            t.daemon = True
            t.start()

    def _run_merge_multi(self, ids):
        key = self.api_key_var.get().strip()
        client = OpenAI(api_key=key)
        success_cnt = 0
        
        for bid in ids:
            self.append_log(f"--- 병합 시작: {bid} ---")
            try:
                jobs = load_batch_jobs()
                local_job = next((j for j in jobs if j["batch_id"] == bid), None)
                remote = client.batches.retrieve(bid)
                content = client.files.content(remote.output_file_id).content
                
                if local_job and local_job.get("src_excel"):
                    src_path = local_job["src_excel"]
                    base, _ = os.path.splitext(src_path)
                    out_jsonl = f"{base}_stage3_batch_output.jsonl"
                    out_excel = f"{base}_stage3_batch_done.xlsx"
                else:
                    out_jsonl = f"output_{bid}.jsonl"
                    out_excel = f"output_{bid}.xlsx"
                    src_path = None

                with open(out_jsonl, "wb") as f: f.write(content)
                
                results_map = {}
                with open(out_jsonl, "r", encoding="utf-8") as f:
                    for line in f:
                        if not line.strip(): continue
                        data = json.loads(line)
                        cid = data.get("custom_id")
                        try:
                            val = data["response"]["body"]["choices"][0]["message"]["content"].strip()
                            results_map[cid] = val
                        except: results_map[cid] = ""
                
                if src_path and os.path.exists(src_path):
                    df = pd.read_excel(src_path)
                    if "ST3_결과상품명" not in df.columns: df["ST3_결과상품명"] = ""
                    df["ST3_결과상품명"] = df["ST3_결과상품명"].astype(str)
                    cnt = 0
                    for cid, val in results_map.items():
                        try:
                            idx = int(cid.split("_")[1])
                            if 0 <= idx < len(df):
                                df.at[idx, "ST3_결과상품명"] = val
                                cnt += 1
                        except: pass
                    df.to_excel(out_excel, index=False)
                    upsert_batch_job(bid, out_excel=out_excel, status="merged")
                    
                    self.append_log(f"✅ 병합 완료 ({cnt}건): {os.path.basename(out_excel)}")
                    success_cnt += 1
                else:
                    self.append_log(f"⚠️ 원본 없음. JSONL만 저장.")
            except Exception as e:
                self.append_log(f"❌ {bid} 병합 실패: {e}")
        
        self.append_log(f"=== 병합 완료 ({success_cnt}/{len(ids)}) ===")
        self._load_jobs_all()
        messagebox.showinfo("완료", f"{success_cnt}건 병합 완료.")

    def _report_selected_unified(self):
        ids = self._get_selected_ids(self.tree_active)
        if not ids: return
        jobs = load_batch_jobs()
        targets = [bid for bid in ids if next((j for j in jobs if j["batch_id"] == bid), {}).get("status") == "merged"]
        if not targets:
            messagebox.showinfo("알림", "상태가 'merged'인 작업이 없습니다.")
            return

        if messagebox.askyesno("리포트", f"선택한 {len(targets)}건의 통합 리포트를 생성합니까?"):
            t = threading.Thread(target=self._run_report_unified, args=(targets,))
            t.daemon = True
            t.start()

    def _run_report_unified(self, ids):
        self.append_log(f"--- 통합 리포트 생성 ({len(ids)}건) ---")
        jobs = load_batch_jobs()
        all_reps = []
        
        for bid in ids:
            local_job = next((j for j in jobs if j["batch_id"] == bid), None)
            if not local_job: continue
            out_path = local_job.get("out_excel")
            
            if not out_path or not os.path.exists(out_path):
                self.append_log(f"❌ 파일 없음: {bid}")
                continue
            
            try:
                df = pd.read_excel(out_path)
                for idx, row in df.iterrows():
                    st3 = safe_str(row.get("ST3_결과상품명", ""))
                    cands = [x for x in st3.split('\n') if x.strip()]
                    
                    all_reps.append({
                        "Batch_ID": bid,
                        "행번호": idx + 2,
                        "상품코드": safe_str(row.get("상품코드", "")),
                        "후보수": len(cands),
                        "ST3_첫줄": cands[0] if cands else ""
                    })
            except: pass

        if not all_reps:
            messagebox.showinfo("알림", "리포트 데이터 없음")
            return

        try:
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            save_dir = os.path.dirname(os.path.abspath(__file__))
            report_path = os.path.join(save_dir, f"Stage3_Unified_Report_{ts}.xlsx")
            pd.DataFrame(all_reps).to_excel(report_path, index=False)
            
            self.append_log(f"📊 생성 완료: {report_path}")
            if messagebox.askyesno("완료", "파일을 여시겠습니까?"):
                os.startfile(report_path)
        except Exception as e:
            self.append_log(f"실패: {e}")

    def _archive_selected(self):
        ids = self._get_selected_ids(self.tree_active)
        if ids and messagebox.askyesno("보관", f"{len(ids)}건 휴지통 이동?"):
            archive_batch_job(ids, True)
            self._load_jobs_all()
            self._load_archive_list()

    def _restore_selected(self):
        ids = self._get_selected_ids(self.tree_arch)
        if ids and messagebox.askyesno("복구", f"{len(ids)}건 복구?"):
            archive_batch_job(ids, False)
            self._load_jobs_all()
            self._load_archive_list()

    def _hard_delete_selected(self):
        ids = self._get_selected_ids(self.tree_arch)
        if ids and messagebox.askyesno("삭제", "영구 삭제하시겠습니까?"):
            hard_delete_batch_job(ids)
            self._load_archive_list()

    def _on_tree_double_click(self, event):
        sel = self.tree_active.selection()
        if not sel: return
        bid = self.tree_active.item(sel[0])['values'][0]
        self.batch_id_var.set(bid)
        self.main_tabs.select(self.tab_merge)

    # ----------------------------------------------------
    # Tab 3: Merge (Manual)
    # ----------------------------------------------------
    def _init_tab_merge(self):
        container = ttk.Frame(self.tab_merge, padding=20)
        container.pack(fill='both', expand=True)
        f_in = ttk.LabelFrame(container, text="개별 작업", padding=15)
        f_in.pack(fill='x')
        ttk.Label(f_in, text="Batch ID:").pack(side='left')
        ttk.Entry(f_in, textvariable=self.batch_id_var, width=45, font=("Consolas", 10)).pack(side='left', padx=10)
        
        f_btn = ttk.Frame(container)
        f_btn.pack(fill='x', pady=20)
        ttk.Button(f_btn, text="1. 결과 병합", command=self._start_merge, style="Primary.TButton").pack(fill='x', pady=5)
        ttk.Button(f_btn, text="2. 단일 리포트", command=self._start_diff_report).pack(fill='x', pady=5)

    def _start_merge(self):
        t = threading.Thread(target=self._run_merge)
        t.daemon = True
        t.start()

    def _run_merge(self):
        bid = self.batch_id_var.get().strip()
        if bid: self._run_merge_multi([bid])

    def _start_diff_report(self):
        t = threading.Thread(target=self._run_diff_report)
        t.daemon = True
        t.start()

    def _run_diff_report(self):
        bid = self.batch_id_var.get().strip()
        if bid: self._run_report_unified([bid])

if __name__ == "__main__":
    app = Stage3BatchGUI()
    app.mainloop()