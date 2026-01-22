"""
stage4_2_gui.py

Stage 4-2: LLM 최종 심사 및 정렬 GUI
- 기능 추가: [상세 비교 리포트]
  (Input vs Output을 줄 단위로 비교하여 '제거된 후보'를 명확히 적출)
"""

import os
import re
import threading
import pytz
import time
import json
from datetime import datetime

import tkinter as tk
from tkinter import ttk, filedialog, messagebox
from tkinter.scrolledtext import ScrolledText
import pandas as pd

from stage4_2_core import (
    Stage4_2Core,
    Stage4_2Settings,
    build_stage4_2_request_from_row,
    MODEL_PRICING_USD_PER_MTOK,
    load_api_key_from_file,
    save_api_key_to_file,
    safe_str  # 안전한 문자열 변환 함수
)
from stage4_2_run_history import append_run_history

# =========================================================
# [런처 연동] JobManager & 유틸 (표준화됨)
# =========================================================
def get_root_filename(filename):
    """
    파일명에서 버전 정보(_T*_I* 또는 _t*_i*, T4(완)_I*, _I*(업완) 포함) 및 기타 꼬리표를 떼고 원본명(Key)만 추출
    예: 나이키_T3_I0.xlsx -> 나이키.xlsx
    예: 아디다스_T4_I1.xlsx -> 아디다스.xlsx
    예: 나이키_T4_I0(업완).xlsx -> 나이키.xlsx
    예: 나이키_T4_I0_T4(완)_I1.xlsx -> 나이키.xlsx (여러 버전 패턴 제거)
    예: 나이키_T4_I5(업완).xlsx -> 나이키.xlsx
    """
    name = os.path.basename(filename)
    base, ext = os.path.splitext(name)
    
    # 1. 버전 패턴 (_T숫자(괄호)?_I숫자(괄호)? 또는 _t숫자(괄호)?_i숫자(괄호)?) 반복 제거 (대소문자 구분 없음)
    # 패턴이 여러 번 나올 수 있으므로 반복 제거, T4(완)_I* 및 _I*(업완) 패턴도 포함
    while True:
        new_base = re.sub(r"_[Tt]\d+\([^)]*\)_[Ii]\d+(\([^)]+\))?", "", base, flags=re.IGNORECASE)  # T4(완)_I*(업완)? 패턴 제거
        new_base = re.sub(r"_[Tt]\d+_[Ii]\d+(\([^)]+\))?", "", new_base, flags=re.IGNORECASE)  # 일반 T*_I*(업완)? 패턴 제거
        if new_base == base:
            break
        base = new_base
    
    # 2. 괄호 안의 텍스트 제거 (예: (업완), (완료) 등) - 버전 패턴의 괄호는 이미 제거됨
    base = re.sub(r"\([^)]*\)", "", base)
    
    # 3. 기타 구형 꼬리표 제거 (호환성 유지)
    suffixes = ["_stage1_mapping", "_stage1_img_mapping", "_stage2_analysis", "_stage3_done", "_stage4_2_done", "_with_images", "_filtered"]
    for s in suffixes:
        base = base.replace(s, "")
    
    # 4. 끝에 남은 언더스코어 제거
    base = base.rstrip("_")
        
    return base + ext

class JobManager:
    DB_FILE = None

    @classmethod
    def find_db_path(cls):
        if cls.DB_FILE and os.path.exists(cls.DB_FILE): return cls.DB_FILE
        
        current_dir = os.path.dirname(os.path.abspath(__file__))
        search_dirs = [
            current_dir,
            os.path.abspath(os.path.join(current_dir, "..")), 
            os.path.abspath(os.path.join(current_dir, "..", ".."))
        ]
        
        for d in search_dirs:
            target = os.path.join(d, "job_history.json")
            if os.path.exists(target):
                cls.DB_FILE = target
                print(f"[JobManager] DB Found: {target}")
                return target
        
        default_path = os.path.abspath(os.path.join(current_dir, "..", "job_history.json"))
        cls.DB_FILE = default_path
        return default_path

    @classmethod
    def load_jobs(cls):
        db_path = cls.find_db_path()
        if not os.path.exists(db_path): return {}
        try:
            with open(db_path, 'r', encoding='utf-8') as f: return json.load(f)
        except: return {}

    @classmethod
    def update_status(cls, filename, text_msg=None, img_msg=None):
        """런처 현황판 상태 업데이트"""
        db_path = cls.find_db_path()
        data = cls.load_jobs()
        now = datetime.now().strftime("%m-%d %H:%M")
        
        # 파일명 Key로 사용 (확장자 포함 or 제외 통일 필요, 여기선 get_root_filename 결과 사용)
        if filename not in data:
            data[filename] = {
                "start_time": datetime.now().strftime("%Y-%m-%d %H:%M"),
                "text_status": "대기", "text_time": "-",
                "image_status": "대기", "image_time": "-", "memo": ""
            }

        if text_msg:
            data[filename]["text_status"] = text_msg
            data[filename]["text_time"] = now
        if img_msg:
            data[filename]["image_status"] = img_msg
            data[filename]["image_time"] = now
            
        data[filename]["last_update"] = now
        
        try:
            with open(db_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=4)
        except Exception as e:
            print(f"[JobManager Error] {e}")

def get_seoul_now() -> datetime:
    return datetime.now(pytz.timezone("Asia/Seoul"))

class Stage4_2GUI(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Stage 4-2: Final Ranker (Detail Diff Report)")
        self.geometry("1000x850")
        
        self._configure_styles()

        # 변수 초기화
        self.api_key_var = tk.StringVar()
        self.input_file_path = tk.StringVar()
        self.output_file_path = "" 
        
        # 기본값
        self.model_var = tk.StringVar(value="gpt-5-mini")
        self.effort_var = tk.StringVar(value="low")

        # 상태 변수
        self.is_running = False
        self.stop_requested = False
        
        # 통계 변수
        self.stat_progress = tk.StringVar(value="0.0%")
        self.stat_count = tk.StringVar(value="0 / 0")
        self.stat_success = tk.StringVar(value="0")
        self.stat_fail = tk.StringVar(value="0")
        self.stat_cost = tk.StringVar(value="$0.0000")
        self.stat_time = tk.StringVar(value="00:00:00")
        self.status_msg = tk.StringVar(value="파일을 선택하고 작업을 시작하세요.")

        self._init_ui()
        self._load_key()

    def _configure_styles(self):
        style = ttk.Style()
        try:
            style.theme_use('clam')
        except:
            pass
        
        style.configure("TFrame", background="#f5f5f5")
        style.configure("TLabelframe", background="#f5f5f5", font=("맑은 고딕", 10, "bold"))
        style.configure("TLabelframe.Label", background="#f5f5f5", foreground="#333333")
        style.configure("TLabel", background="#f5f5f5", font=("맑은 고딕", 10))
        
        style.configure("Header.TLabel", font=("맑은 고딕", 11, "bold"), foreground="#444")
        style.configure("Stat.TLabel", font=("맑은 고딕", 12, "bold"), foreground="#0052cc")
        style.configure("Cost.TLabel", font=("맑은 고딕", 12, "bold"), foreground="#d32f2f")
        
        style.configure("Action.TButton", font=("맑은 고딕", 11, "bold"), padding=5)
        style.configure("Stop.TButton", font=("맑은 고딕", 11, "bold"), foreground="red", padding=5)
        style.configure("Diff.TButton", font=("맑은 고딕", 11, "bold"), foreground="#00695c", padding=5)

        self.configure(background="#f5f5f5")

    def _init_ui(self):
        main_frame = ttk.Frame(self, padding=20)
        main_frame.pack(fill='both', expand=True)

        # 1. 상단 설정
        setting_frame = ttk.LabelFrame(main_frame, text="기본 설정 (Configuration)", padding=15)
        setting_frame.pack(fill='x', pady=(0, 10))

        row1 = ttk.Frame(setting_frame)
        row1.pack(fill='x', pady=2)
        ttk.Label(row1, text="OpenAI Key:", width=12).pack(side='left')
        entry_key = ttk.Entry(row1, textvariable=self.api_key_var, show="*", width=45)
        entry_key.pack(side='left', padx=5)
        ttk.Button(row1, text="Key 저장", command=self._save_key, width=8).pack(side='left')

        row2 = ttk.Frame(setting_frame)
        row2.pack(fill='x', pady=5)
        ttk.Label(row2, text="Model:", width=12).pack(side='left')
        models = list(MODEL_PRICING_USD_PER_MTOK.keys())
        ttk.Combobox(row2, textvariable=self.model_var, values=models, state="readonly", width=18).pack(side='left', padx=5)
        
        ttk.Label(row2, text="Effort:", width=8).pack(side='left', padx=(20,0))
        ttk.Combobox(row2, textvariable=self.effort_var, values=["low", "medium", "high"], state="readonly", width=12).pack(side='left', padx=5)

        # 2. 파일 선택
        file_frame = ttk.LabelFrame(main_frame, text="작업 대상 (Target File)", padding=15)
        file_frame.pack(fill='x', pady=(0, 10))
        
        f_inner = ttk.Frame(file_frame)
        f_inner.pack(fill='x')
        ttk.Entry(f_inner, textvariable=self.input_file_path, font=("맑은 고딕", 10)).pack(side='left', fill='x', expand=True, padx=(0, 5))
        ttk.Button(f_inner, text="📂 파일 선택", command=self._select_file).pack(side='right')

        # 3. 대시보드
        dash_frame = ttk.LabelFrame(main_frame, text="실시간 현황 (Dashboard)", padding=15)
        dash_frame.pack(fill='x', pady=(0, 10))

        d1 = ttk.Frame(dash_frame)
        d1.pack(fill='x', pady=5)
        ttk.Label(d1, text="진행률:", style="Header.TLabel", width=10).pack(side='left')
        self.pb = ttk.Progressbar(d1, maximum=100, mode='determinate')
        self.pb.pack(side='left', fill='x', expand=True, padx=5)
        ttk.Label(d1, textvariable=self.stat_progress, style="Stat.TLabel", width=8).pack(side='right')

        d2 = ttk.Frame(dash_frame)
        d2.pack(fill='x', pady=5)
        ttk.Label(d2, text="처리 건수:", width=10).pack(side='left')
        ttk.Label(d2, textvariable=self.stat_count, width=15, foreground="blue", font=("맑은 고딕", 10, "bold")).pack(side='left')
        
        ttk.Label(d2, text="성공/실패:", width=10).pack(side='left')
        lbl_succ = ttk.Label(d2, textvariable=self.stat_success, foreground="green", font=("맑은 고딕", 10, "bold"))
        lbl_succ.pack(side='left')
        ttk.Label(d2, text=" / ").pack(side='left')
        lbl_fail = ttk.Label(d2, textvariable=self.stat_fail, foreground="red", font=("맑은 고딕", 10, "bold"))
        lbl_fail.pack(side='left')

        d3 = ttk.Frame(dash_frame)
        d3.pack(fill='x', pady=5)
        ttk.Label(d3, text="예상 비용:", width=10).pack(side='left')
        ttk.Label(d3, textvariable=self.stat_cost, style="Cost.TLabel", width=15).pack(side='left')
        ttk.Label(d3, text="경과 시간:", width=10).pack(side='left')
        ttk.Label(d3, textvariable=self.stat_time, font=("맑은 고딕", 10)).pack(side='left')

        # 4. 컨트롤 버튼
        btn_frame = ttk.Frame(main_frame)
        btn_frame.pack(fill='x', pady=(0, 10))
        
        self.btn_start = ttk.Button(btn_frame, text="▶ 작업 시작 (이어하기)", style="Action.TButton", command=self._start_thread)
        self.btn_start.pack(side='left', fill='x', expand=True, padx=(0, 5))
        
        self.btn_stop = ttk.Button(btn_frame, text="⏹ 저장 후 중단", style="Stop.TButton", command=self._request_stop, state='disabled')
        self.btn_stop.pack(side='left', fill='x', expand=True, padx=5)

        self.btn_diff = ttk.Button(btn_frame, text="🔍 상세 비교 리포트 생성", style="Diff.TButton", command=self._generate_diff_report, state='disabled')
        self.btn_diff.pack(side='right', fill='x', expand=True, padx=(5, 0))

        ttk.Label(main_frame, textvariable=self.status_msg, foreground="#555", anchor='center').pack(fill='x', pady=(0, 5))

        # 5. 로그창
        log_frame = ttk.LabelFrame(main_frame, text="상세 로그", padding=10)
        log_frame.pack(fill='both', expand=True)
        self.log_widget = ScrolledText(log_frame, height=10, state='disabled', font=("Consolas", 9))
        self.log_widget.pack(fill='both', expand=True)

    # --- 유틸 메서드 ---
    def _save_key(self):
        k = self.api_key_var.get().strip()
        if k:
            save_api_key_to_file(k)
            messagebox.showinfo("알림", "API Key 저장 완료")

    def _load_key(self):
        k = load_api_key_from_file()
        if k:
            self.api_key_var.set(k)

    def _select_file(self):
        p = filedialog.askopenfilename(
            title="Stage4-2 엑셀 선택 (T4 버전만 가능)",
            filetypes=[("Excel Files", "*.xlsx;*.xls")]
        )
        if p:
            # T4 포함 여부 검증
            base_name = os.path.splitext(os.path.basename(p))[0]
            if not re.search(r"_T4_[Ii]\d+", base_name, re.IGNORECASE):
                messagebox.showerror(
                    "오류", 
                    f"이 도구는 T4 버전의 엑셀 파일만 처리할 수 있습니다.\n\n"
                    f"선택한 파일: {os.path.basename(p)}\n"
                    f"파일명에 '_T4_I*' 패턴이 포함되어 있어야 합니다."
                )
                return
            
            # [스마트 이어하기] _done 파일 감지
            dir_name = os.path.dirname(p)
            base_name, ext = os.path.splitext(os.path.basename(p))
            
            if "_stage4_2_done" in base_name or "T4(완)" in base_name:
                self.input_file_path.set(p)
                self._log(f"결과 파일 선택됨: {os.path.basename(p)}")
                self.status_msg.set("이어서 작업을 진행합니다.")
                return

            done_file_name = f"{base_name}_stage4_2_done{ext}"
            done_file_path = os.path.join(dir_name, done_file_name)

            if os.path.exists(done_file_path):
                ans = messagebox.askyesno(
                    "이어하기 감지",
                    f"이전에 작업하던 결과 파일이 발견되었습니다.\n\n'{done_file_name}'\n\n이 파일을 로드하여 이어서 하시겠습니까?"
                )
                if ans:
                    self.input_file_path.set(done_file_path)
                    self._log(f"작업 중이던 파일 로드: {done_file_name}")
                    self.status_msg.set("작업 재개 준비 완료")
                else:
                    self.input_file_path.set(p)
                    self._log(f"원본 파일 새로 선택: {os.path.basename(p)}")
                    self.status_msg.set("새 작업 준비 완료")
            else:
                self.input_file_path.set(p)
                self._log(f"파일 선택됨: {os.path.basename(p)}")
                self.status_msg.set("준비 완료.")
            
            self.btn_diff.config(state='disabled')
            self.output_file_path = ""

    def _log(self, msg):
        self.log_widget.after(0, self._append_log, msg)

    def _append_log(self, msg):
        t = datetime.now().strftime("%H:%M:%S")
        self.log_widget.config(state='normal')
        self.log_widget.insert(tk.END, f"[{t}] {msg}\n")
        self.log_widget.see(tk.END)
        self.log_widget.config(state='disabled')

    def _request_stop(self):
        if self.is_running:
            self.stop_requested = True
            self.status_msg.set("⚠️ 저장 후 중단 요청됨! 잠시만 기다려주세요...")
            self.btn_stop.config(state='disabled')

    # --- [강화된] 상세 비교 리포트 생성 ---
    def _generate_diff_report(self):
        input_path = self.input_file_path.get()
        output_path = self.output_file_path

        if not output_path or not os.path.exists(output_path):
            messagebox.showwarning("경고", "결과 파일이 없습니다.")
            return

        try:
            self._log("--- 상세 비교 리포트 생성 중... ---")
            df_in = pd.read_excel(input_path)
            df_out = pd.read_excel(output_path)

            cand_col = 'ST3_결과상품명'
            for c in df_in.columns:
                if 'filtered' in c or '정제결과' in c:
                    cand_col = c
            
            res_col = 'ST4_최종결과'
            if res_col not in df_out.columns:
                messagebox.showerror("오류", "결과 컬럼이 없습니다.")
                return

            diff_list = []
            
            for idx, row_in in df_in.iterrows():
                p_code = safe_str(row_in.get('상품코드', ''))
                st1 = safe_str(row_in.get('ST1_정제상품명', ''))
                
                # 1. 입력 후보군 리스트화
                raw_st3 = safe_str(row_in.get(cand_col, ''))
                candidates = [x.strip() for x in raw_st3.split('\n') if x.strip()]
                
                # 2. 출력 결과 리스트화
                st4_val = ""
                if idx < len(df_out):
                    st4_val = safe_str(df_out.iloc[idx].get(res_col, ''))
                
                results = [x.strip() for x in st4_val.split('\n') if x.strip()]

                # 3. 제거된 항목 찾기 (Input에는 있는데 Output에는 없는 것)
                removed_items = [c for c in candidates if c not in results]
                removed_str = "\n".join(removed_items)

                # 4. 상태 판별
                if not st4_val:
                    status = "❌ 실패/공란"
                elif not removed_items and len(candidates) == len(results):
                    # 순서만 바뀌었거나 그대로인 경우
                    if candidates == results:
                        status = "변동 없음"
                    else:
                        status = "순서 변경"
                else:
                    # 무언가 삭제되었거나 (Safety Net으로) 내용이 바뀐 경우
                    status = f"✅ 최적화됨 ({len(removed_items)}개 삭제)"

                diff_list.append({
                    "행번호": idx + 2,
                    "상품코드": p_code,
                    "ST1_기준명": st1,
                    "입력_후보수": len(candidates),
                    "출력_결과수": len(results),
                    "삭제된_후보_목록": removed_str,  # [핵심] 삭제된 것만 모아서 보여줌
                    "ST4_최종결과": st4_val,
                    "상태": status
                })

            df_diff = pd.DataFrame(diff_list)
            
            # 저장
            base, _ = os.path.splitext(output_path)
            diff_path = f"{base}_detail_diff_report.xlsx"
            df_diff.to_excel(diff_path, index=False)
            
            self._log(f"리포트 생성 완료: {diff_path}")
            
            if messagebox.askyesno("완료", f"상세 리포트가 생성되었습니다.\n삭제된 후보를 확인하시겠습니까?\n\n파일: {os.path.basename(diff_path)}"):
                os.startfile(diff_path)

        except Exception as e:
            self._log(f"리포트 오류: {e}")
            messagebox.showerror("오류", str(e))

    # --- 메인 스레드 ---
    def _start_thread(self):
        if self.is_running: return
        key = self.api_key_var.get().strip()
        path = self.input_file_path.get().strip()
        
        if not key:
            messagebox.showwarning("오류", "API Key가 없습니다.")
            return
        if not path or not os.path.exists(path):
            messagebox.showwarning("오류", "파일이 존재하지 않습니다.")
            return
        
        # T4 포함 여부 검증
        base_name = os.path.splitext(os.path.basename(path))[0]
        if not re.search(r"_T4_[Ii]\d+", base_name, re.IGNORECASE):
            messagebox.showerror(
                "오류", 
                f"이 도구는 T4 버전의 엑셀 파일만 처리할 수 있습니다.\n\n"
                f"선택한 파일: {os.path.basename(path)}\n"
                f"파일명에 '_T4_I*' 패턴이 포함되어 있어야 합니다."
            )
            return
            
        self.is_running = True
        self.stop_requested = False
        self.btn_start.config(state='disabled')
        self.btn_stop.config(state='normal')
        self.btn_diff.config(state='disabled')
        self.status_msg.set("작업 초기화 중...")
        
        t = threading.Thread(target=self._run_process, args=(key, path))
        t.daemon = True
        t.start()

    def _run_process(self, api_key, input_path):
        try:
            core = Stage4_2Core(api_key)
            df = pd.read_excel(input_path)
            
            cand_col = 'ST3_결과상품명'
            for c in df.columns:
                if 'filtered' in c or '정제결과' in c:
                    cand_col = c
            
            self._log(f"▶ 입력: {os.path.basename(input_path)} ({len(df)}행)")

            target_col = 'ST4_최종결과'
            if target_col not in df.columns:
                df[target_col] = ""
            df[target_col] = df[target_col].astype(str)

            # 저장 경로 (T4 → T4(완) 형식으로 저장, I 버전과 (업완) 등 모든 후속 정보 유지)
            base_dir = os.path.dirname(input_path)
            base_name = os.path.splitext(os.path.basename(input_path))[0]
            
            # 입력 파일명에서 버전 정보 추출 (I 버전 이후의 모든 부분 포함)
            pattern = r"_T(\d+)_I(\d+)(.*?)(?:_[Tt]\d+_[Ii]\d+|$)"
            match = re.search(pattern, base_name, re.IGNORECASE)
            if match:
                current_t = int(match.group(1))
                current_i = int(match.group(2))
                i_suffix = match.group(3)  # I 버전 이후의 모든 부분 (예: (업완))
                # 원본명 추출 (버전 정보 제거)
                original_name = base_name[: match.start()].rstrip("_")
                # T4(완)_I* 형식으로 저장 (I 버전과 후속 정보 그대로 유지)
                out_filename = f"{original_name}_T4(완)_I{current_i}{i_suffix}.xlsx"
            else:
                # 버전 정보가 없으면 T4(완)_I0으로 생성
                out_filename = f"{base_name}_T4(완)_I0.xlsx"
            out_path = os.path.join(base_dir, out_filename)

            total_rows = len(df)
            start_dt = get_seoul_now()
            
            stats = {
                "in_tok": 0, "out_tok": 0, "reason_tok": 0,
                "in_cost": 0.0, "out_cost": 0.0, "total_cost": 0.0,
                "success": 0, "fail": 0, "api_calls": 0,
                "skipped": 0
            }

            settings = Stage4_2Settings(
                model_name=self.model_var.get(),
                reasoning_effort=self.effort_var.get()
            )

            self._update_timer(start_dt)
            processed_now = 0

            for idx, row in df.iterrows():
                if self.stop_requested:
                    self._log("⛔ 사용자 요청으로 중단함.")
                    break

                val = str(row.get(target_col, "")).strip()
                if val and val != "nan":
                    stats['skipped'] += 1
                    self._update_ui_stats(idx + 1, total_rows, stats)
                    continue

                req = build_stage4_2_request_from_row(row, idx, cand_col)
                res = core.execute_request(req, settings)

                if res.error and "Safety Net" not in res.error:
                    self._log(f"[Row {idx+1}] ❌ {res.error}")
                    stats['fail'] += 1
                else:
                    df.at[idx, target_col] = res.output_text
                    
                    if res.usage.total_cost > 0:
                        stats['api_calls'] += 1
                        stats['in_tok'] += res.usage.input_tokens
                        stats['out_tok'] += res.usage.output_tokens
                        stats['reason_tok'] += res.usage.reasoning_tokens
                        stats['in_cost'] += res.usage.input_cost
                        stats['out_cost'] += res.usage.output_cost
                        stats['total_cost'] += res.usage.total_cost
                    
                    if res.error: 
                        self._log(f"[Row {idx+1}] ⚠️ Safety Net 발동")
                    
                    stats['success'] += 1
                
                processed_now += 1
                self._update_ui_stats(idx + 1, total_rows, stats)

                # 자동 저장
                if processed_now % 10 == 0:
                    df.to_excel(out_path, index=False)
                    self._log(f"💾 자동 저장 ({processed_now}건 완료)")

            finish_dt = get_seoul_now()
            
            df.to_excel(out_path, index=False)
            self.output_file_path = out_path
            self._log(f"💾 최종 저장 완료: {os.path.basename(out_path)}")

            if stats['api_calls'] > 0 or stats['success'] > 0:
                append_run_history(
                    stage="Stage 4-2",
                    model_name=settings.model_name,
                    reasoning_effort=settings.reasoning_effort,
                    src_file=input_path,
                    out_file=out_path,
                    total_rows=total_rows,
                    api_rows=stats['api_calls'],
                    elapsed_seconds=(finish_dt - start_dt).total_seconds(),
                    total_in_tok=stats['in_tok'],
                    total_out_tok=stats['out_tok'],
                    total_reasoning_tok=stats['reason_tok'],
                    input_cost_usd=stats['in_cost'],
                    output_cost_usd=stats['out_cost'],
                    total_cost_usd=stats['total_cost'],
                    success_rows=stats['success'],
                    fail_rows=stats['fail'],
                    start_dt=start_dt,
                    finish_dt=finish_dt
                )

            if self.stop_requested:
                msg = "작업이 안전하게 중단되었습니다."
            else:
                msg = "모든 작업이 완료되었습니다."
                self._log("🎉 작업 완료")

            # 메인 런처 현황판에 T4-2(최종완료) 상태 기록 (img 상태는 변경하지 않음)
            try:
                root_name = get_root_filename(out_path)
                JobManager.update_status(root_name, text_msg="T4-2(최종완료)")
                self._log(f"[INFO] 런처 현황판 업데이트: {root_name} -> T4-2(최종완료)")
            except Exception as e:
                self._log(f"[WARN] 런처 현황판 업데이트 실패: {e}")
            
            self.btn_diff.config(state='normal')
            self.status_msg.set(msg)
            self._show_completion_dialog(msg, stats, out_path)

        except Exception as e:
            self._log(f"🔥 오류: {e}")
            messagebox.showerror("오류", str(e))
        finally:
            self.is_running = False
            self.stop_requested = False
            self.btn_start.config(state='normal')
            self.btn_stop.config(state='disabled')

    def _update_ui_stats(self, current_idx, total_rows, stats):
        pct = (current_idx / total_rows) * 100
        self.pb['value'] = pct
        self.stat_progress.set(f"{pct:.1f}%")
        
        done = stats['success'] + stats['fail'] + stats['skipped']
        self.stat_count.set(f"{done} / {total_rows}")
        self.stat_success.set(str(stats['success']))
        self.stat_fail.set(str(stats['fail']))
        self.stat_cost.set(f"${round(stats['total_cost'], 4)}")
        
        msg = f"처리 중... {current_idx}/{total_rows}"
        if stats['skipped'] > 0:
            msg += f" (Skip: {stats['skipped']})"
        self.status_msg.set(msg)
        self.update_idletasks()

    def _update_timer(self, start_dt):
        if not self.is_running: return
        now = get_seoul_now()
        diff = now - start_dt
        s = int(diff.total_seconds())
        h, rem = divmod(s, 3600)
        m, sec = divmod(rem, 60)
        self.stat_time.set(f"{h:02}:{m:02}:{sec:02}")
        self.after(500, lambda: self._update_timer(start_dt))

    def _show_completion_dialog(self, title, stats, path):
        total_done = stats['success'] + stats['fail'] + stats['skipped']
        msg = (
            f"[{title}]\n\n"
            f"총 처리: {total_done}건\n"
            f" - 신규 성공: {stats['success']}\n"
            f" - 이미 완료: {stats['skipped']}\n"
            f" - 실패: {stats['fail']}\n\n"
            f"총 비용: ${round(stats['total_cost'], 4)}\n\n"
            f"저장 파일:\n{os.path.basename(path)}"
        )
        messagebox.showinfo("완료", msg)

if __name__ == "__main__":
    app = Stage4_2GUI()
    app.mainloop()