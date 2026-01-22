"""
stage3_LLM_gui.py

- Stage 3: LLM 기반 최종 상품명 생성 GUI (Final Integrated Version)
- 원본의 강력한 로직(안전 파싱, 백업 저장, 툴팁) 100% 유지
- 최신 디자인(대시보드, 중단, 스마트 이어하기) 적용
"""

import os
import re
import time
import threading
import subprocess
import json
from datetime import datetime
import pandas as pd
import tkinter as tk
from tkinter import ttk, filedialog, messagebox
from tkinter.scrolledtext import ScrolledText

from openai import OpenAI

# -----------------------------------------------------------
# stage3_core / history 의존성 임포트
# (사용자 환경에 해당 파일들이 존재한다고 가정)
# -----------------------------------------------------------
try:
    from stage3_core import (
        safe_str,
        Stage3Settings,
        Stage3Request,
        build_stage3_request_from_row,
    )
    from stage3_run_history import append_run_history
except ImportError:
    # 비상용 더미 (의존성 파일이 없을 경우 대비)
    def safe_str(x): return str(x) if x is not None else ""
    def append_run_history(*args, **kwargs): pass
    # 실제로는 stage3_core.py 등이 있어야 함

# =========================================================
# [런처 연동] JobManager & 유틸 (표준화됨)
# =========================================================
def get_root_filename(filename):
    """
    파일명에서 버전 정보(_T*_I* 또는 _t*_i*, _I*(업완) 포함) 및 기타 꼬리표를 떼고 원본명(Key)만 추출
    예: 나이키_T0_I0.xlsx -> 나이키.xlsx
    예: 아디다스_T2_I1.xlsx -> 아디다스.xlsx
    예: 나이키_T2_I0(업완).xlsx -> 나이키.xlsx
    예: 나이키_T2_I0_T3_I1.xlsx -> 나이키.xlsx (여러 버전 패턴 제거)
    예: 나이키_T2_I5(업완).xlsx -> 나이키.xlsx
    """
    name = os.path.basename(filename)
    base, ext = os.path.splitext(name)
    
    # 1. 버전 패턴 (_T숫자_I숫자(괄호)? 또는 _t숫자_i숫자(괄호)?) 반복 제거 (대소문자 구분 없음)
    # 패턴이 여러 번 나올 수 있으므로 반복 제거, 괄호가 붙은 경우도 포함
    while True:
        new_base = re.sub(r"_[Tt]\d+_[Ii]\d+(\([^)]+\))?", "", base, flags=re.IGNORECASE)
        if new_base == base:
            break
        base = new_base
    
    # 2. 괄호 안의 텍스트 제거 (예: (업완), (완료) 등) - 버전 패턴의 괄호는 이미 제거됨
    base = re.sub(r"\([^)]*\)", "", base)
    
    # 3. 기타 구형 꼬리표 제거 (호환성 유지)
    suffixes = ["_stage1_mapping", "_stage1_img_mapping", "_stage2_analysis", "_stage3_done", "_with_images"]
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

# =======================
#  설정 및 상수
# =======================
API_KEY_FILE = ".openai_api_key_stage3_llm"

# 모델별 가격 (USD)
MODEL_PRICING_USD_PER_MTOK = {
    "gpt-5":       {"input": 1.25, "output": 10.0},
    "gpt-5-mini":  {"input": 0.25, "output": 2.00},
    "gpt-5-nano":  {"input": 0.05, "output": 0.40},
    "gpt-4o":      {"input": 2.50, "output": 10.00},
}

# 서울 시간 헬퍼
try:
    from zoneinfo import ZoneInfo
    def get_seoul_now() -> datetime:
        return datetime.now(ZoneInfo("Asia/Seoul"))
except ImportError:
    import pytz
    def get_seoul_now() -> datetime:
        return datetime.now(pytz.timezone("Asia/Seoul"))
    # 만약 둘 다 없으면 로컬 시간
    # def get_seoul_now(): return datetime.now()

# =======================
#  UI 헬퍼 클래스 (ToolTip)
# =======================
class ToolTip:
    """마우스 오버 시 도움말 팝업"""
    def __init__(self, widget, text: str, wraplength: int = 400):
        self.widget = widget
        self.text = text
        self.wraplength = wraplength
        self.tipwindow = None
        self.widget.bind("<Enter>", self.show_tip)
        self.widget.bind("<Leave>", self.hide_tip)

    def show_tip(self, event=None):
        if self.tipwindow or not self.text:
            return
        x = self.widget.winfo_rootx() + 20
        y = self.widget.winfo_rooty() + 20
        self.tipwindow = tw = tk.Toplevel(self.widget)
        tw.wm_overrideredirect(True)
        tw.wm_geometry(f"+{x}+{y}")
        label = tk.Label(
            tw, text=self.text, justify="left",
            background="#ffffe0", relief="solid", borderwidth=1,
            wraplength=self.wraplength, font=("맑은 고딕", 9)
        )
        label.pack(ipadx=4, ipady=2)

    def hide_tip(self, event=None):
        if self.tipwindow:
            self.tipwindow.destroy()
            self.tipwindow = None

# =======================
#  API 키 관리
# =======================
def load_api_key_from_file() -> str:
    if os.path.exists(API_KEY_FILE):
        try:
            with open(API_KEY_FILE, "r", encoding="utf-8") as f:
                return f.read().strip()
        except:
            return ""
    return ""

def save_api_key_to_file(key: str) -> None:
    try:
        with open(API_KEY_FILE, "w", encoding="utf-8") as f:
            f.write(key.strip())
    except:
        pass

# =======================
#  메인 GUI 클래스
# =======================
class Stage3LLMGUI(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Stage 3: Product Naming Generator (Integrated Pro)")
        self.geometry("1000x850")
        
        # 스타일 설정
        self._configure_styles()

        # --- 변수 초기화 ---
        self.api_key_var = tk.StringVar(value=load_api_key_from_file())
        self.input_file_path = tk.StringVar()
        self.output_file_path = ""

        # 옵션 변수
        self.model_var = tk.StringVar(value="gpt-5-mini")
        self.effort_var = tk.StringVar(value="medium") # none/low/medium/high
        self.skip_filled_var = tk.BooleanVar(value=True)

        # Stage 3 설정 변수
        self.market_var = tk.StringVar(value="네이버 50자")
        self.max_len_var = tk.IntVar(value=50) # 직접 입력용
        self.num_cand_var = tk.IntVar(value=10)
        self.naming_strategy_var = tk.StringVar(value="통합형")

        # 상태 및 통계 변수
        self.is_running = False
        self.stop_requested = False
        
        self.stat_progress = tk.StringVar(value="0.0%")
        self.stat_count = tk.StringVar(value="0 / 0")
        self.stat_success = tk.StringVar(value="0")
        self.stat_fail = tk.StringVar(value="0")
        self.stat_cost = tk.StringVar(value="$0.0000")
        self.stat_time = tk.StringVar(value="00:00:00")
        self.status_msg = tk.StringVar(value="파일을 선택하고 작업을 시작하세요.")

        # UI 구성
        self._init_ui()

    def _configure_styles(self):
        style = ttk.Style()
        try:
            style.theme_use('clam')
        except:
            pass
        
        bg_color = "#f5f5f5"
        self.configure(background=bg_color)
        
        style.configure("TFrame", background=bg_color)
        style.configure("TLabelframe", background=bg_color, font=("맑은 고딕", 10, "bold"))
        style.configure("TLabelframe.Label", background=bg_color, foreground="#333333")
        style.configure("TLabel", background=bg_color, font=("맑은 고딕", 10))
        
        style.configure("Header.TLabel", font=("맑은 고딕", 11, "bold"), foreground="#444")
        style.configure("Stat.TLabel", font=("맑은 고딕", 12, "bold"), foreground="#0052cc")
        style.configure("Cost.TLabel", font=("맑은 고딕", 12, "bold"), foreground="#d32f2f")
        
        style.configure("Action.TButton", font=("맑은 고딕", 11, "bold"), padding=5)
        style.configure("Stop.TButton", font=("맑은 고딕", 11, "bold"), foreground="red", padding=5)

    def _init_ui(self):
        main_frame = ttk.Frame(self, padding=20)
        main_frame.pack(fill='both', expand=True)

        # 1. 상단: API & Model 설정
        frame_top = ttk.LabelFrame(main_frame, text="API 및 모델 설정", padding=15)
        frame_top.pack(fill='x', pady=(0, 10))

        # Row 1: API Key
        r1 = ttk.Frame(frame_top)
        r1.pack(fill='x', pady=2)
        ttk.Label(r1, text="API Key:", width=10).pack(side='left')
        entry_key = ttk.Entry(r1, textvariable=self.api_key_var, show="*", width=50)
        entry_key.pack(side='left', padx=5)
        ttk.Button(r1, text="저장", command=self._save_key, width=8).pack(side='left')
        
        # 도움말 아이콘
        lbl_help = ttk.Label(r1, text="❓도움말", foreground="blue", cursor="hand2")
        lbl_help.pack(side='right', padx=5)
        ToolTip(lbl_help, "Stage3 전용 API 키 사용 권장.\n모델별 가격 정책 확인 필요.")

        # Row 2: Model & Effort
        r2 = ttk.Frame(frame_top)
        r2.pack(fill='x', pady=5)
        ttk.Label(r2, text="모델:", width=10).pack(side='left')
        models = list(MODEL_PRICING_USD_PER_MTOK.keys())
        ttk.Combobox(r2, textvariable=self.model_var, values=models, state="readonly", width=18).pack(side='left', padx=5)
        
        ttk.Label(r2, text="Effort:", width=8).pack(side='left', padx=(20,0))
        ttk.Combobox(r2, textvariable=self.effort_var, values=["none", "low", "medium", "high"], state="readonly", width=12).pack(side='left', padx=5)

        # 2. Stage3 상세 옵션
        frame_opt = ttk.LabelFrame(main_frame, text="Stage 3 생성 옵션", padding=15)
        frame_opt.pack(fill='x', pady=(0, 10))

        ro = ttk.Frame(frame_opt)
        ro.pack(fill='x')

        ttk.Label(ro, text="마켓설정:").pack(side='left')
        market_opts = ["네이버 50자", "쿠팡 100자", "지마켓/옥션 45자", "기타(직접입력)"]
        cb_market = ttk.Combobox(ro, textvariable=self.market_var, values=market_opts, state="readonly", width=15)
        cb_market.pack(side='left', padx=5)
        cb_market.bind("<<ComboboxSelected>>", self._on_market_change)

        ttk.Label(ro, text="최대글자:").pack(side='left', padx=(10, 0))
        ttk.Spinbox(ro, from_=10, to=200, textvariable=self.max_len_var, width=5).pack(side='left', padx=5)

        ttk.Label(ro, text="출력개수:").pack(side='left', padx=(10, 0))
        ttk.Spinbox(ro, from_=1, to=30, textvariable=self.num_cand_var, width=5).pack(side='left', padx=5)

        ttk.Label(ro, text="전략:").pack(side='left', padx=(10, 0))
        ttk.Combobox(ro, textvariable=self.naming_strategy_var, values=["통합형", "옵션포함형"], state="readonly", width=10).pack(side='left', padx=5)

        # 3. 파일 선택 & 이어하기 옵션
        frame_file = ttk.LabelFrame(main_frame, text="작업 대상 파일", padding=15)
        frame_file.pack(fill='x', pady=(0, 10))
        
        rf = ttk.Frame(frame_file)
        rf.pack(fill='x')
        ttk.Entry(rf, textvariable=self.input_file_path).pack(side='left', fill='x', expand=True, padx=(0, 5))
        ttk.Button(rf, text="📂 파일 선택", command=self._select_file).pack(side='right')
        
        # 건너뛰기 체크박스
        ttk.Checkbutton(frame_file, text="이미 결과(ST3_결과상품명)가 있는 행은 건너뛰기", variable=self.skip_filled_var).pack(anchor='w', pady=(5,0))

        # 4. 대시보드 (Dashboard)
        dash_frame = ttk.LabelFrame(main_frame, text="실시간 현황 (Dashboard)", padding=15)
        dash_frame.pack(fill='x', pady=(0, 10))

        # 1행: 진행률
        d1 = ttk.Frame(dash_frame)
        d1.pack(fill='x', pady=5)
        ttk.Label(d1, text="진행률:", style="Header.TLabel", width=10).pack(side='left')
        self.pb = ttk.Progressbar(d1, maximum=100, mode='determinate')
        self.pb.pack(side='left', fill='x', expand=True, padx=5)
        ttk.Label(d1, textvariable=self.stat_progress, style="Stat.TLabel", width=8).pack(side='right')

        # 2행: 통계
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

        # 3행: 비용/시간
        d3 = ttk.Frame(dash_frame)
        d3.pack(fill='x', pady=5)
        ttk.Label(d3, text="예상 비용:", width=10).pack(side='left')
        ttk.Label(d3, textvariable=self.stat_cost, style="Cost.TLabel", width=15).pack(side='left')
        
        ttk.Label(d3, text="경과 시간:", width=10).pack(side='left')
        ttk.Label(d3, textvariable=self.stat_time).pack(side='left')

        # 5. 액션 버튼
        btn_frame = ttk.Frame(main_frame)
        btn_frame.pack(fill='x', pady=(0, 10))
        
        self.btn_start = ttk.Button(btn_frame, text="▶ 작업 시작", style="Action.TButton", command=self._start_thread)
        self.btn_start.pack(side='left', fill='x', expand=True, padx=(0, 5))
        
        self.btn_stop = ttk.Button(btn_frame, text="⏹ 중단 (Safe Stop)", style="Stop.TButton", command=self._request_stop, state='disabled')
        self.btn_stop.pack(side='right', fill='x', expand=True, padx=(5, 0))

        ttk.Label(main_frame, textvariable=self.status_msg, foreground="#555", anchor='center').pack(fill='x', pady=(0, 5))

        # 6. 로그창
        log_frame = ttk.LabelFrame(main_frame, text="상세 로그", padding=10)
        log_frame.pack(fill='both', expand=True)
        self.log_widget = ScrolledText(log_frame, height=10, state='disabled', font=("Consolas", 9))
        self.log_widget.pack(fill='both', expand=True)

    # --- UI 이벤트 핸들러 ---
    def _on_market_change(self, event=None):
        """마켓 콤보박스 선택 시 최대글자수 자동 세팅"""
        val = self.market_var.get()
        if "네이버" in val:
            self.max_len_var.set(50)
        elif "쿠팡" in val:
            self.max_len_var.set(100)
        elif "지마켓" in val or "옥션" in val:
            self.max_len_var.set(45)

    def _save_key(self):
        k = self.api_key_var.get().strip()
        if k:
            save_api_key_to_file(k)
            messagebox.showinfo("저장", "API Key가 저장되었습니다.")

    def _select_file(self):
        p = filedialog.askopenfilename(
            title="Stage3 엑셀 선택 (T2 버전만 가능)",
            filetypes=[("Excel Files", "*.xlsx;*.xls")]
        )
        if p:
            # T2 포함 여부 검증
            base_name = os.path.splitext(os.path.basename(p))[0]
            if not re.search(r"_T2_[Ii]\d+", base_name, re.IGNORECASE):
                messagebox.showerror(
                    "오류", 
                    f"이 도구는 T2 버전의 엑셀 파일만 처리할 수 있습니다.\n\n"
                    f"선택한 파일: {os.path.basename(p)}\n"
                    f"파일명에 '_T2_I*' 패턴이 포함되어 있어야 합니다."
                )
                return
            
            # 스마트 이어하기 로직
            dir_name = os.path.dirname(p)
            base, ext = os.path.splitext(os.path.basename(p))
            
            # 이미 결과 파일인 경우
            if "_stage3_done" in base:
                self.input_file_path.set(p)
                self.status_msg.set("이어서 작업을 진행합니다.")
                return

            done_file = f"{base}_stage3_done{ext}"
            done_path = os.path.join(dir_name, done_file)
            
            if os.path.exists(done_path):
                if messagebox.askyesno("이어하기 감지", f"작업 중이던 파일이 있습니다.\n\n{done_file}\n\n이 파일을 로드하여 이어서 하시겠습니까?"):
                    self.input_file_path.set(done_path)
                    self._log(f"작업 중이던 파일 로드: {done_file}")
                    self.status_msg.set("작업 재개 준비 완료")
                else:
                    self.input_file_path.set(p)
                    self._log(f"새 원본 파일 선택: {os.path.basename(p)}")
                    self.status_msg.set("새 작업 준비 완료")
            else:
                self.input_file_path.set(p)
                self._log(f"파일 선택됨: {os.path.basename(p)}")
                self.status_msg.set("준비 완료.")

    def _log(self, msg):
        self.log_widget.after(0, self._append_log, msg)

    def _append_log(self, msg):
        t = datetime.now().strftime("%H:%M:%S")
        self.log_widget.config(state='normal')
        self.log_widget.insert("end", f"[{t}] {msg}\n")
        self.log_widget.see("end")
        self.log_widget.config(state='disabled')

    def _request_stop(self):
        if self.is_running:
            self.stop_requested = True
            self.status_msg.set("⚠️ 중단 요청됨! 현재 행 처리 후 멈춥니다.")
            self.btn_stop.config(state='disabled')

    # --- 핵심 로직 (원본 기능 복원 + 최적화) ---
    def _save_df_with_backup(self, df: pd.DataFrame, excel_path: str) -> str:
        """
        엑셀 저장 실패 시 백업 생성
        반환값: 실제로 저장에 성공한 파일 경로 (T2 → T3로 버전 업)
        """
        # T2 → T3로 버전 업 파일명 생성
        base_dir = os.path.dirname(excel_path)
        base_name = os.path.splitext(os.path.basename(excel_path))[0]
        
        # 입력 파일명에서 버전 정보 추출 (괄호 포함 가능, 예: _I5(업완))
        pattern = r"_T(\d+)_I(\d+)(\([^)]+\))?"
        match = re.search(pattern, base_name, re.IGNORECASE)
        if match:
            current_t = int(match.group(1))
            current_i = int(match.group(2))
            i_suffix = match.group(3) or ""  # 괄호 부분이 있으면 유지 (예: (업완))
            # 원본명 추출 (버전 정보 제거, 괄호 포함)
            original_name = re.sub(r"_T\d+_I\d+(\([^)]+\))?.*$", "", base_name, flags=re.IGNORECASE).rstrip("_")
            # T 버전만 +1 (I는 유지, 괄호도 유지)
            new_t = current_t + 1
            new_i = current_i
            out_filename = f"{original_name}_T{new_t}_I{new_i}{i_suffix}.xlsx"
        else:
            # 버전 정보가 없으면 T3_I0으로 생성
            out_filename = f"{base_name}_T3_I0.xlsx"
        out_path = os.path.join(base_dir, out_filename)
        
        try:
            df.to_excel(out_path, index=False)
            return out_path
        except Exception as e:
            base, ext = os.path.splitext(out_path)
            ts = get_seoul_now().strftime("%Y%m%d_%H%M%S")
            backup_path = f"{base}_stage3_partial_{ts}{ext}"
            try:
                df.to_excel(backup_path, index=False)
                self._log(f"⚠️ 원본 저장 실패(열림 등). 백업 저장: {os.path.basename(backup_path)}")
                return backup_path
            except Exception as e2:
                self._log(f"🔥 백업 저장도 실패: {e2}")
                return out_path

    def _extract_text_from_response(self, resp) -> str:
        """다양한 API 응답 구조 안전 파싱"""
        text_chunks = []
        outputs = getattr(resp, "output", None) or getattr(resp, "choices", None)
        
        if outputs:
            try:
                for out in outputs:
                    if hasattr(out, "message"): # choices 구조
                        content = out.message.content
                        if content: text_chunks.append(content)
                        continue
                    
                    content_list = getattr(out, "content", None) # output 구조
                    if content_list:
                        for item in content_list:
                            txt = getattr(item, "text", None)
                            if txt:
                                val = getattr(txt, "value", None)
                                if val: text_chunks.append(val)
            except:
                pass

        full_text = "\n".join(text_chunks).strip()
        if not full_text:
            try:
                return str(resp.choices[0].message.content).strip()
            except:
                return ""
        return full_text

    def _extract_usage_tokens(self, resp):
        usage = getattr(resp, "usage", None)
        if not usage: return 0, 0, 0
        
        i = getattr(usage, "prompt_tokens", 0) or 0
        o = getattr(usage, "completion_tokens", 0) or 0
        r = 0
        details = getattr(usage, "completion_tokens_details", None)
        if details:
            r = getattr(details, "reasoning_tokens", 0) or 0
        return i, o, r

    def _calc_cost(self, model, i, o, r):
        price = MODEL_PRICING_USD_PER_MTOK.get(model, {"input":0, "output":0})
        i_cost = (i / 1_000_000) * price["input"]
        o_cost = ((o + r) / 1_000_000) * price["output"]
        return i_cost + o_cost

    def _open_file(self, path: str):
        if path and os.path.exists(path):
            try:
                os.startfile(path)
            except:
                pass

    # --- 작업 스레드 ---
    def _start_thread(self):
        if self.is_running: return
        key = self.api_key_var.get().strip()
        path = self.input_file_path.get().strip()
        
        if not key:
            messagebox.showwarning("오류", "API Key가 없습니다.")
            return
        if not path or not os.path.exists(path):
            messagebox.showwarning("오류", "파일이 없습니다.")
            return
        
        # T2 포함 여부 검증
        base_name = os.path.splitext(os.path.basename(path))[0]
        if not re.search(r"_T2_[Ii]\d+", base_name, re.IGNORECASE):
            messagebox.showerror(
                "오류", 
                f"이 도구는 T2 버전의 엑셀 파일만 처리할 수 있습니다.\n\n"
                f"선택한 파일: {os.path.basename(path)}\n"
                f"파일명에 '_T2_I*' 패턴이 포함되어 있어야 합니다."
            )
            return
            
        self.is_running = True
        self.stop_requested = False
        self.btn_start.config(state='disabled')
        self.btn_stop.config(state='normal')
        self.status_msg.set("작업 초기화 중...")
        
        t = threading.Thread(target=self._run_process, args=(key, path))
        t.daemon = True
        t.start()

    def _run_process(self, api_key, input_path):
        try:
            client = OpenAI(api_key=api_key)
            
            # 설정값 수집
            market_val = self.market_var.get()
            market_name = "네이버"
            if "쿠팡" in market_val: market_name = "쿠팡"
            elif "지마켓" in market_val: market_name = "지마켓/옥션"
            
            settings = Stage3Settings(
                market=market_name,
                max_len=self.max_len_var.get(),
                num_candidates=self.num_cand_var.get(),
                naming_strategy=self.naming_strategy_var.get()
            )
            
            # 모델 관련 설정은 별도로 관리 (Stage3Settings에 포함되지 않음)
            model_name = self.model_var.get()
            reasoning_effort = self.effort_var.get()

            df = pd.read_excel(input_path)
            if "ST2_JSON" not in df.columns:
                raise ValueError("ST2_JSON 컬럼 누락")

            # 컬럼 준비
            for col in ["ST3_프롬프트", "ST3_결과상품명"]:
                if col not in df.columns: df[col] = ""
            df["ST3_결과상품명"] = df["ST3_결과상품명"].astype(str)

            # 저장 경로 (T2 → T3로 버전 업)
            base_dir = os.path.dirname(input_path)
            base_name = os.path.splitext(os.path.basename(input_path))[0]
            
            # 입력 파일명에서 버전 정보 추출
            pattern = r"_T(\d+)_I(\d+)"
            match = re.search(pattern, base_name, re.IGNORECASE)
            if match:
                current_t = int(match.group(1))
                current_i = int(match.group(2))
                # 원본명 추출 (버전 정보 제거)
                original_name = re.sub(r"_T\d+_I\d+.*$", "", base_name, flags=re.IGNORECASE).rstrip("_")
                # T 버전만 +1 (I는 유지)
                new_t = current_t + 1
                new_i = current_i
                out_filename = f"{original_name}_T{new_t}_I{new_i}.xlsx"
            else:
                # 버전 정보가 없으면 T3_I0으로 생성
                out_filename = f"{base_name}_T3_I0.xlsx"
            out_path = os.path.join(base_dir, out_filename)
            self.output_file_path = out_path

            total_rows = len(df)
            start_dt = get_seoul_now()
            self._update_timer(start_dt)

            stats = {
                "in": 0, "out": 0, "reason": 0, "cost": 0.0,
                "success": 0, "fail": 0, "skip": 0, "api": 0
            }
            processed_now = 0
            
            self._log(f"▶ 시작: {len(df)}행, 모델={model_name}")

            for idx, row in df.iterrows():
                if self.stop_requested:
                    self._log("⛔ 사용자 중단 요청.")
                    break

                # 건너뛰기 체크
                if self.skip_filled_var.get():
                    val = safe_str(row.get("ST3_결과상품명", ""))
                    if val and val != "nan":
                        stats["skip"] += 1
                        self._update_ui_stats(idx+1, total_rows, stats)
                        continue

                # 실행
                try:
                    # 1. 프롬프트 생성
                    req = build_stage3_request_from_row(row, settings)
                    df.at[idx, "ST3_프롬프트"] = req.prompt

                    # 2. API 호출
                    params = {
                        "model": model_name,
                        "messages": [{"role": "user", "content": req.prompt}],
                    }
                    if "gpt-5" in model_name or "o1" in model_name:
                        if reasoning_effort != "none":
                            params["reasoning_effort"] = reasoning_effort
                    else:
                        params["temperature"] = 0.7

                    resp = client.chat.completions.create(**params)
                    
                    # 3. 결과 파싱 및 저장
                    res_text = self._extract_text_from_response(resp)
                    df.at[idx, "ST3_결과상품명"] = res_text
                    
                    # 4. 비용 계산
                    i, o, r = self._extract_usage_tokens(resp)
                    cost = self._calc_cost(model_name, i, o, r)
                    
                    stats["in"] += i; stats["out"] += o; stats["reason"] += r
                    stats["cost"] += cost
                    stats["api"] += 1
                    stats["success"] += 1
                    
                except Exception as e:
                    self._log(f"[Row {idx+1}] 오류: {e}")
                    stats["fail"] += 1

                processed_now += 1
                self._update_ui_stats(idx+1, total_rows, stats)

                # 자동 저장
                if processed_now % 10 == 0:
                    self._save_df_with_backup(df, out_path)
                    self._log(f"💾 자동 저장 ({processed_now}건)")

            finish_dt = get_seoul_now()
            
            # ST3_결과상품명이 있는 행과 없는 행 분리
            if "ST3_결과상품명" in df.columns:
                # ST3_결과상품명이 비어있거나 None인 행 찾기
                df_with_st3 = df[df["ST3_결과상품명"].notna() & (df["ST3_결과상품명"] != '') & (df["ST3_결과상품명"].astype(str) != 'nan')].copy()
                df_no_st3 = df[(df["ST3_결과상품명"].isna()) | (df["ST3_결과상품명"] == '') | (df["ST3_결과상품명"].astype(str) == 'nan')].copy()
            else:
                # 컬럼이 없으면 모든 행이 ST3_결과상품명 없음으로 처리
                df_with_st3 = pd.DataFrame()
                df_no_st3 = df.copy()
            
            # ST3_결과상품명이 없는 행들을 T3(실패) 버전으로 별도 파일 저장
            no_st3_path = None
            if len(df_no_st3) > 0:
                base_dir = os.path.dirname(out_path)
                base_name, ext = os.path.splitext(os.path.basename(out_path))
                
                # 현재 파일명에서 버전 정보 추출 (예: _T3_I0)
                # T3(실패) 버전으로 변경
                name_only_clean = re.sub(r"\([^)]*\)", "", base_name)  # 기존 괄호 제거
                all_matches = list(re.finditer(r"_([Tt])(\d+)_([Ii])(\d+)", name_only_clean, re.IGNORECASE))
                
                if all_matches:
                    # 마지막 버전 패턴 사용
                    match = all_matches[-1]
                    original_name = name_only_clean[: match.start()].rstrip("_")
                    current_t = int(match.group(2))
                    current_i = int(match.group(4))
                    # T3(실패) 버전으로 생성
                    new_filename = f"{original_name}_T{current_t}_I{current_i}(실패){ext}"
                else:
                    # 버전 패턴이 없으면 기본적으로 T3(실패)_I0로 생성
                    new_filename = f"{base_name}(실패){ext}"
                
                no_st3_path = os.path.join(base_dir, new_filename)
                df_no_st3.to_excel(no_st3_path, index=False)
                
                self._log(f" - T3(실패) 분리 파일: {os.path.basename(no_st3_path)} ({len(df_no_st3)}개 행)")
                self._log(f"   ※ 이 파일은 T3 작업에 실패한 항목입니다.")
                
                # 분리된 파일의 런처 상태 업데이트
                try:
                    no_st3_root_name = get_root_filename(no_st3_path)
                    JobManager.update_status(no_st3_root_name, text_msg="T3(실패)")
                    self._log(f"[Launcher] 분리 파일 상태 업데이트: {no_st3_root_name} -> T3(실패)")
                except Exception as e:
                    self._log(f"[Launcher] 분리 파일 상태 업데이트 실패: {e}")
            
            # ST3_결과상품명이 있는 행들만 저장
            if len(df_with_st3) > 0:
                df = df_with_st3
            else:
                self._log("⚠️ ST3_결과상품명이 있는 행이 없습니다.")
            
            # 최종 저장
            self._save_df_with_backup(df, out_path)
            self._log(f"💾 저장 완료: {os.path.basename(out_path)}")

            # 히스토리
            if stats["api"] > 0:
                elapsed = (finish_dt - start_dt).total_seconds()
                append_run_history(
                    stage="Stage 3",
                    model_name=model_name,
                    reasoning_effort=reasoning_effort,
                    src_file=input_path,
                    out_file=out_path,
                    total_rows=total_rows,
                    api_rows=stats["api"],
                    elapsed_seconds=elapsed,
                    total_in_tok=stats["in"],
                    total_out_tok=stats["out"],
                    total_reasoning_tok=stats["reason"],
                    input_cost_usd=0, # 약식 (필요시 상세 계산)
                    output_cost_usd=0,
                    total_cost_usd=stats["cost"],
                    start_dt=start_dt,
                    finish_dt=finish_dt,
                    success_rows=stats["success"],
                    fail_rows=stats["fail"]
                )

            # 메인 런처 현황판에 T3(생성완료) 상태 기록 (img 상태는 변경하지 않음)
            try:
                root_name = get_root_filename(out_path)
                JobManager.update_status(root_name, text_msg="T3(생성완료)")
                self._log(f"[INFO] 런처 현황판 업데이트: {root_name} -> T3(생성완료)")
            except Exception as e:
                self._log(f"[WARN] 런처 현황판 업데이트 실패: {e}")
            
            msg = "작업 중단됨" if self.stop_requested else "작업 완료됨"
            self.status_msg.set(msg)
            self._show_completion(msg, stats, out_path)

        except Exception as e:
            self._log(f"🔥 오류: {e}")
            messagebox.showerror("오류", str(e))
        finally:
            self.is_running = False
            self.stop_requested = False
            self.btn_start.config(state='normal')
            self.btn_stop.config(state='disabled')

    def _update_ui_stats(self, curr, total, stats):
        pct = (curr / total) * 100
        self.pb['value'] = pct
        self.stat_progress.set(f"{pct:.1f}%")
        self.stat_count.set(f"{curr} / {total}")
        self.stat_success.set(str(stats['success']))
        self.stat_fail.set(str(stats['fail']))
        self.stat_cost.set(f"${stats['cost']:.4f}")
        
        msg = f"처리 중... {curr}/{total}"
        if stats['skip'] > 0: msg += f" (Skip: {stats['skip']})"
        self.status_msg.set(msg)
        self.update_idletasks()

    def _update_timer(self, start_dt):
        if not self.is_running: return
        now = get_seoul_now()
        diff = int((now - start_dt).total_seconds())
        h, r = divmod(diff, 3600)
        m, s = divmod(r, 60)
        self.stat_time.set(f"{h:02}:{m:02}:{s:02}")
        self.after(500, lambda: self._update_timer(start_dt))

    def _show_completion(self, title, stats, path):
        msg = (
            f"[{title}]\n\n"
            f"성공: {stats['success']}\n"
            f"실패: {stats['fail']}\n"
            f"건너뜀: {stats['skip']}\n"
            f"총 비용: ${stats['cost']:.4f}\n\n"
            f"파일: {os.path.basename(path)}"
        )
        if messagebox.askyesno("완료", msg + "\n\n파일을 여시겠습니까?"):
            self._open_file(path)

if __name__ == "__main__":
    app = Stage3LLMGUI()
    app.mainloop()