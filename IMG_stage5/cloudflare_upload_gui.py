"""
cloudflare_upload_gui.py

Cloudflare R2 이미지 호스팅 GUI
- 기능: 엑셀 파일에서 이미지 경로를 읽어 R2에 업로드하고 URL을 엑셀에 기록
- 입력: 엑셀 파일 (이미지 경로 컬럼 포함)
- 출력: 업로드된 이미지 URL이 추가된 엑셀 파일
"""

import os
import re
import json
import threading
import time
from datetime import datetime
from typing import Optional, List, Dict, Any
from pathlib import Path

import pandas as pd
import boto3
from botocore.exceptions import NoCredentialsError, ClientError
import tkinter as tk
from tkinter import ttk, filedialog, messagebox
from tkinter.scrolledtext import ScrolledText

# ========================================================
# 메인 런처 연동용 JobManager & 파일명 유틸
# ========================================================
def get_root_filename(filename: str) -> str:
    """
    파일명에서 버전 정보(_T*_I* 또는 _t*_i*, T4(완)_I* 포함) 및 기타 꼬리표를 떼고 원본명(Key)만 추출
    예: 나이키_T0_I0.xlsx -> 나이키.xlsx
    예: 아디다스_T3_I5.xlsx -> 아디다스.xlsx
    예: 나이키_T0_I0(업완).xlsx -> 나이키.xlsx
    예: 나이키_T0_I0_T1_I1.xlsx -> 나이키.xlsx (여러 버전 패턴 제거)
    예: 나이키_T4(완)_I5.xlsx -> 나이키.xlsx
    """
    name = os.path.basename(filename)
    base, ext = os.path.splitext(name)
    
    # 1. 버전 패턴 (_T숫자(괄호)?_I숫자 또는 _t숫자(괄호)?_i숫자) 반복 제거 (대소문자 구분 없음)
    # 패턴이 여러 번 나올 수 있으므로 반복 제거, T4(완)_I* 패턴도 포함
    while True:
        new_base = re.sub(r"_[Tt]\d+\([^)]*\)_[Ii]\d+", "", base, flags=re.IGNORECASE)  # T4(완)_I* 패턴 제거
        new_base = re.sub(r"_[Tt]\d+_[Ii]\d+", "", new_base, flags=re.IGNORECASE)  # 일반 T*_I* 패턴 제거
        if new_base == base:
            break
        base = new_base
    
    # 2. 괄호 안의 텍스트 제거 (예: (업완), (완료) 등) - 버전 패턴의 괄호는 이미 제거됨
    base = re.sub(r"\([^)]*\)", "", base)
    
    # 3. 기타 구형 꼬리표 제거 (호환성 유지)
    suffixes = ["_bg_prompt_done", "_bg_prompt_batch_done", "_bg_generation_done", "_bg_mixing_done", "_stage1_mapping", "_stage1_img_mapping", "_stage2_analysis", "_stage3_done", "_stage4_2_done", "_stage5_review_done", "_with_images"]
    for s in suffixes:
        base = base.replace(s, "")
    
    # 4. 끝에 남은 언더스코어 제거
    base = base.rstrip("_")
        
    return base + ext

class JobManager:
    DB_FILE = None

    @classmethod
    def find_db_path(cls):
        if cls.DB_FILE and os.path.exists(cls.DB_FILE):
            return cls.DB_FILE
        current_dir = os.path.dirname(os.path.abspath(__file__))
        search_dirs = [
            current_dir,
            os.path.abspath(os.path.join(current_dir, "..")),
            os.path.abspath(os.path.join(current_dir, "..", "..")),
        ]
        for d in search_dirs:
            target = os.path.join(d, "job_history.json")
            if os.path.exists(target):
                cls.DB_FILE = target
                return target
        default_path = os.path.abspath(os.path.join(current_dir, "..", "job_history.json"))
        cls.DB_FILE = default_path
        return default_path

    @classmethod
    def load_jobs(cls):
        db_path = cls.find_db_path()
        if not os.path.exists(db_path):
            return {}
        try:
            with open(db_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}

    @classmethod
    def update_status(cls, filename, text_msg=None, img_msg=None, img_s3_1_msg=None, img_s3_2_msg=None, img_s4_1_msg=None, img_s4_2_msg=None, img_s5_1_msg=None, img_s5_2_msg=None):
        """
        작업 상태를 업데이트합니다.
        
        Args:
            filename: 파일명 (root filename)
            text_msg: 텍스트 상태 메시지
            img_msg: 이미지 전체 상태 메시지 (하위 호환성, 우선순위 높음)
            img_s3_1_msg: Stage 3-1 (썸네일 분석) 상태 메시지
            img_s3_2_msg: Stage 3-2 (전처리) 상태 메시지
            img_s4_1_msg: Stage 4-1 (배경 생성) 상태 메시지
            img_s4_2_msg: Stage 4-2 (합성) 상태 메시지
            img_s5_1_msg: Stage 5-1 (품질 검증) 상태 메시지
            img_s5_2_msg: Stage 5-2 (이미지 업로드) 상태 메시지
        """
        db_path = cls.find_db_path()
        data = cls.load_jobs()
        now = datetime.now().strftime("%m-%d %H:%M")
        
        if filename not in data:
            data[filename] = {
                "start_time": datetime.now().strftime("%Y-%m-%d %H:%M"),
                "text_status": "대기",
                "text_time": "-",
                "image_status": "대기",
                "image_time": "-",
                "image_s3_1_status": "-",
                "image_s3_1_time": "-",
                "image_s3_2_status": "-",
                "image_s3_2_time": "-",
                "image_s4_1_status": "-",
                "image_s4_1_time": "-",
                "image_s4_2_status": "-",
                "image_s4_2_time": "-",
                "image_s5_1_status": "-",
                "image_s5_1_time": "-",
                "image_s5_2_status": "-",
                "image_s5_2_time": "-",
                "memo": "",
            }
        
        if text_msg:
            data[filename]["text_status"] = text_msg
            data[filename]["text_time"] = now
        
        # img_msg가 있으면 우선적으로 사용 (하위 호환성)
        if img_msg:
            data[filename]["image_status"] = img_msg
            data[filename]["image_time"] = now
        
        # 가장 최근 단계만 표시하도록 통합 업데이트 (우선순위: I5 > I4 > I3)
        def update_image_status_from_stages():
            """가장 최근 단계만 표시하도록 image_status 업데이트"""
            if img_msg:
                return  # img_msg가 있으면 그대로 사용
            
            parts = []
            img_s5_1 = data[filename].get("image_s5_1_status", "-")
            img_s5_2 = data[filename].get("image_s5_2_status", "-")
            img_s4_1 = data[filename].get("image_s4_1_status", "-")
            img_s4_2 = data[filename].get("image_s4_2_status", "-")
            img_s3_1 = data[filename].get("image_s3_1_status", "-")
            img_s3_2 = data[filename].get("image_s3_2_status", "-")
            
            if img_s5_1 != "-" or img_s5_2 != "-":
                # I5 단계 표시
                if img_s5_1 != "-":
                    parts.append(img_s5_1)
                if img_s5_2 != "-":
                    parts.append(img_s5_2)
                i_time = (data[filename].get("image_s5_2_time") or 
                         data[filename].get("image_s5_1_time") or 
                         data[filename].get("image_time", now))
            elif img_s4_1 != "-" or img_s4_2 != "-":
                # I4 단계 표시
                if img_s4_1 != "-":
                    parts.append(img_s4_1)
                if img_s4_2 != "-":
                    parts.append(img_s4_2)
                i_time = (data[filename].get("image_s4_2_time") or 
                         data[filename].get("image_s4_1_time") or 
                         data[filename].get("image_time", now))
            elif img_s3_1 != "-" or img_s3_2 != "-":
                # I3 단계 표시
                if img_s3_1 != "-":
                    parts.append(img_s3_1)
                if img_s3_2 != "-":
                    parts.append(img_s3_2)
                i_time = (data[filename].get("image_s3_2_time") or 
                         data[filename].get("image_s3_1_time") or 
                         data[filename].get("image_time", now))
            
            if parts:
                data[filename]["image_status"] = " / ".join(parts)
                data[filename]["image_time"] = i_time
        
        if img_s3_1_msg:
            data[filename]["image_s3_1_status"] = img_s3_1_msg
            data[filename]["image_s3_1_time"] = now
            update_image_status_from_stages()
        
        if img_s3_2_msg:
            data[filename]["image_s3_2_status"] = img_s3_2_msg
            data[filename]["image_s3_2_time"] = now
            update_image_status_from_stages()
        
        if img_s4_1_msg:
            data[filename]["image_s4_1_status"] = img_s4_1_msg
            data[filename]["image_s4_1_time"] = now
            update_image_status_from_stages()
        
        if img_s4_2_msg:
            data[filename]["image_s4_2_status"] = img_s4_2_msg
            data[filename]["image_s4_2_time"] = now
            update_image_status_from_stages()
        
        if img_s5_1_msg:
            data[filename]["image_s5_1_status"] = img_s5_1_msg
            data[filename]["image_s5_1_time"] = now
            update_image_status_from_stages()
        
        if img_s5_2_msg:
            data[filename]["image_s5_2_status"] = img_s5_2_msg
            data[filename]["image_s5_2_time"] = now
            update_image_status_from_stages()
        
        data[filename]["last_update"] = now
        
        try:
            # 디렉토리가 없으면 생성
            db_dir = os.path.dirname(db_path)
            if db_dir and not os.path.exists(db_dir):
                os.makedirs(db_dir, exist_ok=True)
            
            with open(db_path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=4)
        except Exception as e:
            import traceback
            error_msg = f"[JobManager Error] 파일명: {filename}, 경로: {db_path}, 오류: {e}\n{traceback.format_exc()}"
            print(error_msg)
            # GUI가 있으면 로그에도 출력 (선택적)
            try:
                # CloudflareUploadGUI 인스턴스가 전역에 있으면 로그 출력
                import sys
                if hasattr(sys.modules.get(__name__), '_current_gui_instance'):
                    gui = sys.modules[__name__]._current_gui_instance
                    if gui and hasattr(gui, '_log'):
                        gui._log(error_msg)
            except:
                pass

# ========================================================
# Cloudflare R2 설정
# ========================================================
R2_ACCOUNT_ID = "400d9de3ac7d9e34b7bb1b88b8d915e5"
R2_ACCESS_KEY_ID = "a895886eefaafb6f809ee29f9b6b1b7a"
R2_SECRET_ACCESS_KEY = "5f889aa58a3b454deb3928963d9e11686264e995bcd61e67e005ad69355abd0c"
BUCKET_NAME = "images001"
PUBLIC_DEVELOPMENT_URL = "https://pub-c3e2ead4e8884b79a78e3ea2eb6d23bf.r2.dev"
R2_ENDPOINT_URL = f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com"

# ========================================================
# 유틸리티 함수
# ========================================================
def safe_save_excel(df: pd.DataFrame, path: str) -> bool:
    """엑셀 파일 저장 (재시도 포함)"""
    while True:
        try:
            df.to_excel(path, index=False)
            return True
        except PermissionError:
            if not messagebox.askretrycancel("저장 실패", f"엑셀 파일이 열려있습니다!\n[{os.path.basename(path)}]\n\n파일을 닫고 '다시 시도'를 눌러주세요."):
                return False
        except Exception as e:
            messagebox.showerror("오류", f"저장 중 알 수 없는 오류: {e}")
            return False

# ========================================================
# GUI Class
# ========================================================
class CloudflareUploadGUI(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Cloudflare R2 이미지 업로드 (I5 파일 전용)")
        self.geometry("1100x850")
        
        # 스타일 설정
        self._configure_styles()
        
        # 변수 초기화
        self.input_file_path = tk.StringVar()
        self.image_column_var_1 = tk.StringVar()  # 첫 번째 이미지 경로 컬럼
        self.image_column_var_2 = tk.StringVar()  # 두 번째 이미지 경로 컬럼
        self.output_column_var_1 = tk.StringVar()  # 첫 번째 출력 URL 컬럼명
        self.output_column_var_2 = tk.StringVar()  # 두 번째 출력 URL 컬럼명
        self.prefix_path_var = tk.StringVar(value="")  # R2 내부 경로 prefix (선택사항)
        
        # 상태 변수
        self.is_running = False
        self.stop_requested = False
        
        # 데이터
        self.df = None
        self.image_columns = []  # 이미지 경로 컬럼 후보 목록
        
        # 통계 변수
        self.stat_total = tk.StringVar(value="0")
        self.stat_success = tk.StringVar(value="0")
        self.stat_fail = tk.StringVar(value="0")
        self.stat_skip = tk.StringVar(value="0")
        self.stat_progress = tk.StringVar(value="0.0%")
        
        # R2 클라이언트
        self.s3_client = None
        
        # UI 구성
        self._init_ui()
        
        # R2 연결 테스트
        self._test_r2_connection()
    
    def _configure_styles(self):
        """스타일 설정"""
        style = ttk.Style()
        try:
            style.theme_use('clam')
        except:
            pass
        
        bg_color = "#f8f9fa"
        self.configure(background=bg_color)
        
        style.configure("TFrame", background=bg_color)
        style.configure("TLabelframe", background=bg_color, font=("맑은 고딕", 10, "bold"), borderwidth=2, relief="solid")
        style.configure("TLabelframe.Label", background=bg_color, font=("맑은 고딕", 10, "bold"), foreground="#2c3e50")
        style.configure("TLabel", background=bg_color, font=("맑은 고딕", 9))
        style.configure("Action.TButton", font=("맑은 고딕", 11, "bold"), padding=8)
        style.configure("Stop.TButton", font=("맑은 고딕", 10), padding=6)
    
    def _init_ui(self):
        """UI 초기화"""
        main_frame = ttk.Frame(self, padding=20)
        main_frame.pack(fill='both', expand=True)
        
        # 1. 파일 선택
        frame_file = ttk.LabelFrame(main_frame, text="📁 엑셀 파일 선택 (I5 파일만)", padding=15)
        frame_file.pack(fill='x', pady=(0, 12))
        
        rf1 = ttk.Frame(frame_file)
        rf1.pack(fill='x', pady=3)
        ttk.Label(rf1, text="엑셀 파일:", width=12, font=("맑은 고딕", 9)).pack(side='left')
        entry_file = ttk.Entry(rf1, textvariable=self.input_file_path, width=50, font=("맑은 고딕", 9))
        entry_file.pack(side='left', fill='x', expand=True, padx=(5, 10))
        btn_select = ttk.Button(rf1, text="📂 파일 선택", command=self._select_file, width=15)
        btn_select.pack(side='right')
        
        # 2. 컬럼 설정 (I5 파일 전용: 자동 설정)
        frame_column = ttk.LabelFrame(main_frame, text="⚙️ 이미지 경로 컬럼 설정 (자동)", padding=18)
        frame_column.pack(fill='x', pady=(0, 12))
        
        # 컬럼 정보를 그리드 형태로 배치
        col_grid = ttk.Frame(frame_column)
        col_grid.pack(fill='x', pady=5)
        
        # 첫 번째 이미지 경로 컬럼 (누끼)
        rc1 = ttk.Frame(col_grid)
        rc1.pack(fill='x', pady=6)
        
        label_nukki = ttk.Label(rc1, text="누끼 이미지:", width=14, font=("맑은 고딕", 9))
        label_nukki.pack(side='left')
        
        input_nukki = ttk.Label(rc1, text="IMG_S5_누끼_최종경로", font=("맑은 고딕", 9, "bold"), 
                                foreground="#2c3e50", background="#e8f4f8", relief="solid", borderwidth=1, padding=5)
        input_nukki.pack(side='left', padx=(5, 15), fill='x', expand=True)
        
        arrow_label = ttk.Label(rc1, text="→", font=("맑은 고딕", 12, "bold"), foreground="#7f8c8d", width=3)
        arrow_label.pack(side='left')
        
        output_nukki = ttk.Label(rc1, text="누끼url", font=("맑은 고딕", 9, "bold"), 
                                foreground="#ffffff", background="#3498db", relief="solid", borderwidth=1, padding=5)
        output_nukki.pack(side='left', padx=(5, 0), ipadx=10)
        
        # 두 번째 이미지 경로 컬럼 (믹스)
        rc2 = ttk.Frame(col_grid)
        rc2.pack(fill='x', pady=6)
        
        label_mix = ttk.Label(rc2, text="믹스 이미지:", width=14, font=("맑은 고딕", 9))
        label_mix.pack(side='left')
        
        input_mix = ttk.Label(rc2, text="IMG_S5_믹스_최종경로", font=("맑은 고딕", 9, "bold"), 
                              foreground="#2c3e50", background="#e8f4f8", relief="solid", borderwidth=1, padding=5)
        input_mix.pack(side='left', padx=(5, 15), fill='x', expand=True)
        
        arrow_label2 = ttk.Label(rc2, text="→", font=("맑은 고딕", 12, "bold"), foreground="#7f8c8d", width=3)
        arrow_label2.pack(side='left')
        
        output_mix = ttk.Label(rc2, text="믹스url", font=("맑은 고딕", 9, "bold"), 
                              foreground="#ffffff", background="#3498db", relief="solid", borderwidth=1, padding=5)
        output_mix.pack(side='left', padx=(5, 0), ipadx=10)
        
        # 구분선
        separator = ttk.Separator(frame_column, orient='horizontal')
        separator.pack(fill='x', pady=10)
        
        # R2 경로 Prefix
        rc3 = ttk.Frame(frame_column)
        rc3.pack(fill='x', pady=(5, 0))
        
        ttk.Label(rc3, text="R2 경로 Prefix:", width=14, font=("맑은 고딕", 9)).pack(side='left')
        entry_prefix = ttk.Entry(rc3, textvariable=self.prefix_path_var, width=35, font=("맑은 고딕", 9))
        entry_prefix.pack(side='left', padx=5, fill='x', expand=True)
        
        help_label = ttk.Label(rc3, text="💡 선택사항: R2 내부 폴더 구조 (예: products/2024/)", 
                             font=("맑은 고딕", 8), foreground="#7f8c8d")
        help_label.pack(side='left', padx=(10, 0))
        
        # 3. 대시보드
        frame_dash = ttk.LabelFrame(main_frame, text="📊 진행 상황", padding=18)
        frame_dash.pack(fill='x', pady=(0, 12))
        
        # 통계 정보 (그리드 레이아웃)
        stats_grid = ttk.Frame(frame_dash)
        stats_grid.pack(fill='x', pady=(0, 10))
        
        # 통계 항목들을 균등하게 배치
        stat_items = [
            ("전체", self.stat_total, "#3498db"),
            ("성공", self.stat_success, "#27ae60"),
            ("실패", self.stat_fail, "#e74c3c"),
            ("스킵", self.stat_skip, "#f39c12")
        ]
        
        for i, (label_text, var, color) in enumerate(stat_items):
            stat_frame = ttk.Frame(stats_grid)
            stat_frame.pack(side='left', fill='x', expand=True, padx=5)
            
            label = ttk.Label(stat_frame, text=label_text + ":", font=("맑은 고딕", 9), foreground="#7f8c8d")
            label.pack()
            
            value_label = ttk.Label(stat_frame, textvariable=var, font=("맑은 고딕", 14, "bold"), 
                                   foreground=color, background="#ffffff", relief="solid", 
                                   borderwidth=1, padding=8)
            value_label.pack(fill='x', pady=(3, 0))
        
        # 진행률 바
        d2 = ttk.Frame(frame_dash)
        d2.pack(fill='x', pady=(5, 0))
        
        progress_header = ttk.Frame(d2)
        progress_header.pack(fill='x', pady=(0, 5))
        ttk.Label(progress_header, text="진행률:", font=("맑은 고딕", 9), foreground="#7f8c8d").pack(side='left')
        ttk.Label(progress_header, textvariable=self.stat_progress, font=("맑은 고딕", 10, "bold"), 
                 foreground="#3498db", width=8).pack(side='right')
        
        self.pb = ttk.Progressbar(d2, maximum=100, mode='determinate', length=400)
        self.pb.pack(fill='x', expand=True)
        
        # 4. 액션 버튼
        btn_frame = ttk.Frame(main_frame)
        btn_frame.pack(fill='x', pady=(0, 12))
        
        self.btn_start = ttk.Button(btn_frame, text="▶ 업로드 시작", style="Action.TButton", 
                                    command=self._start_upload, width=20)
        self.btn_start.pack(side='left', fill='x', expand=True, padx=(0, 8))
        
        self.btn_stop = ttk.Button(btn_frame, text="⏹ 중단", style="Stop.TButton", 
                                  command=self._request_stop, state='disabled', width=15)
        self.btn_stop.pack(side='right', fill='x', expand=True, padx=(8, 0))
        
        # 5. 로그창
        self.log_frame = ttk.LabelFrame(main_frame, text="📝 상세 로그", padding=12)
        self.log_frame.pack(fill='both', expand=True)
        
        # 로그 위젯 스타일 개선
        log_container = ttk.Frame(self.log_frame)
        log_container.pack(fill='both', expand=True)
        
        self.log_widget = ScrolledText(log_container, height=12, state='disabled', 
                                      font=("Consolas", 9), wrap='word',
                                      bg="#ffffff", fg="#2c3e50",
                                      selectbackground="#3498db", selectforeground="#ffffff",
                                      borderwidth=1, relief="solid")
        self.log_widget.pack(fill='both', expand=True)
    
    def _test_r2_connection(self):
        """R2 연결 테스트"""
        try:
            self.s3_client = boto3.client(
                service_name='s3',
                endpoint_url=R2_ENDPOINT_URL,
                aws_access_key_id=R2_ACCESS_KEY_ID,
                aws_secret_access_key=R2_SECRET_ACCESS_KEY,
                region_name='auto'
            )
            # 버킷 존재 확인
            self.s3_client.head_bucket(Bucket=BUCKET_NAME)
            self._log("✅ R2 연결 성공")
        except Exception as e:
            self._log(f"❌ R2 연결 실패: {e}")
            messagebox.showerror("연결 실패", f"Cloudflare R2에 연결할 수 없습니다:\n{e}")
    
    def _select_file(self):
        """엑셀 파일 선택 (I5 파일만 허용)"""
        path = filedialog.askopenfilename(
            title="엑셀 파일 선택 (I5 파일만)",
            filetypes=[("Excel files", "*.xlsx;*.xls"), ("All files", "*.*")]
        )
        if path:
            try:
                base_name = os.path.basename(path)
                # I5 파일만 허용
                i_match = re.search(r"_I(\d+)", base_name)
                
                if i_match:
                    i_version = int(i_match.group(1))
                    if i_version != 5:
                        messagebox.showwarning(
                            "파일 버전 오류",
                            f"입력 파일은 I5 단계만 허용됩니다.\n\n"
                            f"선택한 파일: {base_name}\n"
                            f"현재 버전: I{i_version}\n\n"
                            f"I5 품질 검증이 완료된 파일을 선택해주세요."
                        )
                        return
                else:
                    messagebox.showwarning(
                        "파일 버전 오류",
                        f"파일명에 버전 정보(_T*_I*)가 없습니다.\n\n"
                        f"선택한 파일: {base_name}\n\n"
                        f"I5 단계 파일을 선택해주세요."
                    )
                    return
                
                self.df = pd.read_excel(path)
                
                # 필수 컬럼 체크
                required_cols = ["IMG_S5_누끼_최종경로", "IMG_S5_믹스_최종경로"]
                missing_cols = [col for col in required_cols if col not in self.df.columns]
                
                if missing_cols:
                    messagebox.showerror(
                        "필수 컬럼 누락",
                        f"'{base_name}' 파일에 다음 컬럼이 없습니다:\n\n"
                        f"{', '.join(missing_cols)}\n\n"
                        f"필수 컬럼: IMG_S5_누끼_최종경로, IMG_S5_믹스_최종경로"
                    )
                    return
                
                self.input_file_path.set(path)
                self._log(f"엑셀 파일 로드 완료: {len(self.df)}행, {len(self.df.columns)}컬럼")
                
                # 컬럼 자동 설정
                self.image_column_var_1.set("IMG_S5_누끼_최종경로")
                self.image_column_var_2.set("IMG_S5_믹스_최종경로")
                self.output_column_var_1.set("누끼url")
                self.output_column_var_2.set("믹스url")
                self._log("컬럼 자동 설정 완료: IMG_S5_누끼_최종경로 → 누끼url, IMG_S5_믹스_최종경로 → 믹스url")
                
            except Exception as e:
                messagebox.showerror("오류", f"엑셀 파일을 읽는 중 오류가 발생했습니다:\n{e}")
    
    def _get_i5_uploaded_output_path(self, input_path: str) -> str:
        """
        I5 파일을 I5(업완) 형식으로 변환
        예: 상품_T3_I5.xlsx -> 상품_T3_I5(업완).xlsx
        """
        dir_name = os.path.dirname(input_path)
        base_name = os.path.basename(input_path)
        name_only, ext = os.path.splitext(base_name)
        
        # I5 패턴 찾기
        pattern = r"_T(\d+)_I5$"
        match = re.search(pattern, name_only)
        
        if match:
            current_t = int(match.group(1))
            original_name = name_only[: match.start()]
            new_filename = f"{original_name}_T{current_t}_I5(업완){ext}"
        else:
            # I5 패턴이 없으면 그냥 (업완) 추가
            new_filename = f"{name_only}(업완){ext}"
        
        return os.path.join(dir_name, new_filename)
    
    def _log(self, msg: str):
        """로그 출력"""
        ts = datetime.now().strftime("%H:%M:%S")
        self.log_widget.config(state='normal')
        self.log_widget.insert('end', f"[{ts}] {msg}\n")
        self.log_widget.see('end')
        self.log_widget.config(state='disabled')
    
    def _start_upload(self):
        """업로드 시작"""
        if self.is_running:
            return
        
        if not self.input_file_path.get():
            messagebox.showwarning("오류", "엑셀 파일을 선택해주세요.")
            return
        
        # I5 파일 전용이므로 컬럼은 자동 설정됨
        if not self.df is None:
            if "IMG_S5_누끼_최종경로" not in self.df.columns or "IMG_S5_믹스_최종경로" not in self.df.columns:
                messagebox.showerror("오류", "필수 컬럼(IMG_S5_누끼_최종경로, IMG_S5_믹스_최종경로)이 없습니다.")
                return
        
        if not self.s3_client:
            messagebox.showerror("오류", "R2 연결이 실패했습니다. 프로그램을 재시작해주세요.")
            return
        
        self.is_running = True
        self.stop_requested = False
        self.btn_start.config(state='disabled')
        self.btn_stop.config(state='normal')
        
        # 백그라운드 스레드에서 실행
        threading.Thread(target=self._run_upload, daemon=True).start()
    
    def _request_stop(self):
        """중단 요청"""
        self.stop_requested = True
        self._log("⛔ 중단 요청됨...")
    
    def _run_upload(self):
        """업로드 실행 (백그라운드 스레드)"""
        input_path = self.input_file_path.get()
        # I5 파일 전용: 컬럼 자동 설정
        image_col_1 = "IMG_S5_누끼_최종경로"
        image_col_2 = "IMG_S5_믹스_최종경로"
        output_col_1 = "누끼url"
        output_col_2 = "믹스url"
        prefix = self.prefix_path_var.get().strip()
        
        # 메인 런처 현황판 업데이트 (I5-2: 이미지 업로드 진행중) - img 상태만 업데이트 (text 상태는 변경하지 않음)
        try:
            if not input_path or input_path.strip() == "":
                self._log(f"[WARN] 런처 현황판 업데이트 실패: 입력 경로가 없습니다")
            else:
                root_name = get_root_filename(input_path)
                if not root_name or root_name.strip() == "":
                    self._log(f"[WARN] 런처 현황판 업데이트 실패: 파일명 추출 실패 (입력: {os.path.basename(input_path)})")
                else:
                    db_path = JobManager.find_db_path()
                    self._log(f"[INFO] 런처 현황판 업데이트 시도: {root_name} -> I5-2 (진행중) (DB: {db_path})")
                    JobManager.update_status(root_name, img_s5_2_msg="I5-2 (진행중)")
                    self._log(f"[INFO] 런처 현황판 업데이트 완료: {root_name} -> I5-2 (진행중)")
        except Exception as e:
            import traceback
            self._log(f"[ERROR] 런처 현황판 업데이트 실패: {e}\n{traceback.format_exc()}")
        
        stats = {"total": 0, "success": 0, "fail": 0, "skip": 0}
        
        try:
            # 엑셀 파일 로드
            self._log(f"엑셀 파일 로드 중... {os.path.basename(input_path)}")
            df = pd.read_excel(input_path)
            
            # 출력 컬럼 추가 (없으면)
            if output_col_1 not in df.columns:
                df[output_col_1] = ""
            if output_col_2 not in df.columns:
                df[output_col_2] = ""
            
            # 처리할 항목 필터링 (2개 컬럼 모두 처리)
            items = []
            for idx, row in df.iterrows():
                if self.stop_requested:
                    break
                
                # 첫 번째 이미지 경로
                image_path_1 = str(row.get(image_col_1, "")).strip()
                # 두 번째 이미지 경로
                image_path_2 = str(row.get(image_col_2, "")).strip()
                
                # 두 경로 모두 빈 값이면 스킵 (빈 행은 업로드하지 않음)
                if (not image_path_1 or image_path_1 == "nan") and (not image_path_2 or image_path_2 == "nan"):
                    stats["skip"] += 1
                    continue
                
                # 각 경로별로 처리할 항목 추가
                item_data = {"idx": idx, "paths": []}
                
                # 첫 번째 경로 처리
                if image_path_1 and image_path_1 != "nan":
                    # 이미 URL이면 스킵 (로컬 파일이 아님)
                    if not (image_path_1.startswith("http://") or image_path_1.startswith("https://")):
                        # 로컬 파일 경로 확인
                        if os.path.exists(image_path_1):
                            # 항상 업로드 (덮어쓰기 허용)
                            item_data["paths"].append({
                                "image_path": image_path_1,
                                "filename": os.path.basename(image_path_1),
                                "output_col": output_col_1,
                                "col_name": "누끼"
                            })
                        else:
                            self._log(f"[Row {idx+1}] 파일이 존재하지 않습니다 (누끼): {os.path.basename(image_path_1)}")
                
                # 두 번째 경로 처리
                if image_path_2 and image_path_2 != "nan":
                    # 이미 URL이면 스킵 (로컬 파일이 아님)
                    if not (image_path_2.startswith("http://") or image_path_2.startswith("https://")):
                        # 로컬 파일 경로 확인
                        if os.path.exists(image_path_2):
                            # 항상 업로드 (덮어쓰기 허용)
                            item_data["paths"].append({
                                "image_path": image_path_2,
                                "filename": os.path.basename(image_path_2),
                                "output_col": output_col_2,
                                "col_name": "믹스"
                            })
                        else:
                            self._log(f"[Row {idx+1}] 파일이 존재하지 않습니다 (믹스): {os.path.basename(image_path_2)}")
                
                # 처리할 경로가 있으면 추가
                if item_data["paths"]:
                    items.append(item_data)
                else:
                    stats["skip"] += 1
            
            # 전체 처리할 파일 수 계산
            total_files = sum(len(item["paths"]) for item in items)
            stats["total"] = total_files
            self.after(0, lambda: self.stat_total.set(str(stats["total"])))
            self._log(f"처리 대상: {stats['total']}개 파일 ({len(items)}개 행, 스킵: {stats['skip']}개 행)")
            
            if stats["total"] == 0:
                self._log("업로드할 항목이 없습니다.")
                self.after(0, self._on_upload_complete)
                return
            
            # 각 이미지 업로드
            file_count = 0
            for row_idx, item in enumerate(items, 1):
                if self.stop_requested:
                    self._log("⛔ 사용자 중단 요청으로 작업을 중단합니다.")
                    break
                
                idx = item["idx"]
                
                # 각 행의 모든 경로 처리
                for path_info in item["paths"]:
                    file_count += 1
                    
                    try:
                        # 진행률 업데이트
                        progress = (file_count / stats["total"]) * 100
                        self.after(0, lambda p=progress: self.pb.config(value=p))
                        self.after(0, lambda p=progress: self.stat_progress.set(f"{p:.1f}%"))
                        
                        image_path = path_info["image_path"]
                        filename = path_info["filename"]
                        output_col = path_info["output_col"]
                        col_name = path_info["col_name"]
                        
                        # R2에 저장될 경로 생성
                        if prefix:
                            # prefix 끝에 /가 없으면 추가
                            if not prefix.endswith("/"):
                                prefix = prefix + "/"
                            s3_path = f"{prefix}{filename}"
                        else:
                            s3_path = filename
                        
                        # 파일 업로드 (덮어쓰기 허용)
                        # boto3의 upload_file은 기본적으로 덮어쓰기를 지원합니다
                        self._log(f"[{file_count}/{stats['total']}] [{col_name}] 업로드 중: {filename} (덮어쓰기 허용)")
                        self.s3_client.upload_file(image_path, BUCKET_NAME, s3_path)
                        
                        # 공개 URL 생성
                        public_url = f"{PUBLIC_DEVELOPMENT_URL}/{s3_path}"
                        
                        # 엑셀에 URL 기록 (항상 업데이트)
                        df.at[idx, output_col] = public_url
                        
                        stats["success"] += 1
                        self.after(0, lambda: self.stat_success.set(str(stats["success"])))
                        self._log(f"[{file_count}/{stats['total']}] ✅ 완료 [{col_name}]: {public_url}")
                        
                    except FileNotFoundError:
                        stats["fail"] += 1
                        self.after(0, lambda: self.stat_fail.set(str(stats["fail"])))
                        self._log(f"[{file_count}/{stats['total']}] ❌ 파일을 찾을 수 없음 [{col_name}]: {filename}")
                    except Exception as e:
                        stats["fail"] += 1
                        self.after(0, lambda: self.stat_fail.set(str(stats["fail"])))
                        self._log(f"[{file_count}/{stats['total']}] ❌ 업로드 실패 [{col_name}]: {filename} / 오류: {e}")
            
            # 엑셀 파일 저장
            if stats["success"] > 0 or stats["fail"] > 0:
                self._log("엑셀 저장 중...")
                # 출력 파일명 생성 (I5(업완) 형식)
                output_path = self._get_i5_uploaded_output_path(input_path)
                
                if safe_save_excel(df, output_path):
                    self._log(f"엑셀 저장 완료: {os.path.basename(output_path)}")
                else:
                    self._log("엑셀 저장 실패 (사용자가 취소)")
            
            # 완료 메시지
            self._log("=== 업로드 완료 ===")
            self._log(f"성공: {stats['success']}건, 실패: {stats['fail']}건, 스킵: {stats['skip']}행")
            
            # 메인 런처 현황판 업데이트 (I5-2: 이미지 업로드 완료) - img 상태만 I5-2(업로드완료)로 업데이트 (text 상태는 변경하지 않음)
            try:
                if not input_path or input_path.strip() == "":
                    self._log(f"[WARN] 런처 현황판 업데이트 실패: 입력 경로가 없습니다")
                else:
                    root_name = get_root_filename(input_path)
                    if not root_name or root_name.strip() == "":
                        self._log(f"[WARN] 런처 현황판 업데이트 실패: 파일명 추출 실패 (입력: {os.path.basename(input_path)})")
                    else:
                        db_path = JobManager.find_db_path()
                        self._log(f"[INFO] 런처 현황판 업데이트 시도: {root_name} -> I5-2(업로드완료) (DB: {db_path})")
                        JobManager.update_status(root_name, img_s5_2_msg="I5-2(업로드완료)")
                        self._log(f"[INFO] 런처 현황판 업데이트 완료: {root_name} -> I5-2(업로드완료)")
            except Exception as e:
                import traceback
                self._log(f"[ERROR] 런처 현황판 업데이트 실패: {e}\n{traceback.format_exc()}")
            
            self.after(0, lambda: messagebox.showinfo(
                "완료",
                f"업로드가 완료되었습니다.\n\n"
                f"성공: {stats['success']}개 파일\n"
                f"실패: {stats['fail']}개 파일\n"
                f"스킵: {stats['skip']}개 행"
            ))
            
        except Exception as e:
            self._log(f"❌ 오류 발생: {e}")
            import traceback
            self._log(traceback.format_exc())
            self.after(0, lambda: messagebox.showerror("오류", str(e)))
        finally:
            self.after(0, self._on_upload_complete)
    
    def _on_upload_complete(self):
        """업로드 완료 후 UI 상태 복원"""
        self.is_running = False
        self.btn_start.config(state='normal')
        self.btn_stop.config(state='disabled')

if __name__ == "__main__":
    app = CloudflareUploadGUI()
    app.mainloop()

