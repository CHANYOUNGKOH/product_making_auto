"""
Stage5_Review.py

Stage 5: 품질 검증 (Quality Review)
- 기능: 합성된 이미지와 누끼 이미지, 원본 이미지를 비교하여 최종 선택
- 입력: I4 이상 엑셀 파일
- 출력: 선택된 이미지를 상품코드 기반으로 파일명 변경하여 복사
"""

import os
import re
import json
import shutil
import threading
import subprocess
import sys
from datetime import datetime
from typing import Optional, Dict, Any, List
from pathlib import Path

import pandas as pd
import tkinter as tk
from tkinter import ttk, filedialog, messagebox
from tkinter.scrolledtext import ScrolledText

try:
    from PIL import Image, ImageTk
    PIL_AVAILABLE = True
except ImportError:
    PIL_AVAILABLE = False

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
                # Stage5_ReviewGUI 인스턴스가 전역에 있으면 로그 출력
                import sys
                if hasattr(sys.modules.get(__name__), '_current_gui_instance'):
                    gui = sys.modules[__name__]._current_gui_instance
                    if gui and hasattr(gui, '_log'):
                        gui._log(error_msg)
            except:
                pass

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

def resize_and_convert_to_jpg(input_path: str, output_path: str, target_size: tuple = (1000, 1000)) -> bool:
    """
    이미지를 1000x1000px JPG로 변환
    - 동일 비율 유지하며 중앙 크롭 또는 패딩
    """
    if not PIL_AVAILABLE:
        return False
    
    try:
        img = Image.open(input_path)
        
        # RGBA를 RGB로 변환 (투명도 제거)
        if img.mode in ('RGBA', 'LA', 'P'):
            # 투명 배경을 흰색으로 변환
            background = Image.new('RGB', img.size, (255, 255, 255))
            if img.mode == 'P':
                img = img.convert('RGBA')
            background.paste(img, mask=img.split()[-1] if img.mode == 'RGBA' else None)
            img = background
        elif img.mode != 'RGB':
            img = img.convert('RGB')
        
        # 1000x1000으로 리사이즈 (동일 비율 유지, 중앙 크롭)
        img.thumbnail(target_size, Image.Resampling.LANCZOS)
        
        # 정확히 1000x1000으로 만들기 (패딩 또는 크롭)
        new_img = Image.new('RGB', target_size, (255, 255, 255))
        # 중앙에 배치
        paste_x = (target_size[0] - img.size[0]) // 2
        paste_y = (target_size[1] - img.size[1]) // 2
        new_img.paste(img, (paste_x, paste_y))
        
        # JPG로 저장 (품질 95)
        new_img.save(output_path, "JPEG", quality=95, optimize=True)
        return True
    except Exception as e:
        print(f"[이미지 변환 오류] {input_path}: {e}")
        return False

# ========================================================
# GUI Class
# ========================================================
class Stage5ReviewGUI(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Stage 5: 품질 검증 (Quality Review)")
        self.geometry("1400x900")
        
        # 스타일 설정
        self._configure_styles()
        
        # 변수 초기화
        self.input_file_path = tk.StringVar()
        self.output_dir_path = tk.StringVar()
        
        # 상태 변수
        self.is_running = False
        self.stop_requested = False
        
        # 데이터
        self.df = None
        self.items = []  # 처리할 항목 목록
        self.current_index = -1
        self.selections = {}  # {idx: 1 or 3} - 선택 결과 저장
        self.progress_file = None  # 진행 상황 저장 파일 경로
        
        # 시간 추적 변수
        self.start_time = None  # 검증 시작 시간
        self.item_start_times = {}  # 각 항목별 시작 시간 {idx: timestamp}
        self.total_review_time = 0.0  # 총 검증 시간 (초)
        
        # 이미지 미리보기 관련
        self.preview_window = None
        self.current_original_path = None
        self.current_nukki_path = None
        self.current_mix_path = None
        self.current_product_code = None
        self.current_product_name = None
        self.current_row_idx = None
        
        # 이미지 PhotoImage 객체 (가비지 컬렉션 방지)
        self.photo_original = None
        self.photo_nukki = None
        self.photo_mix = None
        
        # UI 구성
        self._init_ui()
    
    def _configure_styles(self):
        """스타일 설정"""
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
        style.configure("Action.TButton", font=("맑은 고딕", 11, "bold"), padding=5)
    
    def _init_ui(self):
        """UI 초기화"""
        main_frame = ttk.Frame(self, padding=20)
        main_frame.pack(fill='both', expand=True)
        
        # 1. 파일 선택
        frame_file = ttk.LabelFrame(main_frame, text="작업 대상 파일 (I5)", padding=15)
        frame_file.pack(fill='x', pady=(0, 10))
        
        rf1 = ttk.Frame(frame_file)
        rf1.pack(fill='x', pady=5)
        ttk.Label(rf1, text="엑셀 파일:", width=12).pack(side='left')
        ttk.Entry(rf1, textvariable=self.input_file_path, width=60).pack(side='left', fill='x', expand=True, padx=5)
        ttk.Button(rf1, text="📂 파일 선택", command=self._select_file).pack(side='right')
        
        rf2 = ttk.Frame(frame_file)
        rf2.pack(fill='x', pady=5)
        ttk.Label(rf2, text="출력 폴더:", width=12).pack(side='left')
        ttk.Entry(rf2, textvariable=self.output_dir_path, width=60).pack(side='left', fill='x', expand=True, padx=5)
        ttk.Button(rf2, text="📂 폴더 선택", command=self._select_output_dir).pack(side='right')
        
        # 2. 진행 상황
        frame_progress = ttk.LabelFrame(main_frame, text="진행 상황", padding=15)
        frame_progress.pack(fill='x', pady=(0, 10))
        
        p1 = ttk.Frame(frame_progress)
        p1.pack(fill='x', pady=5)
        ttk.Label(p1, text="현재 항목:", width=12).pack(side='left')
        self.progress_label = ttk.Label(p1, text="0 / 0", font=("맑은 고딕", 11, "bold"), foreground="#0052cc")
        self.progress_label.pack(side='left', padx=5)
        
        ttk.Label(p1, text="선택 완료:", width=12).pack(side='left', padx=(20, 0))
        self.completed_label = ttk.Label(p1, text="0", font=("맑은 고딕", 11, "bold"), foreground="green")
        self.completed_label.pack(side='left', padx=5)
        
        # 시간 정보 표시
        p2 = ttk.Frame(frame_progress)
        p2.pack(fill='x', pady=5)
        ttk.Label(p2, text="시작 시간:", width=12).pack(side='left')
        self.start_time_label = ttk.Label(p2, text="-", font=("맑은 고딕", 10), foreground="#666")
        self.start_time_label.pack(side='left', padx=5)
        
        ttk.Label(p2, text="경과 시간:", width=12).pack(side='left', padx=(20, 0))
        self.elapsed_time_label = ttk.Label(p2, text="00:00:00", font=("맑은 고딕", 10, "bold"), foreground="#007bff")
        self.elapsed_time_label.pack(side='left', padx=5)
        
        ttk.Label(p2, text="평균 시간:", width=12).pack(side='left', padx=(20, 0))
        self.avg_time_label = ttk.Label(p2, text="-", font=("맑은 고딕", 10, "bold"), foreground="#28a745")
        self.avg_time_label.pack(side='left', padx=5)
        
        # 진행률 표시 (이미지 복사 진행률)
        p3 = ttk.Frame(frame_progress)
        p3.pack(fill='x', pady=5)
        ttk.Label(p3, text="처리 진행률:", width=12).pack(side='left')
        self.progress_bar = ttk.Progressbar(p3, mode='determinate', length=400)
        self.progress_bar.pack(side='left', fill='x', expand=True, padx=5)
        self.progress_percent_label = ttk.Label(p3, text="0%", font=("맑은 고딕", 10, "bold"), foreground="#007bff", width=6)
        self.progress_percent_label.pack(side='left', padx=5)
        
        # 경과 시간 업데이트 타이머
        self.time_timer_id = None
        
        # 3. 액션 버튼
        btn_frame = ttk.Frame(main_frame)
        btn_frame.pack(fill='x', pady=(0, 10))
        
        self.btn_start = ttk.Button(btn_frame, text="▶ 검증 시작", style="Action.TButton", command=self._start_review)
        self.btn_start.pack(side='left', fill='x', expand=True, padx=(0, 5))
        
        self.btn_save = ttk.Button(btn_frame, text="💾 중간 저장", style="Action.TButton", command=self._manual_save, state='disabled')
        self.btn_save.pack(side='left', fill='x', expand=True, padx=5)
        
        self.btn_finish = ttk.Button(btn_frame, text="✅ 최종 처리 (이미지 복사 및 파일명 변경)", style="Action.TButton", command=self._finish_processing, state='disabled')
        self.btn_finish.pack(side='right', fill='x', expand=True, padx=(5, 0))
        
        # 4. 로그창
        self.log_frame = ttk.LabelFrame(main_frame, text="상세 로그", padding=10)
        self.log_frame.pack(fill='both', expand=True)
        self.log_widget = ScrolledText(self.log_frame, height=15, state='disabled', font=("Consolas", 9))
        self.log_widget.pack(fill='both', expand=True)
    
    def _select_file(self):
        """엑셀 파일 선택"""
        path = filedialog.askopenfilename(
            title="엑셀 파일 선택 (I5 버전만 가능)",
            filetypes=[("Excel files", "*.xlsx"), ("All files", "*.*")]
        )
        if path:
            base_name = os.path.basename(path)
            
            # I5 파일만 허용
            i_match = re.search(r"_I(\d+)", base_name, re.IGNORECASE)
            
            if not i_match:
                messagebox.showerror(
                    "오류",
                    f"파일명에 버전 정보(_T*_I*)가 없습니다.\n\n"
                    f"선택한 파일: {base_name}\n\n"
                    f"I5 버전 파일을 선택해주세요.\n"
                    f"(예: 상품_T3_I5.xlsx, 상품_T4_I5.xlsx)"
                )
                return
            
            i_version = int(i_match.group(1))
            if i_version != 5:
                messagebox.showerror(
                    "오류",
                    f"입력 파일은 I5 버전만 허용됩니다.\n\n"
                    f"선택한 파일: {base_name}\n"
                    f"현재 버전: I{i_version}\n\n"
                    f"I5 배경 합성이 완료된 파일을 선택해주세요.\n"
                    f"(예: 상품_T3_I5.xlsx, 상품_T4_I5.xlsx)"
                )
                return
            
            # 필수 컬럼 체크
            try:
                df_check = pd.read_excel(path)
                required_cols = ["썸네일경로", "IMG_S1_누끼", "IMG_S4_mix_생성경로", "view_point"]
                missing_cols = [col for col in required_cols if col not in df_check.columns]
                
                if missing_cols:
                    messagebox.showerror(
                        "필수 컬럼 누락",
                        f"'{base_name}' 파일에 다음 컬럼이 없습니다:\n\n"
                        f"{', '.join(missing_cols)}\n\n"
                        f"필수 컬럼: 썸네일경로, IMG_S1_누끼, IMG_S4_mix_생성경로, view_point"
                    )
                    return
                
                # view_point가 공란인 행 개수 확인 (안내용)
                view_point_empty = df_check["view_point"].isna() | (df_check["view_point"].astype(str).str.strip() == "")
                empty_count = view_point_empty.sum()
                if empty_count > 0:
                    self._log(f"[안내] view_point가 공란인 행: {empty_count}건 (검증 대상에서 제외됩니다)")
                
                self.input_file_path.set(path)
                self._log(f"파일 선택됨: {base_name}")
                
                # 엑셀 파일이 있는 디렉토리를 기준으로 출력 폴더 자동 설정
                excel_dir = os.path.dirname(path)
                # 엑셀 파일명에서 확장자 제거한 이름으로 하위 폴더 생성
                excel_base_name = os.path.splitext(base_name)[0]
                # 버전 정보 제거 (예: _T3_I4 제거)
                excel_base_name = re.sub(r"_T\d+_I\d+$", "", excel_base_name)
                # 기본 출력 폴더: 엑셀파일과 같은 디렉토리 / "최종이미지_엑셀파일명"
                default_output_dir = os.path.join(excel_dir, f"최종이미지_{excel_base_name}")
                
                # 출력 폴더가 비어있거나 기본값이면 자동 설정
                if not self.output_dir_path.get() or self.output_dir_path.get() == "":
                    self.output_dir_path.set(default_output_dir)
                    self._log(f"출력 폴더 자동 설정: {default_output_dir}")
            except Exception as e:
                messagebox.showerror("파일 읽기 오류", f"엑셀 파일을 읽는 중 오류가 발생했습니다:\n{e}")
    
    def _select_output_dir(self):
        """출력 폴더 선택"""
        path = filedialog.askdirectory(title="출력 폴더 선택")
        if path:
            self.output_dir_path.set(path)
            self._log(f"출력 폴더 선택됨: {path}")
    
    def _log(self, msg: str):
        """로그 출력"""
        ts = datetime.now().strftime("%H:%M:%S")
        self.log_widget.config(state='normal')
        self.log_widget.insert('end', f"[{ts}] {msg}\n")
        self.log_widget.see('end')
        self.log_widget.config(state='disabled')
    
    def _start_review(self):
        """검증 시작"""
        if self.is_running:
            return
        
        if not self.input_file_path.get():
            messagebox.showwarning("오류", "엑셀 파일을 선택해주세요.")
            return
        
        if not self.output_dir_path.get():
            messagebox.showwarning("오류", "출력 폴더를 선택해주세요.")
            return
        
        # 엑셀 파일 로드
        try:
            self.df = pd.read_excel(self.input_file_path.get())
            self._log(f"엑셀 파일 로드 완료: {len(self.df)}행")
        except Exception as e:
            messagebox.showerror("오류", f"엑셀 파일을 읽는 중 오류가 발생했습니다:\n{e}")
            return
        
        # 처리할 항목 필터링
        self.items = []
        skipped_count = 0
        for idx, row in self.df.iterrows():
            # view_point 컬럼이 공란인 행은 제외
            view_point = str(row.get("view_point", "")).strip()
            if not view_point or view_point == "nan" or view_point == "":
                skipped_count += 1
                self._log(f"[Row {idx+1}] view_point가 공란입니다. 검증 대상에서 제외됩니다.")
                continue
            
            original_path = str(row.get("썸네일경로", "")).strip()
            nukki_path = str(row.get("IMG_S1_누끼", "")).strip()
            mix_path = str(row.get("IMG_S4_mix_생성경로", "")).strip()
            
            # 필수 이미지 경로 확인
            if not nukki_path or nukki_path == "nan" or not os.path.exists(nukki_path):
                self._log(f"[Row {idx+1}] IMG_S1_누끼 경로가 없거나 파일이 존재하지 않습니다. 건너뜁니다.")
                skipped_count += 1
                continue
            
            # 원본 이미지는 선택사항
            if original_path and original_path != "nan" and not os.path.exists(original_path):
                original_path = None
            
            # 합성 이미지는 선택사항 (1번 선택 시 사용 안 함)
            if mix_path and mix_path != "nan" and not os.path.exists(mix_path):
                mix_path = None
            
            product_code = str(row.get("상품코드", row.get("코드", ""))).strip()
            product_name = str(row.get("원본상품명", row.get("상품명", ""))).strip()
            
            self.items.append({
                "idx": idx,
                "original_path": original_path if original_path and original_path != "nan" else None,
                "nukki_path": nukki_path,
                "mix_path": mix_path if mix_path and mix_path != "nan" else None,
                "product_code": product_code if product_code and product_code != "nan" else f"ITEM_{idx+1}",
                "product_name": product_name if product_name and product_name != "nan" else ""
            })
        
        if not self.items:
            messagebox.showwarning("오류", "처리할 항목이 없습니다.")
            return
        
        self._log(f"처리 대상: {len(self.items)}건 (제외: {skipped_count}건)")
        
        # 진행 상황 복구 시도 (우선순위: 엑셀 → JSON → 중간저장)
        if not self.selections:  # 선택 정보가 없을 때만 복구 시도
            self._load_progress()
        
        # 복구된 선택 정보가 있으면 첫 번째 미선택 항목으로 이동
        if self.selections:
            # 이미 선택된 항목 중 가장 마지막 항목의 다음으로 이동
            selected_indices = sorted([item['idx'] for item in self.items if item['idx'] in self.selections])
            if selected_indices:
                last_selected_idx = selected_indices[-1]
                # 마지막 선택된 항목의 다음 항목 찾기
                next_index = 0
                for i, item in enumerate(self.items):
                    if item['idx'] > last_selected_idx:
                        next_index = i
                        break
                else:
                    # 모든 항목이 선택되었으면 마지막 항목으로
                    next_index = len(self.items) - 1
                self.current_index = next_index
            else:
                self.current_index = 0
        else:
            self.current_index = 0
        
        # 검증 시작
        self.is_running = True
        self.btn_start.config(state='disabled')
        self.btn_save.config(state='normal')  # 중간 저장 버튼 활성화
        self.btn_finish.config(state='disabled')
        
        # 시작 시간 기록
        self.start_time = datetime.now()
        self.start_time_label.config(text=self.start_time.strftime("%H:%M:%S"))
        self.total_review_time = 0.0
        self.item_start_times = {}
        
        # 경과 시간 타이머 시작
        self._start_time_timer()
        
        # 현재 항목 표시
        self._show_current_item()
    
    def _show_current_item(self):
        """현재 항목 표시"""
        if self.current_index < 0 or self.current_index >= len(self.items):
            return
        
        item = self.items[self.current_index]
        
        # 현재 항목 시작 시간 기록
        if item['idx'] not in self.item_start_times:
            self.item_start_times[item['idx']] = datetime.now()
        
        # 진행 상황 업데이트
        self.progress_label.config(text=f"{self.current_index + 1} / {len(self.items)}")
        # items에 포함된 항목만 확인 (처리 대상 항목만)
        items_idx_set = {item['idx'] for item in self.items}
        completed_count = sum(1 for idx in items_idx_set if idx in self.selections)
        self.completed_label.config(text=str(completed_count))
        
        # 평균 시간 계산 및 표시
        if completed_count > 0 and self.start_time:
            elapsed_total = (datetime.now() - self.start_time).total_seconds()
            avg_seconds = elapsed_total / completed_count
            avg_minutes = int(avg_seconds // 60)
            avg_secs = int(avg_seconds % 60)
            self.avg_time_label.config(text=f"{avg_minutes:02d}:{avg_secs:02d}")
        
        # 미리보기 창 표시
        self._show_preview_window(
            original_path=item['original_path'],
            nukki_path=item['nukki_path'],
            mix_path=item['mix_path'],
            product_code=item['product_code'],
            product_name=item['product_name'],
            row_idx=item['idx']
        )
    
    def _show_preview_window(self, original_path: Optional[str], nukki_path: str, 
                            mix_path: Optional[str], product_code: str, product_name: str, row_idx: int):
        """이미지 비교 미리보기 창 표시"""
        if self.preview_window is None or not self.preview_window.winfo_exists():
            # 새 팝업 창 생성
            self.preview_window = tk.Toplevel(self)
            self.preview_window.title("🖼️ 품질 검증 - 이미지 비교")
            # 3개 이미지를 1:1 비율로 표시하기 위해 넓게 설정
            # 세로를 더 키워서 버튼이 잘 보이도록 (1500x900)
            self.preview_window.geometry("1500x900")
            self.preview_window.resizable(True, True)
            
            # 창 닫기 이벤트 처리
            self.preview_window.protocol("WM_DELETE_WINDOW", self._close_preview_window)
            
            # 메인 프레임
            main_preview_frame = ttk.Frame(self.preview_window, padding=10)
            main_preview_frame.pack(fill='both', expand=True)
            
            # 상품 정보 표시 영역
            info_frame = ttk.Frame(main_preview_frame)
            info_frame.pack(fill='x', pady=(0, 10))
            
            # 왼쪽: 상품 정보
            left_info = ttk.Frame(info_frame)
            left_info.pack(side='left', fill='x', expand=True)
            
            self.preview_product_code_label = ttk.Label(
                left_info, 
                text="상품코드: -", 
                font=("맑은 고딕", 11, "bold"),
                foreground="#333"
            )
            self.preview_product_code_label.pack(side='left', padx=10)
            
            self.preview_product_name_label = ttk.Label(
                left_info, 
                text="원본상품명: -", 
                font=("맑은 고딕", 10),
                foreground="#666"
            )
            self.preview_product_name_label.pack(side='left', padx=10)
            
            # 오른쪽: 진행 상황 및 선택 상태
            right_info = ttk.Frame(info_frame)
            right_info.pack(side='right')
            
            # 진행 개수 표시 (검증창 내부)
            self.preview_progress_label = ttk.Label(
                right_info,
                text="0 / 0",
                font=("맑은 고딕", 10, "bold"),
                foreground="#0052cc"
            )
            self.preview_progress_label.pack(side='left', padx=10)
            
            # 현재 선택 상태 표시
            self.selection_status_label = ttk.Label(
                right_info,
                text="선택: - (권장: [3]번 둘 다 사용)",
                font=("맑은 고딕", 10, "bold"),
                foreground="#ff6600"
            )
            self.selection_status_label.pack(side='left', padx=10)
            
            # 툴팁 함수 정의
            def create_tooltip(widget, text):
                def on_enter(event):
                    tooltip = tk.Toplevel()
                    tooltip.wm_overrideredirect(True)
                    tooltip.wm_geometry(f"+{event.x_root+10}+{event.y_root+10}")
                    label = tk.Label(
                        tooltip, 
                        text=text, 
                        background="#ffffe0", 
                        relief='solid', 
                        borderwidth=1,
                        font=("맑은 고딕", 9),
                        wraplength=300,
                        justify='left'
                    )
                    label.pack()
                    widget.tooltip = tooltip
                
                def on_leave(event):
                    if hasattr(widget, 'tooltip'):
                        widget.tooltip.destroy()
                        del widget.tooltip
                
                widget.bind('<Enter>', on_enter)
                widget.bind('<Leave>', on_leave)
            
            # 선택 상태 레이블에 툴팁 추가
            create_tooltip(
                self.selection_status_label,
                "💡 선택 가이드:\n\n"
                "[3]번 (둘 다 사용): 대부분의 경우 권장\n"
                "  - 누끼 이미지와 합성 이미지 모두 사용\n"
                "  - 더 많은 선택지 제공\n\n"
                "[1]번 (누끼만): 합성 이미지가 정말 사용 불가능한 경우만\n"
                "  - 합성 품질이 매우 낮거나 오류가 있는 경우\n\n"
                "[8]번 (둘 다 사용 안 함): 둘 다 너무 안 좋을 때\n"
                "  - 누끼와 합성 이미지 모두 사용하지 않음\n"
                "  - 경로를 빈셀로 만듭니다"
            )
            
            # 이미지 표시 영역 (3개 이미지 좌우 배치)
            preview_content = ttk.Frame(main_preview_frame)
            preview_content.pack(fill='both', expand=True, padx=5, pady=5)
            
            # 왼쪽: 원본 이미지 (썸네일)
            left_frame = ttk.LabelFrame(preview_content, text="원본 이미지 (썸네일경로)", padding=5)
            left_frame.pack(side='left', fill='both', expand=True, padx=(0, 5))
            
            self.preview_original_label = ttk.Label(left_frame, text="이미지 없음", anchor='center')
            self.preview_original_label.pack(fill='both', expand=True)
            
            # 중앙: 누끼 이미지
            center_frame = ttk.LabelFrame(preview_content, text="누끼 이미지 (IMG_S1_누끼)", padding=5)
            center_frame.pack(side='left', fill='both', expand=True, padx=5)
            
            self.preview_nukki_label = ttk.Label(center_frame, text="이미지 없음", anchor='center')
            self.preview_nukki_label.pack(fill='both', expand=True)
            
            # 오른쪽: 합성된 이미지
            right_frame = ttk.LabelFrame(preview_content, text="합성된 이미지 (IMG_S4_mix_생성경로)", padding=5)
            right_frame.pack(side='right', fill='both', expand=True, padx=(5, 0))
            
            self.preview_mix_label = ttk.Label(right_frame, text="이미지 없음", anchor='center')
            self.preview_mix_label.pack(fill='both', expand=True)
            
            # 하단: 선택 버튼 및 안내
            bottom_frame = ttk.Frame(main_preview_frame)
            bottom_frame.pack(fill='x', pady=(10, 0))
            
            # 선택 버튼 영역 (1번, 3번 주 버튼)
            selection_frame = ttk.Frame(bottom_frame)
            selection_frame.pack(fill='x', pady=10)
            
            # 왼쪽: 1번 버튼 (누끼만) - 큰 버튼
            btn_frame_1 = ttk.Frame(selection_frame)
            btn_frame_1.pack(side='left', fill='both', expand=True, padx=10)
            
            self.btn_select_1 = tk.Button(
                btn_frame_1,
                text="[1] 누끼만 사용\n(합성 불가능한 경우만)",
                font=("맑은 고딕", 14, "bold"),
                bg="#ff6600",
                fg="white",
                relief="raised",
                bd=3,
                cursor="hand2",
                command=lambda: self._select_option(1),
                padx=20,
                pady=15
            )
            self.btn_select_1.pack(fill='both', expand=True)
            
            # 중앙: 8번 버튼 (작은 경고 버튼) - 1번, 3번 사이 중앙에 배치
            btn_frame_8 = ttk.Frame(selection_frame)
            btn_frame_8.pack(side='left', padx=5)
            
            self.btn_select_8 = tk.Button(
                btn_frame_8,
                text="[8] 둘 다\n사용 안 함 ❌",
                font=("맑은 고딕", 10, "bold"),
                bg="#dc3545",
                fg="white",
                relief="raised",
                bd=2,
                cursor="hand2",
                command=lambda: self._select_option(8),
                padx=12,
                pady=8,
                width=8
            )
            self.btn_select_8.pack()
            
            # 오른쪽: 3번 버튼 (둘 다 사용 - 권장) - 큰 버튼
            btn_frame_3 = ttk.Frame(selection_frame)
            btn_frame_3.pack(side='right', fill='both', expand=True, padx=10)
            
            self.btn_select_3 = tk.Button(
                btn_frame_3,
                text="[3] 둘 다 사용 (권장) ✅\n누끼 + 합성",
                font=("맑은 고딕", 14, "bold"),
                bg="#28a745",
                fg="white",
                relief="raised",
                bd=3,
                cursor="hand2",
                command=lambda: self._select_option(3),
                padx=20,
                pady=15
            )
            self.btn_select_3.pack(fill='both', expand=True)
            
            # 하단: 안내 및 네비게이션
            guide_frame = ttk.Frame(bottom_frame)
            guide_frame.pack(fill='x', pady=(5, 0))
            
            guide_label = ttk.Label(
                guide_frame,
                text="💡 키보드 단축키: [1] 누끼만 | [3] 둘 다 사용 (권장) | [8] 둘 다 사용 안 함 | [←] 이전 | [→] 다음",
                font=("맑은 고딕", 9),
                foreground="#666"
            )
            guide_label.pack(side='left', padx=10)
            
            # 네비게이션 버튼
            nav_frame = ttk.Frame(guide_frame)
            nav_frame.pack(side='right', padx=10)
            
            btn_prev = ttk.Button(nav_frame, text="◀ 이전", command=self._prev_item)
            btn_prev.pack(side='left', padx=5)
            
            btn_next = ttk.Button(nav_frame, text="다음 ▶", command=self._next_item)
            btn_next.pack(side='left', padx=5)
            
            # 키보드 바인딩
            self.preview_window.bind('<Key-1>', lambda e: self._select_option(1))
            self.preview_window.bind('<Key-3>', lambda e: self._select_option(3))
            self.preview_window.bind('<Key-8>', lambda e: self._select_option(8))
            # 좌우 화살표 키 (일반 키보드 및 키패드 모두 지원)
            self.preview_window.bind('<Left>', lambda e: self._prev_item())
            self.preview_window.bind('<Right>', lambda e: self._next_item())
            # 키패드 좌우 화살표도 지원
            self.preview_window.bind('<KP_Left>', lambda e: self._prev_item())
            self.preview_window.bind('<KP_Right>', lambda e: self._next_item())
            self.preview_window.focus_set()  # 키보드 포커스 설정
            
            # 창 크기 변경 이벤트 바인딩
            self.preview_window.bind('<Configure>', lambda e: self._on_preview_window_resize())
        
        # 현재 이미지 정보 저장
        self.current_original_path = original_path
        self.current_nukki_path = nukki_path
        self.current_mix_path = mix_path
        self.current_product_code = product_code
        self.current_product_name = product_name
        self.current_row_idx = row_idx
        
        # 이미지 업데이트
        self._update_preview_images()
        
        # 진행 개수 업데이트 (검증창 내부)
        if hasattr(self, 'preview_progress_label') and self.preview_progress_label.winfo_exists():
            self.preview_progress_label.config(text=f"{self.current_index + 1} / {len(self.items)}")
        
        # 선택 상태 표시
        if row_idx in self.selections:
            selection = self.selections[row_idx]
            if selection == 1:
                status_text = "1번 선택됨 (누끼만) ⚠️"
                status_color = "#ff6600"  # 주황색 (경고)
            elif selection == 3:
                status_text = "3번 선택됨 (누끼+합성) ✅"
                status_color = "green"  # 초록색 (권장)
            elif selection == 8:
                status_text = "8번 선택됨 (둘 다 사용 안 함) ❌"
                status_color = "#dc3545"  # 빨간색 (주의)
            else:
                status_text = f"{selection}번 선택됨"
                status_color = "#333"
            self.selection_status_label.config(text=f"선택: {status_text}", foreground=status_color)
        else:
            self.selection_status_label.config(text="선택: - (권장: [3]번 둘 다 사용)", foreground="#ff6600")
    
    def _update_preview_images(self):
        """미리보기 이미지 업데이트"""
        if not PIL_AVAILABLE:
            return
        
        if self.preview_window is None or not self.preview_window.winfo_exists():
            return
        
        def update_ui():
            try:
                # 상품 정보 업데이트
                if hasattr(self, 'preview_product_code_label'):
                    code_text = f"상품코드: {self.current_product_code if self.current_product_code else '-'}"
                    self.preview_product_code_label.config(text=code_text)
                
                if hasattr(self, 'preview_product_name_label'):
                    name_text = f"원본상품명: {self.current_product_name if self.current_product_name else '-'}"
                    if self.current_product_name and len(self.current_product_name) > 50:
                        name_text = f"원본상품명: {self.current_product_name[:47]}..."
                    self.preview_product_name_label.config(text=name_text)
                
                # 창 크기에 맞춰 이미지 크기 계산
                window_width = self.preview_window.winfo_width()
                window_height = self.preview_window.winfo_height()
                if window_width > 1 and window_height > 1:
                    # 3개 이미지를 1:1 비율로 표시 (각 이미지당 약 1/3 공간)
                    max_size = min((window_width - 100) // 3, window_height - 150)
                else:
                    max_size = 300  # 기본값
                
                # 원본 이미지
                if self.current_original_path and os.path.exists(self.current_original_path):
                    img = Image.open(self.current_original_path)
                    img.thumbnail((max_size, max_size), Image.Resampling.LANCZOS)
                    self.photo_original = ImageTk.PhotoImage(img)
                    self.preview_original_label.config(image=self.photo_original, text="")
                else:
                    self.preview_original_label.config(image="", text="원본 이미지 없음")
                
                # 누끼 이미지
                if self.current_nukki_path and os.path.exists(self.current_nukki_path):
                    img = Image.open(self.current_nukki_path)
                    img.thumbnail((max_size, max_size), Image.Resampling.LANCZOS)
                    self.photo_nukki = ImageTk.PhotoImage(img)
                    self.preview_nukki_label.config(image=self.photo_nukki, text="")
                else:
                    self.preview_nukki_label.config(image="", text="누끼 이미지 없음")
                
                # 합성 이미지
                if self.current_mix_path and os.path.exists(self.current_mix_path):
                    img = Image.open(self.current_mix_path)
                    img.thumbnail((max_size, max_size), Image.Resampling.LANCZOS)
                    self.photo_mix = ImageTk.PhotoImage(img)
                    self.preview_mix_label.config(image=self.photo_mix, text="")
                else:
                    self.preview_mix_label.config(image="", text="합성 이미지 없음")
            except Exception as e:
                self._log(f"[미리보기] 이미지 로드 오류: {e}")
        
        self.after(0, update_ui)
    
    def _on_preview_window_resize(self):
        """팝업 창 크기 변경 시 이미지 재조정"""
        if self.preview_window and self.preview_window.winfo_exists():
            self._update_preview_images()
    
    def _close_preview_window(self):
        """미리보기 창 닫기"""
        if self.preview_window:
            self.preview_window.destroy()
            self.preview_window = None
            # 창을 닫아도 검증은 계속 진행 가능하도록 상태 유지
            # is_running은 유지하고, 단지 미리보기 창만 닫힘
    
    def _select_option(self, option: int):
        """선택 옵션 처리 (1: 누끼만, 3: 누끼+합성, 8: 둘 다 사용 안 함)"""
        if self.current_row_idx is None:
            return
        
        # 8번 선택 시 확인 메시지
        if option == 8:
            # 품질검증 창이 있으면 그 창을 부모로 지정하여 메시지 박스가 창 중심에 표시되도록 함
            parent_window = self.preview_window if (self.preview_window and self.preview_window.winfo_exists()) else self
            if not messagebox.askyesno(
                "확인",
                "정말로 둘 다 지우겠습니까?\n\n"
                "이 선택은 누끼 이미지와 합성 이미지 경로를 모두 빈셀로 만듭니다.\n"
                "계속하시겠습니까?",
                parent=parent_window
            ):
                return  # 사용자가 취소하면 선택하지 않음
        
        # 현재 항목 처리 시간 계산
        if self.current_row_idx in self.item_start_times:
            item_start = self.item_start_times[self.current_row_idx]
            item_elapsed = (datetime.now() - item_start).total_seconds()
            self.total_review_time += item_elapsed
        
        self.selections[self.current_row_idx] = option
        # items에 포함된 항목만 확인 (처리 대상 항목만)
        items_idx_set = {item['idx'] for item in self.items}
        completed_count = sum(1 for idx in items_idx_set if idx in self.selections)
        self.completed_label.config(text=str(completed_count))
        
        # 선택 상태 표시
        if option == 1:
            status_text = "1번 선택됨 (누끼만) ⚠️"
            status_color = "#ff6600"  # 주황색 (경고)
        elif option == 3:
            status_text = "3번 선택됨 (누끼+합성) ✅"
            status_color = "green"  # 초록색 (권장)
        elif option == 8:
            status_text = "8번 선택됨 (둘 다 사용 안 함) ❌"
            status_color = "#dc3545"  # 빨간색 (주의)
        else:
            status_text = f"{option}번 선택됨"
            status_color = "#333"
        self.selection_status_label.config(text=f"선택: {status_text}", foreground=status_color)
        
        # 처리 시간 로그
        option_text = {
            1: "누끼만",
            3: "누끼+합성",
            8: "둘 다 사용 안 함"
        }.get(option, f"{option}번")
        
        if self.current_row_idx in self.item_start_times:
            item_elapsed = (datetime.now() - self.item_start_times[self.current_row_idx]).total_seconds()
            self._log(f"[Row {self.current_row_idx + 1}] {option_text} 선택됨 (소요: {item_elapsed:.1f}초)")
        else:
            self._log(f"[Row {self.current_row_idx + 1}] {option_text} 선택됨")
        
        # 🔒 선택 정보 자동 저장 (중단 시 복구 가능)
        self._save_progress()
        
        # 자동으로 다음 항목으로 이동
        self._next_item()
    
    def _save_progress(self):
        """선택 정보를 JSON 파일로 저장 (중단 시 복구용)"""
        if not self.input_file_path.get() or not self.selections:
            return
        
        try:
            excel_path = self.input_file_path.get()
            excel_dir = os.path.dirname(excel_path)
            excel_base = os.path.splitext(os.path.basename(excel_path))[0]
            
            # 진행 상황 파일 경로
            self.progress_file = os.path.join(excel_dir, f"{excel_base}_stage5_progress.json")
            
            # items에 포함된 항목만 확인 (처리 대상 항목만)
            items_idx_set = {item['idx'] for item in self.items}
            completed_count = sum(1 for idx in items_idx_set if idx in self.selections)
            
            progress_data = {
                "selections": self.selections,
                "input_file": excel_path,
                "output_dir": self.output_dir_path.get(),
                "saved_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "total_items": len(self.items),
                "completed_count": completed_count
            }
            
            with open(self.progress_file, 'w', encoding='utf-8') as f:
                json.dump(progress_data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            # 진행 상황 저장 실패해도 작업은 계속 진행
            self._log(f"[WARN] 진행 상황 저장 실패: {e}")
    
    def _load_progress(self):
        """저장된 진행 상황 복구 (우선순위: 엑셀 → JSON → 중간저장)"""
        if not self.input_file_path.get() or self.df is None:
            return False
        
        excel_path = self.input_file_path.get()
        excel_dir = os.path.dirname(excel_path)
        excel_base = os.path.splitext(os.path.basename(excel_path))[0]
        
        # 처리 대상 항목 인덱스 집합 (한 번만 계산)
        items_idx_set = {item['idx'] for item in self.items}
        
        recovered_selections = {}
        recovery_source = None
        recovery_count = 0
        
        # 1단계: 엑셀 파일에서 이미 선택된 항목 확인
        nukki_col = "IMG_S5_누끼_최종경로"
        mix_col = "IMG_S5_믹스_최종경로"
        
        if nukki_col in self.df.columns or mix_col in self.df.columns:
            excel_recovered = 0
            # self.items에 포함된 항목만 확인 (처리 대상 항목만)
            
            for row_idx in items_idx_set:
                if row_idx >= len(self.df):
                    continue
                
                row = self.df.iloc[row_idx]
                nukki_path = str(row.get(nukki_col, "")).strip() if nukki_col in self.df.columns else ""
                mix_path = str(row.get(mix_col, "")).strip() if mix_col in self.df.columns else ""
                
                # 경로가 있으면 선택 정보 추출
                if nukki_path and nukki_path != "nan" and nukki_path != "" and not nukki_path.startswith("[선택:"):
                    # 실제 경로가 있으면 3번 선택 (둘 다 사용)
                    if mix_path and mix_path != "nan" and mix_path != "" and not mix_path.startswith("[선택:"):
                        recovered_selections[row_idx] = 3
                        excel_recovered += 1
                    else:
                        # 누끼만 있으면 1번 선택
                        recovered_selections[row_idx] = 1
                        excel_recovered += 1
                elif nukki_path == "" and mix_path == "":
                    # 둘 다 빈셀이면 8번 선택 (둘 다 사용 안 함)
                    # 단, 처음부터 빈셀일 수도 있으므로 확인 필요
                    # 중간저장 파일이나 JSON에서 확인된 경우만 8번으로 처리
                    pass
            
            if excel_recovered > 0:
                recovery_source = "엑셀 파일"
                recovery_count = excel_recovered
                self._log(f"[진행 상황 복구] 엑셀 파일에서 {excel_recovered}건의 선택 정보 발견")
        
        # 2단계: JSON 파일에서 진행 상황 복구 (엑셀에 없는 항목만 추가, items에 포함된 항목만)
        progress_file = os.path.join(excel_dir, f"{excel_base}_stage5_progress.json")
        items_idx_set = {item['idx'] for item in self.items}  # 처리 대상 항목 인덱스 집합
        
        if os.path.exists(progress_file):
            try:
                with open(progress_file, 'r', encoding='utf-8') as f:
                    progress_data = json.load(f)
                
                # 입력 파일 경로 확인
                if progress_data.get("input_file") == excel_path:
                    json_selections = {int(k): int(v) for k, v in progress_data.get("selections", {}).items()}
                    json_recovered = 0
                    
                    # 엑셀에 없고 items에 포함된 항목만 추가
                    for row_idx, selection in json_selections.items():
                        if row_idx in items_idx_set and row_idx not in recovered_selections:
                            recovered_selections[row_idx] = selection
                            json_recovered += 1
                    
                    if json_recovered > 0:
                        if recovery_source:
                            recovery_source += f" + JSON 파일 ({json_recovered}건 추가)"
                        else:
                            recovery_source = "JSON 파일"
                            recovery_count = json_recovered
                        self._log(f"[진행 상황 복구] JSON 파일에서 {json_recovered}건의 선택 정보 추가 (처리 대상 항목만)")
                    
                    # 출력 폴더 복구
                    if progress_data.get("output_dir") and not self.output_dir_path.get():
                        self.output_dir_path.set(progress_data["output_dir"])
            except Exception as e:
                self._log(f"[WARN] JSON 파일 복구 실패: {e}")
        
        # 3단계: 중간 저장 파일 확인 (엑셀과 JSON에 없는 항목만 추가)
        temp_save_path = os.path.join(excel_dir, f"{excel_base}_stage5_중간저장.xlsx")
        if os.path.exists(temp_save_path):
            try:
                df_temp = pd.read_excel(temp_save_path)
                
                if nukki_col in df_temp.columns or mix_col in df_temp.columns:
                    temp_recovered = 0
                    for item in self.items:
                        row_idx = item['idx']
                        if row_idx >= len(df_temp) or row_idx in recovered_selections:
                            continue
                        
                        row = df_temp.iloc[row_idx]
                        nukki_path = str(row.get(nukki_col, "")).strip() if nukki_col in df_temp.columns else ""
                        mix_path = str(row.get(mix_col, "")).strip() if mix_col in df_temp.columns else ""
                        
                        # 선택 정보 추출
                        if nukki_path.startswith("[선택: 누끼만]"):
                            recovered_selections[row_idx] = 1
                            temp_recovered += 1
                        elif nukki_path.startswith("[선택: 누끼+합성]") or mix_path.startswith("[선택: 누끼+합성]"):
                            recovered_selections[row_idx] = 3
                            temp_recovered += 1
                        elif nukki_path == "" and mix_path == "":
                            # 중간저장에서 둘 다 빈셀이면 8번 선택 가능성
                            # 하지만 확실하지 않으므로 건너뜀
                            pass
                    
                    if temp_recovered > 0:
                        if recovery_source:
                            recovery_source += f" + 중간저장 파일 ({temp_recovered}건 추가)"
                        else:
                            recovery_source = "중간저장 파일"
                            recovery_count = temp_recovered
                        self._log(f"[진행 상황 복구] 중간저장 파일에서 {temp_recovered}건의 선택 정보 추가")
            except Exception as e:
                self._log(f"[WARN] 중간저장 파일 복구 실패: {e}")
        
        # 복구된 선택 정보가 있으면 사용자에게 확인
        if recovered_selections:
            total_recovered = len(recovered_selections)
            result = messagebox.askyesno(
                "진행 상황 복구",
                f"이전에 작업하던 진행 상황이 발견되었습니다.\n\n"
                f"복구 소스: {recovery_source}\n"
                f"완료된 선택: {total_recovered}건\n\n"
                f"이어서 진행하시겠습니까?"
            )
            
            if result:
                self.selections = recovered_selections
                self._log(f"진행 상황 복구 완료: {total_recovered}건의 선택 정보 복구됨 ({recovery_source})")
                return True
            else:
                # 복구하지 않으면 진행 상황 파일 삭제
                if os.path.exists(progress_file):
                    try:
                        os.remove(progress_file)
                        self._log("진행 상황 파일 삭제됨 (새로 시작)")
                    except:
                        pass
                self.selections = {}
                return False
        
        return False
    
    def _start_time_timer(self):
        """경과 시간 실시간 업데이트 타이머 시작"""
        def update_time():
            if not self.is_running or not self.start_time:
                return
            
            elapsed = (datetime.now() - self.start_time).total_seconds()
            hours = int(elapsed // 3600)
            minutes = int((elapsed % 3600) // 60)
            seconds = int(elapsed % 60)
            self.elapsed_time_label.config(text=f"{hours:02d}:{minutes:02d}:{seconds:02d}")
            
            # 1초마다 업데이트
            self.time_timer_id = self.after(1000, update_time)
        
        # 첫 업데이트 즉시 실행
        update_time()
    
    def _stop_time_timer(self):
        """경과 시간 타이머 중지"""
        if self.time_timer_id:
            self.after_cancel(self.time_timer_id)
            self.time_timer_id = None
    
    def _prev_item(self):
        """이전 항목으로 이동"""
        if self.current_index > 0:
            self.current_index -= 1
            self._show_current_item()
    
    def _next_item(self):
        """다음 항목으로 이동"""
        if self.current_index < len(self.items) - 1:
            self.current_index += 1
            self._show_current_item()
        else:
            # 마지막 항목이면 완료 안내
            # items에 포함된 항목만 확인 (처리 대상 항목만)
            items_idx_set = {item['idx'] for item in self.items}
            selected_items_count = sum(1 for idx in items_idx_set if idx in self.selections)
            
            if selected_items_count == len(self.items):
                # 총 소요 시간 계산
                total_elapsed = (datetime.now() - self.start_time).total_seconds() if self.start_time else 0
                avg_time = total_elapsed / len(self.items) if len(self.items) > 0 else 0
                hours = int(total_elapsed // 3600)
                minutes = int((total_elapsed % 3600) // 60)
                seconds = int(total_elapsed % 60)
                avg_minutes = int(avg_time // 60)
                avg_secs = int(avg_time % 60)
                
                messagebox.showinfo(
                    "완료",
                    f"모든 항목의 선택이 완료되었습니다.\n\n"
                    f"총 소요 시간: {hours:02d}:{minutes:02d}:{seconds:02d}\n"
                    f"평균 검증 시간: {avg_minutes:02d}:{avg_secs:02d} / 항목\n\n"
                    f"'최종 처리' 버튼을 눌러 이미지를 복사하고 파일명을 변경하세요."
                )
                self.btn_finish.config(state='normal')
                self._stop_time_timer()  # 타이머 중지
            else:
                messagebox.showwarning("미완료", f"아직 선택하지 않은 항목이 있습니다.\n\n완료: {selected_items_count} / {len(self.items)}")
    
    def _manual_save(self):
        """수동 저장: 현재까지의 선택 정보를 엑셀에 반영하고 저장"""
        if not self.input_file_path.get():
            messagebox.showwarning("오류", "엑셀 파일이 선택되지 않았습니다.")
            return
        
        if not self.selections:
            messagebox.showwarning("저장할 내용 없음", "아직 선택한 항목이 없습니다.")
            return
        
        # 엑셀 파일이 로드되어 있는지 확인
        if self.df is None:
            try:
                self.df = pd.read_excel(self.input_file_path.get())
                self._log(f"엑셀 파일 로드 완료: {len(self.df)}행")
            except Exception as e:
                messagebox.showerror("오류", f"엑셀 파일을 읽는 중 오류가 발생했습니다:\n{e}")
                return
        
        # 엑셀에 새 컬럼 추가 (없으면)
        nukki_col = "IMG_S5_누끼_최종경로"
        mix_col = "IMG_S5_믹스_최종경로"
        
        if nukki_col not in self.df.columns:
            self.df[nukki_col] = ""
        if mix_col not in self.df.columns:
            self.df[mix_col] = ""
        
        # 선택 정보를 엑셀에 반영 (이미지 복사는 하지 않고 선택 정보만 기록)
        saved_count = 0
        for item in self.items:
            row_idx = item['idx']
            if row_idx not in self.selections:
                continue  # 선택되지 않은 항목은 건너뜀
            
            selection = self.selections[row_idx]
            
            # 선택 정보를 컬럼에 기록 (이미지 복사는 하지 않음)
            if selection == 1:
                # 1번 선택: 누끼만 사용 (경로는 나중에 최종 처리 시 채워짐)
                self.df.at[row_idx, nukki_col] = "[선택: 누끼만]"
                self.df.at[row_idx, mix_col] = ""
            elif selection == 3:
                # 3번 선택: 둘 다 사용 (경로는 나중에 최종 처리 시 채워짐)
                self.df.at[row_idx, nukki_col] = "[선택: 누끼+합성]"
                self.df.at[row_idx, mix_col] = "[선택: 누끼+합성]"
            elif selection == 8:
                # 8번 선택: 둘 다 사용 안 함
                self.df.at[row_idx, nukki_col] = ""
                self.df.at[row_idx, mix_col] = ""
            
            saved_count += 1
        
        # 엑셀 파일 백업 생성
        try:
            excel_path = self.input_file_path.get()
            excel_dir = os.path.dirname(excel_path)
            excel_base = os.path.basename(excel_path)
            excel_name, excel_ext = os.path.splitext(excel_base)
            backup_path = os.path.join(excel_dir, f"{excel_name}_백업_{datetime.now().strftime('%Y%m%d_%H%M%S')}{excel_ext}")
            shutil.copy2(excel_path, backup_path)
            self._log(f"엑셀 파일 백업 생성: {os.path.basename(backup_path)}")
        except Exception as e:
            self._log(f"[WARN] 백업 생성 실패: {e}")
        
        # 중간 저장 파일로 저장
        try:
            excel_path = self.input_file_path.get()
            excel_dir = os.path.dirname(excel_path)
            excel_base = os.path.basename(excel_path)
            excel_name, excel_ext = os.path.splitext(excel_base)
            
            # 중간 저장 파일 경로
            temp_save_path = os.path.join(excel_dir, f"{excel_name}_stage5_중간저장{excel_ext}")
            
            if safe_save_excel(self.df, temp_save_path):
                self._log(f"[중간저장] {saved_count}건의 선택 정보 저장 완료: {os.path.basename(temp_save_path)}")
                messagebox.showinfo(
                    "저장 완료",
                    f"중간 저장이 완료되었습니다.\n\n"
                    f"저장된 선택 정보: {saved_count}건\n"
                    f"저장 파일: {os.path.basename(temp_save_path)}\n\n"
                    f"원본 파일은 백업되었습니다."
                )
            else:
                messagebox.showerror("저장 실패", "엑셀 파일 저장에 실패했습니다.\n파일이 열려있을 수 있습니다.")
        except Exception as e:
            self._log(f"[ERROR] 중간 저장 실패: {e}")
            messagebox.showerror("저장 오류", f"중간 저장 중 오류가 발생했습니다:\n{e}")
    
    def _finish_processing(self):
        """최종 처리: 이미지 복사 및 파일명 변경"""
        if not self.output_dir_path.get():
            messagebox.showwarning("오류", "출력 폴더를 선택해주세요.")
            return
        
        output_dir = self.output_dir_path.get()
        # 출력 폴더가 없으면 자동 생성
        try:
            os.makedirs(output_dir, exist_ok=True)
            self._log(f"출력 폴더 확인/생성: {output_dir}")
        except Exception as e:
            messagebox.showerror("오류", f"출력 폴더를 생성할 수 없습니다:\n{output_dir}\n\n오류: {e}")
            return
        
        # 메인 런처 현황판 업데이트 (I5-1: 품질 검증 진행중) - img 상태만 업데이트 (text 상태는 변경하지 않음)
        try:
            input_path = self.input_file_path.get()
            root_name = get_root_filename(input_path)
            if not root_name or root_name.strip() == "":
                self._log(f"[WARN] 런처 현황판 업데이트 실패: 파일명 추출 실패 (입력: {os.path.basename(input_path) if input_path else 'None'})")
            else:
                db_path = JobManager.find_db_path()
                self._log(f"[INFO] 런처 현황판 업데이트 시도: {root_name} -> I5-1 (진행중) (DB: {db_path})")
                JobManager.update_status(root_name, img_s5_1_msg="I5-1 (진행중)")
                self._log(f"[INFO] 런처 현황판 업데이트 완료: {root_name} -> I5-1 (진행중)")
        except Exception as e:
            import traceback
            self._log(f"[ERROR] 런처 현황판 업데이트 실패: {e}\n{traceback.format_exc()}")
        
        success_count = 0
        fail_count = 0
        
        # 엑셀에 새 컬럼 추가 (없으면)
        nukki_col = "IMG_S5_누끼_최종경로"
        mix_col = "IMG_S5_믹스_최종경로"
        
        if nukki_col not in self.df.columns:
            self.df[nukki_col] = ""
        if mix_col not in self.df.columns:
            self.df[mix_col] = ""
        
        self._log("=== 이미지 복사 및 파일명 변경 시작 ===")
        
        # 엑셀 파일 백업 생성
        try:
            excel_path = self.input_file_path.get()
            excel_dir = os.path.dirname(excel_path)
            excel_base = os.path.basename(excel_path)
            excel_name, excel_ext = os.path.splitext(excel_base)
            backup_path = os.path.join(excel_dir, f"{excel_name}_백업_{datetime.now().strftime('%Y%m%d_%H%M%S')}{excel_ext}")
            shutil.copy2(excel_path, backup_path)
            self._log(f"엑셀 파일 백업 생성: {os.path.basename(backup_path)}")
        except Exception as e:
            self._log(f"[WARN] 백업 생성 실패: {e}")
        
        # 선택된 항목만 필터링
        selected_items = [item for item in self.items if item['idx'] in self.selections]
        total_items = len(selected_items)
        processed_count = 0
        
        # 진행률 바 최대값 설정 및 초기화
        self.progress_bar['maximum'] = total_items
        self.progress_bar['value'] = 0
        self.progress_percent_label.config(text="0%")
        
        for item in selected_items:
            row_idx = item['idx']
            product_code = item['product_code']
            selection = self.selections[row_idx]
            processed_count += 1
            
            try:
                # 1번 선택: 누끼만 사용
                if selection == 1:
                    nukki_path = item['nukki_path']
                    if os.path.exists(nukki_path):
                        output_filename = f"{product_code}_01.jpg"
                        output_path = os.path.join(output_dir, output_filename)
                        
                        # 이미지 변환 및 복사
                        if resize_and_convert_to_jpg(nukki_path, output_path):
                            success_count += 1
                            # 엑셀에 경로 업데이트
                            self.df.at[row_idx, nukki_col] = output_path
                            self.df.at[row_idx, mix_col] = ""  # 믹스는 사용 안 함
                            # 로그 출력 빈도 줄이기 (100건마다 또는 마지막)
                            if processed_count % 100 == 0 or processed_count == total_items:
                                self._log(f"[진행] {processed_count}/{total_items}건 처리 완료 (성공: {success_count}건)")
                        else:
                            fail_count += 1
                            if processed_count % 100 == 0 or processed_count == total_items:
                                self._log(f"[오류] {processed_count}건 처리 중 {output_filename} 생성 실패")
                    else:
                        fail_count += 1
                        if processed_count % 100 == 0 or processed_count == total_items:
                            self._log(f"[오류] {processed_count}건 처리 중 누끼 이미지 파일 없음: {nukki_path}")
                
                # 3번 선택: 누끼 + 합성 둘 다 사용
                elif selection == 3:
                    nukki_path = item['nukki_path']
                    mix_path = item['mix_path']
                    
                    nukki_success = False
                    mix_success = False
                    
                    # 누끼 이미지 복사
                    if nukki_path and os.path.exists(nukki_path):
                        output_filename_01 = f"{product_code}_01.jpg"
                        output_path_01 = os.path.join(output_dir, output_filename_01)
                        
                        if resize_and_convert_to_jpg(nukki_path, output_path_01):
                            nukki_success = True
                            self.df.at[row_idx, nukki_col] = output_path_01
                            # 로그 출력 빈도 줄이기
                            if processed_count % 100 == 0 or processed_count == total_items:
                                self._log(f"[진행] {processed_count}/{total_items}건 처리 완료 (성공: {success_count}건)")
                        else:
                            fail_count += 1
                            if processed_count % 100 == 0 or processed_count == total_items:
                                self._log(f"[오류] {processed_count}건 처리 중 {output_filename_01} 생성 실패")
                    else:
                        fail_count += 1
                        if processed_count % 100 == 0 or processed_count == total_items:
                            self._log(f"[오류] {processed_count}건 처리 중 누끼 이미지 파일 없음: {nukki_path}")
                    
                    # 합성 이미지 복사
                    if mix_path and os.path.exists(mix_path):
                        output_filename_02 = f"{product_code}_02.jpg"
                        output_path_02 = os.path.join(output_dir, output_filename_02)
                        
                        if resize_and_convert_to_jpg(mix_path, output_path_02):
                            mix_success = True
                            self.df.at[row_idx, mix_col] = output_path_02
                            # 로그 출력 빈도 줄이기
                            if processed_count % 100 == 0 or processed_count == total_items:
                                self._log(f"[진행] {processed_count}/{total_items}건 처리 완료 (성공: {success_count}건)")
                        else:
                            fail_count += 1
                            if processed_count % 100 == 0 or processed_count == total_items:
                                self._log(f"[오류] {processed_count}건 처리 중 {output_filename_02} 생성 실패")
                    else:
                        fail_count += 1
                        if processed_count % 100 == 0 or processed_count == total_items:
                            self._log(f"[오류] {processed_count}건 처리 중 합성 이미지 파일 없음: {mix_path}")
                    
                    # 둘 다 성공하면 success_count 증가
                    if nukki_success and mix_success:
                        success_count += 1
                    elif nukki_success or mix_success:
                        # 하나만 성공한 경우는 부분 성공으로 처리
                        pass
                
                # 8번 선택: 둘 다 사용하지 않음 (경로를 빈셀로)
                elif selection == 8:
                    # 누끼 경로와 합성 경로를 모두 빈셀로 설정
                    self.df.at[row_idx, nukki_col] = ""
                    self.df.at[row_idx, mix_col] = ""
                    # 로그 출력 빈도 줄이기
                    if processed_count % 100 == 0 or processed_count == total_items:
                        self._log(f"[진행] {processed_count}/{total_items}건 처리 완료")
                    # 8번은 이미지 복사 없이 경로만 빈셀로 만드는 것이므로 success_count는 증가하지 않음
                
            except Exception as e:
                fail_count += 1
                # 오류는 항상 로그 출력 (중요)
                self._log(f"[Row {row_idx+1}] ❌ 처리 실패: {e}")
                import traceback
                if processed_count % 10 == 0:  # 오류 상세는 10건마다만
                    self._log(f"[Row {row_idx+1}] 오류 상세: {traceback.format_exc()}")
            
            # 진행률 업데이트 (매 항목마다)
            progress_percent = int((processed_count / total_items) * 100) if total_items > 0 else 0
            self.progress_bar['value'] = processed_count
            self.progress_percent_label.config(text=f"{progress_percent}%")
            self.update()  # GUI 업데이트 (진행률 표시)
            
            # 🔒 중간 저장 (100건마다 또는 마지막 항목) - 빈도 줄이기
            if processed_count % 100 == 0 or processed_count == total_items:
                try:
                    excel_path = self.input_file_path.get()
                    excel_dir = os.path.dirname(excel_path)
                    excel_base = os.path.basename(excel_path)
                    excel_name, excel_ext = os.path.splitext(excel_base)
                    
                    # 중간 저장 파일
                    temp_save_path = os.path.join(excel_dir, f"{excel_name}_stage5_중간저장{excel_ext}")
                    if safe_save_excel(self.df, temp_save_path):
                        self._log(f"[중간저장] {processed_count}/{total_items}건 처리 완료 ({progress_percent}%), 임시 저장: {os.path.basename(temp_save_path)}")
                except Exception as e:
                    self._log(f"[WARN] 중간 저장 실패: {e}")
        
        # 엑셀 파일 저장 (T*_I5 형식 그대로 저장)
        try:
            excel_path = self.input_file_path.get()
            # 입력 파일이 이미 I5이므로 그대로 저장
            output_excel_path = excel_path
            
            # 엑셀 파일 저장 (재시도 포함)
            if safe_save_excel(self.df, output_excel_path):
                self._log(f"엑셀 파일 저장 완료: {os.path.basename(output_excel_path)} (T*_I5 형식 그대로 저장)")
            else:
                self._log(f"[WARNING] 엑셀 파일 저장 실패 또는 취소됨")
        except Exception as e:
            self._log(f"[ERROR] 엑셀 파일 저장 실패: {e}")
            import traceback
            self._log(f"[ERROR] 상세: {traceback.format_exc()}")
        
        # 메인 런처 현황판 업데이트 (I5-1: 품질 검증 완료) - img 상태만 I5-1(품질완료)로 업데이트 (text 상태는 변경하지 않음)
        try:
            input_path = self.input_file_path.get()
            root_name = get_root_filename(input_path)
            if not root_name or root_name.strip() == "":
                self._log(f"[WARN] 런처 현황판 업데이트 실패: 파일명 추출 실패 (입력: {os.path.basename(input_path) if input_path else 'None'})")
            else:
                db_path = JobManager.find_db_path()
                self._log(f"[INFO] 런처 현황판 업데이트 시도: {root_name} -> I5-1(품질완료) (DB: {db_path})")
                JobManager.update_status(root_name, img_s5_1_msg="I5-1(품질완료)")
                self._log(f"[INFO] 런처 현황판 업데이트 완료: {root_name} -> I5-1(품질완료)")
        except Exception as e:
            import traceback
            self._log(f"[ERROR] 런처 현황판 업데이트 실패: {e}\n{traceback.format_exc()}")
        
        self._log("=== 이미지 복사 및 파일명 변경 완료 ===")
        self._log(f"성공: {success_count}건, 실패: {fail_count}건")
        
        messagebox.showinfo(
            "완료",
            f"이미지 복사 및 파일명 변경이 완료되었습니다.\n\n"
            f"성공: {success_count}건\n"
            f"실패: {fail_count}건\n\n"
            f"출력 폴더: {output_dir}"
        )
        
        # 출력 폴더 열기
        try:
            if os.name == 'nt':
                os.startfile(output_dir)
            else:
                subprocess.run(['open' if sys.platform == 'darwin' else 'xdg-open', output_dir])
        except:
            pass
        
        # 진행 상황 파일 삭제 (작업 완료)
        if hasattr(self, 'progress_file') and self.progress_file and os.path.exists(self.progress_file):
            try:
                os.remove(self.progress_file)
                self._log("진행 상황 파일 삭제됨 (작업 완료)")
            except:
                pass
        
        # 중간 저장 파일 삭제 (최종 저장 성공 시)
        try:
            excel_path = self.input_file_path.get()
            excel_dir = os.path.dirname(excel_path)
            excel_base = os.path.basename(excel_path)
            excel_name, excel_ext = os.path.splitext(excel_base)
            temp_save_path = os.path.join(excel_dir, f"{excel_name}_stage5_중간저장{excel_ext}")
            if os.path.exists(temp_save_path):
                os.remove(temp_save_path)
                self._log("중간 저장 파일 삭제됨 (최종 저장 완료)")
        except:
            pass
        
        self.is_running = False
        self._stop_time_timer()  # 타이머 중지
        self.btn_start.config(state='normal')
        self.btn_save.config(state='disabled')  # 중간 저장 버튼 비활성화
        self.btn_finish.config(state='disabled')
        
        # 진행률 바 초기화
        self.progress_bar['value'] = 0
        self.progress_percent_label.config(text="0%")
        
        # 시간 정보 초기화
        self.start_time = None
        self.item_start_times = {}
        self.total_review_time = 0.0
        self.start_time_label.config(text="-")
        self.elapsed_time_label.config(text="00:00:00")
        self.avg_time_label.config(text="-")

if __name__ == "__main__":
    import subprocess
    import sys
    app = Stage5ReviewGUI()
    app.mainloop()
