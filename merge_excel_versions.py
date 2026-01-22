"""
merge_excel_versions.py

엑셀 버전 병합 도구
- 같은 이름(공통 분모)을 가진 여러 버전의 엑셀 파일들을 찾아서
- 상품코드 기준으로 행 매핑하여 병합
- 기존 컬럼은 업데이트하지 않고, 없는 컬럼만 추가
- 최신 버전으로 저장
"""

import os
import re
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple, Optional
import pandas as pd
import tkinter as tk
from tkinter import ttk, filedialog, messagebox
from tkinter.scrolledtext import ScrolledText

# ========================================================
# ToolTip 클래스
# ========================================================
class ToolTip:
    def __init__(self, widget, text, wraplength=400):
        self.widget = widget
        self.text = text
        self.wraplength = wraplength
        self.tipwindow = None
        # 마우스 이벤트를 가로채지 않도록 지연 시간 추가
        self.widget.bind("<Enter>", self._on_enter)
        self.widget.bind("<Leave>", self.hide_tip)
        self.widget.bind("<Button-1>", self.hide_tip)  # 클릭 시 툴팁 숨김
        self._after_id = None

    def _on_enter(self, event=None):
        """마우스 진입 시 지연 후 툴팁 표시"""
        # 기존 타이머 취소
        if self._after_id:
            self.widget.after_cancel(self._after_id)
        # 500ms 후 툴팁 표시 (클릭 이벤트를 방해하지 않도록)
        self._after_id = self.widget.after(500, self.show_tip)

    def show_tip(self, event=None):
        if self.tipwindow or not self.text: return
        x = self.widget.winfo_rootx() + 20
        y = self.widget.winfo_rooty() + 20
        self.tipwindow = tw = tk.Toplevel(self.widget)
        tw.wm_overrideredirect(True)
        # 툴팁이 클릭 이벤트를 받지 않도록 설정
        tw.attributes("-topmost", True)
        tw.wm_geometry(f"+{x}+{y}")
        label = tk.Label(tw, text=self.text, justify='left', background="#ffffe0", relief='solid', borderwidth=1, font=("맑은 고딕", 9), wraplength=self.wraplength)
        label.pack(ipadx=4, ipady=2)
        # 툴팁 클릭 시 즉시 숨김
        tw.bind("<Button-1>", lambda e: self.hide_tip())

    def hide_tip(self, event=None):
        if self.tipwindow:
            self.tipwindow.destroy()
            self.tipwindow = None
        if self._after_id:
            self.widget.after_cancel(self._after_id)
            self._after_id = None

# ========================================================
# 메인 런처 연동용 JobManager & 파일명 유틸
# ========================================================
def get_root_filename(filename: str) -> str:
    """
    파일명에서 버전 정보(_T*_I* 또는 _t*_i*) 및 기타 꼬리표를 떼고 원본명(Key)만 추출
    예: 상품_T0_I0.xlsx -> 상품.xlsx
    예: 상품_T2_I1.xlsx -> 상품.xlsx
    예: 상품_T4(완)_I0.xlsx -> 상품.xlsx
    예: 상품_T*_I5(업완).xlsx -> 상품.xlsx
    """
    name = os.path.basename(filename)
    base, ext = os.path.splitext(name)
    
    # 1. 버전 패턴 (_T숫자_I숫자 또는 _t숫자_i숫자) 반복 제거 (대소문자 구분 없음)
    # T4(완)_I* 패턴도 처리
    while True:
        new_base = re.sub(r"_[Tt]\d+\([^)]*\)_[Ii]\d+", "", base, flags=re.IGNORECASE)  # T4(완)_I* 패턴
        new_base = re.sub(r"_[Tt]\d+_[Ii]\d+", "", new_base, flags=re.IGNORECASE)  # 일반 T*_I* 패턴
        if new_base == base:
            break
        base = new_base
    
    # 2. 괄호 안의 텍스트 제거 (예: (업완), (완료) 등)
    base = re.sub(r"\([^)]*\)", "", base)
    
    # 3. 기타 구형 꼬리표 제거 (호환성 유지)
    suffixes = ["_stage1_mapping", "_stage1_img_mapping", "_stage2_analysis", "_stage3_done", "_stage4_2_done", "_with_images", "_bg"]
    for s in suffixes:
        base = base.replace(s, "")
    
    # 4. 끝에 남은 언더스코어 제거
    base = base.rstrip("_")
        
    return base + ext

def extract_version_info(filename: str) -> Tuple[Optional[int], Optional[int]]:
    """
    파일명에서 T 버전과 I 버전을 추출
    예: 상품_T1_I0.xlsx -> (1, 0)
    예: 상품_T4(완)_I5(업완).xlsx -> (4, 5)
    예: 상품_T2_I5(업완).xlsx -> (2, 5)
    """
    name = os.path.basename(filename)
    base, ext = os.path.splitext(name)
    
    # T 버전 추출 (괄호가 있어도 숫자 추출)
    # 패턴: _T숫자 또는 _T숫자(텍스트)
    t_match = re.search(r"_T(\d+)(?:\([^)]*\))?", base, re.IGNORECASE)
    t_version = int(t_match.group(1)) if t_match else None
    
    # I 버전 추출 (괄호가 있어도 숫자 추출)
    # 패턴: _I숫자 또는 _I숫자(텍스트)
    i_match = re.search(r"_I(\d+)(?:\([^)]*\))?", base, re.IGNORECASE)
    i_version = int(i_match.group(1)) if i_match else None
    
    return t_version, i_version

def extract_version_suffixes(filename: str) -> Tuple[Optional[str], Optional[str]]:
    """
    파일명에서 T 버전과 I 버전의 괄호 정보(접미사)를 추출
    예: 상품_T4(완)_I3.xlsx -> ("(완)", None)
    예: 상품_T4_I4.xlsx -> (None, None)
    예: 상품_T2_I5(업완).xlsx -> (None, "(업완)")
    
    반환: (t_suffix, i_suffix) - 괄호 포함 문자열 또는 None
    """
    name = os.path.basename(filename)
    base, ext = os.path.splitext(name)
    
    # T 버전의 괄호 정보 추출
    t_match = re.search(r"_T\d+(\([^)]*\))?", base, re.IGNORECASE)
    t_suffix = t_match.group(1) if t_match and t_match.group(1) else None
    
    # I 버전의 괄호 정보 추출
    i_match = re.search(r"_I\d+(\([^)]*\))?", base, re.IGNORECASE)
    i_suffix = i_match.group(1) if i_match and i_match.group(1) else None
    
    return t_suffix, i_suffix

def find_matching_pairs(directory: str) -> List[Tuple[str, str, str]]:
    """
    디렉토리에서 같은 공통 분모를 가진 엑셀 파일들을 2개씩 쌍으로 묶기
    최적 쌍 찾기: 최대 버전 조합을 만드는 쌍을 우선 선택
    (예: T0_I1 + T1_I0 → T1_I1, T0_I0 + T0_I1보다 우선)
    
    반환: [(root_name, file1_path, file2_path), ...]
    """
    # 1. 같은 root_name을 가진 파일들을 그룹화
    groups: Dict[str, List[str]] = {}
    
    for file_path in Path(directory).glob("*.xlsx"):
        root_name = get_root_filename(str(file_path))
        if root_name not in groups:
            groups[root_name] = []
        groups[root_name].append(str(file_path))
    
    pairs: List[Tuple[str, str, str]] = []
    
    # 2. 각 그룹에서 최적 쌍 만들기
    for root_name, file_list in groups.items():
        if len(file_list) < 2:
            continue
        
        # 3. 각 파일의 버전 정보 추출
        file_versions = []
        for file_path in file_list:
            t_ver, i_ver = extract_version_info(file_path)
            file_versions.append((file_path, t_ver, i_ver))
        
        # 4. 가능한 모든 쌍 생성 및 점수 계산 (최대 버전 조합 우선)
        possible_pairs = []
        for i in range(len(file_versions)):
            file1_path, t1, i1 = file_versions[i]
            for j in range(i + 1, len(file_versions)):
                file2_path, t2, i2 = file_versions[j]
                
                # T 버전이 다르거나 I 버전이 다른 경우만 고려
                if (t1 is not None and t2 is not None and t1 != t2) or \
                   (i1 is not None and i2 is not None and i1 != i2):
                    # 안전장치: root_name 재확인
                    pair_root1 = get_root_filename(file1_path)
                    pair_root2 = get_root_filename(file2_path)
                    
                    if pair_root1 == pair_root2 == root_name:
                        # 점수 계산: 최대 T 버전 + 최대 I 버전 조합
                        max_t = max(t1 or 0, t2 or 0)
                        max_i = max(i1 or 0, i2 or 0)
                        score = max_t * 1000 + max_i  # T 버전이 더 중요하도록 가중치
                        possible_pairs.append((score, i, j, file1_path, file2_path))
        
        # 5. 점수 순으로 정렬 (높은 점수 우선)
        possible_pairs.sort(reverse=True, key=lambda x: x[0])
        
        # 6. 최적 쌍 선택 (겹치지 않도록)
        used_indices = set()
        for score, i, j, file1_path, file2_path in possible_pairs:
            if i not in used_indices and j not in used_indices:
                pairs.append((root_name, file1_path, file2_path))
                used_indices.add(i)
                used_indices.add(j)
        
        # 7. 남은 파일이 있으면 첫 번째와 두 번째를 쌍으로 묶기 (버전이 같아도)
        # 단, root_name이 일치하는 경우에만
        remaining = [fv for idx, fv in enumerate(file_versions) if idx not in used_indices]
        while len(remaining) >= 2:
            file1_path, file2_path = remaining[0][0], remaining[1][0]
            # 안전장치: root_name 재확인
            rem_root1 = get_root_filename(file1_path)
            rem_root2 = get_root_filename(file2_path)
            
            if rem_root1 == rem_root2 == root_name:
                pairs.append((root_name, file1_path, file2_path))
                remaining = remaining[2:]
            else:
                # root_name이 일치하지 않으면 스킵
                remaining = remaining[1:]  # 첫 번째만 제거하고 계속
    
    return pairs

class JobManager:
    DB_FILE = None

    @classmethod
    def find_db_path(cls):
        if cls.DB_FILE and os.path.exists(cls.DB_FILE): return cls.DB_FILE
        current_dir = os.path.dirname(os.path.abspath(__file__))
        search_dirs = [current_dir, os.path.abspath(os.path.join(current_dir, "..")), os.path.abspath(os.path.join(current_dir, "..", ".."))]
        for d in search_dirs:
            target = os.path.join(d, "job_history.json")
            if os.path.exists(target):
                cls.DB_FILE = target
                return target
        return os.path.join(current_dir, "job_history.json")

    @classmethod
    def update_status(cls, filename, text_msg=None, img_msg=None):
        db_path = cls.find_db_path()
        try:
            if os.path.exists(db_path):
                with open(db_path, 'r', encoding='utf-8') as f: data = json.load(f)
            else: data = {}
        except: data = {}

        now = datetime.now().strftime("%m-%d %H:%M")
        
        if filename not in data:
            data[filename] = {"start_time": datetime.now().strftime("%Y-%m-%d %H:%M"), "text_status": "대기", "image_status": "대기"}

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
        except Exception as e: print(f"[JobManager Error] {e}")

# ========================================================
# 메인 GUI 클래스
# ========================================================
# ========================================================
# UI 색상 팔레트
# ========================================================
COLOR_BG = "#F5F7FA"
COLOR_PRIMARY = "#00BCD4"  # 청록색
COLOR_PRIMARY_HOVER = "#0097A7"
COLOR_SUCCESS = "#4CAF50"
COLOR_WARNING = "#FF9800"
COLOR_INFO = "#2196F3"
COLOR_TEXT = "#333333"
COLOR_TEXT_LIGHT = "#666666"
COLOR_BORDER = "#E0E0E0"
COLOR_BUTTON_AREA = "#FFF9C4"  # 연노란색 (작업 버튼 영역)

class MergeExcelVersionsGUI(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("엑셀 버전 병합 도구 (Excel Version Merger)")
        self.geometry("950x750")
        self.configure(bg=COLOR_BG)
        
        self.directory_path = tk.StringVar()
        self.product_code_col = tk.StringVar(value="상품코드")
        
        self._setup_styles()
        self._init_ui()
    
    def _setup_styles(self):
        """ttk 스타일 설정"""
        style = ttk.Style()
        try:
            style.theme_use('clam')
        except:
            pass
        
        # 프레임 스타일
        style.configure("TFrame", background=COLOR_BG)
        style.configure("TLabelFrame", background=COLOR_BG, foreground=COLOR_TEXT, 
                       font=("맑은 고딕", 10, "bold"))
        style.configure("TLabelFrame.Label", background=COLOR_BG, foreground=COLOR_TEXT,
                       font=("맑은 고딕", 10, "bold"))
        
        # 작업 섹션용 LabelFrame 스타일 (연노란색 배경)
        # 스타일 생성 시 예외 처리 (다른 PC에서 테마가 다를 수 있음)
        self._button_area_style_available = False
        try:
            # TLabelFrame 레이아웃이 있는지 확인 후 복사
            # 일부 환경에서는 layout이 None을 반환할 수 있음
            try:
                base_layout = style.layout("TLabelFrame")
                if base_layout:
                    style.layout("ButtonArea.TLabelFrame", base_layout)
            except (AttributeError, tk.TclError):
                # 레이아웃 복사 실패해도 configure는 시도
                pass
            
            # 스타일 설정 시도
            style.configure("ButtonArea.TLabelFrame", background=COLOR_BUTTON_AREA, 
                           foreground=COLOR_TEXT, font=("맑은 고딕", 10, "bold"))
            style.configure("ButtonArea.TLabelFrame.Label", background=COLOR_BUTTON_AREA, 
                           foreground=COLOR_TEXT, font=("맑은 고딕", 10, "bold"))
            
            # 스타일이 실제로 사용 가능한지 확인
            try:
                # 테스트용 위젯 생성하여 스타일 적용 가능 여부 확인
                test_frame = ttk.LabelFrame(self, style="ButtonArea.TLabelFrame")
                test_frame.destroy()
                self._button_area_style_available = True
            except:
                self._button_area_style_available = False
        except Exception as e:
            # 스타일 생성 실패 시 기본 스타일 사용
            self._button_area_style_available = False
            # 경고 메시지는 한 번만 출력
            if not hasattr(self, '_style_warning_shown'):
                print(f"[경고] ButtonArea.TLabelFrame 스타일 생성 실패: {e}")
                print(f"[경고] 기본 스타일을 사용합니다.")
                self._style_warning_shown = True
        
        # 버튼 스타일
        style.configure("Primary.TButton", font=("맑은 고딕", 9, "bold"))
        style.configure("Action.TButton", font=("맑은 고딕", 9))
    
    def _init_ui(self):
        main_frame = ttk.Frame(self, padding=20)
        main_frame.pack(fill='both', expand=True)
        
        # 헤더
        header_frame = tk.Frame(main_frame, bg=COLOR_BG)
        header_frame.pack(fill='x', pady=(0, 15))
        
        title_label = tk.Label(header_frame, text="🔄 엑셀 버전 병합 도구", 
                              font=("맑은 고딕", 16, "bold"), 
                              bg=COLOR_BG, fg=COLOR_PRIMARY)
        title_label.pack()
        
        subtitle_label = tk.Label(header_frame, text="T*_I* 버전이 다른 엑셀 파일을 상품코드 기준으로 병합합니다", 
                                  font=("맑은 고딕", 10), 
                                  bg=COLOR_BG, fg=COLOR_TEXT_LIGHT)
        subtitle_label.pack(pady=(5, 0))
        
        # 1. 디렉토리 선택
        dir_frame = ttk.LabelFrame(main_frame, text="📁 작업 디렉토리", padding=12)
        dir_frame.pack(fill='x', pady=(0, 10))
        
        dir_inner = tk.Frame(dir_frame, bg=COLOR_BG)
        dir_inner.pack(fill='x')
        
        tk.Label(dir_inner, text="디렉토리:", font=("맑은 고딕", 10), 
                bg=COLOR_BG, fg=COLOR_TEXT).pack(side='left', padx=(0, 8))
        
        dir_entry = ttk.Entry(dir_inner, textvariable=self.directory_path, width=50, font=("맑은 고딕", 9))
        dir_entry.pack(side='left', fill='x', expand=True, padx=(0, 8))
        ToolTip(dir_entry, "병합할 엑셀 파일들이 있는 폴더 경로를 입력하거나\n'📂 선택' 버튼을 클릭하여 폴더를 선택하세요.")
        
        # 디렉토리 선택 버튼 (예외 처리 포함)
        def on_select_click():
            try:
                self._log("[DEBUG] 디렉토리 선택 버튼 클릭됨")
                self._select_directory()
            except Exception as e:
                error_msg = f"버튼 클릭 처리 중 오류: {str(e)}"
                self._log(f"❌ {error_msg}")
                messagebox.showerror("오류", error_msg)
                import traceback
                self._log(f"상세 오류:\n{traceback.format_exc()}")
        
        dir_btn = tk.Button(dir_inner, text="📂 선택", command=on_select_click,
                           bg=COLOR_PRIMARY, fg="white", font=("맑은 고딕", 9, "bold"),
                           relief="flat", cursor="hand2", padx=15, pady=5,
                           activebackground=COLOR_PRIMARY_HOVER, activeforeground="white")
        dir_btn.pack(side='left')
        ToolTip(dir_btn, "병합할 엑셀 파일들이 있는 폴더를 선택합니다.")
        
        # 2. 사용법 및 안내 (좌우 배치)
        guide_frame = ttk.Frame(main_frame)
        guide_frame.pack(fill='x', pady=(0, 10))
        
        # 좌측: 사용법
        usage_frame = ttk.LabelFrame(guide_frame, text="📖 사용법", padding=12)
        usage_frame.pack(side='left', fill='both', expand=True, padx=(0, 8))
        
        usage_inner = tk.Frame(usage_frame, bg=COLOR_BG)
        usage_inner.pack(fill='both', expand=True)
        
        usage_text = (
            "1️⃣ 디렉토리 선택\n"
            "2️⃣ 상품코드 컬럼명 입력\n"
            "3️⃣ 파일 검색 (자동) 또는 수동 선택\n"
            "4️⃣ 쌍 선택 (체크박스)\n"
            "5️⃣ 병합 실행"
        )
        tk.Label(usage_inner, text=usage_text, font=("맑은 고딕", 9), 
                bg=COLOR_BG, fg=COLOR_INFO, justify='left', anchor='w').pack(anchor='w', padx=5, pady=3)
        
        # 우측: 안내
        info_frame = ttk.LabelFrame(guide_frame, text="⚠️ 안내", padding=12)
        info_frame.pack(side='left', fill='both', expand=True, padx=(8, 0))
        
        info_inner = tk.Frame(info_frame, bg=COLOR_BG)
        info_inner.pack(fill='both', expand=True)
        
        info_text = (
            "• 자동 검색: 최적 쌍 우선 선택\n"
            "• 수동 선택: 파일 2개 직접 선택\n"
            "• 같은 공통 분모만 병합 (안전장치)\n"
            "• 최신 버전(T/I 최대값)으로 저장"
        )
        tk.Label(info_inner, text=info_text, font=("맑은 고딕", 9), 
                bg=COLOR_BG, fg=COLOR_WARNING, justify='left', anchor='w').pack(anchor='w', padx=5, pady=3)
        
        # 3. 설정 및 버튼
        control_frame = ttk.Frame(main_frame)
        control_frame.pack(fill='x', pady=(0, 10))
        
        # 설정
        settings_frame = ttk.LabelFrame(control_frame, text="⚙️ 병합 설정", padding=12)
        settings_frame.pack(side='left', fill='x', expand=True, padx=(0, 10))
        
        settings_inner = tk.Frame(settings_frame, bg=COLOR_BG)
        settings_inner.pack(fill='x')
        
        tk.Label(settings_inner, text="상품코드 컬럼명:", font=("맑은 고딕", 10), 
                bg=COLOR_BG, fg=COLOR_TEXT).pack(side='left', padx=(0, 8))
        
        product_code_entry = ttk.Entry(settings_inner, textvariable=self.product_code_col, 
                                      width=20, font=("맑은 고딕", 9))
        product_code_entry.pack(side='left')
        ToolTip(product_code_entry, "병합 기준이 될 상품코드 컬럼명을 입력하세요.\n기본값: '상품코드'\n이 컬럼을 기준으로 두 파일의 행을 매핑하여 병합합니다.")
        
        # 버튼 (연노란색 배경으로 강조)
        # 스타일이 사용 가능한지 확인 후 적용
        if hasattr(self, '_button_area_style_available') and self._button_area_style_available:
            btn_frame = ttk.LabelFrame(control_frame, text="🔧 작업", padding=12, style="ButtonArea.TLabelFrame")
        else:
            # 스타일이 없으면 기본 스타일 사용
            btn_frame = ttk.LabelFrame(control_frame, text="🔧 작업", padding=12)
        
        btn_frame.pack(side='left', padx=(0, 0))
        
        # 연노란색 배경을 위한 내부 프레임 (버튼들이 제대로 보이도록 수정)
        btn_inner = tk.Frame(btn_frame, bg=COLOR_BUTTON_AREA, relief="flat", bd=0)
        btn_inner.pack(fill='x', expand=False)  # fill='both', expand=True 제거
        
        # 버튼 클릭 이벤트 래퍼 함수 (디버깅 및 안전성)
        def safe_scan_files():
            try:
                self._log("[DEBUG] 파일 검색 버튼 클릭됨")
                self._scan_files()
            except Exception as e:
                self._log(f"❌ 파일 검색 오류: {str(e)}")
                messagebox.showerror("오류", f"파일 검색 중 오류가 발생했습니다:\n{str(e)}")
                import traceback
                self._log(f"상세 오류:\n{traceback.format_exc()}")
        
        def safe_merge_files():
            try:
                self._log("[DEBUG] 병합 실행 버튼 클릭됨")
                self._merge_files()
            except Exception as e:
                self._log(f"❌ 병합 실행 오류: {str(e)}")
                messagebox.showerror("오류", f"병합 실행 중 오류가 발생했습니다:\n{str(e)}")
                import traceback
                self._log(f"상세 오류:\n{traceback.format_exc()}")
        
        scan_btn = tk.Button(btn_inner, text="🔍 파일 검색", command=safe_scan_files,
                            bg=COLOR_INFO, fg="white", font=("맑은 고딕", 9, "bold"),
                            relief="raised", cursor="hand2", padx=12, pady=6,
                            activebackground="#1976D2", activeforeground="white",
                            bd=1, highlightthickness=0)
        scan_btn.pack(side='left', padx=3, pady=2)
        ToolTip(scan_btn, "디렉토리에서 병합 가능한 파일 쌍을 자동으로 찾습니다.\n같은 공통 분모를 가진 파일 중 T 버전이 다르거나 I 버전이 다른 파일을 2개씩 쌍으로 묶습니다.\n최적 쌍(최대 버전 조합)을 우선 선택합니다.")
        
        manual_btn = tk.Button(btn_inner, text="📁 수동 선택", command=self._manual_select_pair,
                               bg=COLOR_WARNING, fg="white", font=("맑은 고딕", 9, "bold"),
                               relief="raised", cursor="hand2", padx=12, pady=6,
                               activebackground="#F57C00", activeforeground="white",
                               bd=1, highlightthickness=0)
        manual_btn.pack(side='left', padx=3, pady=2)
        ToolTip(manual_btn, "파일 2개를 직접 선택하여 쌍을 만듭니다.\n같은 공통 분모를 가진 파일만 선택 가능합니다.")
        
        self.merge_btn = tk.Button(btn_inner, text="🔄 병합 실행", command=safe_merge_files, 
                                   state='disabled', bg=COLOR_PRIMARY, fg="white", 
                                   font=("맑은 고딕", 9, "bold"), relief="raised", 
                                   cursor="hand2", padx=12, pady=6,
                                   activebackground=COLOR_PRIMARY_HOVER, activeforeground="white",
                                   bd=1, highlightthickness=0)
        self.merge_btn.pack(side='left', padx=3, pady=2)
        ToolTip(self.merge_btn, "선택된 쌍들을 병합합니다.\n상품코드 기준으로 행을 매핑하고, 기존 컬럼은 보존하며 없는 컬럼만 추가합니다.\n최신 버전(T, I 중 최대값)으로 저장됩니다.")
        
        select_all_btn = tk.Button(btn_inner, text="✅ 전체 선택", command=self._select_all,
                                   bg=COLOR_SUCCESS, fg="white", font=("맑은 고딕", 9),
                                   relief="raised", cursor="hand2", padx=10, pady=6,
                                   activebackground="#45A049", activeforeground="white",
                                   bd=1, highlightthickness=0)
        select_all_btn.pack(side='left', padx=3, pady=2)
        ToolTip(select_all_btn, "검색된 모든 쌍을 선택합니다.")
        
        deselect_all_btn = tk.Button(btn_inner, text="❌ 전체 해제", command=self._deselect_all,
                                     bg="#E0E0E0", fg=COLOR_TEXT, font=("맑은 고딕", 9),
                                     relief="raised", cursor="hand2", padx=10, pady=6,
                                     activebackground="#BDBDBD", activeforeground="white",
                                     bd=1, highlightthickness=0)
        deselect_all_btn.pack(side='left', padx=3, pady=2)
        ToolTip(deselect_all_btn, "선택된 모든 쌍을 해제합니다.")
        
        # 6. 결과 표시
        result_frame = ttk.LabelFrame(main_frame, text="📋 검색 결과 (2개씩 쌍으로 표시)", padding=12)
        result_frame.pack(fill='both', expand=True)
        
        # 결과 설명
        result_info_frame = tk.Frame(result_frame, bg=COLOR_BG)
        result_info_frame.pack(fill='x', pady=(0, 8))
        
        result_info = tk.Label(result_info_frame, 
                              text="💡 '선택' 컬럼 클릭으로 쌍 선택 | 같은 공통 분모만 병합 가능",
                              font=("맑은 고딕", 9), bg=COLOR_BG, fg=COLOR_TEXT_LIGHT)
        result_info.pack(anchor='w', padx=5)
        
        # 트리뷰 컨테이너
        tree_container = tk.Frame(result_frame, bg=COLOR_BG)
        tree_container.pack(fill='both', expand=True)
        
        # 트리뷰 (체크박스 컬럼 추가)
        columns = ("select", "root", "file1", "file2", "versions")
        self.tree = ttk.Treeview(tree_container, columns=columns, show='headings', height=18)
        
        # 헤더 스타일 설정
        style = ttk.Style()
        style.configure("Treeview.Heading", font=("맑은 고딕", 9, "bold"), background="#E3F2FD")
        style.configure("Treeview", font=("맑은 고딕", 9), rowheight=25)
        style.map("Treeview", background=[("selected", COLOR_PRIMARY)])
        
        self.tree.heading("select", text="선택")
        self.tree.column("select", width=70, anchor='center')
        self.tree.heading("root", text="공통 분모")
        self.tree.column("root", width=160)
        self.tree.heading("file1", text="파일 1")
        self.tree.column("file1", width=260)
        self.tree.heading("file2", text="파일 2")
        self.tree.column("file2", width=260)
        self.tree.heading("versions", text="버전 정보")
        self.tree.column("versions", width=140)
        
        # 클릭 이벤트 바인딩 (선택 토글)
        self.tree.bind("<Button-1>", self._on_tree_click)
        
        scrollbar = ttk.Scrollbar(tree_container, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscrollcommand=scrollbar.set)
        
        self.tree.pack(side='left', fill='both', expand=True)
        scrollbar.pack(side='right', fill='y')
        
        # 7. 로그
        log_frame = ttk.LabelFrame(main_frame, text="📝 처리 로그", padding=12)
        log_frame.pack(fill='x', pady=(10, 0))
        
        log_inner = tk.Frame(log_frame, bg=COLOR_BG)
        log_inner.pack(fill='both', expand=True)
        
        self.log_text = ScrolledText(log_inner, height=5, state='disabled', 
                                     font=("Consolas", 9), bg="#FAFAFA", 
                                     fg=COLOR_TEXT, relief="flat", bd=1)
        self.log_text.pack(fill='both', expand=True)
        
        # 쌍 리스트: [(root_name, file1_path, file2_path), ...]
        self.file_pairs: List[Tuple[str, str, str]] = []
    
    def _select_directory(self):
        """디렉토리 선택 다이얼로그"""
        try:
            self._log("디렉토리 선택 다이얼로그 열기 시도...")
            self.update()  # UI 업데이트 강제 (self가 tk.Tk이므로 self.update() 사용)
            
            # 초기 디렉토리 설정 (현재 선택된 경로가 있으면 사용)
            initial_dir = self.directory_path.get().strip()
            if initial_dir and os.path.exists(initial_dir):
                initial_dir = os.path.normpath(initial_dir)
            else:
                # 기본 디렉토리로 현재 작업 디렉토리 또는 사용자 홈 디렉토리 사용
                try:
                    initial_dir = os.getcwd()
                except:
                    initial_dir = os.path.expanduser("~")
            
            self._log(f"초기 디렉토리: {initial_dir}")
            
            # filedialog.askdirectory 호출
            path = filedialog.askdirectory(
                title="엑셀 파일이 있는 디렉토리 선택",
                initialdir=initial_dir if initial_dir else None
            )
            
            self._log(f"filedialog 반환값: {path} (타입: {type(path)})")
            
            if path:
                # 경로 정규화 및 검증
                path = os.path.normpath(str(path))
                if os.path.isdir(path):
                    self.directory_path.set(path)
                    self._log(f"✅ 디렉토리 선택 완료: {path}")
                else:
                    self._log(f"❌ 선택한 경로가 유효한 디렉토리가 아닙니다: {path}")
                    messagebox.showerror("오류", f"선택한 경로가 유효한 디렉토리가 아닙니다:\n{path}")
            else:
                self._log("디렉토리 선택이 취소되었습니다.")
        except tk.TclError as e:
            error_msg = f"Tkinter 오류: {str(e)}"
            self._log(f"❌ {error_msg}")
            messagebox.showerror("오류", f"디렉토리 선택 다이얼로그를 열 수 없습니다.\n\n{error_msg}\n\n다른 방법: 경로를 직접 입력하세요.")
        except Exception as e:
            error_msg = f"디렉토리 선택 중 오류 발생: {str(e)}"
            self._log(f"❌ {error_msg}")
            messagebox.showerror("오류", f"{error_msg}\n\n다른 방법: 경로를 직접 입력하세요.")
            import traceback
            self._log(f"상세 오류:\n{traceback.format_exc()}")
    
    def _log(self, msg: str):
        self.log_text.config(state='normal')
        t = datetime.now().strftime("%H:%M:%S")
        self.log_text.insert("end", f"[{t}] {msg}\n")
        self.log_text.see("end")
        self.log_text.config(state='disabled')
    
    def _manual_select_pair(self):
        """수동으로 파일 2개를 선택하여 쌍 만들기"""
        try:
            self._log("수동 파일 선택 모드")
            self.update()
            
            # 첫 번째 파일 선택
            initial_dir = self.directory_path.get().strip()
            if not initial_dir or not os.path.exists(initial_dir):
                initial_dir = None
            
            file1_path = filedialog.askopenfilename(
                title="첫 번째 엑셀 파일 선택",
                initialdir=initial_dir,
                filetypes=[("Excel files", "*.xlsx"), ("All files", "*.*")]
            )
            
            if not file1_path:
                self._log("파일 선택이 취소되었습니다.")
                return
            
            file1_path = os.path.normpath(file1_path)
            root1 = get_root_filename(file1_path)
            self._log(f"첫 번째 파일: {os.path.basename(file1_path)} (공통 분모: {root1})")
            
            # 두 번째 파일 선택
            file2_path = filedialog.askopenfilename(
                title="두 번째 엑셀 파일 선택 (같은 공통 분모를 가진 파일)",
                initialdir=os.path.dirname(file1_path),
                filetypes=[("Excel files", "*.xlsx"), ("All files", "*.*")]
            )
            
            if not file2_path:
                self._log("파일 선택이 취소되었습니다.")
                return
            
            file2_path = os.path.normpath(file2_path)
            root2 = get_root_filename(file2_path)
            self._log(f"두 번째 파일: {os.path.basename(file2_path)} (공통 분모: {root2})")
            
            # 안전장치: root_name 확인
            if root1 != root2:
                error_msg = f"공통 분모가 일치하지 않습니다!\n\n첫 번째: {root1}\n두 번째: {root2}\n\n같은 공통 분모를 가진 파일만 병합할 수 있습니다."
                self._log(f"❌ {error_msg}")
                messagebox.showerror("오류", error_msg)
                return
            
            # 버전 정보 확인
            t1, i1 = extract_version_info(file1_path)
            t2, i2 = extract_version_info(file2_path)
            
            # T 버전이나 I 버전이 다른지 확인
            if (t1 == t2 and i1 == i2):
                warning_msg = f"두 파일의 버전이 동일합니다.\n\n첫 번째: T{t1}_I{i1}\n두 번째: T{t2}_I{i2}\n\n그래도 병합하시겠습니까?"
                if not messagebox.askyesno("경고", warning_msg):
                    self._log("사용자가 병합을 취소했습니다.")
                    return
            
            # 중복 체크: 같은 파일 쌍이 이미 있는지 확인
            new_pair = (root1, file1_path, file2_path)
            reverse_pair = (root1, file2_path, file1_path)
            
            # 기존 쌍과 중복 확인
            is_duplicate = False
            for existing_root, existing_f1, existing_f2 in self.file_pairs:
                if (existing_root == root1 and 
                    ((existing_f1 == file1_path and existing_f2 == file2_path) or
                     (existing_f1 == file2_path and existing_f2 == file1_path))):
                    is_duplicate = True
                    break
            
            if is_duplicate:
                self._log(f"⚠️ 이미 추가된 쌍입니다: {os.path.basename(file1_path)} + {os.path.basename(file2_path)}")
                messagebox.showinfo("알림", "이미 추가된 쌍입니다.")
                return
            
            # 트리뷰에 추가
            file1_name = os.path.basename(file1_path)
            file2_name = os.path.basename(file2_path)
            
            version_info = f"T{t1 or '?'}_I{i1 or '?'} ↔ T{t2 or '?'}_I{i2 or '?'}"
            
            # 체크박스는 "☐" (미선택) 또는 "☑" (선택) - _scan_files와 동일한 형식 사용
            item = self.tree.insert("", "end", values=(
                "☐",  # 체크박스 (False 대신 "☐" 사용)
                root1,
                file1_name,
                file2_name,
                version_info
            ))
            
            # 체크박스 클릭 이벤트 바인딩 (이미 바인딩되어 있을 수 있지만 안전하게)
            if not hasattr(self, '_tree_click_bound'):
                self.tree.bind("<Button-1>", self._on_tree_click)
                self._tree_click_bound = True
            
            # file_pairs에 추가
            self.file_pairs.append(new_pair)
            
            self._log(f"✅ 쌍 추가 완료: {file1_name} + {file2_name}")
            self.merge_btn.config(state='normal')
            
        except Exception as e:
            error_msg = f"수동 파일 선택 중 오류: {str(e)}"
            self._log(f"❌ {error_msg}")
            messagebox.showerror("오류", error_msg)
            import traceback
            self._log(f"상세 오류:\n{traceback.format_exc()}")
    
    def _scan_files(self):
        directory = self.directory_path.get().strip()
        if not directory or not os.path.exists(directory):
            messagebox.showwarning("경고", "디렉토리를 선택해주세요.")
            return
        
        # 트리뷰 초기화
        for item in self.tree.get_children():
            self.tree.delete(item)
        
        self._log("파일 검색 중... (최적 쌍 찾기: 최대 버전 조합 우선)...")
        self.file_pairs = find_matching_pairs(directory)
        
        if not self.file_pairs:
            self._log("병합 가능한 파일 쌍을 찾을 수 없습니다.")
            messagebox.showinfo("알림", "병합 가능한 파일 쌍을 찾을 수 없습니다.\n\n같은 이름을 가진 2개의 엑셀 파일이 필요합니다.\n(T 버전이 다르거나 I 버전이 다른 파일 쌍)")
            self.merge_btn.config(state='disabled')
            return
        
        # 트리뷰에 표시 (체크박스 포함)
        valid_pairs = []
        for root_name, file1_path, file2_path in self.file_pairs:
            # 안전장치: root_name 재확인
            file1_root = get_root_filename(file1_path)
            file2_root = get_root_filename(file2_path)
            
            if file1_root != file2_root:
                self._log(f"  ⚠️ 안전장치: 쌍 제외됨 - root_name 불일치")
                self._log(f"     {os.path.basename(file1_path)} -> {file1_root}")
                self._log(f"     {os.path.basename(file2_path)} -> {file2_root}")
                continue
            
            if file1_root != root_name:
                self._log(f"  ⚠️ 경고: root_name 불일치, 실제 root_name({file1_root}) 사용")
                root_name = file1_root
            
            file1_name = os.path.basename(file1_path)
            file2_name = os.path.basename(file2_path)
            
            # 버전 정보 추출
            t1, i1 = extract_version_info(file1_path)
            t2, i2 = extract_version_info(file2_path)
            version_info = f"T{t1 or '?'}_I{i1 or '?'} ↔ T{t2 or '?'}_I{i2 or '?'}"
            
            # 체크박스는 "☐" (미선택) 또는 "☑" (선택)
            self.tree.insert("", "end", values=("☐", root_name, file1_name, file2_name, version_info))
            valid_pairs.append((root_name, file1_path, file2_path))
        
        # 유효한 쌍만 저장
        self.file_pairs = valid_pairs
        
        if not self.file_pairs:
            self._log("  ❌ 유효한 쌍이 없습니다.")
            self.merge_btn.config(state='disabled')
            return
        
        self._log(f"총 {len(self.file_pairs)}개 유효한 쌍 발견 (안전장치 통과)")
        self.merge_btn.config(state='normal')
    
    def _on_tree_click(self, event):
        """트리뷰 클릭 시 선택 상태 토글"""
        region = self.tree.identify_region(event.x, event.y)
        if region == "cell":
            column = self.tree.identify_column(event.x)
            if column == "#1":  # 선택 컬럼 클릭
                item = self.tree.identify_row(event.y)
                if item:
                    current_values = list(self.tree.item(item, "values"))
                    # 체크박스 값이 "☐", False, 또는 기타 값일 수 있음
                    current_check = str(current_values[0]) if current_values[0] else "☐"
                    if current_check == "☐" or current_check == "False" or current_check == "":
                        current_values[0] = "☑"
                    else:
                        current_values[0] = "☐"
                    self.tree.item(item, values=current_values)
    
    def _select_all(self):
        """모든 쌍 선택"""
        for item in self.tree.get_children():
            values = list(self.tree.item(item, "values"))
            values[0] = "☑"
            self.tree.item(item, values=values)
    
    def _deselect_all(self):
        """모든 쌍 선택 해제"""
        for item in self.tree.get_children():
            values = list(self.tree.item(item, "values"))
            values[0] = "☐"
            self.tree.item(item, values=values)
    
    def _merge_files(self):
        if not self.file_pairs:
            messagebox.showwarning("경고", "먼저 파일을 검색해주세요.")
            return
        
        # 선택된 쌍 찾기
        selected_pairs = []
        tree_items = list(self.tree.get_children())
        
        for idx, item in enumerate(tree_items):
            values = list(self.tree.item(item, "values"))
            # 체크박스 값 확인 (다양한 형식 지원)
            check_value = str(values[0]) if values[0] else ""
            is_selected = (check_value == "☑" or check_value == "True" or 
                          (check_value != "☐" and check_value != "False" and check_value != ""))
            
            if is_selected:
                # 트리뷰의 인덱스로 file_pairs에서 찾기
                if idx < len(self.file_pairs):
                    selected_pairs.append(self.file_pairs[idx])
                    self._log(f"선택된 쌍 {idx+1}: {os.path.basename(self.file_pairs[idx][1])} + {os.path.basename(self.file_pairs[idx][2])}")
                else:
                    self._log(f"⚠️ 인덱스 오류: idx={idx}, file_pairs 길이={len(self.file_pairs)}")
        
        if not selected_pairs:
            messagebox.showwarning("경고", "병합할 쌍을 선택해주세요.\n\n체크박스(☐)를 클릭하여 ☑로 변경한 후 다시 시도해주세요.")
            return
        
        product_code_col = self.product_code_col.get().strip()
        if not product_code_col:
            messagebox.showwarning("경고", "상품코드 컬럼명을 입력해주세요.")
            return
        
        directory = self.directory_path.get().strip()
        if not directory:
            messagebox.showwarning("경고", "디렉토리를 선택해주세요.")
            return
        
        # 확인 메시지
        result = messagebox.askyesno(
            "병합 확인",
            f"선택된 {len(selected_pairs)}개 쌍의 파일을 병합하시겠습니까?\n\n"
            f"• 상품코드 컬럼: {product_code_col}\n"
            f"• 기존 컬럼은 업데이트하지 않고, 없는 컬럼만 추가합니다.\n"
            f"• 최신 버전으로 저장됩니다."
        )
        
        if not result:
            return
        
        self.merge_btn.config(state='disabled')
        self._log("=" * 60)
        self._log("병합 작업 시작")
        
        success_count = 0
        fail_count = 0
        
        for root_name, file1_path, file2_path in selected_pairs:
            try:
                self._log(f"\n[쌍] {root_name}")
                self._log(f"  파일 1: {os.path.basename(file1_path)}")
                self._log(f"  파일 2: {os.path.basename(file2_path)}")
                
                # 병합 실행 (2개 파일만)
                merged_path = self._merge_pair(root_name, file1_path, file2_path, product_code_col, directory)
                
                if merged_path:
                    self._log(f"  ✅ 병합 완료: {os.path.basename(merged_path)}")
                    success_count += 1
                else:
                    self._log(f"  ❌ 병합 실패")
                    fail_count += 1
                    
            except Exception as e:
                self._log(f"  ❌ 오류: {e}")
                fail_count += 1
        
        self._log("=" * 60)
        self._log(f"병합 작업 완료: 성공 {success_count}개, 실패 {fail_count}개")
        
        messagebox.showinfo(
            "완료",
            f"병합 작업이 완료되었습니다.\n\n"
            f"성공: {success_count}개\n"
            f"실패: {fail_count}개"
        )
        
        self.merge_btn.config(state='normal')
    
    def _merge_pair(self, root_name: str, file1_path: str, file2_path: str, product_code_col: str, directory: str) -> Optional[str]:
        """
        2개 파일을 쌍으로 병합
        안전장치: 같은 root_name을 가진 파일만 병합 가능
        """
        # 0. 안전장치: 두 파일의 root_name이 일치하는지 확인
        file1_root = get_root_filename(file1_path)
        file2_root = get_root_filename(file2_path)
        
        if file1_root != file2_root:
            self._log(f"  ❌ 안전장치: 두 파일의 공통 분모가 일치하지 않습니다.")
            self._log(f"     파일 1: {os.path.basename(file1_path)} -> {file1_root}")
            self._log(f"     파일 2: {os.path.basename(file2_path)} -> {file2_root}")
            self._log(f"     병합을 중단합니다.")
            return None
        
        if file1_root != root_name:
            self._log(f"  ⚠️ 경고: 전달된 root_name({root_name})과 실제 root_name({file1_root})이 일치하지 않습니다.")
            self._log(f"     실제 root_name({file1_root})을 사용합니다.")
            root_name = file1_root
        
        self._log(f"  ✅ 안전장치 통과: 공통 분모 '{root_name}' 확인됨")
        
        # 1. 두 파일 로드
        dataframes: List[Tuple[pd.DataFrame, str, int, int, Optional[str], Optional[str]]] = []  # (df, path, t_version, i_version, t_suffix, i_suffix)
        
        for file_path in [file1_path, file2_path]:
            try:
                df = pd.read_excel(file_path, dtype=str, engine='openpyxl')
                
                # 상품코드 컬럼 확인
                if product_code_col not in df.columns:
                    self._log(f"  ⚠️ {os.path.basename(file_path)}: '{product_code_col}' 컬럼이 없습니다. 스킵합니다.")
                    continue
                
                t_version, i_version = extract_version_info(file_path)
                t_suffix, i_suffix = extract_version_suffixes(file_path)
                self._log(f"  파일: {os.path.basename(file_path)} -> T{t_version or 0}{t_suffix or ''}, I{i_version or 0}{i_suffix or ''}")
                dataframes.append((df, file_path, t_version or 0, i_version or 0, t_suffix, i_suffix))
                
            except Exception as e:
                self._log(f"  ⚠️ {os.path.basename(file_path)} 로드 실패: {e}")
                continue
        
        if len(dataframes) < 2:
            self._log(f"  ❌ 2개 파일을 모두 로드하지 못했습니다.")
            return None
        
        # 2. T와 I를 독립적으로 평가하여 최고 우선순위 결정
        # T 비교: 숫자 높은 게 우선, 같으면 괄호 있는 게 우선
        # I 비교: 숫자 높은 게 우선, 같으면 괄호 있는 게 우선
        
        # 최대 T 버전과 최대 I 버전 찾기
        max_t = max(dataframes[0][2] if dataframes[0][2] is not None else 0,
                   dataframes[1][2] if dataframes[1][2] is not None else 0)
        max_i = max(dataframes[0][3] if dataframes[0][3] is not None else 0,
                   dataframes[1][3] if dataframes[1][3] is not None else 0)
        
        # T 최고 우선순위 파일 찾기 (숫자 높은 게 우선, 같으면 괄호 있는 게 우선)
        best_t_file = None
        for df, path, t_ver, i_ver, t_suffix, i_suffix in dataframes:
            if t_ver == max_t:
                if best_t_file is None:
                    best_t_file = (df, path, t_ver, i_ver, t_suffix, i_suffix)
                elif t_suffix and not best_t_file[4]:  # 현재 파일에 괄호가 있고 베스트 파일에는 없으면
                    best_t_file = (df, path, t_ver, i_ver, t_suffix, i_suffix)
        
        # I 최고 우선순위 파일 찾기 (숫자 높은 게 우선, 같으면 괄호 있는 게 우선)
        best_i_file = None
        for df, path, t_ver, i_ver, t_suffix, i_suffix in dataframes:
            if i_ver == max_i:
                if best_i_file is None:
                    best_i_file = (df, path, t_ver, i_ver, t_suffix, i_suffix)
                elif i_suffix and not best_i_file[5]:  # 현재 파일에 괄호가 있고 베스트 파일에는 없으면
                    best_i_file = (df, path, t_ver, i_ver, t_suffix, i_suffix)
        
        # T와 I의 괄호 정보 선택
        final_t_suffix = best_t_file[4] if best_t_file else None
        final_i_suffix = best_i_file[5] if best_i_file else None
        
        # 3. 베이스 파일 선택: T와 I 우선순위를 종합하여 결정
        # T 우선순위 점수와 I 우선순위 점수를 합산
        def calculate_priority_score(df_tuple):
            df, path, t_ver, i_ver, t_suffix, i_suffix = df_tuple
            # T 점수: 버전 * 1000 + 괄호 여부 * 100
            t_score = (t_ver or 0) * 1000 + (100 if t_suffix else 0)
            # I 점수: 버전 * 10 + 괄호 여부 * 1
            i_score = (i_ver or 0) * 10 + (1 if i_suffix else 0)
            return t_score + i_score
        
        # 우선순위 점수가 높은 파일을 베이스로 선택
        dataframes.sort(key=calculate_priority_score, reverse=True)
        
        # 4. 첫 번째(기준) DataFrame을 기준으로 시작
        base_df, base_path, base_t, base_i, base_t_suffix, base_i_suffix = dataframes[0]
        merged_df = base_df.copy()
        self._log(f"  기준 파일: {os.path.basename(base_path)} (T{base_t or 0}{base_t_suffix or ''}, I{base_i or 0}{base_i_suffix or ''})")
        
        # 4. 두 번째 파일 병합
        df, path, t_ver, i_ver, t_suffix, i_suffix = dataframes[1]
        self._log(f"  병합 중: {os.path.basename(path)} (T{t_ver or 0}{t_suffix or ''}, I{i_ver or 0}{i_suffix or ''})")
        
        # 상품코드 기준으로 병합
        merged_df = self._merge_dataframes(merged_df, df, product_code_col)
        
        # 5. 최신 버전 파일명 생성 (괄호 정보 보존)
        root_base, ext = os.path.splitext(root_name)
        t_suffix_str = final_t_suffix or ""
        i_suffix_str = final_i_suffix or ""
        base_output_filename = f"{root_base}_T{max_t}{t_suffix_str}_I{max_i}{i_suffix_str}{ext}"
        output_path = os.path.join(directory, base_output_filename)
        
        # 기존 입력 파일과 이름이 겹치면 안 됨 (새로운 파일 생성)
        input_file_names = {os.path.basename(file1_path), os.path.basename(file2_path)}
        
        # 출력 파일명이 입력 파일 중 하나와 같으면 번호 추가
        if os.path.basename(output_path) in input_file_names:
            self._log(f"  ⚠️ 출력 파일명이 입력 파일과 겹칩니다. 번호를 추가합니다.")
            counter = 1
            while os.path.basename(output_path) in input_file_names or os.path.exists(output_path):
                output_filename = f"{root_base}_T{max_t}{t_suffix_str}_I{max_i}{i_suffix_str}({counter}){ext}"
                output_path = os.path.join(directory, output_filename)
                counter += 1
                if counter > 100:  # 무한 루프 방지
                    self._log(f"  ❌ 적절한 파일명을 생성할 수 없습니다.")
                    return None
        else:
            # 기존 파일이 이미 존재하면 번호 추가
            if os.path.exists(output_path):
                self._log(f"  ⚠️ 동일한 이름의 파일이 이미 존재합니다. 번호를 추가합니다.")
                counter = 1
                while os.path.exists(output_path):
                    output_filename = f"{root_base}_T{max_t}{t_suffix_str}_I{max_i}{i_suffix_str}({counter}){ext}"
                    output_path = os.path.join(directory, output_filename)
                    counter += 1
                    if counter > 100:  # 무한 루프 방지
                        self._log(f"  ❌ 적절한 파일명을 생성할 수 없습니다.")
                        return None
        
        output_filename = os.path.basename(output_path)
        self._log(f"  출력 파일명: {output_filename}")
        
        # 6. 저장 (새로운 파일로 저장)
        try:
            merged_df.to_excel(output_path, index=False, engine='openpyxl')
            self._log(f"  ✅ 새로운 파일로 저장 완료: {output_filename}")
            self._log(f"  최신 버전: T{max_t}{t_suffix_str or ''}, I{max_i}{i_suffix_str or ''}")
            self._log(f"  저장 경로: {output_path}")
            
            # 7. 병합 완료 (job_history.json은 업데이트하지 않음)
            # 병합된 파일은 새로운 파일로 생성되며, 
            # 이후 다른 스크립트가 처리할 때 자동으로 job_history.json이 업데이트됩니다.
            # 메인 런처 대시보드는 root_name 기준으로 표시되므로 별도 업데이트 불필요
            
            return output_path
        except Exception as e:
            self._log(f"  ❌ 저장 실패: {e}")
            return None
    
    def _merge_dataframes(self, base_df: pd.DataFrame, new_df: pd.DataFrame, product_code_col: str) -> pd.DataFrame:
        """
        두 DataFrame을 상품코드 기준으로 병합
        - 공통 컬럼: base_df가 비어있고 new_df에 값이 있으면 채움 (기존 값은 보존)
        - 새 컬럼: new_df에만 있는 컬럼을 추가하고 값 채움
        - base_df에만 있는 컬럼: 유지
        """
        # 상품코드를 인덱스로 설정
        base_df = base_df.set_index(product_code_col)
        new_df = new_df.set_index(product_code_col)
        
        # 새 컬럼 추가 (new_df에만 있는 컬럼)
        for col in new_df.columns:
            if col not in base_df.columns:
                # 새 컬럼 추가 (초기값 None)
                base_df[col] = None
        
        # base_df에만 있는 컬럼도 new_df에 추가 (None으로 초기화, 나중에 채울 수 있도록)
        for col in base_df.columns:
            if col not in new_df.columns:
                new_df[col] = None
        
        # 상품코드 기준으로 병합
        for product_code in new_df.index:
            if product_code in base_df.index:
                # 기존 행이 있는 경우
                for col in new_df.columns:
                    base_value = base_df.at[product_code, col]
                    new_value = new_df.at[product_code, col]
                    
                    # 빈 값 체크 함수
                    def is_empty(val):
                        if pd.isna(val):
                            return True
                        val_str = str(val).strip()
                        return val_str == "" or val_str.lower() in ["nan", "none", "null"]
                    
                    # 공통 컬럼인 경우: base_df가 비어있고 new_df에 값이 있으면 채움
                    if col in base_df.columns:
                        if is_empty(base_value) and not is_empty(new_value):
                            base_df.at[product_code, col] = new_value
                    # 새 컬럼인 경우: new_df에 값이 있으면 채움
                    else:
                        if not is_empty(new_value):
                            base_df.at[product_code, col] = new_value
            else:
                # 새 행 추가
                new_row = new_df.loc[[product_code]].copy()
                # base_df에만 있는 컬럼을 None으로 채우기
                for col in base_df.columns:
                    if col not in new_row.columns:
                        new_row[col] = None
                base_df = pd.concat([base_df, new_row])
        
        # 인덱스를 컬럼으로 복원
        base_df = base_df.reset_index()
        
        return base_df

if __name__ == "__main__":
    app = MergeExcelVersionsGUI()
    app.mainloop()

