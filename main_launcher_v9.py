import os
import sys
import subprocess
import tkinter as tk
from tkinter import ttk, messagebox, simpledialog # simpledialog 추가됨
from pathlib import Path
import json
from datetime import datetime
import shutil

SCRIPT_REQUIRED_MODULES = {
    "Ownerclan_Converter": ["PyQt5", "pandas", "openpyxl"],
}

# =============================================================================
# [설정] 프로그램별 실행 파일 경로 매핑
# =============================================================================
SCRIPTS = {
    # --- [공통] ---
    "Common_Mapping": {
        "folder": "stage1_product_name", 
        "file": "stage1_mapping_tool.py",
        "desc": "★ 필수 시작점\n도매처 원본 엑셀을 시스템 표준 포맷으로 변환합니다."
    },
    "DB_Entry": {
        "folder": "DB_save",
        "file": "data_entry.py",
        "desc": "데이터 입고 도구\n1. 엑셀 매핑 이후: 중복검사 및 필터링\n2. 가공 완료 후: 엑셀 파일을 SQLite DB에 저장"
    },
    "DB_Export": {
        "folder": "DB_save",
        "file": "data_export.py",
        "desc": "데이터 출고 도구\nDB에서 마켓 업로드용 데이터를 내보냅니다.\n중복 방지 및 출고 이력 기록"
    },
    "Ownerclan_Converter": {
        "folder": r"OC_ES_converter\scripts\gui",
        "file": "main.py",
        "desc": "오너클랜 → 이셀러스 변환기\n오너클랜 양식을 이셀러스 양식으로 변환합니다.\n스토어별 특이사항에 맞게 변환됩니다."
    },
    "Upload_Mapper": {
        "folder": "Upload_Mapper",
        "file": "main.py",
        "desc": "상품 등록 맵퍼\n등록 솔루션 엑셀과 가공된 엑셀을 매핑하여 업로드용 엑셀을 생성합니다."
    },
    "Market_Price_UI": {
        "folder": r"C:\\Users\\kohaz\\Desktop\\Python\\.cursor",
        "file": "market_price_ui_tk.py",
        "desc": "\ub9c8\ucf13 \ud310\ub9e4\uac00\uc728 \uc790\ub3d9 \uc870\uc815 UI \uc2e4\ud589"
    },
    "Merge_Versions": {
        "folder": "",
        "file": "merge_excel_versions.py",
        "desc": "엑셀 버전 병합 도구\n같은 이름을 가진 여러 버전의 엑셀 파일을 상품코드 기준으로 병합합니다.\n기존 컬럼은 유지하고, 없는 컬럼만 추가합니다."
    },

    # --- [A] 텍스트 가공 ---
    "Text_S1_API": {
        "folder": "stage1_product_name", 
        "file": "stage1_api_ver_runner.py",
        "desc": "[건별/실시간] 소량 데이터를 빠르게 정제하고 결과를 즉시 확인합니다."
    },
    "Text_S1_Batch": {
        "folder": "stage1_product_name", 
        "file": "Gui_stage1_batch_Casche.py",
        "desc": "[대량/배치] 많은 데이터를 50% 비용으로 일괄 처리합니다. (시간 소요)"
    },
    
    # [Stage 2] 상세설명 & 이미지 분석
    "Text_S2_Extract": {
        "folder": "stage2_product_name", 
        "file": "Product_detaildescription.py",
        "desc": "★ Stage 2 필수 전처리\n상세페이지 HTML에서 이미지를 추출하여 로컬에 다운로드합니다."
    },
    "Text_S2_GUI": {
        "folder": "stage2_product_name", 
        "file": "stage2_LLM_gui.py",
        "desc": "[건별/실시간] 다운로드된 이미지와 텍스트를 분석하여 정보를 추출합니다."
    },
    "Text_S2_Batch": {
        "folder": "stage2_product_name", 
        "file": "stage2_batch_api_Cachever_resize.py",
        "desc": "[대량/배치] 이미지 분석 작업을 서버에 일괄 요청합니다."
    },
    
    # [Stage 3] 최종 상품명 생성
    "Text_S3_GUI": {
        "folder": "stage3_product_name", 
        "file": "stage3_LLM_gui.py",
        "desc": "[건별/실시간] 추출된 정보를 바탕으로 최종 상품명을 생성합니다."
    },
    "Text_S3_Batch": {
        "folder": "stage3_product_name", 
        "file": "stage3_batch_api_Casche.py",
        "desc": "[대량/배치] 상품명 생성 작업을 일괄 처리합니다."
    },
    
    # [Stage 4] 검수 및 필터링
    "Text_S4_Filter": {
        "folder": "stage4_product_name", 
        "file": "stage4_1_filter_gui.py",
        "desc": "[전처리] 금지어 및 필터링 규칙을 적용하여 1차 검수를 진행합니다."
    },
    "Text_S4_2_GUI": {
        "folder": "stage4_product_name", 
        "file": "stage4_2_gui.py",
        "desc": "[건별/실시간] 최종 결과물을 AI가 순위별로 정렬하고 검수합니다."
    },
    "Text_S4_2_Batch": {
        "folder": "stage4_product_name", 
        "file": "stage4_2_batch_api_Casche.py",
        "desc": "[대량/배치] 최종 검수 및 정렬을 일괄 수행합니다."
    },

    # --- [B] 이미지 가공 ---
    "Img_S1_BG": {
        "folder": "Remove_imgBG", 
        "file": "Remove_BG_gui_from_excel_I1.py",
        "desc": "이미지의 배경을 제거하여 누끼 이미지를 생성합니다."
    },
    "Img_S2_Label": {
        "folder": "Remove_imgBG", 
        "file": "bg_label_gui_I2.py",
        "desc": "배경 제거된 이미지를 검수하고 라벨링합니다."
    },
    "Img_S2_AI_Label": {
        "folder": "IMG_stage2", 
        "file": "ai_labeling_gui.py",
        "desc": "[준비중] AI를 이용한 자동 라벨링"
    },
    
    # [Stage 3] 썸네일 이미지 분석 전처리
    "Img_S3_Thumbnail_Analysis_GUI": {
        "folder": "IMG_stage3", 
        "file": "IMG_analysis_gui_Casche.py",
        "desc": "[단건/실시간] 썸네일 구도 분석"
    },
    "Img_S3_Thumbnail_Analysis_Batch": {
        "folder": "IMG_stage3", 
        "file": "IMG_Batch_analysis_gui_Casche_resize.py",
        "desc": "[대량/배치] 썸네일 구도 분석"
    },
    "Img_S3_Preprocess_GUI": {
        "folder": "IMG_stage3", 
        "file": "bg_prompt_gui.py",
        "desc": "[단건/실시간] 배경 생성 프롬프트 작성"
    },
    "Img_S3_Preprocess_Batch": {
        "folder": "IMG_stage3", 
        "file": "bg_Batch_prompt_gui_Casche_resize.py",
        "desc": "[대량/배치] 배경 생성 프롬프트 작성"
    },
    
    # [Stage 4] 배경 생성 및 합성
    "Img_S4_BG_Generate": {
        "folder": "IMG_stage4", 
        "file": "Bg_Generation_V2.py",
        "desc": "생성된 프롬프트로 ComfyUI를 통해 배경 이미지를 생성합니다."
    },
    "Img_S4_Composite": {
        "folder": "IMG_stage4", 
        "file": "IMG_mixing.py",
        "desc": "누끼 이미지와 생성된 배경을 합성합니다."
    },
    
    # [Stage 5] 품질 검증
    "Img_S5_Review": {
        "folder": "IMG_stage5", 
        "file": "Stage5_Review.py",
        "desc": "합성된 이미지와 누끼 이미지, 원본 이미지를 비교하여 최종 선택합니다."
    },
    "Img_S5_Upload": {
        "folder": "IMG_stage5", 
        "file": "cloudflare_upload_gui.py",
        "desc": "I5 파일의 최종 이미지를 Cloudflare R2에 업로드하고 URL을 엑셀에 기록합니다."
    },
}

# --- UI 디자인 ---
COLOR_BG = "#F0F2F5"
COLOR_HEADER = "#2C3E50"
COLOR_COMMON = "#546E7A" 
COLOR_STATUS_BAR = "#E9ECEF"

# 스테이지별 포인트 컬러
COLOR_S1 = "#1976D2" # 파랑
COLOR_S2 = "#0097A7" # 청록
COLOR_S3 = "#388E3C" # 초록
COLOR_S4 = "#7B1FA2" # 보라
COLOR_S5 = "#F57C00" # 주황

def get_base_dir() -> Path:
    """
    PyInstaller로 빌드된 환경과 일반 실행 환경을 구분하여 기본 경로 반환
    --onefile 모드: sys._MEIPASS (임시 압축 해제 디렉토리)
    --onedir 모드: sys.executable의 부모 디렉토리
    일반 실행: __file__의 부모 디렉토리
    """
    if getattr(sys, "frozen", False):
        # PyInstaller로 빌드된 경우
        if hasattr(sys, "_MEIPASS"):
            # --onefile 모드: 임시 디렉토리 사용
            return Path(sys._MEIPASS)
        else:
            # --onedir 모드: 실행 파일과 같은 디렉토리
            return Path(sys.executable).resolve().parent
    # 일반 Python 실행
    return Path(__file__).resolve().parent

BASE_DIR = get_base_dir()

def get_preferred_launcher_python() -> str | None:
    """상품가공프로그램용으로 우선 사용할 Python 경로를 찾습니다."""
    candidates = [
        BASE_DIR / ".venv" / "Scripts" / "python.exe",
        BASE_DIR / "venv" / "Scripts" / "python.exe",
        BASE_DIR / "python.exe",
    ]

    for path in candidates:
        if path.exists():
            return str(path)

    base_prefix = getattr(sys, "base_prefix", "")
    if base_prefix:
        base_python = Path(base_prefix) / "python.exe"
        if base_python.exists():
            return str(base_python)

    common_paths = [
        r"C:\Program Files\Python312\python.exe",
        r"C:\Python312\python.exe",
        r"C:\Program Files\Python311\python.exe",
        r"C:\Python311\python.exe",
        r"C:\Program Files\Python310\python.exe",
        r"C:\Python310\python.exe",
    ]
    for path in common_paths:
        if os.path.exists(path):
            return path

    return None

def should_relaunch_with_preferred_python() -> bool:
    """다른 프로젝트 가상환경에서 실행 중이면 True를 반환합니다."""
    current_python = str(sys.executable).strip().lower()
    base_dir_lower = str(BASE_DIR).lower()

    if base_dir_lower in current_python:
        return False

    return ".venv" in current_python or "\\venv\\" in current_python

def relaunch_with_preferred_python_if_needed():
    """잘못된 가상환경에서 실행 중이면 권장 Python으로 재실행합니다."""
    if os.environ.get("PRODUCT_PIPELINE_RELAUNCHED") == "1":
        return

    if not should_relaunch_with_preferred_python():
        return

    preferred_python = get_preferred_launcher_python()
    current_python = str(sys.executable).strip()

    if not preferred_python or os.path.normcase(preferred_python) == os.path.normcase(current_python):
        return

    env = os.environ.copy()
    env["PRODUCT_PIPELINE_RELAUNCHED"] = "1"
    env["PYTHONUTF8"] = "1"
    env["PYTHONIOENCODING"] = "utf-8"

    subprocess.Popen([preferred_python, "-X", "utf8", str(Path(__file__).resolve())], env=env)
    sys.exit(0)

# ========================================================
# [CORE] 작업 이력 관리자 (JSON DB)
# ========================================================
class JobManager:
    # [수정] 런처 실행 위치(BASE_DIR)를 기준으로 절대 경로 생성
    DB_FILE = os.path.join(BASE_DIR, "job_history.json")
    DELETED_DB_FILE = os.path.join(BASE_DIR, "job_history_deleted.json")

    @classmethod
    def load_jobs(cls):
        """JSON 파일에서 작업 목록을 불러옵니다."""
        if not os.path.exists(cls.DB_FILE):
            return {}
        try:
            with open(cls.DB_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception:
            return {}

    @classmethod
    def init_db(cls):
        """파일이 없으면 빈 파일을 생성합니다."""
        # 경로 디버깅용 출력 (실행 시 콘솔 확인)
        print(f"[DEBUG] MainLauncher DB Path: {cls.DB_FILE}")
        
        if not os.path.exists(cls.DB_FILE):
            with open(cls.DB_FILE, 'w', encoding='utf-8') as f:
                json.dump({}, f, ensure_ascii=False, indent=4)
        
        # 휴지통 DB 파일도 초기화
        if not os.path.exists(cls.DELETED_DB_FILE):
            with open(cls.DELETED_DB_FILE, 'w', encoding='utf-8') as f:
                json.dump({}, f, ensure_ascii=False, indent=4)

    @classmethod
    def update_job_memo(cls, filename, memo_text):
        """특정 파일에 대한 메모만 수정합니다."""
        data = cls.load_jobs()
        if filename in data:
            data[filename]["memo"] = memo_text
            with open(cls.DB_FILE, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=4)
    
    @classmethod
    def load_deleted_jobs(cls):
        """휴지통에서 삭제된 작업 목록을 불러옵니다."""
        if not os.path.exists(cls.DELETED_DB_FILE):
            return {}
        try:
            with open(cls.DELETED_DB_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception:
            return {}
    
    @classmethod
    def delete_job(cls, filename):
        """작업을 휴지통으로 이동합니다."""
        data = cls.load_jobs()
        if filename not in data:
            return False
        
        # 삭제할 항목 가져오기
        deleted_item = data.pop(filename)
        deleted_item["deleted_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # 메인 DB에서 제거
        with open(cls.DB_FILE, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        
        # 휴지통 DB에 추가
        deleted_data = cls.load_deleted_jobs()
        deleted_data[filename] = deleted_item
        with open(cls.DELETED_DB_FILE, 'w', encoding='utf-8') as f:
            json.dump(deleted_data, f, ensure_ascii=False, indent=4)
        
        return True
    
    @classmethod
    def restore_job(cls, filename):
        """휴지통에서 작업을 복원합니다."""
        deleted_data = cls.load_deleted_jobs()
        if filename not in deleted_data:
            return False
        
        # 복원할 항목 가져오기
        restored_item = deleted_data.pop(filename)
        restored_item.pop("deleted_at", None)  # 삭제 시간 필드 제거
        
        # 휴지통 DB에서 제거
        with open(cls.DELETED_DB_FILE, 'w', encoding='utf-8') as f:
            json.dump(deleted_data, f, ensure_ascii=False, indent=4)
        
        # 메인 DB에 복원
        data = cls.load_jobs()
        data[filename] = restored_item
        with open(cls.DB_FILE, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        
        return True
    
    @classmethod
    def permanently_delete_job(cls, filename):
        """휴지통에서 작업을 완전히 삭제합니다."""
        deleted_data = cls.load_deleted_jobs()
        if filename not in deleted_data:
            return False
        
        # 휴지통 DB에서 제거
        deleted_data.pop(filename)
        with open(cls.DELETED_DB_FILE, 'w', encoding='utf-8') as f:
            json.dump(deleted_data, f, ensure_ascii=False, indent=4)
        
        return True
    
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
        data = cls.load_jobs()
        now = datetime.now().strftime("%m-%d %H:%M")
        
        if filename not in data:
            data[filename] = {
                "start_time": datetime.now().strftime("%Y-%m-%d %H:%M"),
                "text_status": "대기",
                "text_time": "-",
                "image_status": "대기",
                "image_time": "-",
                "image_s3_1_status": "-",  # Stage 3-1: 썸네일 분석
                "image_s3_1_time": "-",
                "image_s3_2_status": "-",  # Stage 3-2: 전처리
                "image_s3_2_time": "-",
                "image_s4_1_status": "-",  # Stage 4-1: 배경 생성
                "image_s4_1_time": "-",
                "image_s4_2_status": "-",  # Stage 4-2: 합성
                "image_s4_2_time": "-",
                "image_s5_1_status": "-",  # Stage 5-1: 품질 검증
                "image_s5_1_time": "-",
                "image_s5_2_status": "-",  # Stage 5-2: 이미지 업로드
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
            with open(cls.DB_FILE, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=4)
        except Exception as e:
            print(f"[JobManager Error] {e}")

# ========================================================
# 툴팁 클래스
# ========================================================
class ToolTip:
    def __init__(self, widget, text):
        self.widget = widget
        self.text = text
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
        y = self.widget.winfo_rooty() + 30
        self.tipwindow = tw = tk.Toplevel(self.widget)
        tw.wm_overrideredirect(True)
        # 툴팁이 클릭 이벤트를 받지 않도록 설정
        tw.attributes("-topmost", True)
        tw.wm_geometry(f"+{x}+{y}")
        label = tk.Label(tw, text=self.text, justify='left',
                         background="#ffffe0", relief='solid', borderwidth=0,
                         font=("맑은 고딕", 9))
        label.pack(ipadx=5, ipady=2)
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
# 메인 런처 클래스
# ========================================================
class PipelineLauncher(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("상품 가공 파이프라인 통합 런처")
        self.geometry("1400x1050") # 좌우 분할을 위해 넓게 설정, 세로 크기 증가
        self.configure(bg=COLOR_BG)
        
        self.status_var = tk.StringVar(value="System Ready...")
        self.advanced_mode = False  # 고급 모드 (단건/실시간 버튼 표시 여부)
        self.button_refs = {}  # 버튼 참조 저장 (모드 변경 시 표시/숨김 제어용)

        self._setup_styles()
        self._init_ui()
        self.after(100, self._warn_if_wrong_python_environment)

    def _setup_styles(self):
        style = ttk.Style()
        try: style.theme_use('clam')
        except: pass
        
        style.configure("TFrame", background=COLOR_BG)
        style.configure("TLabel", background=COLOR_BG, font=("맑은 고딕", 10))
        style.configure("TNotebook", background=COLOR_BG)
        style.configure("TNotebook.Tab", padding=[20, 10], font=("맑은 고딕", 11, "bold"))
        
        style.map("TNotebook.Tab", 
                  background=[("selected", "#FFFFFF"), ("!selected", "#E0E0E0")],
                  foreground=[("selected", "#333333"), ("!selected", "#888888")])

    def _init_ui(self):
        # 1. 헤더
        header = tk.Frame(self, bg=COLOR_HEADER, height=60)
        header.pack(fill="x")
        header.pack_propagate(False)
        
        # 제목 레이블
        title_label = tk.Label(header, text="🚀 상품 가공 자동화 시스템", font=("맑은 고딕", 17, "bold"), bg=COLOR_HEADER, fg="white")
        title_label.pack(side="left", padx=20)
        
        # 우측 버튼 영역
        header_right = tk.Frame(header, bg=COLOR_HEADER)
        header_right.pack(side="right", padx=20)
        
        # 고급 모드 토글 버튼
        self.advanced_mode_btn = tk.Button(
            header_right, 
            text="⚙️ 고급 모드 OFF", 
            command=self.toggle_advanced_mode,
            bg="#6c757d", 
            fg="white", 
            font=("맑은 고딕", 9, "bold"),
            relief="raised",
            cursor="hand2",
            padx=12,
            pady=5,
            bd=1,
            highlightthickness=0
        )
        self.advanced_mode_btn.pack(side="right", padx=(10, 0))
        ToolTip(self.advanced_mode_btn, "고급 모드 ON: 단건/실시간 버튼도 표시됩니다.\nOFF: 대량/배치 버튼만 표시됩니다. (기본값)")

        # 2. [NEW] 메인 컨테이너를 좌우로 분할 (PanedWindow 사용)
        main_pane = tk.PanedWindow(self, orient="horizontal", bg=COLOR_BG, sashwidth=5)
        main_pane.pack(fill="both", expand=True, padx=10, pady=10)

        # =================================================================
        # [왼쪽 패널] 기존 컨트롤 패널 (버튼 영역)
        # =================================================================
        left_panel = tk.Frame(main_pane, bg=COLOR_BG)
        main_pane.add(left_panel, minsize=400) # 최소 너비 설정

        # [STEP 0] 공통 준비 영역
        self._create_common_section(left_panel)

        # [하단] 데이터 출고 도구 섹션을 먼저 생성 (하단 고정용)
        self._create_db_export_section(left_panel)

        # 중간 영역: 메인 탭 (expand=True로 남은 공간 사용)
        notebook = ttk.Notebook(left_panel)
        notebook.pack(fill="both", expand=True, pady=(6, 6))

        tab_text = ttk.Frame(notebook)
        tab_img = ttk.Frame(notebook)
        
        notebook.add(tab_text, text=" 📝 상품명 (Text) ")
        notebook.add(tab_img, text=" 🖼️ 이미지 (Image) ")

        self._build_text_tab(tab_text)
        self._build_image_tab(tab_img)

        # =================================================================
        # [오른쪽 패널] 작업 현황판 + 메모장 + 휴지통
        # =================================================================
        right_panel = tk.Frame(main_pane, bg="white", bd=1, relief="solid")
        main_pane.add(right_panel, minsize=550)

        # 오른쪽 패널에 탭 추가 (대시보드 / 휴지통)
        right_notebook = ttk.Notebook(right_panel)
        right_notebook.pack(fill="both", expand=True, padx=5, pady=5)

        # 대시보드 탭
        dashboard_tab = tk.Frame(right_notebook, bg="white")
        right_notebook.add(dashboard_tab, text=" 📊 작업 현황 ")

        # 휴지통 탭
        trash_tab = tk.Frame(right_notebook, bg="white")
        right_notebook.add(trash_tab, text=" 🗑️ 휴지통 ")

        # --- A. 상단 헤더 (제목 + 버튼들) ---
        dashboard_header = tk.Frame(dashboard_tab, bg="white")
        dashboard_header.pack(fill="x", padx=10, pady=(15, 5))

        tk.Label(dashboard_header, text="📊 실시간 작업 현황 (Dashboard)", 
                 font=("맑은 고딕", 12, "bold"), bg="white", fg="#333").pack(side="left")

        btn_frame = tk.Frame(dashboard_header, bg="white")
        btn_frame.pack(side="right")
        
        btn_guide = tk.Button(btn_frame, text="📖 사용법", 
                             command=self.show_guide_popup, 
                             bg="#3498db", fg="white", relief="raised",
                             font=("맑은 고딕", 9, "bold"), cursor="hand2", padx=12, pady=5,
                             bd=1, highlightthickness=0)
        btn_guide.pack(side="left", padx=(0, 5))
        
        btn_refresh = tk.Button(btn_frame, text="🔄 새로고침", 
                                command=self.refresh_dashboard, 
                                bg="#f1f3f5", fg="#333", relief="raised",
                                font=("맑은 고딕", 9), cursor="hand2", padx=12, pady=5,
                                bd=1, highlightthickness=0)
        btn_refresh.pack(side="left")

        # --- B. 작업 현황판 (Treeview) ---
        tree_frame = tk.Frame(dashboard_tab, bg="white")
        tree_frame.pack(fill="both", expand=True, padx=5, pady=(5, 0))

        # 컬럼 정의: 파일명 / Text상태 / Text시간 / Img상태 / Img시간 / 메모
        columns = ("file", "text_stat", "text_time", "img_stat", "img_time", "memo")
        self.tree = ttk.Treeview(tree_frame, columns=columns, show='headings', height=15, selectmode="extended")
        
        # [컬럼 설정]
        self.tree.heading("file", text="파일 (Root Name)"); self.tree.column("file", width=180, anchor="w")
        
        self.tree.heading("text_stat", text="Text 상태"); self.tree.column("text_stat", width=90, anchor="center")
        self.tree.heading("text_time", text="최근변경"); self.tree.column("text_time", width=90, anchor="center")
        
        self.tree.heading("img_stat", text="Img 상태"); self.tree.column("img_stat", width=150, anchor="center")  # I3-1/I3-2 표시를 위해 넓게 조정
        self.tree.heading("img_time", text="최근변경"); self.tree.column("img_time", width=90, anchor="center")
        
        self.tree.heading("memo", text="비고(메모)"); self.tree.column("memo", width=150, anchor="w")

        # 스크롤바
        scrollbar = ttk.Scrollbar(tree_frame, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscrollcommand=scrollbar.set)
        
        self.tree.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")
        
        # [이벤트] 더블 클릭 시 메모 수정, 우클릭 시 컨텍스트 메뉴
        self.tree.bind("<Double-1>", self.on_tree_double_click)
        self.tree.bind("<Button-3>", self.on_tree_right_click)  # 우클릭 메뉴
        
        # 컨텍스트 메뉴 생성
        self.context_menu = tk.Menu(self, tearoff=0)
        self.context_menu.add_command(label="📝 메모 수정", command=self.edit_selected_memo)
        self.context_menu.add_command(label="🗑️ 휴지통으로 이동", command=self.delete_selected_job)
        self.context_menu.add_separator()
        self.context_menu.add_command(label="🔄 새로고침", command=self.refresh_dashboard)

        # --- C. 하단: 전체 사용자 메모장 ---
        memo_frame = tk.LabelFrame(dashboard_tab, text=" 📝 공통 메모 (Common Memo) ", 
                                   font=("맑은 고딕", 10, "bold"), bg="white", fg="#555", bd=1, relief="solid")
        memo_frame.pack(fill="x", padx=10, pady=(5, 10))

        self.txt_memo = tk.Text(memo_frame, height=8, font=("맑은 고딕", 10), bg="#FEF9E7", relief="flat")
        self.txt_memo.pack(fill="both", expand=True, padx=5, pady=5)

        btn_save_memo = tk.Button(memo_frame, text="💾 메모 저장 (Save)", 
                                  command=self.save_user_memo, 
                                  bg="#546E7A", fg="white", font=("맑은 고딕", 9, "bold"), 
                                  relief="raised", cursor="hand2", bd=1, highlightthickness=0)
        btn_save_memo.pack(fill="x", padx=5, pady=(0, 5))

        # 3. 상태바 (최하단)
        self._create_status_bar()
        
        # 휴지통 탭 구성
        self._build_trash_tab(trash_tab)

        # [초기화] DB 및 메모 로드
        JobManager.init_db()
        self.refresh_dashboard()
        self.load_user_memo()

    # ========================================================
    # [기능] 대시보드 및 데이터 로직
    # ========================================================
    def refresh_dashboard(self):
        """JSON 파일을 읽어서 트리뷰(표)를 최신 상태로 업데이트합니다."""
        for item in self.tree.get_children():
            self.tree.delete(item)
            
        jobs = JobManager.load_jobs()
        if not jobs: return

        # 최신 업데이트 순으로 정렬
        sorted_jobs = sorted(jobs.items(), key=lambda x: x[1].get('last_update', ''), reverse=True)

        for filename, info in sorted_jobs:
            # 꼬리표 떼고 깔끔한 이름만 보여주기 (이미 DB에 깔끔하게 들어가 있겠지만 안전장치)
            clean_name = filename.replace("_stage1_mapping", "").replace(".xlsx", "")
            
            t_stat = info.get("text_status", "-")
            t_time = info.get("text_time", "-")
            i_stat = info.get("image_status", "-")
            i_time = info.get("image_time", "-")
            memo = info.get("memo", "") # 파일별 메모
            
            # Stage 3, Stage 4, Stage 5 세부 단계 정보가 있으면 가장 최근 단계만 표시
            img_s3_1 = info.get("image_s3_1_status", "-")
            img_s3_2 = info.get("image_s3_2_status", "-")
            img_s4_1 = info.get("image_s4_1_status", "-")
            img_s4_2 = info.get("image_s4_2_status", "-")
            img_s5_1 = info.get("image_s5_1_status", "-")
            img_s5_2 = info.get("image_s5_2_status", "-")
            
            # 가장 최근 단계만 표시 (우선순위: I5 > I4 > I3)
            parts = []
            if img_s5_1 != "-" or img_s5_2 != "-":
                # I5 단계 표시
                if img_s5_1 != "-":
                    parts.append(img_s5_1)  # "I5-1 (진행중)" 형식 그대로
                if img_s5_2 != "-":
                    parts.append(img_s5_2)  # "I5-2 (완료)" 형식 그대로
                i_time = (info.get("image_s5_2_time") or 
                         info.get("image_s5_1_time") or 
                         i_time)
            elif img_s4_1 != "-" or img_s4_2 != "-":
                # I4 단계 표시
                if img_s4_1 != "-":
                    parts.append(img_s4_1)  # "I4-1 (진행중)" 형식 그대로
                if img_s4_2 != "-":
                    parts.append(img_s4_2)  # "I4-2 (완료)" 형식 그대로
                i_time = (info.get("image_s4_2_time") or 
                         info.get("image_s4_1_time") or 
                         i_time)
            elif img_s3_1 != "-" or img_s3_2 != "-":
                # I3 단계 표시
                if img_s3_1 != "-":
                    parts.append(img_s3_1)  # "I3-1 (진행중)" 형식 그대로
                if img_s3_2 != "-":
                    parts.append(img_s3_2)  # "I3-2 (완료)" 형식 그대로
                i_time = (info.get("image_s3_2_time") or 
                         info.get("image_s3_1_time") or 
                         i_time)
            
            if parts:
                i_stat = " / ".join(parts)
            
            # 상태에 따라 색상 다르게 (나중에 tag 적용 가능)
            self.tree.insert("", "end", values=(clean_name, t_stat, t_time, i_stat, i_time, memo))
            
        self._update_status("ready", f"현황판 업데이트 완료 ({datetime.now().strftime('%H:%M:%S')})")

    def on_tree_double_click(self, event):
        """트리뷰의 행을 더블 클릭하면 해당 파일의 메모를 수정합니다."""
        self.edit_selected_memo()
    
    def on_tree_right_click(self, event):
        """트리뷰에서 우클릭 시 컨텍스트 메뉴를 표시합니다."""
        item_id = self.tree.identify_row(event.y)
        if item_id:
            # 우클릭한 항목이 이미 선택되어 있으면 기존 선택 유지, 아니면 해당 항목만 선택
            current_selection = self.tree.selection()
            if item_id not in current_selection:
                self.tree.selection_set(item_id)
            try:
                self.context_menu.tk_popup(event.x_root, event.y_root)
            finally:
                self.context_menu.grab_release()
    
    def edit_selected_memo(self):
        """선택된 항목의 메모를 수정합니다."""
        selected = self.tree.selection()
        if not selected:
            messagebox.showwarning("선택 필요", "메모를 수정할 항목을 선택해주세요.")
            return
        
        item_id = selected[0]
        values = self.tree.item(item_id, "values")
        if not values: return
        
        filename = values[0]
        current_memo = values[5]
        
        # 팝업 입력창
        new_memo = simpledialog.askstring("메모 수정", f"[{filename}]\n비고 사항을 입력하세요:", initialvalue=current_memo)
        
        if new_memo is not None:
            # 실제 DB 키를 찾아야 함 (파일명이 축약되었을 수 있으므로)
            jobs = JobManager.load_jobs()
            # 정확한 키 매칭 시도 (확장자 포함 등)
            target_key = next((k for k in jobs.keys() if filename in k), filename)
            
            JobManager.update_job_memo(target_key, new_memo) # DB 저장
            self.refresh_dashboard() # 화면 갱신
    
    def delete_selected_job(self):
        """선택된 항목들을 휴지통으로 이동합니다."""
        selected = self.tree.selection()
        if not selected:
            messagebox.showwarning("선택 필요", "삭제할 항목을 선택해주세요.")
            return
        
        # 여러 항목 선택 시
        count = len(selected)
        if count > 1:
            result = messagebox.askyesno(
                "휴지통으로 이동",
                f"선택된 {count}개 항목을 휴지통으로 이동하시겠습니까?\n\n휴지통에서 복원하거나 완전히 삭제할 수 있습니다."
            )
        else:
            item_id = selected[0]
            values = self.tree.item(item_id, "values")
            if not values: return
            filename = values[0]
            result = messagebox.askyesno(
                "휴지통으로 이동",
                f"[{filename}]\n\n이 항목을 휴지통으로 이동하시겠습니까?\n\n휴지통에서 복원하거나 완전히 삭제할 수 있습니다."
            )
        
        if not result:
            return
        
        # 실제 DB 키를 찾아서 삭제
        jobs = JobManager.load_jobs()
        success_count = 0
        fail_count = 0
        failed_names = []
        
        for item_id in selected:
            values = self.tree.item(item_id, "values")
            if not values: continue
            
            filename = values[0]
            target_key = next((k for k in jobs.keys() if filename in k), filename)
            
            if JobManager.delete_job(target_key):
                success_count += 1
            else:
                fail_count += 1
                failed_names.append(filename)
        
        # 결과 메시지
        if fail_count == 0:
            if count > 1:
                messagebox.showinfo("완료", f"{success_count}개 항목이 휴지통으로 이동되었습니다.")
            else:
                messagebox.showinfo("완료", f"휴지통으로 이동되었습니다.")
        else:
            messagebox.showwarning(
                "부분 완료",
                f"성공: {success_count}개\n실패: {fail_count}개\n\n실패한 항목:\n" + "\n".join(failed_names[:5])
            )
        
        self.refresh_dashboard()
        # 휴지통 탭이 있으면 새로고침
        if hasattr(self, 'trash_tree'):
            self.refresh_trash()

    def load_user_memo(self):
        """하단 전체 메모 불러오기"""
        if os.path.exists("user_memo.txt"):
            try:
                with open("user_memo.txt", "r", encoding="utf-8") as f:
                    self.txt_memo.delete("1.0", tk.END)
                    self.txt_memo.insert("1.0", f.read())
            except Exception: pass

    def save_user_memo(self):
        """하단 전체 메모 저장하기"""
        content = self.txt_memo.get("1.0", tk.END).strip()
        try:
            with open("user_memo.txt", "w", encoding="utf-8") as f:
                f.write(content)
            self._update_status("ready", "사용자 메모가 저장되었습니다.")
            messagebox.showinfo("알림", "메모가 저장되었습니다.")
        except Exception as e:
            messagebox.showerror("오류", f"저장 실패: {e}")

    # ========================================================
    # [UI] 기존 버튼 및 실행 로직
    # ========================================================
    def _create_common_section(self, parent):
        frame = tk.LabelFrame(parent, text=" [STEP 0] 공통 데이터 준비 ", font=("맑은 고딕", 11, "bold"), bg="#ECEFF1", fg=COLOR_COMMON, bd=2, relief="groove")
        frame.pack(fill="x", pady=(0, 6), ipady=3)
        
        # 엑셀 매핑 도구
        btn_frame = tk.Frame(frame, bg="#ECEFF1")
        btn_frame.pack(fill="x", padx=12, pady=2)
        
        lbl = tk.Label(btn_frame, text="작업 시작 전 필수!\n원본 엑셀을 표준 포맷으로 변환합니다.", 
                    bg="#ECEFF1", fg="#455A64", font=("맑은 고딕", 9), justify="left")
        lbl.pack(side="left", padx=6)
        
        btn = tk.Button(btn_frame, text="📂 엑셀 매핑 도구 실행\n(Click to Start)", 
            bg=COLOR_COMMON, fg="white", font=("맑은 고딕", 11, "bold"),
            relief="raised", width=22, height=2, cursor="hand2",
            activebackground="#455A64", activeforeground="white",
            command=lambda: self.run_script("Common_Mapping"),
            bd=1, highlightthickness=0
        )
        btn.pack(side="right", padx=5)
        info = SCRIPTS.get("Common_Mapping")
        if info: ToolTip(btn, info["desc"])
        
        # 구분선
        tk.Frame(frame, bg="#CFD8DC", height=1).pack(fill="x", padx=12, pady=2)
        
        # 데이터 입고 도구
        entry_frame = tk.Frame(frame, bg="#ECEFF1")
        entry_frame.pack(fill="x", padx=12, pady=2)
        
        entry_lbl = tk.Label(entry_frame, text="① 엑셀 매핑 이후: 중복검사 및 필터링\n② 가공 완료 후: DB에 저장", 
                    bg="#ECEFF1", fg="#455A64", font=("맑은 고딕", 9), justify="left")
        entry_lbl.pack(side="left", padx=6)
        
        entry_btn = tk.Button(entry_frame, text="💾 데이터 입고 도구\n(Data Entry)", 
            bg="#27ae60", fg="white", font=("맑은 고딕", 11, "bold"),
            relief="raised", width=22, height=2, cursor="hand2",
            activebackground="#229954", activeforeground="white",
            command=lambda: self.run_script("DB_Entry"),
            bd=1, highlightthickness=0
        )
        entry_btn.pack(side="right", padx=5)
        entry_info = SCRIPTS.get("DB_Entry")
        if entry_info: ToolTip(entry_btn, entry_info["desc"])
        
        # 구분선
        tk.Frame(frame, bg="#CFD8DC", height=1).pack(fill="x", padx=12, pady=2)
        
        # 엑셀 버전 병합 도구
        merge_frame = tk.Frame(frame, bg="#ECEFF1")
        merge_frame.pack(fill="x", padx=12, pady=2)
        
        merge_lbl = tk.Label(merge_frame, text="T*_I* 버전이 다른 엑셀 파일들을 상품코드 기준으로 병합", 
                    bg="#ECEFF1", fg="#455A64", font=("맑은 고딕", 9), justify="left")
        merge_lbl.pack(side="left", padx=6)
        
        merge_btn = tk.Button(merge_frame, text="🔄 엑셀 버전 병합\n(Merge Versions)", 
            bg="#00BCD4", fg="white", font=("맑은 고딕", 11, "bold"),
            relief="raised", width=22, height=2, cursor="hand2",
            activebackground="#0097A7", activeforeground="white",
            command=lambda: self.run_script("Merge_Versions"),
            bd=1, highlightthickness=0
        )
        merge_btn.pack(side="right", padx=5)
        merge_info = SCRIPTS.get("Merge_Versions")
        if merge_info: ToolTip(merge_btn, merge_info["desc"])
    
    def _create_db_export_section(self, parent):
        """데이터 출고 도구 별도 섹션 생성"""
        frame = tk.LabelFrame(parent, text=" [하단] 데이터 출고 도구 ", font=("맑은 고딕", 11, "bold"), bg="#E3F2FD", fg="#546E7A", bd=2, relief="groove")
        # 하단에 고정 배치 (expand=False로 고정 크기 유지, side="bottom"으로 하단 고정)
        frame.pack(fill="x", pady=(0, 0), ipady=3, side="bottom", anchor="sw")
        
        export_frame = tk.Frame(frame, bg="#E3F2FD")
        export_frame.pack(fill="x", padx=12, pady=2)
        
        # 버튼들을 담을 프레임 생성 (가로 공간 확보를 위해 설명 텍스트 제거)
        btn_container = tk.Frame(export_frame, bg="#E3F2FD")
        btn_container.pack(side="left", padx=5, expand=True, fill="x")
        
        # 버튼들을 균등하게 배치하기 위한 프레임
        btn_wrapper = tk.Frame(btn_container, bg="#E3F2FD")
        btn_wrapper.pack(anchor="center")
        
        # 데이터 출고 도구 버튼
        export_btn = tk.Button(btn_wrapper, text="📤 데이터 출고\n(Data Export)", 
            bg="#546E7A", fg="white", font=("맑은 고딕", 10, "bold"),
            relief="raised", width=18, height=2, cursor="hand2",
            activebackground="#455A64", activeforeground="white",
            command=lambda: self.run_script("DB_Export"),
            bd=1, highlightthickness=0
        )
        export_btn.pack(side="left", padx=3)
        export_info = SCRIPTS.get("DB_Export")
        if export_info: ToolTip(export_btn, export_info["desc"])
        
        # 오너클랜 → 이셀러스 변환기 버튼
        converter_btn = tk.Button(btn_wrapper, text="🔄 OC→ES 변환\n(Converter)", 
            bg="#FF9800", fg="white", font=("맑은 고딕", 10, "bold"),
            relief="raised", width=18, height=2, cursor="hand2",
            activebackground="#F57C00", activeforeground="white",
            command=lambda: self.run_script("Ownerclan_Converter"),
            bd=1, highlightthickness=0)
        converter_btn.pack(side="left", padx=3)
        converter_info = SCRIPTS.get("Ownerclan_Converter")
        if converter_info: ToolTip(converter_btn, converter_info["desc"])
        
        # 상품 등록 맵퍼 버튼
        # ??? UI ??
        market_price_btn = tk.Button(btn_wrapper, text="\ub4f1\ub85d\uac00 \uacc4\uc0b0\uae30\n(Market Price)", 
            bg="#26A69A", fg="white", font=("?? ??", 10, "bold"),
            relief="raised", width=18, height=2, cursor="hand2",
            activebackground="#1E8E83", activeforeground="white",
            command=lambda: self.run_script("Market_Price_UI"),
            bd=1, highlightthickness=0)
        market_price_btn.pack(side="left", padx=3)
        market_price_info = SCRIPTS.get("Market_Price_UI")
        if market_price_info: ToolTip(market_price_btn, market_price_info["desc"])

        mapper_btn = tk.Button(btn_wrapper, text="📋 상품 등록 맵퍼\n(Upload Mapper)", 
            bg="#6dc951", fg="white", font=("맑은 고딕", 10, "bold"),
            relief="raised", width=18, height=2, cursor="hand2",
            activebackground="#6dc951", activeforeground="white",
            command=lambda: self.run_script("Upload_Mapper"),
            bd=1, highlightthickness=0)
        mapper_btn.pack(side="left", padx=3)
        mapper_info = SCRIPTS.get("Upload_Mapper")
        if mapper_info: ToolTip(mapper_btn, mapper_info["desc"])

    def _build_text_tab(self, parent):
        container = tk.Frame(parent, bg=COLOR_BG, padx=12, pady=10)
        container.pack(fill="both", expand=True)
        
        # Stage 1: 텍스트 기초 정제 (건별/배치 좌우 배치)
        frame_t1 = tk.LabelFrame(container, text=" Stage 1: 텍스트 기초 정제 ", font=("맑은 고딕", 11, "bold"), bg=COLOR_BG, fg=COLOR_S1, bd=2, relief="groove")
        frame_t1.pack(fill="x", pady=6, ipady=4)
        frame_t1_row = tk.Frame(frame_t1, bg=COLOR_BG)
        frame_t1_row.pack(fill="x", padx=10, pady=3)
        tk.Label(frame_t1_row, text="텍스트 기초 정제", bg=COLOR_BG, font=("맑은 고딕", 10, "normal"), width=22, anchor="w", fg="#333").pack(side="left", padx=5)
        self._add_btn(frame_t1_row, "(단건/실시간)", "Text_S1_API", COLOR_S1, width=16, side="left", btn_type="single")
        self._add_separator_label(frame_t1_row, "Text_S1")
        self._add_btn(frame_t1_row, "(대량/배치)", "Text_S1_Batch", COLOR_S1, width=16, side="left", btn_type="batch")
        # Stage 2: 상세정보 & 재료 추출 (건별/배치 좌우 배치)
        frame_t2 = tk.LabelFrame(container, text=" Stage 2: 상세정보 & 재료 추출 ", font=("맑은 고딕", 11, "bold"), bg=COLOR_BG, fg=COLOR_S2, bd=2, relief="groove")
        frame_t2.pack(fill="x", pady=8, ipady=5)
        
        # ① 상세이미지 다운로드 (단독)
        frame_t2_1 = tk.Frame(frame_t2, bg=COLOR_BG)
        frame_t2_1.pack(fill="x", padx=10, pady=3)
        tk.Label(frame_t2_1, text="① 상세이미지 다운(필수)", bg=COLOR_BG, font=("맑은 고딕", 10, "normal"), width=22, anchor="w", fg="#333").pack(side="left", padx=5)
        self._add_btn(frame_t2_1, "실행", "Text_S2_Extract", COLOR_S2, width=16, side="left")
        
        # 구분선
        tk.Frame(frame_t2, bg="#E0E0E0", height=1).pack(fill="x", padx=10, pady=3)
        
        # ② 분석 (건별/배치 좌우 배치)
        frame_t2_2 = tk.Frame(frame_t2, bg=COLOR_BG)
        frame_t2_2.pack(fill="x", padx=10, pady=3)
        tk.Label(frame_t2_2, text="② 이미지 & 텍스트 분석", bg=COLOR_BG, font=("맑은 고딕", 10, "normal"), width=22, anchor="w", fg="#333").pack(side="left", padx=5)
        self._add_btn(frame_t2_2, "(단건/실시간)", "Text_S2_GUI", COLOR_S2, width=16, side="left", btn_type="single")
        self._add_separator_label(frame_t2_2, "Text_S2")
        self._add_btn(frame_t2_2, "(대량/배치)", "Text_S2_Batch", COLOR_S2, width=16, side="left", btn_type="batch")
        
        # Stage 3: 최종 상품명 생성 (건별/배치 좌우 배치)
        frame_t3 = tk.LabelFrame(container, text=" Stage 3: 최종 상품명 생성 ", font=("맑은 고딕", 11, "bold"), bg=COLOR_BG, fg=COLOR_S3, bd=2, relief="groove")
        frame_t3.pack(fill="x", pady=6, ipady=4)
        frame_t3_row = tk.Frame(frame_t3, bg=COLOR_BG)
        frame_t3_row.pack(fill="x", padx=10, pady=3)
        tk.Label(frame_t3_row, text="최종 상품명 생성", bg=COLOR_BG, font=("맑은 고딕", 10, "normal"), width=22, anchor="w", fg="#333").pack(side="left", padx=5)
        self._add_btn(frame_t3_row, "(단건/실시간)", "Text_S3_GUI", COLOR_S3, width=16, side="left", btn_type="single")
        self._add_separator_label(frame_t3_row, "Text_S3")
        self._add_btn(frame_t3_row, "(대량/배치)", "Text_S3_Batch", COLOR_S3, width=16, side="left", btn_type="batch")
        
        # Stage 4: 필터링 및 검수 (건별/배치 좌우 배치)
        frame_t4 = tk.LabelFrame(container, text=" Stage 4: 필터링 및 검수 ", font=("맑은 고딕", 11, "bold"), bg=COLOR_BG, fg=COLOR_S4, bd=2, relief="groove")
        frame_t4.pack(fill="x", pady=6, ipady=4)
        
        # ① 필터링 (단독)
        frame_t4_1 = tk.Frame(frame_t4, bg=COLOR_BG)
        frame_t4_1.pack(fill="x", padx=10, pady=3)
        tk.Label(frame_t4_1, text="① 필터링 (금지어)", bg=COLOR_BG, font=("맑은 고딕", 10, "normal"), width=22, anchor="w", fg="#333").pack(side="left", padx=5)
        self._add_btn(frame_t4_1, "실행", "Text_S4_Filter", COLOR_S4, width=16, side="left")
        
        # 구분선
        tk.Frame(frame_t4, bg="#E0E0E0", height=1).pack(fill="x", padx=10, pady=3)
        
        # ② 검수 (건별/배치 좌우 배치)
        frame_t4_2 = tk.Frame(frame_t4, bg=COLOR_BG)
        frame_t4_2.pack(fill="x", padx=10, pady=3)
        tk.Label(frame_t4_2, text="② 최종 검수", bg=COLOR_BG, font=("맑은 고딕", 10, "normal"), width=22, anchor="w", fg="#333").pack(side="left", padx=5)
        self._add_btn(frame_t4_2, "(단건/실시간)", "Text_S4_2_GUI", COLOR_S4, width=16, side="left", btn_type="single")
        self._add_separator_label(frame_t4_2, "Text_S4_2")
        self._add_btn(frame_t4_2, "(대량/배치)", "Text_S4_2_Batch", COLOR_S4, width=16, side="left", btn_type="batch")

    def _build_image_tab(self, parent):
        # 스크롤 가능한 컨테이너 생성
        canvas = tk.Canvas(parent, bg=COLOR_BG, highlightthickness=0)
        scrollbar = ttk.Scrollbar(parent, orient="vertical", command=canvas.yview)
        scrollable_frame = tk.Frame(canvas, bg=COLOR_BG)
        
        def update_scroll_region(event=None):
            canvas.update_idletasks()
            canvas.configure(scrollregion=canvas.bbox("all"))
        
        scrollable_frame.bind("<Configure>", update_scroll_region)
        
        canvas_window = canvas.create_window((0, 0), window=scrollable_frame, anchor="nw")
        
        def _on_canvas_configure(event):
            canvas_width = event.width
            canvas.itemconfig(canvas_window, width=canvas_width)
        
        canvas.bind("<Configure>", _on_canvas_configure)
        canvas.configure(yscrollcommand=scrollbar.set)
        
        # 마우스 휠 스크롤 바인딩 (Windows)
        def _on_mousewheel(event):
            canvas.yview_scroll(int(-1*(event.delta/120)), "units")
        
        canvas.bind("<MouseWheel>", _on_mousewheel)
        
        canvas.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")
        
        container = tk.Frame(scrollable_frame, bg=COLOR_BG, padx=12, pady=10)
        container.pack(fill="both", expand=True)
        
        # 컨테이너 내용이 변경될 때마다 스크롤 영역 업데이트
        def on_container_change(event=None):
            update_scroll_region()
        
        container.bind("<Configure>", on_container_change)
        
        self._add_stage_group(container, "Stage 1: 배경 제거 (Remove BG)", COLOR_S1, [
            ("▶ (누끼) 배경제거", "Img_S1_BG")
        ])
        self._add_stage_group(container, "Stage 2: 라벨링 (Labeling)", COLOR_S2, [
            ("①-a 휴먼 라벨링 도구", "Img_S2_Label"),
            ("(준비중) AI 자동 라벨링", "Img_S2_AI_Label")
        ])
        # Stage 3: 이미지 분석 전처리 (건별/배치 좌우 배치)
        frame_s3 = tk.LabelFrame(container, text=" Stage 3: 이미지 분석 전처리 ", font=("맑은 고딕", 11, "bold"), bg=COLOR_BG, fg=COLOR_S3, bd=2, relief="groove")
        frame_s3.pack(fill="x", pady=6, ipady=4)
        
        # ① 썸네일 구도·조명 분석
        frame_s3_1 = tk.Frame(frame_s3, bg=COLOR_BG)
        frame_s3_1.pack(fill="x", padx=10, pady=3)
        tk.Label(frame_s3_1, text="① 썸네일 구도·조명 분석", bg=COLOR_BG, font=("맑은 고딕", 10, "normal"), width=22, anchor="w", fg="#333").pack(side="left", padx=5)
        self._add_btn(frame_s3_1, "(단건/실시간)", "Img_S3_Thumbnail_Analysis_GUI", COLOR_S3, width=16, side="left", btn_type="single")
        self._add_separator_label(frame_s3_1, "Img_S3_Thumbnail")
        self._add_btn(frame_s3_1, "(대량/배치)", "Img_S3_Thumbnail_Analysis_Batch", COLOR_S3, width=16, side="left", btn_type="batch")
        
        # 구분선
        tk.Frame(frame_s3, bg="#E0E0E0", height=1).pack(fill="x", padx=10, pady=3)
        
        # ② 배경 생성 프롬프트 작성
        frame_s3_2 = tk.Frame(frame_s3, bg=COLOR_BG)
        frame_s3_2.pack(fill="x", padx=10, pady=3)
        tk.Label(frame_s3_2, text="② 배경 생성 프롬프트", bg=COLOR_BG, font=("맑은 고딕", 10, "normal"), width=22, anchor="w", fg="#333").pack(side="left", padx=5)
        self._add_btn(frame_s3_2, "(단건/실시간)", "Img_S3_Preprocess_GUI", COLOR_S3, width=16, side="left", btn_type="single")
        self._add_separator_label(frame_s3_2, "Img_S3_Preprocess")
        self._add_btn(frame_s3_2, "(대량/배치)", "Img_S3_Preprocess_Batch", COLOR_S3, width=16, side="left", btn_type="batch")
        self._add_stage_group(container, "Stage 4: 배경 생성 및 합성", COLOR_S4, [
            ("① 배경 생성", "Img_S4_BG_Generate"),
            ("② 합성", "Img_S4_Composite")
        ])
        self._add_stage_group(container, "Stage 5: 품질 검증", COLOR_S5, [
            ("품질 검증", "Img_S5_Review"),
            ("이미지 업로드 (R2)", "Img_S5_Upload")
        ])
    
    def show_guide_popup(self):
        """사용법 가이드 팝업 창 표시"""
        # 이미 열려있으면 포커스만 이동
        if hasattr(self, 'guide_window') and self.guide_window and self.guide_window.winfo_exists():
            self.guide_window.lift()
            self.guide_window.focus()
            return
        
        # 새 팝업 창 생성
        guide_window = tk.Toplevel(self)
        guide_window.title("📖 상품 가공 자동화 시스템 사용법")
        guide_window.geometry("900x1000")
        guide_window.configure(bg="white")
        
        self.guide_window = guide_window
        
        # 스크롤 가능한 컨테이너 생성
        canvas = tk.Canvas(guide_window, bg="white", highlightthickness=0)
        scrollbar = ttk.Scrollbar(guide_window, orient="vertical", command=canvas.yview)
        scrollable_frame = tk.Frame(canvas, bg="white")
        
        def update_scroll_region(event=None):
            canvas.update_idletasks()
            canvas.configure(scrollregion=canvas.bbox("all"))
        
        scrollable_frame.bind("<Configure>", update_scroll_region)
        
        canvas_window = canvas.create_window((0, 0), window=scrollable_frame, anchor="nw")
        
        def _on_canvas_configure(event):
            canvas_width = event.width
            canvas.itemconfig(canvas_window, width=canvas_width)
        
        canvas.bind("<Configure>", _on_canvas_configure)
        canvas.configure(yscrollcommand=scrollbar.set)
        
        # 마우스 휠 스크롤 바인딩
        def _on_mousewheel(event):
            canvas.yview_scroll(int(-1*(event.delta/120)), "units")
        
        canvas.bind("<MouseWheel>", _on_mousewheel)
        
        canvas.pack(side="left", fill="both", expand=True, padx=5, pady=5)
        scrollbar.pack(side="right", fill="y", pady=5)
        
        container = tk.Frame(scrollable_frame, bg="white", padx=20, pady=20)
        container.pack(fill="both", expand=True)
        
        # 제목
        title_frame = tk.Frame(container, bg="white")
        title_frame.pack(fill="x", pady=(0, 20))
        
        tk.Label(title_frame, text="📖 상품 가공 자동화 시스템 사용법", 
                 font=("맑은 고딕", 16, "bold"), bg="white", fg="#2C3E50").pack()
        
        tk.Label(title_frame, text="전체 워크플로우 및 단계별 작업 가이드", 
                 font=("맑은 고딕", 11), bg="white", fg="#7F8C8D").pack(pady=(5, 0))
        
        # 구분선
        tk.Frame(container, bg="#E0E0E0", height=2).pack(fill="x", pady=15)
        
        # ============================================================
        # STEP 0: 공통 데이터 준비
        # ============================================================
        section0 = tk.LabelFrame(container, text=" [STEP 0] 공통 데이터 ", 
                                 font=("맑은 고딕", 12, "bold"), bg="#ECEFF1", fg="#546E7A", 
                                 bd=2, relief="groove", padx=15, pady=15)
        section0.pack(fill="x", pady=10)
        
        step0_content = """
1️⃣ 엑셀 매핑 도구 (필수 시작점)
   • 목적: 도매처 원본 엑셀을 시스템 표준 포맷으로 변환
   • 입력: 도매처별 원본 엑셀 파일
   • 출력: _T0_I0.xlsx (표준 포맷)
   • 주의: 모든 작업의 시작점이므로 반드시 먼저 실행

2️⃣ 데이터 입고 도구 (2가지 용도)
   
   [용도 1] 엑셀 매핑 이후: 중복검사 및 필터링
   • 목적: DB에 이미 존재하는 상품코드와 중복 확인
   • 입력: _T0_I0.xlsx 파일
   • 기능: 중복된 상품코드 필터링하여 재가공 방지
   
   [용도 2] 가공 완료 후: DB에 저장
   • 목적: 완료된 상품명/이미지 데이터를 SQLite DB에 저장
   • 입력: 최종 가공 완료된 엑셀 파일
   • 출력: SQLite DB에 저장 (마켓 업로드 준비)
        """
        
        tk.Label(section0, text=step0_content.strip(), 
                 font=("맑은 고딕", 10), bg="#ECEFF1", fg="#333", 
                 justify="left", anchor="nw").pack(fill="x", padx=10, pady=5)
        
        # ============================================================
        # 상품명 가공 (Text) 워크플로우
        # ============================================================
        section_text = tk.LabelFrame(container, text=" 📝 상품명 가공 (Text) 워크플로우 ", 
                                     font=("맑은 고딕", 12, "bold"), bg="#E3F2FD", fg="#1976D2", 
                                     bd=2, relief="groove", padx=15, pady=15)
        section_text.pack(fill="x", pady=10)
        
        text_content = """
📌 파일 버전 규칙: _T{단계}_I{이미지단계}
   예: 상품_T0_I0.xlsx → 상품_T1_I0.xlsx → ... → 상품_T4(완)_I0.xlsx

Stage 1: 텍스트 기초 정제 (T0 → T1)
   • 입력: _T0_I0.xlsx
   • 출력: _T1_I0.xlsx
   • 옵션: (단건/실시간) 또는 (대량/배치)
   • 기능: 원본 상품명을 정제하고 판매형태 분류

Stage 2: 상세정보 & 재료 추출 (T1 → T2)
   
   ① 상세이미지 다운로드 (필수 전처리)
   • 입력: _T1_I*.xlsx
   • 출력: _T1_I* (동일 버전 유지)
   • 기능: 상세페이지 HTML에서 이미지 추출 및 다운로드
   
   ② 이미지 & 텍스트 분석
   • 입력: _T1_I*.xlsx
   • 출력: _T2_I*.xlsx
   • 옵션: (단건/실시간) 또는 (대량/배치)
   • 기능: 다운로드된 이미지와 텍스트를 AI로 분석하여 재료/특징 추출

Stage 3: 최종 상품명 생성 (T2 → T3)
   • 입력: _T2_I*.xlsx
   • 출력: _T3_I*.xlsx
   • 옵션: (단건/실시간) 또는 (대량/배치)
   • 기능: 추출된 정보를 바탕으로 최종 상품명 생성 (10개 후보)

Stage 4: 필터링 및 검수 (T3 → T4 → T4(완))
   
   ① 필터링 (금지어)
   • 입력: _T3_I*.xlsx
   • 출력: _T4_I*.xlsx
   • 기능: 금지어 및 필터링 규칙 적용하여 1차 검수
   
   ② 최종 검수
   • 입력: _T4_I*.xlsx
   • 출력: _T4(완)_I*.xlsx
   • 옵션: (단건/실시간) 또는 (대량/배치)
   • 기능: AI가 상품명을 순위별로 정렬하고 최종 검수
        """
        
        tk.Label(section_text, text=text_content.strip(), 
                 font=("맑은 고딕", 10), bg="#E3F2FD", fg="#333", 
                 justify="left", anchor="nw").pack(fill="x", padx=10, pady=5)
        
        # ============================================================
        # 이미지 가공 (Image) 워크플로우
        # ============================================================
        section_img = tk.LabelFrame(container, text=" 🖼️ 이미지 가공 (Image) 워크플로우 ", 
                                    font=("맑은 고딕", 12, "bold"), bg="#FFF3E0", fg="#F57C00", 
                                    bd=2, relief="groove", padx=15, pady=15)
        section_img.pack(fill="x", pady=10)
        
        img_content = """
📌 파일 버전 규칙: _T{텍스트단계}_I{단계}
   예: 상품_T3_I0.xlsx → 상품_T3_I1.xlsx → ... → 상품_T3_I5.xlsx

Stage 1: 배경 제거 (I0 → I1)
   • 입력: _T*_I0.xlsx
   • 출력: _T*_I1.xlsx
   • 기능: 이미지의 배경을 제거하여 누끼 이미지 생성

Stage 2: 라벨링 (I1 → I2)
   • 입력: _T*_I1.xlsx
   • 출력: _T*_I2.xlsx
   • 기능: 배경 제거된 이미지를 검수하고 라벨링 (휴먼 또는 AI)

Stage 3: 이미지 분석 전처리 (I2 → I3 → I4)
   
   ① 썸네일 구도·조명 분석
   • 입력: _T*_I2.xlsx
   • 출력: _T*_I3.xlsx
   • 옵션: (단건/실시간) 또는 (대량/배치)
   • 기능: 썸네일 이미지의 구도, 조명, 색조 등을 분석
   
   ② 배경 생성 프롬프트 작성
   • 입력: _T2이상_I3.xlsx (예: T2_I3, T4_I3, T4(완)_I3)
   • 출력: _T*_I4.xlsx
   • 옵션: (단건/실시간) 또는 (대량/배치)
   • 기능: 분석 결과를 바탕으로 배경 생성용 프롬프트 작성

Stage 4: 배경 생성 및 합성 (I4 → I5)
   
   ① 배경 생성
   • 입력: _T*_I4.xlsx
   • 출력: _T*_I5.xlsx
   • 기능: ComfyUI를 통해 AI 배경 이미지 생성
   
   ② 합성
   • 입력: _T*_I5.xlsx (배경 생성 완료된 파일)
   • 출력: _T*_I5.xlsx (동일 버전, 합성 이미지 경로 추가)
   • 기능: 누끼 이미지와 생성된 배경을 합성

Stage 5: 품질 검증 및 업로드 (I5 → I5(업완))
   
   ① 품질 검증
   • 입력: _T*_I5.xlsx
   • 출력: _T*_I5.xlsx (동일 버전, 최종 이미지 선택)
   • 기능: 합성된 이미지와 누끼 이미지, 원본 이미지를 비교하여 최종 선택
   • 선택 옵션:
     - [1] 누끼만 사용 (합성 품질이 낮은 경우)
     - [3] 둘 다 사용 (권장: 누끼 + 합성)
   
   ② 이미지 업로드 (R2)
   • 입력: _T*_I5.xlsx
   • 출력: _T*_I5(업완).xlsx
   • 기능: 최종 선택된 이미지를 Cloudflare R2에 업로드하고 URL 기록
        """
        
        tk.Label(section_img, text=img_content.strip(), 
                 font=("맑은 고딕", 10), bg="#FFF3E0", fg="#333", 
                 justify="left", anchor="nw").pack(fill="x", padx=10, pady=5)
        
        # ============================================================
        # 데이터 출고
        # ============================================================
        section_export = tk.LabelFrame(container, text=" 📤 데이터 출고 ", 
                                       font=("맑은 고딕", 12, "bold"), bg="#E8F5E9", fg="#388E3C", 
                                       bd=2, relief="groove", padx=15, pady=15)
        section_export.pack(fill="x", pady=10)
        
        export_content = """
목적: SQLite DB에서 마켓 업로드용 데이터를 내보냅니다.

기능:
   • 마켓 업로드용 데이터 출고
     - 카테고리 및 마켓/스토어 선택
     - 믹스url 우선, ST3_결과상품명 첫 줄 사용
     - 중복 방지 및 출고 이력 기록
   
   • 미완료 DB 재가공용 출고
     - ST3_결과상품명, 누끼url, 믹스url 중 공란이 있는 항목
     - 재가공 후 다시 입고 가능
        """
        
        tk.Label(section_export, text=export_content.strip(), 
                 font=("맑은 고딕", 10), bg="#E8F5E9", fg="#333", 
                 justify="left", anchor="nw").pack(fill="x", padx=10, pady=5)
        
        # ============================================================
        # 주의사항 및 팁
        # ============================================================
        section_tips = tk.LabelFrame(container, text=" ⚠️ 주의사항 및 팁 ", 
                                     font=("맑은 고딕", 12, "bold"), bg="#FFF9C4", fg="#F57C00", 
                                     bd=2, relief="groove", padx=15, pady=15)
        section_tips.pack(fill="x", pady=10)
        
        tips_content = """
⚠️ 중요 사항:
   • 작업 순서를 반드시 지켜주세요 (T0 → T1 → T2 → T3 → T4 → T4(완))
   • 이미지 작업은 텍스트 작업과 병렬로 진행 가능 (독립적)
   • 각 단계 완료 후 파일명 버전이 자동으로 업데이트됩니다
   • 작업 현황판에서 실시간으로 진행 상황을 확인할 수 있습니다

💡 효율적인 작업 팁:
   • 소량 데이터: (단건/실시간) 옵션 사용 (빠른 확인 가능)
   • 대량 데이터: (대량/배치) 옵션 사용 (50% 비용, 시간 소요)
   • 중간 저장: 각 단계 완료 후 백업 권장
   • 메모 기능: 작업 현황판에서 각 파일별 메모 작성 가능

📊 작업 현황판 활용:
   • Text 상태: 상품명 가공 진행 상황
   • Img 상태: 이미지 가공 진행 상황 (I3-1, I3-2, I5-1, I5-2 세부 단계 표시)
   • 더블 클릭: 파일별 메모 수정
   • 우클릭: 휴지통으로 이동 또는 기타 작업
        """
        
        tk.Label(section_tips, text=tips_content.strip(), 
                 font=("맑은 고딕", 10), bg="#FFF9C4", fg="#333", 
                 justify="left", anchor="nw").pack(fill="x", padx=10, pady=5)
        
        # 하단 여백
        tk.Frame(container, bg="white", height=20).pack()
        
        # 닫기 버튼
        btn_close = tk.Button(guide_window, text="닫기", 
                             command=guide_window.destroy,
                             bg="#546E7A", fg="white", 
                             font=("맑은 고딕", 10, "bold"),
                             relief="raised", cursor="hand2", padx=20, pady=8,
                             bd=1, highlightthickness=0)
        btn_close.pack(pady=10)
    
    def _build_trash_tab(self, parent):
        """휴지통 탭 UI 구성"""
        container = tk.Frame(parent, bg=COLOR_BG, padx=15, pady=15)
        container.pack(fill="both", expand=True)
        
        # 헤더
        header_frame = tk.Frame(container, bg=COLOR_BG)
        header_frame.pack(fill="x", pady=(0, 10))
        
        tk.Label(header_frame, text="🗑️ 휴지통 (삭제된 작업)", 
                 font=("맑은 고딕", 12, "bold"), bg=COLOR_BG, fg="#333").pack(side="left")
        
        btn_frame = tk.Frame(header_frame, bg=COLOR_BG)
        btn_frame.pack(side="right")
        
        tk.Button(btn_frame, text="🔄 새로고침", command=self.refresh_trash,
                  bg="#f1f3f5", fg="#333", relief="raised",
                  font=("맑은 고딕", 9), cursor="hand2", padx=10, pady=5,
                  bd=1, highlightthickness=0).pack(side="left", padx=5)
        
        tk.Button(btn_frame, text="🗑️ 선택 항목 완전 삭제", command=self.permanently_delete_selected,
                  bg="#dc3545", fg="white", relief="raised",
                  font=("맑은 고딕", 9, "bold"), cursor="hand2", padx=10, pady=5,
                  bd=1, highlightthickness=0).pack(side="left", padx=5)
        
        tk.Button(btn_frame, text="♻️ 선택 항목 복원", command=self.restore_selected_job,
                  bg="#28a745", fg="white", relief="raised",
                  font=("맑은 고딕", 9, "bold"), cursor="hand2", padx=10, pady=5,
                  bd=1, highlightthickness=0).pack(side="left", padx=5)
        
        # 트리뷰
        tree_frame = tk.Frame(container, bg=COLOR_BG)
        tree_frame.pack(fill="both", expand=True)
        
        columns = ("file", "text_stat", "text_time", "img_stat", "img_time", "deleted_at", "memo")
        self.trash_tree = ttk.Treeview(tree_frame, columns=columns, show='headings', height=20)
        
        # 컬럼 설정
        self.trash_tree.heading("file", text="파일 (Root Name)"); self.trash_tree.column("file", width=180, anchor="w")
        self.trash_tree.heading("text_stat", text="Text 상태"); self.trash_tree.column("text_stat", width=90, anchor="center")
        self.trash_tree.heading("text_time", text="Text 시간"); self.trash_tree.column("text_time", width=90, anchor="center")
        self.trash_tree.heading("img_stat", text="Img 상태"); self.trash_tree.column("img_stat", width=150, anchor="center")
        self.trash_tree.heading("img_time", text="Img 시간"); self.trash_tree.column("img_time", width=90, anchor="center")
        self.trash_tree.heading("deleted_at", text="삭제 시간"); self.trash_tree.column("deleted_at", width=120, anchor="center")
        self.trash_tree.heading("memo", text="비고(메모)"); self.trash_tree.column("memo", width=150, anchor="w")
        
        # 스크롤바
        scrollbar = ttk.Scrollbar(tree_frame, orient="vertical", command=self.trash_tree.yview)
        self.trash_tree.configure(yscrollcommand=scrollbar.set)
        
        self.trash_tree.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")
        
        # 우클릭 메뉴
        self.trash_tree.bind("<Button-3>", self.on_trash_tree_right_click)
        
        self.trash_context_menu = tk.Menu(self, tearoff=0)
        self.trash_context_menu.add_command(label="♻️ 복원", command=self.restore_selected_job)
        self.trash_context_menu.add_separator()
        self.trash_context_menu.add_command(label="🗑️ 완전 삭제", command=self.permanently_delete_selected)
        self.trash_context_menu.add_separator()
        self.trash_context_menu.add_command(label="🔄 새로고침", command=self.refresh_trash)
        
        # 초기 로드
        self.refresh_trash()
    
    def refresh_trash(self):
        """휴지통 목록을 새로고침합니다."""
        for item in self.trash_tree.get_children():
            self.trash_tree.delete(item)
        
        deleted_jobs = JobManager.load_deleted_jobs()
        if not deleted_jobs: return
        
        # 삭제 시간 순으로 정렬 (최신 삭제가 위로)
        sorted_jobs = sorted(deleted_jobs.items(), 
                           key=lambda x: x[1].get('deleted_at', ''), reverse=True)
        
        for filename, info in sorted_jobs:
            clean_name = filename.replace("_stage1_mapping", "").replace(".xlsx", "")
            
            t_stat = info.get("text_status", "-")
            t_time = info.get("text_time", "-")
            i_stat = info.get("image_status", "-")
            i_time = info.get("image_time", "-")
            deleted_at = info.get("deleted_at", "-")
            memo = info.get("memo", "")
            
            # Stage 3, Stage 4, Stage 5 세부 단계 정보가 있으면 가장 최근 단계만 표시
            img_s3_1 = info.get("image_s3_1_status", "-")
            img_s3_2 = info.get("image_s3_2_status", "-")
            img_s4_1 = info.get("image_s4_1_status", "-")
            img_s4_2 = info.get("image_s4_2_status", "-")
            img_s5_1 = info.get("image_s5_1_status", "-")
            img_s5_2 = info.get("image_s5_2_status", "-")
            
            # 가장 최근 단계만 표시 (우선순위: I5 > I4 > I3)
            parts = []
            if img_s5_1 != "-" or img_s5_2 != "-":
                # I5 단계 표시
                if img_s5_1 != "-":
                    parts.append(img_s5_1)
                if img_s5_2 != "-":
                    parts.append(img_s5_2)
                i_time = (info.get("image_s5_2_time") or 
                         info.get("image_s5_1_time") or 
                         i_time)
            elif img_s4_1 != "-" or img_s4_2 != "-":
                # I4 단계 표시
                if img_s4_1 != "-":
                    parts.append(img_s4_1)
                if img_s4_2 != "-":
                    parts.append(img_s4_2)
                i_time = (info.get("image_s4_2_time") or 
                         info.get("image_s4_1_time") or 
                         i_time)
            elif img_s3_1 != "-" or img_s3_2 != "-":
                # I3 단계 표시
                if img_s3_1 != "-":
                    parts.append(img_s3_1)
                if img_s3_2 != "-":
                    parts.append(img_s3_2)
                i_time = (info.get("image_s3_2_time") or 
                         info.get("image_s3_1_time") or 
                         i_time)
            
            if parts:
                i_stat = " / ".join(parts)
            
            self.trash_tree.insert("", "end", values=(
                clean_name, t_stat, t_time, i_stat, i_time, deleted_at, memo
            ))
    
    def on_trash_tree_right_click(self, event):
        """휴지통 트리뷰에서 우클릭 시 컨텍스트 메뉴를 표시합니다."""
        item_id = self.trash_tree.identify_row(event.y)
        if item_id:
            # 우클릭한 항목이 이미 선택되어 있으면 기존 선택 유지, 아니면 해당 항목만 선택
            current_selection = self.trash_tree.selection()
            if item_id not in current_selection:
                self.trash_tree.selection_set(item_id)
            try:
                self.trash_context_menu.tk_popup(event.x_root, event.y_root)
            finally:
                self.trash_context_menu.grab_release()
    
    def restore_selected_job(self):
        """선택된 항목을 복원합니다."""
        selected = self.trash_tree.selection()
        if not selected:
            messagebox.showwarning("선택 필요", "복원할 항목을 선택해주세요.")
            return
        
        item_id = selected[0]
        values = self.trash_tree.item(item_id, "values")
        if not values: return
        
        filename = values[0]
        
        # 확인 메시지
        result = messagebox.askyesno(
            "복원",
            f"[{filename}]\n\n이 항목을 복원하시겠습니까?"
        )
        
        if result:
            # 실제 DB 키를 찾아야 함
            deleted_jobs = JobManager.load_deleted_jobs()
            target_key = next((k for k in deleted_jobs.keys() if filename in k), filename)
            
            if JobManager.restore_job(target_key):
                messagebox.showinfo("완료", f"[{filename}]\n복원되었습니다.")
                self.refresh_trash()
                self.refresh_dashboard()  # 메인 대시보드도 새로고침
            else:
                messagebox.showerror("오류", "복원에 실패했습니다.")
    
    def permanently_delete_selected(self):
        """선택된 항목을 완전히 삭제합니다."""
        selected = self.trash_tree.selection()
        if not selected:
            messagebox.showwarning("선택 필요", "완전히 삭제할 항목을 선택해주세요.")
            return
        
        item_id = selected[0]
        values = self.trash_tree.item(item_id, "values")
        if not values: return
        
        filename = values[0]
        
        # 경고 메시지
        result = messagebox.askyesno(
            "⚠️ 완전 삭제",
            f"[{filename}]\n\n이 항목을 완전히 삭제하시겠습니까?\n\n이 작업은 되돌릴 수 없습니다!",
            icon="warning"
        )
        
        if result:
            # 실제 DB 키를 찾아야 함
            deleted_jobs = JobManager.load_deleted_jobs()
            target_key = next((k for k in deleted_jobs.keys() if filename in k), filename)
            
            if JobManager.permanently_delete_job(target_key):
                messagebox.showinfo("완료", f"[{filename}]\n완전히 삭제되었습니다.")
                self.refresh_trash()
            else:
                messagebox.showerror("오류", "삭제에 실패했습니다.")

    def _add_stage_group(self, parent, title, color, buttons):
        frame = tk.LabelFrame(parent, text=f" {title} ", font=("맑은 고딕", 11, "bold"), bg=COLOR_BG, fg=color, bd=2, relief="groove")
        frame.pack(fill="x", pady=6, ipady=4)
        btn_frame = tk.Frame(frame, bg=COLOR_BG)
        btn_frame.pack(fill="x", padx=10, pady=2)
        for txt, key in buttons:
            self._add_btn(btn_frame, txt, key, color, width=None, side="top")

    def _add_separator_label(self, parent, key_prefix):
        """구분선 레이블 생성 및 참조 저장"""
        separator_key = f"{key_prefix}_separator"
        separator = tk.Label(parent, text="|", bg=COLOR_BG, font=("맑은 고딕", 10), fg="#999")
        separator.pack(side="left", padx=8)
        
        # 구분선 참조 저장
        if key_prefix not in self.button_refs:
            self.button_refs[key_prefix] = {"single": None, "batch": None, "separator": None}
        self.button_refs[key_prefix]["separator"] = separator
        
        # 초기 상태: 고급 모드가 꺼져있으면 숨김
        if not self.advanced_mode:
            separator.pack_forget()
    
    def _add_btn(self, parent, text, key, color, width=None, side="top", btn_type=None):
        """
        버튼 생성
        
        Args:
            parent: 부모 위젯
            text: 버튼 텍스트
            key: 스크립트 키
            color: 버튼 색상
            width: 버튼 너비
            side: 배치 방향 (left, right, top)
            btn_type: 버튼 타입 ("single": 단건/실시간, "batch": 대량/배치, None: 기타)
        """
        info = SCRIPTS.get(key)
        is_ready = False
        if info:
            # 절대 경로인지 확인
            folder_path = Path(info["folder"]) if info["folder"] else None
            
            if folder_path and folder_path.is_absolute():
                # 절대 경로인 경우 그대로 사용
                target_path = folder_path / info["file"]
            elif getattr(sys, "frozen", False):
                if hasattr(sys, "_MEIPASS"):
                    # --onefile 모드: 임시 디렉토리
                    if info["folder"]:
                        target_path = Path(sys._MEIPASS) / info["folder"] / info["file"]
                    else:
                        target_path = Path(sys._MEIPASS) / info["file"]
                else:
                    # --onedir 모드: 실행 파일과 같은 디렉토리
                    if info["folder"]:
                        target_path = BASE_DIR / info["folder"] / info["file"]
                    else:
                        target_path = BASE_DIR / info["file"]
            else:
                # 일반 실행
                if info["folder"]:
                    target_path = BASE_DIR / info["folder"] / info["file"]
                else:
                    target_path = BASE_DIR / info["file"]
            is_ready = target_path.exists()
        
        btn_bg = color if is_ready else "#DDDDDD"
        btn_fg = "white" if is_ready else "#888888"
        state = "normal" if is_ready else "disabled"
        cursor = "hand2" if is_ready else "arrow"

        # 버튼 클릭 이벤트 래퍼 함수 (안전성 및 디버깅)
        def safe_run_script(script_key):
            try:
                self.run_script(script_key)
            except Exception as e:
                messagebox.showerror("실행 오류", f"스크립트 실행 중 오류가 발생했습니다:\n{str(e)}")
                import traceback
                print(f"[ERROR] 스크립트 실행 오류: {traceback.format_exc()}")
        
        # 버튼 타입에 따라 너비 조정
        # 고급 모드 OFF이고 배치 버튼인 경우 더 넓게 표시
        if btn_type == "batch" and not self.advanced_mode:
            # 단건 버튼이 없을 때는 더 넓게 (기본 너비의 약 2배)
            adjusted_width = 34 if width and width == 16 else (width * 2 + 2 if width else None)
        else:
            adjusted_width = width
        
        btn = tk.Button(parent, text=text, bg=btn_bg, fg=btn_fg, font=("맑은 고딕", 10, "normal"), 
            relief="raised",  # flat → raised로 변경하여 클릭 가능 영역 명확화
            width=adjusted_width, height=1, cursor=cursor, state=state,
            activebackground=color, activeforeground="white",
            command=lambda k=key: safe_run_script(k),
            padx=6, pady=3, bd=1, highlightthickness=0  # 시각적 피드백 개선
        )
        
        pack_opts = {"pady": 2, "padx": 2}
        if side == "top": pack_opts.update({"fill": "x", "anchor": "center"})
        else: pack_opts["side"] = side
        btn.pack(**pack_opts)
        
        # 버튼 타입별 참조 저장 (단건/배치 버튼만)
        if btn_type in ["single", "batch"]:
            # key에서 prefix 추출 (예: "Text_S1_API" -> "Text_S1", "Img_S3_Thumbnail_Analysis_GUI" -> "Img_S3_Thumbnail")
            parts = key.split("_")
            if len(parts) >= 3:
                # 마지막 부분(API, GUI, Batch 등) 제거하고 prefix 생성
                if parts[-1] in ["API", "GUI", "Batch"]:
                    key_prefix = "_".join(parts[:-1])
                else:
                    key_prefix = "_".join(parts[:-1])
            else:
                key_prefix = "_".join(parts[:-1]) if len(parts) > 1 else key
            
            if key_prefix not in self.button_refs:
                self.button_refs[key_prefix] = {"single": None, "batch": None, "separator": None}
            self.button_refs[key_prefix][btn_type] = btn
            
            # 배치 버튼인 경우 초기 너비 설정 (고급 모드 OFF일 때)
            if btn_type == "batch" and not self.advanced_mode:
                btn.config(width=34)  # 단건 버튼이 없을 때 더 넓게
            
            # 고급 모드가 꺼져있고 단건 버튼이면 숨김
            if btn_type == "single" and not self.advanced_mode:
                btn.pack_forget()
        
        if info:
            tooltip_text = info.get("desc", "")
            if not is_ready: tooltip_text += "\n(※ 파일이 없거나 경로가 잘못되었습니다)"
            ToolTip(btn, tooltip_text)
        else: ToolTip(btn, "준비 중인 기능입니다.")

    def run_script(self, script_key):
        if not script_key: 
            messagebox.showinfo("준비중", "이 기능은 아직 준비 중입니다.")
            return
        info = SCRIPTS.get(script_key)
        if not info: return
        
        # Stage 3 배경 생성 프롬프트 작업 전 확인 메시지
        if script_key in ["Img_S3_Preprocess_GUI", "Img_S3_Preprocess_Batch"]:
            result = messagebox.askyesno(
                "⚠️ 작업 전 확인",
                "상품명 Stage 2 상세설명 분석이 완료된 T2 이상 엑셀로만 진행하세요.\n\n"
                "• 입력 파일: T2 이상 버전 (예: T2_I3, T4_I3, T4(완)_I3)\n"
                "• 필수 컬럼: ST2_JSON, view_point\n\n"
                "계속 진행하시겠습니까?",
                icon="question"
            )
            if not result:
                return
        
        # PyInstaller 환경에서 경로 처리
        folder_path = Path(info["folder"]) if info["folder"] else None
        
        # 절대 경로인지 확인
        if folder_path and folder_path.is_absolute():
            # 절대 경로인 경우 그대로 사용
            target_path = folder_path / info["file"]
            work_dir = folder_path
        elif getattr(sys, "frozen", False):
            if hasattr(sys, "_MEIPASS"):
                # --onefile 모드: 임시 디렉토리에서 파일 찾기
                if info["folder"]:
                    target_path = Path(sys._MEIPASS) / info["folder"] / info["file"]
                    work_dir = Path(sys._MEIPASS) / info["folder"]
                else:
                    target_path = Path(sys._MEIPASS) / info["file"]
                    work_dir = Path(sys._MEIPASS)
            else:
                # --onedir 모드: 실행 파일과 같은 디렉토리
                if info["folder"]:
                    target_path = BASE_DIR / info["folder"] / info["file"]
                    work_dir = BASE_DIR / info["folder"]
                else:
                    target_path = BASE_DIR / info["file"]
                    work_dir = BASE_DIR
        else:
            # 일반 실행
            if info["folder"]:
                target_path = BASE_DIR / info["folder"] / info["file"]
                work_dir = BASE_DIR / info["folder"]
            else:
                target_path = BASE_DIR / info["file"]
                work_dir = BASE_DIR

        if not target_path.exists():
            self._update_status("error", "파일을 찾을 수 없습니다.")
            messagebox.showinfo("파일 없음", f"실행 파일을 찾을 수 없습니다.\n\n예상 경로:\n{target_path}\n\nBASE_DIR: {BASE_DIR}\n\nPyInstaller 모드: {getattr(sys, 'frozen', False)}")
            self.after(2000, lambda: self._update_status("ready", "시스템 준비 완료"))
            return

        try:
            self._update_status("running", f"실행 중... [{info['file']}]")
            self.config(cursor="watch")
            self.update_idletasks()
            
            # Python 인터프리터 경로 찾기
            required_modules = SCRIPT_REQUIRED_MODULES.get(script_key, [])
            python_cmd = self._find_python_executable(required_modules=required_modules)
            
            if not python_cmd:
                # Python 또는 필수 모듈을 찾을 수 없는 경우
                self._update_status("error", "실행 가능한 Python 환경을 찾을 수 없습니다.")
                error_msg = (
                    "실행 가능한 Python 환경을 찾을 수 없습니다.\n\n"
                    "하위 스크립트를 실행하려면 Python이 설치되어 있어야 하며,\n"
                    "필요한 모듈도 같은 Python에 설치되어 있어야 합니다.\n\n"
                    "해결 방법:\n"
                    "1. Python이 설치되어 있는지 확인하세요\n"
                    "2. Python이 PATH 환경 변수에 추가되어 있는지 확인하세요\n"
                    "3. 명령 프롬프트에서 'python --version' 명령어가 작동하는지 확인하세요\n"
                    f"4. 필요 모듈 설치: {', '.join(required_modules) if required_modules else '없음'}\n\n"
                    f"스크립트 경로: {target_path}"
                )
                messagebox.showerror("실행 오류", error_msg)
                self._reset_ui_state()
                return
            
            launch_env = os.environ.copy()
            launch_env["PYTHONUTF8"] = "1"
            launch_env["PYTHONIOENCODING"] = "utf-8"
            if script_key == "Ownerclan_Converter":
                qt_plugin_path = self._get_qt_plugin_path(python_cmd)
                if qt_plugin_path:
                    launch_env["QT_QPA_PLATFORM_PLUGIN_PATH"] = qt_plugin_path

            # 하위 스크립트 실행
            subprocess.Popen(
                [python_cmd, "-X", "utf8", str(target_path)],
                cwd=str(work_dir),
                env=launch_env,
            )
            self.after(3000, lambda: self._reset_ui_state())
        except FileNotFoundError as e:
            # Python을 찾을 수 없는 경우
            self._update_status("error", "Python을 찾을 수 없습니다.")
            error_msg = (
                f"Python 실행 파일을 찾을 수 없습니다.\n\n"
                f"오류: {e}\n\n"
                f"해결 방법:\n"
                f"1. Python이 설치되어 있는지 확인하세요\n"
                f"2. Python이 PATH 환경 변수에 추가되어 있는지 확인하세요\n"
                f"3. 명령 프롬프트에서 'python --version' 명령어가 작동하는지 확인하세요\n\n"
                f"스크립트 경로: {target_path}"
            )
            messagebox.showerror("실행 오류", error_msg)
            self._reset_ui_state()
        except Exception as e:
            self._update_status("error", "실행 오류 발생")
            messagebox.showerror("실행 오류", f"실행 실패:\n{e}\n\n경로: {target_path}")
            self._reset_ui_state()

    def _iter_python_candidates(self):
        """실행 가능한 Python 후보 경로를 중복 없이 반환합니다."""
        seen = set()

        def add_candidate(candidate):
            if not candidate:
                return
            candidate = str(candidate).strip()
            if not candidate or candidate in seen:
                return
            seen.add(candidate)
            yield candidate

        project_python_candidates = [
            BASE_DIR / ".venv" / "Scripts" / "python.exe",
            BASE_DIR / "venv" / "Scripts" / "python.exe",
            BASE_DIR / "python.exe",
        ]
        for path in project_python_candidates:
            if path.exists():
                yield from add_candidate(path)

        current_python = str(sys.executable).strip()
        current_python_lower = current_python.lower()
        base_dir_lower = str(BASE_DIR).lower()
        is_venv_python = ".venv" in current_python_lower or "\\venv\\" in current_python_lower
        is_project_python = current_python_lower.startswith(base_dir_lower)

        if is_project_python or not is_venv_python:
            yield from add_candidate(current_python)

        for cmd in ["python", "python3", "py"]:
            yield from add_candidate(shutil.which(cmd))

        if is_venv_python:
            yield from add_candidate(current_python)

        if not getattr(sys, "frozen", False):
            return

        python_home = os.environ.get("PYTHON_HOME") or os.environ.get("PYTHONHOME")
        if python_home:
            yield from add_candidate(os.path.join(python_home, "python.exe"))

        if sys.platform == "win32":
            common_paths = [
                r"C:\Python312\python.exe",
                r"C:\Python311\python.exe",
                r"C:\Python310\python.exe",
                r"C:\Program Files\Python312\python.exe",
                r"C:\Program Files\Python311\python.exe",
                r"C:\Program Files\Python310\python.exe",
                r"C:\Program Files (x86)\Python312\python.exe",
                r"C:\Program Files (x86)\Python311\python.exe",
                r"C:\Program Files (x86)\Python310\python.exe",
            ]
            for path in common_paths:
                if os.path.exists(path):
                    yield from add_candidate(path)

        if current_python and not is_project_python:
            yield from add_candidate(current_python)

    def _python_supports_modules(self, python_path, required_modules):
        """주어진 Python이 필요한 모듈을 import할 수 있는지 확인합니다."""
        if not required_modules:
            return True, ""

        import_code = "\n".join([f"import {module}" for module in required_modules])
        try:
            result = subprocess.run(
                [python_path, "-X", "utf8", "-c", import_code],
                capture_output=True,
                timeout=8,
                text=True,
                encoding="utf-8",
                errors="replace",
            )
        except Exception as e:
            return False, str(e)

        if result.returncode == 0:
            return True, ""

        error_text = (result.stderr or result.stdout or "").strip()
        return False, error_text

    def _get_qt_plugin_path(self, python_path):
        """선택한 Python에서 Qt plugin 경로를 조회합니다."""
        code = "from PyQt5.QtCore import QLibraryInfo; print(QLibraryInfo.location(QLibraryInfo.PluginsPath))"
        try:
            result = subprocess.run(
                [python_path, "-X", "utf8", "-c", code],
                capture_output=True,
                timeout=8,
                text=True,
                encoding="utf-8",
                errors="replace",
            )
        except Exception:
            return ""

        if result.returncode != 0:
            return ""

        return (result.stdout or "").strip()

    def _find_python_executable(self, required_modules=None):
        """
        Python 실행 파일 경로를 찾습니다.
        여러 방법을 순차적으로 시도합니다.
        """
        for python_path in self._iter_python_candidates():
            try:
                result = subprocess.run(
                    [python_path, "--version"],
                    capture_output=True,
                    timeout=5,
                    text=True,
                    encoding="utf-8",
                    errors="replace",
                )
                if result.returncode != 0:
                    continue
            except Exception:
                continue

            supported, _ = self._python_supports_modules(python_path, required_modules)
            if supported:
                return python_path

        return None
    
    def _reset_ui_state(self):
        self.config(cursor="")
        self._update_status("ready", "시스템 준비 완료")
    
    def _create_status_bar(self):
        status_frame = tk.Frame(self, bg=COLOR_STATUS_BAR, height=35, bd=1, relief="sunken")
        status_frame.pack(side="bottom", fill="x")
        status_frame.pack_propagate(False)
        self.lbl_status_icon = tk.Label(status_frame, text="🟢", bg=COLOR_STATUS_BAR, font=("맑은 고딕", 12))
        self.lbl_status_icon.pack(side="left", padx=(10, 0))
        self.lbl_status_text = tk.Label(status_frame, text="시스템 준비 완료", bg=COLOR_STATUS_BAR, fg="#333333", font=("맑은 고딕", 10, "bold"))
        self.lbl_status_text.pack(side="left", padx=5)
        tk.Label(status_frame, text=f"Root: {BASE_DIR}", font=("Consolas", 8), bg=COLOR_STATUS_BAR, fg="#999").pack(side="right", padx=10)

    def toggle_advanced_mode(self):
        """고급 모드 토글 (단건/실시간 버튼 표시/숨김)"""
        self.advanced_mode = not self.advanced_mode
        
        # 버튼 상태 업데이트
        for key_prefix, refs in self.button_refs.items():
            single_btn = refs.get("single")
            batch_btn = refs.get("batch")
            separator = refs.get("separator")
            
            if not single_btn or not batch_btn:
                continue
            
            # 배치 버튼의 부모 프레임 가져오기
            parent = batch_btn.master
            
            if self.advanced_mode:
                # 고급 모드 ON: 단건 버튼 표시, 구분선 표시
                # 배치 버튼 너비를 원래대로 변경
                batch_btn.config(width=16)
                
                # 배치 버튼의 pack 정보 확인
                batch_info = batch_btn.pack_info()
                
                # 배치 버튼을 임시로 제거
                batch_btn.pack_forget()
                
                # 순서대로 다시 추가: 단건 버튼 -> 구분선 -> 배치 버튼
                single_btn.pack(side="left", padx=2)
                if separator:
                    separator.pack(side="left", padx=8)
                # 배치 버튼을 다시 추가 (원래 위치)
                batch_btn.pack(side="left", padx=2)
            else:
                # 고급 모드 OFF: 단건 버튼 숨김, 구분선 숨김
                single_btn.pack_forget()
                if separator:
                    separator.pack_forget()
                
                # 배치 버튼 너비 확대 (단건 버튼이 없을 때)
                batch_btn.config(width=34)
                # 배치 버튼은 이미 표시되어 있으므로 재배치 불필요
        
        # UI 업데이트
        self.update_idletasks()
        
        # 토글 버튼 텍스트 및 색상 업데이트
        if self.advanced_mode:
            self.advanced_mode_btn.config(text="⚙️ 고급 모드 ON", bg="#28a745")
        else:
            self.advanced_mode_btn.config(text="⚙️ 고급 모드 OFF", bg="#6c757d")
        
        # 상태 업데이트
        mode_text = "ON (단건/실시간 버튼 표시)" if self.advanced_mode else "OFF (대량/배치만 표시)"
        self._update_status("ready", f"고급 모드: {mode_text}")

    def _update_status(self, state, message):
        if state == "ready":
            self.lbl_status_icon.config(text="🟢")
            self.lbl_status_text.config(text=message, fg="#28a745")
        elif state == "running":
            self.lbl_status_icon.config(text="🚀")
            self.lbl_status_text.config(text=message, fg="#007bff")
        elif state == "error":
            self.lbl_status_icon.config(text="❌")
            self.lbl_status_text.config(text=message, fg="#dc3545")

    def _warn_if_wrong_python_environment(self):
        """다른 프로젝트 가상환경에서 런처가 실행된 경우 경고합니다."""
        current_python = str(sys.executable).strip()
        current_python_lower = current_python.lower()
        base_dir_lower = str(BASE_DIR).lower()

        if base_dir_lower in current_python_lower:
            return

        if ".venv" not in current_python_lower and "\\venv\\" not in current_python_lower:
            return

        preferred_python = self._find_python_executable()
        preferred_text = preferred_python if preferred_python else "찾지 못함"

        messagebox.showwarning(
            "실행 환경 경고",
            "현재 런처가 다른 프로젝트의 가상환경 Python으로 실행되었습니다.\n\n"
            f"현재 Python:\n{current_python}\n\n"
            "이 상태에서는 일부 버튼 실행이 꼬일 수 있습니다.\n"
            "상품가공프로그램 폴더 기준 Python으로 실행하세요.\n\n"
            f"권장 Python:\n{preferred_text}"
        )

if __name__ == "__main__":
    relaunch_with_preferred_python_if_needed()
    app = PipelineLauncher()
    app.mainloop()
