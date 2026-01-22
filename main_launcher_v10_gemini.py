"""
main_launcher_v10_gemini.py

상품 가공 파이프라인 통합 런처 v10 (GPT vs Gemini 비교 테스트 버전)
- GPT-5 캐시 버전과 Gemini 2.5 Flash-Lite 버전 선택 가능
- 비용/품질 비교 테스트용
"""

import os
import sys
import subprocess
import tkinter as tk
from tkinter import ttk, messagebox, simpledialog
from pathlib import Path
import json
from datetime import datetime
import shutil

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
        "folder": r"C:\Users\kohaz\Desktop\Python\.cursor\260117_오너클랜_이셀러스변환\.claude\skills\esellers-converter\scripts\gui",
        "file": "main.py",
        "desc": "오너클랜 → 이셀러스 변환기"
    },
    "Upload_Mapper": {
        "folder": "Upload_Mapper",
        "file": "main.py",
        "desc": "상품 등록 맵퍼"
    },
    "Merge_Versions": {
        "folder": "",
        "file": "merge_excel_versions.py",
        "desc": "엑셀 버전 병합 도구"
    },

    # --- [A] 텍스트 가공 (GPT 캐시 버전) ---
    "Text_S1_API": {
        "folder": "stage1_product_name",
        "file": "stage1_api_ver_runner.py",
        "desc": "[GPT/건별] Stage 1 상품명 정제"
    },
    "Text_S1_Batch_GPT": {
        "folder": "stage1_product_name",
        "file": "Gui_stage1_batch_Casche.py",
        "desc": "[GPT/배치] Stage 1 상품명 정제"
    },
    "Text_S2_Extract": {
        "folder": "stage2_product_name",
        "file": "Product_detaildescription.py",
        "desc": "★ Stage 2 필수 전처리\n상세페이지 HTML에서 이미지를 추출"
    },
    "Text_S2_GUI": {
        "folder": "stage2_product_name",
        "file": "stage2_LLM_gui.py",
        "desc": "[GPT/건별] Stage 2 이미지+텍스트 분석"
    },
    "Text_S2_Batch_GPT": {
        "folder": "stage2_product_name",
        "file": "stage2_batch_api_Cachever_resize.py",
        "desc": "[GPT/배치] Stage 2 이미지+텍스트 분석"
    },
    "Text_S3_GUI": {
        "folder": "stage3_product_name",
        "file": "stage3_LLM_gui.py",
        "desc": "[GPT/건별] Stage 3 상품명 생성"
    },
    "Text_S3_Batch_GPT": {
        "folder": "stage3_product_name",
        "file": "stage3_batch_api_Casche.py",
        "desc": "[GPT/배치] Stage 3 상품명 생성"
    },
    "Text_S4_Filter": {
        "folder": "stage4_product_name",
        "file": "stage4_1_filter_gui.py",
        "desc": "[전처리] 금지어 및 필터링"
    },
    "Text_S4_2_GUI": {
        "folder": "stage4_product_name",
        "file": "stage4_2_gui.py",
        "desc": "[GPT/건별] Stage 4 검수 및 정렬"
    },
    "Text_S4_2_Batch_GPT": {
        "folder": "stage4_product_name",
        "file": "stage4_2_batch_api_Casche.py",
        "desc": "[GPT/배치] Stage 4 검수 및 정렬"
    },

    # --- [A] 텍스트 가공 (Gemini 버전) ---
    "Text_S1_Batch_Gemini": {
        "folder": "stage1_product_name",
        "file": "Gui_stage1_batch_gemini.py",
        "desc": "[Gemini/배치] Stage 1 상품명 정제\n비용 3.7배 절감"
    },
    "Text_S2_Batch_Gemini": {
        "folder": "stage2_product_name",
        "file": "stage2_batch_api_gemini.py",
        "desc": "[Gemini/배치] Stage 2 이미지+텍스트 분석\n비용 3.7배 절감"
    },
    "Text_S3_Batch_Gemini": {
        "folder": "stage3_product_name",
        "file": "stage3_batch_api_gemini.py",
        "desc": "[Gemini/배치] Stage 3 상품명 생성\n비용 3.7배 절감"
    },
    "Text_S4_2_Batch_Gemini": {
        "folder": "stage4_product_name",
        "file": "stage4_2_batch_api_gemini.py",
        "desc": "[Gemini/배치] Stage 4 검수 및 정렬\n비용 3.7배 절감"
    },

    # --- [B] 이미지 가공 (GPT 캐시 버전) ---
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
    "Img_S3_Thumbnail_Analysis_GUI": {
        "folder": "IMG_stage3",
        "file": "IMG_analysis_gui_Casche.py",
        "desc": "[GPT/건별] 썸네일 구도 분석"
    },
    "Img_S3_Thumbnail_Analysis_Batch_GPT": {
        "folder": "IMG_stage3",
        "file": "IMG_Batch_analysis_gui_Casche_resize.py",
        "desc": "[GPT/배치] 썸네일 구도 분석"
    },
    "Img_S3_Preprocess_GUI": {
        "folder": "IMG_stage3",
        "file": "bg_prompt_gui.py",
        "desc": "[GPT/건별] 배경 생성 프롬프트"
    },
    "Img_S3_Preprocess_Batch_GPT": {
        "folder": "IMG_stage3",
        "file": "bg_Batch_prompt_gui_Casche_resize.py",
        "desc": "[GPT/배치] 배경 생성 프롬프트"
    },
    "Img_S4_BG_Generate": {
        "folder": "IMG_stage4",
        "file": "Bg_Generation_V2.py",
        "desc": "ComfyUI를 통해 배경 이미지를 생성합니다."
    },
    "Img_S4_Composite": {
        "folder": "IMG_stage4",
        "file": "IMG_mixing.py",
        "desc": "누끼 이미지와 생성된 배경을 합성합니다."
    },
    "Img_S5_Review": {
        "folder": "IMG_stage5",
        "file": "Stage5_Review.py",
        "desc": "합성된 이미지와 원본 이미지를 비교하여 최종 선택합니다."
    },
    "Img_S5_Upload": {
        "folder": "IMG_stage5",
        "file": "cloudflare_upload_gui.py",
        "desc": "최종 이미지를 Cloudflare R2에 업로드합니다."
    },

    # --- [B] 이미지 가공 (Gemini 버전) ---
    "Img_S3_Thumbnail_Analysis_Batch_Gemini": {
        "folder": "IMG_stage3",
        "file": "IMG_Batch_analysis_gui_gemini.py",
        "desc": "[Gemini/배치] 썸네일 구도 분석\n비용 3.7배 절감"
    },
    "Img_S3_Preprocess_Batch_Gemini": {
        "folder": "IMG_stage3",
        "file": "bg_Batch_prompt_gui_gemini.py",
        "desc": "[Gemini/배치] 배경 생성 프롬프트\n비용 3.7배 절감"
    },
}

# --- UI 디자인 ---
COLOR_BG = "#F0F2F5"
COLOR_HEADER = "#2C3E50"
COLOR_COMMON = "#546E7A"
COLOR_STATUS_BAR = "#E9ECEF"

# 스테이지별 포인트 컬러
COLOR_S1 = "#1976D2"  # 파랑
COLOR_S2 = "#0097A7"  # 청록
COLOR_S3 = "#388E3C"  # 초록
COLOR_S4 = "#7B1FA2"  # 보라
COLOR_S5 = "#F57C00"  # 주황

# 모델별 색상
COLOR_GPT = "#10a37f"      # OpenAI 녹색
COLOR_GEMINI = "#4285f4"   # Google 파랑

def get_base_dir() -> Path:
    if getattr(sys, "frozen", False):
        if hasattr(sys, "_MEIPASS"):
            return Path(sys._MEIPASS)
        else:
            return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent

BASE_DIR = get_base_dir()


# ========================================================
# [CORE] 작업 이력 관리자 (JSON DB)
# ========================================================
class JobManager:
    DB_FILE = os.path.join(BASE_DIR, "job_history.json")
    DELETED_DB_FILE = os.path.join(BASE_DIR, "job_history_deleted.json")

    @classmethod
    def load_jobs(cls):
        if not os.path.exists(cls.DB_FILE):
            return {}
        try:
            with open(cls.DB_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception:
            return {}

    @classmethod
    def init_db(cls):
        print(f"[DEBUG] MainLauncher DB Path: {cls.DB_FILE}")

        if not os.path.exists(cls.DB_FILE):
            with open(cls.DB_FILE, 'w', encoding='utf-8') as f:
                json.dump({}, f, ensure_ascii=False, indent=4)

        if not os.path.exists(cls.DELETED_DB_FILE):
            with open(cls.DELETED_DB_FILE, 'w', encoding='utf-8') as f:
                json.dump({}, f, ensure_ascii=False, indent=4)

    @classmethod
    def update_job_memo(cls, filename, memo_text):
        data = cls.load_jobs()
        if filename in data:
            data[filename]["memo"] = memo_text
            with open(cls.DB_FILE, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=4)

    @classmethod
    def load_deleted_jobs(cls):
        if not os.path.exists(cls.DELETED_DB_FILE):
            return {}
        try:
            with open(cls.DELETED_DB_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception:
            return {}

    @classmethod
    def delete_job(cls, filename):
        data = cls.load_jobs()
        if filename not in data:
            return False

        deleted_item = data.pop(filename)
        deleted_item["deleted_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        with open(cls.DB_FILE, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)

        deleted_data = cls.load_deleted_jobs()
        deleted_data[filename] = deleted_item
        with open(cls.DELETED_DB_FILE, 'w', encoding='utf-8') as f:
            json.dump(deleted_data, f, ensure_ascii=False, indent=4)

        return True

    @classmethod
    def restore_job(cls, filename):
        deleted_data = cls.load_deleted_jobs()
        if filename not in deleted_data:
            return False

        restored_item = deleted_data.pop(filename)
        restored_item.pop("deleted_at", None)

        with open(cls.DELETED_DB_FILE, 'w', encoding='utf-8') as f:
            json.dump(deleted_data, f, ensure_ascii=False, indent=4)

        data = cls.load_jobs()
        data[filename] = restored_item
        with open(cls.DB_FILE, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)

        return True

    @classmethod
    def permanently_delete_job(cls, filename):
        deleted_data = cls.load_deleted_jobs()
        if filename not in deleted_data:
            return False

        deleted_data.pop(filename)
        with open(cls.DELETED_DB_FILE, 'w', encoding='utf-8') as f:
            json.dump(deleted_data, f, ensure_ascii=False, indent=4)

        return True


# ========================================================
# 툴팁 클래스
# ========================================================
class ToolTip:
    def __init__(self, widget, text):
        self.widget = widget
        self.text = text
        self.tipwindow = None
        self._after_id = None
        self.widget.bind("<Enter>", self._on_enter)
        self.widget.bind("<Leave>", self.hide_tip)
        self.widget.bind("<Button-1>", self.hide_tip)

    def _on_enter(self, event=None):
        if self._after_id:
            self.widget.after_cancel(self._after_id)
        self._after_id = self.widget.after(500, self.show_tip)

    def show_tip(self, event=None):
        if self.tipwindow or not self.text: return
        x = self.widget.winfo_rootx() + 20
        y = self.widget.winfo_rooty() + 30
        self.tipwindow = tw = tk.Toplevel(self.widget)
        tw.wm_overrideredirect(True)
        tw.attributes("-topmost", True)
        tw.wm_geometry(f"+{x}+{y}")
        label = tk.Label(tw, text=self.text, justify='left',
                         background="#ffffe0", relief='solid', borderwidth=0,
                         font=("맑은 고딕", 9))
        label.pack(ipadx=5, ipady=2)
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
        self.title("상품 가공 파이프라인 통합 런처 v10 (GPT vs Gemini)")
        self.geometry("1450x1050")
        self.configure(bg=COLOR_BG)

        self.status_var = tk.StringVar(value="System Ready...")
        self.advanced_mode = False
        self.button_refs = {}

        self._setup_styles()
        self._init_ui()

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

        title_label = tk.Label(header, text="🚀 상품 가공 자동화 시스템 (GPT vs Gemini 테스트)", font=("맑은 고딕", 15, "bold"), bg=COLOR_HEADER, fg="white")
        title_label.pack(side="left", padx=20)

        header_right = tk.Frame(header, bg=COLOR_HEADER)
        header_right.pack(side="right", padx=20)

        # 고급 모드 토글 버튼
        self.advanced_mode_btn = tk.Button(
            header_right,
            text="⚙️ 고급 모드 OFF",
            command=self.toggle_advanced_mode,
            bg="#6c757d", fg="white", font=("맑은 고딕", 9, "bold"),
            relief="raised", cursor="hand2", padx=12, pady=5, bd=1, highlightthickness=0
        )
        self.advanced_mode_btn.pack(side="right", padx=(10, 0))
        ToolTip(self.advanced_mode_btn, "고급 모드 ON: 단건/실시간 버튼도 표시됩니다.\nOFF: 대량/배치 버튼만 표시됩니다.")

        # 2. 메인 컨테이너
        main_pane = tk.PanedWindow(self, orient="horizontal", bg=COLOR_BG, sashwidth=5)
        main_pane.pack(fill="both", expand=True, padx=10, pady=10)

        # [왼쪽 패널] 버튼 영역
        left_panel = tk.Frame(main_pane, bg=COLOR_BG)
        main_pane.add(left_panel, minsize=500)

        # 공통 준비 영역
        self._create_common_section(left_panel)

        # 하단 출고 도구
        self._create_db_export_section(left_panel)

        # 메인 탭
        notebook = ttk.Notebook(left_panel)
        notebook.pack(fill="both", expand=True, pady=(6, 6))

        tab_text = ttk.Frame(notebook)
        tab_img = ttk.Frame(notebook)

        notebook.add(tab_text, text=" 📝 상품명 (Text) ")
        notebook.add(tab_img, text=" 🖼️ 이미지 (Image) ")

        self._build_text_tab(tab_text)
        self._build_image_tab(tab_img)

        # [오른쪽 패널] 작업 현황판
        right_panel = tk.Frame(main_pane, bg="white", bd=1, relief="solid")
        main_pane.add(right_panel, minsize=500)

        right_notebook = ttk.Notebook(right_panel)
        right_notebook.pack(fill="both", expand=True, padx=5, pady=5)

        # 대시보드 탭
        dashboard_tab = tk.Frame(right_notebook, bg="white")
        right_notebook.add(dashboard_tab, text=" 📊 작업 현황 ")

        # 휴지통 탭
        trash_tab = tk.Frame(right_notebook, bg="white")
        right_notebook.add(trash_tab, text=" 🗑️ 휴지통 ")

        self._build_dashboard_tab(dashboard_tab)
        self._build_trash_tab(trash_tab)

        # 3. 상태바
        self._create_status_bar()

        # 초기화
        JobManager.init_db()
        self.refresh_dashboard()
        self.load_user_memo()

    def toggle_advanced_mode(self):
        """고급 모드 토글"""
        self.advanced_mode = not self.advanced_mode
        if self.advanced_mode:
            self.advanced_mode_btn.config(text="⚙️ 고급 모드 ON", bg="#28a745")
            for key, btn in self.button_refs.items():
                if "single" in key:
                    btn.pack(side="left", padx=3)
        else:
            self.advanced_mode_btn.config(text="⚙️ 고급 모드 OFF", bg="#6c757d")
            for key, btn in self.button_refs.items():
                if "single" in key:
                    btn.pack_forget()

    def run_script(self, key):
        """스크립트 실행"""
        info = SCRIPTS.get(key)
        if not info:
            messagebox.showerror("오류", f"'{key}'에 해당하는 스크립트 정보를 찾을 수 없습니다.")
            return

        folder = info["folder"]
        filename = info["file"]

        # 절대 경로인 경우
        if os.path.isabs(folder):
            script_path = os.path.join(folder, filename)
            working_dir = folder
        else:
            script_path = os.path.join(BASE_DIR, folder, filename) if folder else os.path.join(BASE_DIR, filename)
            working_dir = os.path.dirname(script_path) if folder else BASE_DIR

        if not os.path.exists(script_path):
            messagebox.showerror("오류", f"스크립트를 찾을 수 없습니다:\n{script_path}")
            return

        try:
            subprocess.Popen(
                [sys.executable, script_path],
                cwd=working_dir
            )
            self._update_status("ready", f"[{key}] 실행됨")
        except Exception as e:
            messagebox.showerror("오류", f"스크립트 실행 실패:\n{e}")

    def _add_btn(self, parent, text, key, color, width=16, side="left", btn_type=None):
        """버튼 추가"""
        btn = tk.Button(
            parent, text=text,
            bg=color, fg="white", font=("맑은 고딕", 9, "bold"),
            relief="raised", width=width, cursor="hand2",
            command=lambda: self.run_script(key),
            bd=1, highlightthickness=0
        )

        if btn_type == "single":
            self.button_refs[f"{key}_single"] = btn
            if not self.advanced_mode:
                return btn  # 숨김 상태

        btn.pack(side=side, padx=3)

        info = SCRIPTS.get(key)
        if info:
            ToolTip(btn, info["desc"])

        return btn

    def _add_dual_model_btns(self, parent, stage_name, gpt_key, gemini_key, color):
        """GPT와 Gemini 버튼을 나란히 추가"""
        frame = tk.Frame(parent, bg=COLOR_BG)
        frame.pack(fill="x", padx=10, pady=3)

        tk.Label(frame, text=stage_name, bg=COLOR_BG, font=("맑은 고딕", 10, "normal"), width=22, anchor="w", fg="#333").pack(side="left", padx=5)

        # GPT 배치 버튼
        btn_gpt = tk.Button(
            frame, text="GPT 배치",
            bg=COLOR_GPT, fg="white", font=("맑은 고딕", 9, "bold"),
            relief="raised", width=12, cursor="hand2",
            command=lambda: self.run_script(gpt_key),
            bd=1, highlightthickness=0
        )
        btn_gpt.pack(side="left", padx=3)
        info_gpt = SCRIPTS.get(gpt_key)
        if info_gpt:
            ToolTip(btn_gpt, info_gpt["desc"])

        tk.Label(frame, text="vs", bg=COLOR_BG, font=("맑은 고딕", 9), fg="#888").pack(side="left", padx=5)

        # Gemini 배치 버튼
        btn_gemini = tk.Button(
            frame, text="Gemini 배치",
            bg=COLOR_GEMINI, fg="white", font=("맑은 고딕", 9, "bold"),
            relief="raised", width=12, cursor="hand2",
            command=lambda: self.run_script(gemini_key),
            bd=1, highlightthickness=0
        )
        btn_gemini.pack(side="left", padx=3)
        info_gemini = SCRIPTS.get(gemini_key)
        if info_gemini:
            ToolTip(btn_gemini, info_gemini["desc"])

    def _create_common_section(self, parent):
        """공통 데이터 준비 섹션"""
        frame = tk.LabelFrame(parent, text=" [STEP 0] 공통 데이터 준비 ", font=("맑은 고딕", 11, "bold"), bg="#ECEFF1", fg=COLOR_COMMON, bd=2, relief="groove")
        frame.pack(fill="x", pady=(0, 6), ipady=3)

        # 엑셀 매핑 도구
        btn_frame = tk.Frame(frame, bg="#ECEFF1")
        btn_frame.pack(fill="x", padx=12, pady=2)

        lbl = tk.Label(btn_frame, text="작업 시작 전 필수!", bg="#ECEFF1", fg="#455A64", font=("맑은 고딕", 9), justify="left")
        lbl.pack(side="left", padx=6)

        btn = tk.Button(btn_frame, text="📂 엑셀 매핑 도구",
            bg=COLOR_COMMON, fg="white", font=("맑은 고딕", 11, "bold"),
            relief="raised", width=20, cursor="hand2",
            command=lambda: self.run_script("Common_Mapping"),
            bd=1, highlightthickness=0)
        btn.pack(side="right", padx=5)
        ToolTip(btn, SCRIPTS.get("Common_Mapping", {}).get("desc", ""))

        tk.Frame(frame, bg="#CFD8DC", height=1).pack(fill="x", padx=12, pady=2)

        # 데이터 입고 + 병합 도구
        entry_frame = tk.Frame(frame, bg="#ECEFF1")
        entry_frame.pack(fill="x", padx=12, pady=2)

        entry_btn = tk.Button(entry_frame, text="💾 데이터 입고",
            bg="#27ae60", fg="white", font=("맑은 고딕", 10, "bold"),
            relief="raised", width=15, cursor="hand2",
            command=lambda: self.run_script("DB_Entry"),
            bd=1, highlightthickness=0)
        entry_btn.pack(side="left", padx=5)
        ToolTip(entry_btn, SCRIPTS.get("DB_Entry", {}).get("desc", ""))

        merge_btn = tk.Button(entry_frame, text="🔄 엑셀 버전 병합",
            bg="#00BCD4", fg="white", font=("맑은 고딕", 10, "bold"),
            relief="raised", width=15, cursor="hand2",
            command=lambda: self.run_script("Merge_Versions"),
            bd=1, highlightthickness=0)
        merge_btn.pack(side="left", padx=5)
        ToolTip(merge_btn, SCRIPTS.get("Merge_Versions", {}).get("desc", ""))

    def _create_db_export_section(self, parent):
        """데이터 출고 도구 섹션"""
        frame = tk.LabelFrame(parent, text=" [하단] 데이터 출고 도구 ", font=("맑은 고딕", 11, "bold"), bg="#E3F2FD", fg="#546E7A", bd=2, relief="groove")
        frame.pack(fill="x", pady=(0, 0), ipady=3, side="bottom", anchor="sw")

        export_frame = tk.Frame(frame, bg="#E3F2FD")
        export_frame.pack(fill="x", padx=12, pady=2)

        btn_wrapper = tk.Frame(export_frame, bg="#E3F2FD")
        btn_wrapper.pack(anchor="center")

        export_btn = tk.Button(btn_wrapper, text="📤 데이터 출고",
            bg="#546E7A", fg="white", font=("맑은 고딕", 10, "bold"),
            relief="raised", width=15, cursor="hand2",
            command=lambda: self.run_script("DB_Export"),
            bd=1, highlightthickness=0)
        export_btn.pack(side="left", padx=3)
        ToolTip(export_btn, SCRIPTS.get("DB_Export", {}).get("desc", ""))

        converter_btn = tk.Button(btn_wrapper, text="🔄 OC→ES 변환",
            bg="#FF9800", fg="white", font=("맑은 고딕", 10, "bold"),
            relief="raised", width=15, cursor="hand2",
            command=lambda: self.run_script("Ownerclan_Converter"),
            bd=1, highlightthickness=0)
        converter_btn.pack(side="left", padx=3)
        ToolTip(converter_btn, SCRIPTS.get("Ownerclan_Converter", {}).get("desc", ""))

        mapper_btn = tk.Button(btn_wrapper, text="📋 상품 등록 맵퍼",
            bg="#6dc951", fg="white", font=("맑은 고딕", 10, "bold"),
            relief="raised", width=15, cursor="hand2",
            command=lambda: self.run_script("Upload_Mapper"),
            bd=1, highlightthickness=0)
        mapper_btn.pack(side="left", padx=3)
        ToolTip(mapper_btn, SCRIPTS.get("Upload_Mapper", {}).get("desc", ""))

    def _build_text_tab(self, parent):
        """상품명(Text) 탭"""
        container = tk.Frame(parent, bg=COLOR_BG, padx=12, pady=10)
        container.pack(fill="both", expand=True)

        # 모델 비교 안내
        info_frame = tk.Frame(container, bg="#E8F5E9", bd=1, relief="groove")
        info_frame.pack(fill="x", pady=(0, 10))
        tk.Label(info_frame, text="💡 GPT vs Gemini: Gemini 2.5 Flash-Lite는 GPT 대비 약 3.7배 저렴합니다. (입력 $0.05/M, 출력 $0.20/M)",
                 bg="#E8F5E9", fg="#388E3C", font=("맑은 고딕", 9)).pack(padx=10, pady=5)

        # Stage 1
        frame_t1 = tk.LabelFrame(container, text=" Stage 1: 텍스트 기초 정제 ", font=("맑은 고딕", 11, "bold"), bg=COLOR_BG, fg=COLOR_S1, bd=2, relief="groove")
        frame_t1.pack(fill="x", pady=6, ipady=4)
        self._add_dual_model_btns(frame_t1, "텍스트 기초 정제", "Text_S1_Batch_GPT", "Text_S1_Batch_Gemini", COLOR_S1)

        # Stage 2
        frame_t2 = tk.LabelFrame(container, text=" Stage 2: 상세정보 & 재료 추출 ", font=("맑은 고딕", 11, "bold"), bg=COLOR_BG, fg=COLOR_S2, bd=2, relief="groove")
        frame_t2.pack(fill="x", pady=8, ipady=5)

        frame_t2_1 = tk.Frame(frame_t2, bg=COLOR_BG)
        frame_t2_1.pack(fill="x", padx=10, pady=3)
        tk.Label(frame_t2_1, text="① 상세이미지 다운(필수)", bg=COLOR_BG, font=("맑은 고딕", 10), width=22, anchor="w", fg="#333").pack(side="left", padx=5)
        self._add_btn(frame_t2_1, "실행", "Text_S2_Extract", COLOR_S2, width=12, side="left")

        tk.Frame(frame_t2, bg="#E0E0E0", height=1).pack(fill="x", padx=10, pady=3)

        self._add_dual_model_btns(frame_t2, "② 이미지 & 텍스트 분석", "Text_S2_Batch_GPT", "Text_S2_Batch_Gemini", COLOR_S2)

        # Stage 3
        frame_t3 = tk.LabelFrame(container, text=" Stage 3: 최종 상품명 생성 ", font=("맑은 고딕", 11, "bold"), bg=COLOR_BG, fg=COLOR_S3, bd=2, relief="groove")
        frame_t3.pack(fill="x", pady=6, ipady=4)
        self._add_dual_model_btns(frame_t3, "최종 상품명 생성", "Text_S3_Batch_GPT", "Text_S3_Batch_Gemini", COLOR_S3)

        # Stage 4
        frame_t4 = tk.LabelFrame(container, text=" Stage 4: 필터링 및 검수 ", font=("맑은 고딕", 11, "bold"), bg=COLOR_BG, fg=COLOR_S4, bd=2, relief="groove")
        frame_t4.pack(fill="x", pady=6, ipady=4)

        frame_t4_1 = tk.Frame(frame_t4, bg=COLOR_BG)
        frame_t4_1.pack(fill="x", padx=10, pady=3)
        tk.Label(frame_t4_1, text="① 필터링 (금지어)", bg=COLOR_BG, font=("맑은 고딕", 10), width=22, anchor="w", fg="#333").pack(side="left", padx=5)
        self._add_btn(frame_t4_1, "실행", "Text_S4_Filter", COLOR_S4, width=12, side="left")

        tk.Frame(frame_t4, bg="#E0E0E0", height=1).pack(fill="x", padx=10, pady=3)

        self._add_dual_model_btns(frame_t4, "② 최종 검수", "Text_S4_2_Batch_GPT", "Text_S4_2_Batch_Gemini", COLOR_S4)

    def _build_image_tab(self, parent):
        """이미지(Image) 탭"""
        canvas = tk.Canvas(parent, bg=COLOR_BG, highlightthickness=0)
        scrollbar = ttk.Scrollbar(parent, orient="vertical", command=canvas.yview)
        scrollable_frame = tk.Frame(canvas, bg=COLOR_BG)

        def update_scroll_region(event=None):
            canvas.update_idletasks()
            canvas.configure(scrollregion=canvas.bbox("all"))

        scrollable_frame.bind("<Configure>", update_scroll_region)
        canvas_window = canvas.create_window((0, 0), window=scrollable_frame, anchor="nw")

        def _on_canvas_configure(event):
            canvas.itemconfig(canvas_window, width=event.width)

        canvas.bind("<Configure>", _on_canvas_configure)
        canvas.configure(yscrollcommand=scrollbar.set)

        def _on_mousewheel(event):
            canvas.yview_scroll(int(-1*(event.delta/120)), "units")

        canvas.bind("<MouseWheel>", _on_mousewheel)
        canvas.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")

        container = tk.Frame(scrollable_frame, bg=COLOR_BG, padx=12, pady=10)
        container.pack(fill="both", expand=True)

        # 모델 비교 안내
        info_frame = tk.Frame(container, bg="#E8F5E9", bd=1, relief="groove")
        info_frame.pack(fill="x", pady=(0, 10))
        tk.Label(info_frame, text="💡 GPT vs Gemini: Gemini 2.5 Flash-Lite는 GPT 대비 약 3.7배 저렴합니다.",
                 bg="#E8F5E9", fg="#388E3C", font=("맑은 고딕", 9)).pack(padx=10, pady=5)

        # Stage 1: 배경 제거
        self._add_stage_group(container, "Stage 1: 배경 제거 (Remove BG)", COLOR_S1, [
            ("▶ (누끼) 배경제거", "Img_S1_BG")
        ])

        # Stage 2: 라벨링
        self._add_stage_group(container, "Stage 2: 라벨링 (Labeling)", COLOR_S2, [
            ("① 휴먼 라벨링 도구", "Img_S2_Label"),
        ])

        # Stage 3: 이미지 분석 전처리 (GPT vs Gemini)
        frame_s3 = tk.LabelFrame(container, text=" Stage 3: 이미지 분석 전처리 ", font=("맑은 고딕", 11, "bold"), bg=COLOR_BG, fg=COLOR_S3, bd=2, relief="groove")
        frame_s3.pack(fill="x", pady=6, ipady=4)

        self._add_dual_model_btns(frame_s3, "① 썸네일 구도·조명 분석", "Img_S3_Thumbnail_Analysis_Batch_GPT", "Img_S3_Thumbnail_Analysis_Batch_Gemini", COLOR_S3)

        tk.Frame(frame_s3, bg="#E0E0E0", height=1).pack(fill="x", padx=10, pady=3)

        self._add_dual_model_btns(frame_s3, "② 배경 생성 프롬프트", "Img_S3_Preprocess_Batch_GPT", "Img_S3_Preprocess_Batch_Gemini", COLOR_S3)

        # Stage 4: 배경 생성 및 합성
        self._add_stage_group(container, "Stage 4: 배경 생성 및 합성", COLOR_S4, [
            ("① 배경 생성", "Img_S4_BG_Generate"),
            ("② 합성", "Img_S4_Composite")
        ])

        # Stage 5: 품질 검증
        self._add_stage_group(container, "Stage 5: 품질 검증", COLOR_S5, [
            ("품질 검증", "Img_S5_Review"),
            ("이미지 업로드 (R2)", "Img_S5_Upload")
        ])

    def _add_stage_group(self, parent, title, color, buttons):
        """스테이지 그룹 추가"""
        frame = tk.LabelFrame(parent, text=f" {title} ", font=("맑은 고딕", 11, "bold"), bg=COLOR_BG, fg=color, bd=2, relief="groove")
        frame.pack(fill="x", pady=6, ipady=4)

        for label, key in buttons:
            row = tk.Frame(frame, bg=COLOR_BG)
            row.pack(fill="x", padx=10, pady=3)
            tk.Label(row, text=label, bg=COLOR_BG, font=("맑은 고딕", 10), width=22, anchor="w", fg="#333").pack(side="left", padx=5)
            self._add_btn(row, "실행", key, color, width=12, side="left")

    def _build_dashboard_tab(self, parent):
        """대시보드 탭"""
        # 헤더
        dashboard_header = tk.Frame(parent, bg="white")
        dashboard_header.pack(fill="x", padx=10, pady=(15, 5))

        tk.Label(dashboard_header, text="📊 실시간 작업 현황",
                 font=("맑은 고딕", 12, "bold"), bg="white", fg="#333").pack(side="left")

        btn_frame = tk.Frame(dashboard_header, bg="white")
        btn_frame.pack(side="right")

        btn_refresh = tk.Button(btn_frame, text="🔄 새로고침",
                                command=self.refresh_dashboard,
                                bg="#f1f3f5", fg="#333", relief="raised",
                                font=("맑은 고딕", 9), cursor="hand2", padx=12, pady=5,
                                bd=1, highlightthickness=0)
        btn_refresh.pack(side="left")

        # 작업 현황판 (Treeview)
        tree_frame = tk.Frame(parent, bg="white")
        tree_frame.pack(fill="both", expand=True, padx=5, pady=(5, 0))

        columns = ("file", "text_stat", "text_time", "img_stat", "img_time", "memo")
        self.tree = ttk.Treeview(tree_frame, columns=columns, show='headings', height=15, selectmode="extended")

        self.tree.heading("file", text="파일 (Root Name)"); self.tree.column("file", width=180, anchor="w")
        self.tree.heading("text_stat", text="Text 상태"); self.tree.column("text_stat", width=90, anchor="center")
        self.tree.heading("text_time", text="최근변경"); self.tree.column("text_time", width=90, anchor="center")
        self.tree.heading("img_stat", text="Img 상태"); self.tree.column("img_stat", width=150, anchor="center")
        self.tree.heading("img_time", text="최근변경"); self.tree.column("img_time", width=90, anchor="center")
        self.tree.heading("memo", text="비고(메모)"); self.tree.column("memo", width=150, anchor="w")

        scrollbar = ttk.Scrollbar(tree_frame, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscrollcommand=scrollbar.set)

        self.tree.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")

        self.tree.bind("<Double-1>", self.on_tree_double_click)
        self.tree.bind("<Button-3>", self.on_tree_right_click)

        self.context_menu = tk.Menu(self, tearoff=0)
        self.context_menu.add_command(label="📝 메모 수정", command=self.edit_selected_memo)
        self.context_menu.add_command(label="🗑️ 휴지통으로 이동", command=self.delete_selected_job)
        self.context_menu.add_separator()
        self.context_menu.add_command(label="🔄 새로고침", command=self.refresh_dashboard)

        # 공통 메모
        memo_frame = tk.LabelFrame(parent, text=" 📝 공통 메모 ",
                                   font=("맑은 고딕", 10, "bold"), bg="white", fg="#555", bd=1, relief="solid")
        memo_frame.pack(fill="x", padx=10, pady=(5, 10))

        self.txt_memo = tk.Text(memo_frame, height=6, font=("맑은 고딕", 10), bg="#FEF9E7", relief="flat")
        self.txt_memo.pack(fill="both", expand=True, padx=5, pady=5)

        btn_save_memo = tk.Button(memo_frame, text="💾 메모 저장",
                                  command=self.save_user_memo,
                                  bg="#546E7A", fg="white", font=("맑은 고딕", 9, "bold"),
                                  relief="raised", cursor="hand2", bd=1, highlightthickness=0)
        btn_save_memo.pack(fill="x", padx=5, pady=(0, 5))

    def _build_trash_tab(self, parent):
        """휴지통 탭"""
        header = tk.Frame(parent, bg="white")
        header.pack(fill="x", padx=10, pady=(15, 5))

        tk.Label(header, text="🗑️ 휴지통", font=("맑은 고딕", 12, "bold"), bg="white", fg="#333").pack(side="left")

        btn_frame = tk.Frame(header, bg="white")
        btn_frame.pack(side="right")

        btn_refresh = tk.Button(btn_frame, text="🔄 새로고침", command=self.refresh_trash,
                                bg="#f1f3f5", fg="#333", font=("맑은 고딕", 9), cursor="hand2",
                                relief="raised", padx=8, pady=3)
        btn_refresh.pack(side="left", padx=3)

        # 휴지통 트리뷰
        tree_frame = tk.Frame(parent, bg="white")
        tree_frame.pack(fill="both", expand=True, padx=5, pady=5)

        columns = ("file", "deleted_at")
        self.trash_tree = ttk.Treeview(tree_frame, columns=columns, show='headings', height=10)

        self.trash_tree.heading("file", text="파일명")
        self.trash_tree.column("file", width=250, anchor="w")
        self.trash_tree.heading("deleted_at", text="삭제 시간")
        self.trash_tree.column("deleted_at", width=150, anchor="center")

        scrollbar = ttk.Scrollbar(tree_frame, orient="vertical", command=self.trash_tree.yview)
        self.trash_tree.configure(yscrollcommand=scrollbar.set)

        self.trash_tree.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")

        # 버튼 영역
        btn_frame2 = tk.Frame(parent, bg="white")
        btn_frame2.pack(fill="x", padx=10, pady=5)

        btn_restore = tk.Button(btn_frame2, text="♻️ 복원", command=self.restore_selected_job,
                                bg="#28a745", fg="white", font=("맑은 고딕", 9, "bold"),
                                cursor="hand2", padx=10, pady=5)
        btn_restore.pack(side="left", padx=5)

        btn_perm_delete = tk.Button(btn_frame2, text="🗑️ 완전 삭제", command=self.permanently_delete_selected,
                                    bg="#dc3545", fg="white", font=("맑은 고딕", 9, "bold"),
                                    cursor="hand2", padx=10, pady=5)
        btn_perm_delete.pack(side="left", padx=5)

        self.refresh_trash()

    def refresh_trash(self):
        """휴지통 새로고침"""
        if not hasattr(self, 'trash_tree'):
            return

        for item in self.trash_tree.get_children():
            self.trash_tree.delete(item)

        deleted_jobs = JobManager.load_deleted_jobs()
        for filename, info in deleted_jobs.items():
            deleted_at = info.get("deleted_at", "-")
            self.trash_tree.insert("", "end", values=(filename, deleted_at))

    def restore_selected_job(self):
        """선택한 항목 복원"""
        selected = self.trash_tree.selection()
        if not selected:
            messagebox.showwarning("선택 필요", "복원할 항목을 선택해주세요.")
            return

        for item_id in selected:
            values = self.trash_tree.item(item_id, "values")
            if values:
                JobManager.restore_job(values[0])

        self.refresh_trash()
        self.refresh_dashboard()
        messagebox.showinfo("완료", "선택한 항목이 복원되었습니다.")

    def permanently_delete_selected(self):
        """선택한 항목 완전 삭제"""
        selected = self.trash_tree.selection()
        if not selected:
            messagebox.showwarning("선택 필요", "삭제할 항목을 선택해주세요.")
            return

        if not messagebox.askyesno("경고", "선택한 항목을 완전히 삭제하시겠습니까?\n이 작업은 되돌릴 수 없습니다."):
            return

        for item_id in selected:
            values = self.trash_tree.item(item_id, "values")
            if values:
                JobManager.permanently_delete_job(values[0])

        self.refresh_trash()
        messagebox.showinfo("완료", "선택한 항목이 완전히 삭제되었습니다.")

    def _create_status_bar(self):
        """상태바 생성"""
        status_bar = tk.Frame(self, bg=COLOR_STATUS_BAR, height=25)
        status_bar.pack(fill="x", side="bottom")

        tk.Label(status_bar, textvariable=self.status_var, bg=COLOR_STATUS_BAR,
                 font=("맑은 고딕", 9), fg="#666").pack(side="left", padx=10)

        tk.Label(status_bar, text="v10 GPT vs Gemini", bg=COLOR_STATUS_BAR,
                 font=("맑은 고딕", 9), fg="#888").pack(side="right", padx=10)

    def _update_status(self, level, msg):
        """상태바 업데이트"""
        self.status_var.set(msg)

    def refresh_dashboard(self):
        """대시보드 새로고침"""
        for item in self.tree.get_children():
            self.tree.delete(item)

        jobs = JobManager.load_jobs()
        if not jobs: return

        sorted_jobs = sorted(jobs.items(), key=lambda x: x[1].get('last_update', ''), reverse=True)

        for filename, info in sorted_jobs:
            clean_name = filename.replace("_stage1_mapping", "").replace(".xlsx", "")

            t_stat = info.get("text_status", "-")
            t_time = info.get("text_time", "-")
            i_stat = info.get("image_status", "-")
            i_time = info.get("image_time", "-")
            memo = info.get("memo", "")

            # 세부 단계 정보
            img_s3_1 = info.get("image_s3_1_status", "-")
            img_s3_2 = info.get("image_s3_2_status", "-")
            img_s4_1 = info.get("image_s4_1_status", "-")
            img_s4_2 = info.get("image_s4_2_status", "-")
            img_s5_1 = info.get("image_s5_1_status", "-")
            img_s5_2 = info.get("image_s5_2_status", "-")

            parts = []
            if img_s5_1 != "-" or img_s5_2 != "-":
                if img_s5_1 != "-": parts.append(img_s5_1)
                if img_s5_2 != "-": parts.append(img_s5_2)
            elif img_s4_1 != "-" or img_s4_2 != "-":
                if img_s4_1 != "-": parts.append(img_s4_1)
                if img_s4_2 != "-": parts.append(img_s4_2)
            elif img_s3_1 != "-" or img_s3_2 != "-":
                if img_s3_1 != "-": parts.append(img_s3_1)
                if img_s3_2 != "-": parts.append(img_s3_2)

            if parts:
                i_stat = " / ".join(parts)

            self.tree.insert("", "end", values=(clean_name, t_stat, t_time, i_stat, i_time, memo))

        self._update_status("ready", f"현황판 업데이트 완료 ({datetime.now().strftime('%H:%M:%S')})")

    def on_tree_double_click(self, event):
        """더블 클릭으로 메모 수정"""
        self.edit_selected_memo()

    def on_tree_right_click(self, event):
        """우클릭 메뉴"""
        item_id = self.tree.identify_row(event.y)
        if item_id:
            current_selection = self.tree.selection()
            if item_id not in current_selection:
                self.tree.selection_set(item_id)
            try:
                self.context_menu.tk_popup(event.x_root, event.y_root)
            finally:
                self.context_menu.grab_release()

    def edit_selected_memo(self):
        """선택된 항목 메모 수정"""
        selected = self.tree.selection()
        if not selected:
            messagebox.showwarning("선택 필요", "메모를 수정할 항목을 선택해주세요.")
            return

        item_id = selected[0]
        values = self.tree.item(item_id, "values")
        if not values: return

        filename = values[0]
        current_memo = values[5]

        new_memo = simpledialog.askstring("메모 수정", f"[{filename}]\n비고 사항을 입력하세요:", initialvalue=current_memo)

        if new_memo is not None:
            jobs = JobManager.load_jobs()
            target_key = next((k for k in jobs.keys() if filename in k), filename)
            JobManager.update_job_memo(target_key, new_memo)
            self.refresh_dashboard()

    def delete_selected_job(self):
        """선택된 항목 휴지통으로 이동"""
        selected = self.tree.selection()
        if not selected:
            messagebox.showwarning("선택 필요", "삭제할 항목을 선택해주세요.")
            return

        count = len(selected)
        if not messagebox.askyesno("휴지통으로 이동", f"선택된 {count}개 항목을 휴지통으로 이동하시겠습니까?"):
            return

        jobs = JobManager.load_jobs()
        for item_id in selected:
            values = self.tree.item(item_id, "values")
            if values:
                filename = values[0]
                target_key = next((k for k in jobs.keys() if filename in k), filename)
                JobManager.delete_job(target_key)

        self.refresh_dashboard()
        self.refresh_trash()
        messagebox.showinfo("완료", f"{count}개 항목이 휴지통으로 이동되었습니다.")

    def load_user_memo(self):
        """사용자 메모 불러오기"""
        memo_path = os.path.join(BASE_DIR, "user_memo.txt")
        if os.path.exists(memo_path):
            try:
                with open(memo_path, "r", encoding="utf-8") as f:
                    self.txt_memo.delete("1.0", tk.END)
                    self.txt_memo.insert("1.0", f.read())
            except Exception:
                pass

    def save_user_memo(self):
        """사용자 메모 저장"""
        content = self.txt_memo.get("1.0", tk.END).strip()
        memo_path = os.path.join(BASE_DIR, "user_memo.txt")
        try:
            with open(memo_path, "w", encoding="utf-8") as f:
                f.write(content)
            self._update_status("ready", "사용자 메모가 저장되었습니다.")
            messagebox.showinfo("알림", "메모가 저장되었습니다.")
        except Exception as e:
            messagebox.showerror("오류", f"저장 실패: {e}")


# ========================================================
# Main
# ========================================================
if __name__ == "__main__":
    app = PipelineLauncher()
    app.mainloop()
