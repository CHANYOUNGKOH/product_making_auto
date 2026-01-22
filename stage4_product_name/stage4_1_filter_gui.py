import os
import sys
import re
import json
import subprocess
from datetime import datetime

import tkinter as tk
from tkinter import filedialog, messagebox, ttk
from tkinter.scrolledtext import ScrolledText

import pandas as pd

# ========================================================
# 메인 런처 연동용 JobManager & 파일명 유틸 (Stage4-1: 필터링)
# ========================================================
def get_root_filename(filename: str) -> str:
    """
    파일명에서 버전 정보(_T*_I* 또는 _t*_i*, _I*(업완) 포함) 및 기타 꼬리표를 떼고 원본명(Key)만 추출
    예: 상품_T3_I0.xlsx -> 상품.xlsx
    예: 상품_T4_I1.xlsx -> 상품.xlsx
    예: 상품_T3_I0(업완).xlsx -> 상품.xlsx
    예: 상품_T3_I0_T4_I1.xlsx -> 상품.xlsx (여러 버전 패턴 제거)
    예: 상품_T3_I5(업완).xlsx -> 상품.xlsx
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
    def update_status(cls, filename, text_msg=None, img_msg=None):
        """메인 런처 현황판 상태 업데이트"""
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
                "memo": "",
            }

        if text_msg:
            data[filename]["text_status"] = text_msg
            data[filename]["text_time"] = now
        if img_msg:
            data[filename]["image_status"] = img_msg
            data[filename]["image_time"] = now

        data[filename]["last_update"] = now

        try:
            with open(db_path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=4)
        except Exception as e:
            print(f"[JobManager Error] {e}")

# ========================================================
# 전역 경로 설정
# ========================================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_FILE = os.path.join(BASE_DIR, "stage4_config.json")
GLOBAL_REJECT_LOG = os.path.join(BASE_DIR, "stage4_reject_log.csv")


# ========================================================
# 0. 헬퍼 클래스 (툴팁)
# ========================================================
class CreateToolTip(object):
    """
    위젯에 마우스를 올리면 도움말 팝업을 띄워주는 툴팁 클래스
    """
    def __init__(self, widget, text='widget info'):
        self.waittime = 500     # miliseconds
        self.wraplength = 300   # pixels
        self.widget = widget
        self.text = text
        self.widget.bind("<Enter>", self.enter)
        self.widget.bind("<Leave>", self.leave)
        self.widget.bind("<ButtonPress>", self.leave)
        self.id = None
        self.tw = None

    def enter(self, event=None):
        self.schedule()

    def leave(self, event=None):
        self.unschedule()
        self.hidetip()

    def schedule(self):
        self.unschedule()
        self.id = self.widget.after(self.waittime, self.showtip)

    def unschedule(self):
        id = self.id
        self.id = None
        if id:
            self.widget.after_cancel(id)

    def showtip(self, event=None):
        x = y = 0
        x, y, cx, cy = self.widget.bbox("insert")
        x += self.widget.winfo_rootx() + 25
        y += self.widget.winfo_rooty() + 20
        # creates a toplevel window
        self.tw = tk.Toplevel(self.widget)
        # Leaves only the label and removes the app window
        self.tw.wm_overrideredirect(True)
        self.tw.wm_geometry("+%d+%d" % (x, y))
        label = tk.Label(self.tw, text=self.text, justify='left',
                       background="#ffffe0", relief='solid', borderwidth=1,
                       wraplength = self.wraplength,
                       font=("맑은 고딕", 9, "normal"))
        label.pack(ipadx=1)

    def hidetip(self):
        tw = self.tw
        self.tw= None
        if tw:
            tw.destroy()


# ========================================================
# 1. 설정 관리 클래스
# ========================================================
class Stage4Config:
    def __init__(self, path: str):
        self.path = path
        self.default = {
            "max_length": 50,
            "forbidden_symbols": [
                "/", ",", "[", "]", "(", ")", "{", "}",
                "!", "?", "♥", "★", "☆", "☎", "※", "·"
            ],
            "forbidden_keywords": [
                "무료배송", "무배", "당일발송", "당일배송", "총알배송", "오늘출발",
                "최저가", "특가", "초특가", "할인", "세일", "SALE", "가격파괴", "역대급",
                "1+1", "2+1", "사은품", "증정", "서비스",
                "주문폭주", "인기템", "인기상품", "강력추천", "MD추천", "추천",
                "한정수량", "재고정리", "마감임박", "품절임박"
            ],
            "forbidden_shop_words": ["공식몰", "직영몰", "전문몰"],
            "forbidden_shop_patterns": [r"공식\s*스토어", r"오피셜\s*스토어"],
            "brand_hints": ["나이키", "아디다스"],
            "allowed_phrases": [
                "매장 증정용", "매장증정용",
                "고객 증정", "고객증정",
                "행사 증정", "행사증정",
                "사은품 증정용"
            ],
            "allowed_regex_patterns": [
                r"(쇼핑백|봉투|포장).{0,6}증정용",
                r"증정용.{0,6}(쇼핑백|봉투|포장)"
            ],
            "sentence_endings": [
                "합니다", "해요", "입니다", "이에요", "예요",
                "인가요", "세요", "시오", "했음", "겠음", "입니다만",
                "하나요", "인가", "더라고요"
            ],
            "whitelist_exact": [],
            "whitelist_contains": [],
            "whitelist_regex": []
        }
        self.config = self.default.copy()
        self.load()

    def load(self):
        if os.path.exists(self.path):
            try:
                with open(self.path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                if isinstance(data, dict):
                    self.config.update(data)
            except Exception:
                pass
        else:
            self.save()

    def save(self):
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(self.config, f, ensure_ascii=False, indent=2)


# ========================================================
# 2. 검증 로직 클래스
# ========================================================
class Stage4Validator:
    def __init__(self, cfg: Stage4Config):
        self.update_config(cfg)

    def update_config(self, cfg: Stage4Config):
        """설정 변경 시 재로드"""
        c = cfg.config
        self.max_length = int(c.get("max_length", 50))
        self.forbidden_symbols = c.get("forbidden_symbols", [])
        self.forbidden_keywords = c.get("forbidden_keywords", [])
        self.forbidden_shop_words = c.get("forbidden_shop_words", [])
        self.brand_hints = c.get("brand_hints", [])
        self.allowed_phrases = c.get("allowed_phrases", [])
        self.sentence_endings = tuple(c.get("sentence_endings", []))

        self.forbidden_shop_patterns = [re.compile(p) for p in c.get("forbidden_shop_patterns", [])]
        self.allowed_regex_patterns = [re.compile(p) for p in c.get("allowed_regex_patterns", [])]
        
        self.whitelist_exact = c.get("whitelist_exact", [])
        self.whitelist_contains = c.get("whitelist_contains", [])
        self.whitelist_regex = [re.compile(p) for p in c.get("whitelist_regex", [])]

        self.range_tilde_regex = re.compile(r'\d\s*~\s*\d')
        self.range_hyphen_regex = re.compile(r'\d\s*-\s*\d')

    def _in_whitelist(self, clean_text: str) -> bool:
        if clean_text in self.whitelist_exact: return True
        for sub in self.whitelist_contains:
            if sub and sub in clean_text: return True
        for pat in self.whitelist_regex:
            if pat.search(clean_text): return True
        return False

    def validate(self, text: str):
        if not isinstance(text, str) or not text.strip():
            return False, "빈 문자열"
        clean_text = text.strip()

        if self._in_whitelist(clean_text):
            return True, "WHITELIST_PASS"

        # Rule 0
        if "\n" in clean_text or "\t" in clean_text: return False, "줄바꿈/탭 포함"
        
        # Rule 1
        if len(clean_text) > self.max_length:
            return False, f"길이 초과 ({len(clean_text)}자)"

        # Rule 2
        for ch in self.forbidden_symbols:
            if ch in clean_text: return False, f"금지 기호 포함 ({ch})"

        if "~" in clean_text:
            temp = self.range_tilde_regex.sub('', clean_text)
            if "~" in temp: return False, "금지 기호 포함 (~ : 숫자 범위 아님)"

        if "-" in clean_text:
            temp = self.range_hyphen_regex.sub('', clean_text)
            if "-" in temp: return False, "금지 기호 포함 (- : 사이즈/범위 아님)"

        # Rule 3 (금지 키워드 + 예외 처리)
        for kw in self.forbidden_keywords:
            if kw in clean_text:
                is_allowed = any(ph in clean_text for ph in self.allowed_phrases)
                if not is_allowed:
                    for rgx in self.allowed_regex_patterns:
                        if rgx.search(clean_text):
                            is_allowed = True
                            break
                if not is_allowed:
                    return False, f"금지 키워드 포함 ({kw})"

        for sw in self.forbidden_shop_words:
            if sw in clean_text: return False, f"상점/몰 관련 단어 포함 ({sw})"

        for pat in self.forbidden_shop_patterns:
            if pat.search(clean_text): return False, "상점/몰 관련 표현 포함"

        for b in self.brand_hints:
            if b in clean_text: return False, f"브랜드명 포함 가능성 ({b})"

        for ending in self.sentence_endings:
            if clean_text.endswith(ending): return False, f"문장형 어미 사용 (끝: {ending})"

        if "?" in clean_text or "!" in clean_text:
            return False, "문장형 기호(?,!) 사용"

        return True, "PASS"


# ========================================================
# 3. 설정 편집기 GUI (새 창)
# ========================================================
class ConfigEditor(tk.Toplevel):
    def __init__(self, parent, config_obj: Stage4Config, update_callback):
        super().__init__(parent)
        self.title("키워드 설정 편집")
        self.geometry("800x600")
        self.config_obj = config_obj
        self.update_callback = update_callback

        # 탭 구성
        self.notebook = ttk.Notebook(self)
        self.notebook.pack(fill='both', expand=True, padx=10, pady=10)

        # Tab 1: 금지 키워드
        self.tab_banned = ttk.Frame(self.notebook)
        self.notebook.add(self.tab_banned, text='🚫 금지 키워드')
        
        # [추가] CSV 가져오기 버튼을 위한 프레임
        btn_frame_banned = ttk.Frame(self.tab_banned)
        btn_frame_banned.pack(fill='x', padx=5, pady=5)
        
        btn_import_csv = ttk.Button(btn_frame_banned, text="📂 CSV 파일에서 금지어 가져오기", command=self.import_csv_to_banned)
        btn_import_csv.pack(side='right')
        CreateToolTip(btn_import_csv, "외부 CSV 파일(A열에 단어 목록)을 불러와 금지어 리스트에 추가합니다.\n'-' 뒤의 설명은 자동으로 제거됩니다.")

        self._build_list_editor(
            self.tab_banned, 
            "forbidden_keywords", 
            "이 단어가 포함되면 상품명이 탈락됩니다.\n주의: '덤' 같이 짧은 단어를 넣으면 '랜덤', '덤벨' 등도 탈락될 수 있으니 신중해야 합니다."
        )

        # Tab 2: 예외 허용 문구
        self.tab_allowed = ttk.Frame(self.notebook)
        self.notebook.add(self.tab_allowed, text='✅ 예외 허용 문구')
        self._build_list_editor(
            self.tab_allowed, 
            "allowed_phrases", 
            "금지 키워드가 포함되어 있어도, 이 문구가 함께 있으면 통과시킵니다.\n예: '증정'은 금지지만, '매장 증정용'을 여기에 넣으면 해당 상품명은 살아남습니다."
        )

        # Tab 3: 화이트리스트(포함)
        self.tab_whitelist = ttk.Frame(self.notebook)
        self.notebook.add(self.tab_whitelist, text='🟢 화이트리스트(포함)')
        self._build_list_editor(
            self.tab_whitelist,
            "whitelist_contains",
            "이 단어가 포함된 상품명은 다른 규칙에 걸려도 우선적으로 통과시킵니다.\n예: 팝업스토어, 프리마켓"
        )

        # 하단 저장 버튼
        btn_frame = ttk.Frame(self)
        btn_frame.pack(fill='x', padx=10, pady=10)
        btn_save = ttk.Button(btn_frame, text="설정 저장 및 적용", command=self.on_save)
        btn_save.pack(side='right')

    def import_csv_to_banned(self):
        """CSV 파일을 선택해서 금지어 목록에 추가하는 함수"""
        file_path = filedialog.askopenfilename(
            title="금지어 CSV 파일 선택",
            filetypes=[("CSV files", "*.csv"), ("Excel files", "*.xlsx")]
        )
        if not file_path:
            return

        try:
            new_keywords = set()
            if file_path.endswith('.csv'):
                # 인코딩 자동 감지 시도 (utf-8 -> cp949)
                try:
                    df = pd.read_csv(file_path, encoding='utf-8')
                except UnicodeDecodeError:
                    df = pd.read_csv(file_path, encoding='cp949')
            else:
                df = pd.read_excel(file_path)
            
            # 첫 번째 컬럼을 데이터로 가정
            raw_data = df.iloc[:, 0].dropna().astype(str).tolist()
            
            for item in raw_data:
                # 1. '-' 기준으로 분리하여 앞부분만 가져오기 (설명 제거)
                word = item.split('-')[0].strip()
                # 2. 빈 문자열 제외
                if word:
                    new_keywords.add(word)
            
            # 리스트박스에 추가 (중복 확인)
            # 중요: _build_list_editor에서 setattr로 저장한 이름 사용
            listbox = getattr(self, "listbox_forbidden_keywords", None)
            if listbox:
                current_items = set(listbox.get(0, tk.END))
                added_count = 0
                for kw in new_keywords:
                    if kw not in current_items:
                        listbox.insert(tk.END, kw)
                        added_count += 1
                
                messagebox.showinfo("완료", f"총 {len(new_keywords)}개 단어 중\n{added_count}개의 새로운 금지어가 추가되었습니다.")
            
        except Exception as e:
            messagebox.showerror("에러", f"파일을 읽는 중 오류가 발생했습니다:\n{e}")

    def _build_list_editor(self, parent, key, tooltip_text):
        # 설명 라벨 + 툴팁
        lbl_info = ttk.Label(parent, text=f"ℹ️ {key} 도움말 (마우스를 올리세요)", foreground="blue", cursor="hand2")
        lbl_info.pack(anchor='w', padx=5, pady=5)
        CreateToolTip(lbl_info, tooltip_text)

        # 리스트박스 프레임
        frame_list = ttk.Frame(parent)
        frame_list.pack(fill='both', expand=True, padx=5, pady=0)

        scrollbar = ttk.Scrollbar(frame_list)
        scrollbar.pack(side='right', fill='y')

        listbox = tk.Listbox(frame_list, yscrollcommand=scrollbar.set, selectmode='extended')
        listbox.pack(side='left', fill='both', expand=True)
        scrollbar.config(command=listbox.yview)

        # 기존 데이터 로드
        current_data = self.config_obj.config.get(key, [])
        for item in sorted(current_data):
            listbox.insert(tk.END, item)

        # 조작 버튼 프레임
        frame_ctrl = ttk.Frame(parent)
        frame_ctrl.pack(fill='x', padx=5, pady=5)

        entry_new = ttk.Entry(frame_ctrl)
        entry_new.pack(side='left', fill='x', expand=True, padx=(0, 5))


        def add_item():
            val = entry_new.get().strip()
            if not val:
                return

            # ----- 탭 간 중복/우선순위 처리 -----
            lb_forbidden = getattr(self, "listbox_forbidden_keywords", None)
            lb_allowed   = getattr(self, "listbox_allowed_phrases", None)
            lb_white     = getattr(self, "listbox_whitelist_contains", None)

            forbidden_items = lb_forbidden.get(0, tk.END) if lb_forbidden else ()
            allowed_items   = lb_allowed.get(0, tk.END) if lb_allowed else ()
            white_items     = lb_white.get(0, tk.END) if lb_white else ()

            # 우선순위: 금지키워드 > 예외허용문구 > 화이트리스트
            if key == "forbidden_keywords":
                moved = False
                if val in allowed_items and lb_allowed:
                    idx = allowed_items.index(val)
                    lb_allowed.delete(idx)
                    moved = True
                if val in white_items and lb_white:
                    idx = white_items.index(val)
                    lb_white.delete(idx)
                    moved = True
                if moved:
                    messagebox.showinfo(
                        "키워드 이동",
                        f"'{val}' 은(는) 다른 탭에서 제거되고 [금지 키워드]로 이동합니다."
                    )

            elif key == "allowed_phrases":
                if val in forbidden_items:
                    messagebox.showwarning(
                        "추가 불가",
                        f"'{val}' 은(는) 이미 [금지 키워드]에 있으므로\n"
                        "[예외 허용 문구]에 추가할 수 없습니다.\n\n"
                        "필요하다면 먼저 금지 키워드에서 제거해 주세요."
                    )
                    return
                if val in white_items and lb_white:
                    idx = white_items.index(val)
                    lb_white.delete(idx)
                    messagebox.showinfo(
                        "키워드 이동",
                        f"'{val}' 은(는) [화이트리스트]에서 제거되고\n"
                        "[예외 허용 문구]로 이동합니다."
                    )

            elif key == "whitelist_contains":
                if val in forbidden_items:
                    messagebox.showwarning(
                        "추가 불가",
                        f"'{val}' 은(는) 이미 [금지 키워드]에 있어\n"
                        "[화이트리스트]에 추가할 수 없습니다."
                    )
                    return
                if val in allowed_items:
                    messagebox.showwarning(
                        "추가 불가",
                        f"'{val}' 은(는) 이미 [예외 허용 문구]에 있어\n"
                        "[화이트리스트]에 추가할 수 없습니다."
                    )
                    return

            # ----- 현재 탭 안에서 중복 체크 + 메시지 -----
            existing = listbox.get(0, tk.END)
            if val in existing:
                if key == "forbidden_keywords":
                    tab_name = "금지 키워드"
                elif key == "allowed_phrases":
                    tab_name = "예외 허용 문구"
                else:
                    tab_name = "화이트리스트(포함)"

                messagebox.showwarning(
                    "중복 키워드",
                    f"'{val}' 은(는) 이미 [{tab_name}] 목록에 있습니다."
                )
                return

            # 실제 추가
            listbox.insert(tk.END, val)
            entry_new.delete(0, tk.END)


            # ----- 현재 탭 안에서만 중복 방지 -----
            if val and val not in listbox.get(0, tk.END):
                listbox.insert(tk.END, val)
                entry_new.delete(0, tk.END)

        def del_item():
            selection = listbox.curselection()
            for index in reversed(selection):
                listbox.delete(index)

        btn_add = ttk.Button(frame_ctrl, text="추가", command=add_item)
        btn_add.pack(side='left', padx=2)
        
        btn_del = ttk.Button(frame_ctrl, text="삭제", command=del_item)
        btn_del.pack(side='left', padx=2)

        # 나중에 저장할 때 참조하기 위해 widget 저장 (중요)
        setattr(self, f"listbox_{key}", listbox)
    def on_save(self):
        # Listbox 내용 -> Config 객체 반영
        banned = list(self.listbox_forbidden_keywords.get(0, tk.END))
        allowed = list(self.listbox_allowed_phrases.get(0, tk.END))
        whitelist_contains = list(self.listbox_whitelist_contains.get(0, tk.END))

        # 1) 공백 정리 + 각 리스트 내 중복 제거(안전장치)
        def normalize_list(lst):
            result = []
            seen = set()
            for s in lst:
                s = s.strip()
                if not s:
                    continue
                if s in seen:
                    continue
                seen.add(s)
                result.append(s)
            return result

        banned = normalize_list(banned)
        allowed = normalize_list(allowed)
        whitelist_contains = normalize_list(whitelist_contains)

        # 2) 서로 다른 탭 간 중복 제거 (공존 불가)
        #    우선순위: 금지키워드 > 예외허용문구 > 화이트리스트(포함)
        conflict_count = 0

        # 금지키워드에 있는 단어는 다른 탭에서 제거
        banned_set = set(banned)
        allowed_before = set(allowed)
        whitelist_before = set(whitelist_contains)

        allowed = [x for x in allowed if x not in banned_set]
        whitelist_contains = [x for x in whitelist_contains if x not in banned_set]

        conflict_count += len(allowed_before & banned_set)
        conflict_count += len(whitelist_before & banned_set)

        # 예외허용문구에 있는 단어는 화이트리스트에서 제거
        allowed_set = set(allowed)
        whitelist_contains = [x for x in whitelist_contains if x not in allowed_set]
        conflict_count += len(whitelist_before & allowed_set)

        # 3) Config에 최종 반영
        self.config_obj.config["forbidden_keywords"] = banned
        self.config_obj.config["allowed_phrases"] = allowed
        self.config_obj.config["whitelist_contains"] = whitelist_contains
        
        self.config_obj.save()
        self.update_callback(self.config_obj)  # 메인 GUI에 알림

        # 4) 안내 메시지
        msg = "설정이 저장되고 검증기에 반영되었습니다."
        if conflict_count:
            msg += (
                f"\n\n서로 다른 탭에 중복으로 들어있던 키워드 {conflict_count}개는 "
                "우선순위(금지키워드 > 예외허용문구 > 화이트리스트)에 따라 "
                "자동으로 정리되었습니다."
            )
        messagebox.showinfo("저장 완료", msg)
        self.destroy()


# ========================================================
# 4. 메인 GUI 클래스
# ========================================================
class Stage4FilterGUI(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Stage 4-1: 상품명 필터링 도구 (v7.0 with CSV Import)")
        self.geometry("800x650")

        style = ttk.Style()
        try: style.theme_use('clam')
        except: pass

        self.config_obj = Stage4Config(CONFIG_FILE)
        self.validator = Stage4Validator(self.config_obj)
        self.input_file_path = tk.StringVar()

        self._build_ui()
        self._log(f"[설정] 규칙/화이트리스트 파일: {CONFIG_FILE}")
        self._log(f"[설정] 누적 탈락 로그 파일: {GLOBAL_REJECT_LOG}")

    def _build_ui(self):
        top_frame = ttk.Frame(self, padding="15")
        top_frame.pack(fill="x")

        lbl_title = ttk.Label(
            top_frame,
            text="Stage 4-1 자동 필터링 (Python Code)",
            font=("맑은 고딕", 14, "bold")
        )
        lbl_title.pack(anchor="w", pady=(0, 10))

        # 설정 편집 버튼 추가
        btn_edit_cfg = ttk.Button(top_frame, text="⚙️ 키워드 설정 편집", command=self._open_config_editor)
        btn_edit_cfg.place(relx=1.0, rely=0, anchor='ne') 

        file_frame = ttk.LabelFrame(top_frame, text="입력 파일 (Stage 3 결과 엑셀)", padding="10")
        file_frame.pack(fill="x", pady=(10, 0))

        entry_path = ttk.Entry(file_frame, textvariable=self.input_file_path)
        entry_path.pack(side="left", fill="x", expand=True, padx=(0, 5))
        btn_browse = ttk.Button(file_frame, text="파일 찾기", command=self._select_file)
        btn_browse.pack(side="right")

        mid_frame = ttk.Frame(self, padding="15")
        mid_frame.pack(fill="x")

        btn_run = ttk.Button(mid_frame, text="▶ 검사 및 정제 시작", command=self._run_process)
        btn_run.pack(fill="x", ipady=5)

        log_frame = ttk.LabelFrame(self, text="진행 로그", padding="10")
        log_frame.pack(fill="both", expand=True, padx=15, pady=10)
        self.log_widget = ScrolledText(log_frame, height=15, state='disabled')
        self.log_widget.pack(fill="both", expand=True)

    def _log(self, msg):
        self.log_widget.config(state='normal')
        self.log_widget.insert(tk.END, msg + "\n")
        self.log_widget.see(tk.END)
        self.log_widget.config(state='disabled')
        self.update_idletasks()

    def _select_file(self):
        path = filedialog.askopenfilename(
            title="Stage4-1 엑셀 선택 (T3 또는 T4 버전)",
            filetypes=[("Excel", "*.xlsx *.xls")]
        )
        if path:
            # T3 또는 T4 포함 여부 검증
            base_name = os.path.splitext(os.path.basename(path))[0]
            if not re.search(r"_T[34]_[Ii]\d+", base_name, re.IGNORECASE):
                messagebox.showerror(
                    "오류", 
                    f"이 도구는 T3 또는 T4 버전의 엑셀 파일만 처리할 수 있습니다.\n\n"
                    f"선택한 파일: {os.path.basename(path)}\n"
                    f"파일명에 '_T3_I*' 또는 '_T4_I*' 패턴이 포함되어 있어야 합니다."
                )
                return
            self.input_file_path.set(path)
            self._log(f"[선택] {os.path.basename(path)}")

    def _open_config_editor(self):
        ConfigEditor(self, self.config_obj, self._on_config_updated)

    def _on_config_updated(self, new_config):
        self.validator.update_config(new_config)
        self._log("[알림] 변경된 설정이 적용되었습니다.")

    def _run_process(self):
        input_path = self.input_file_path.get()
        if not input_path or not os.path.exists(input_path):
            messagebox.showwarning("경고", "파일을 선택하세요.")
            return
        
        # T3 또는 T4 포함 여부 검증
        base_name = os.path.splitext(os.path.basename(input_path))[0]
        if not re.search(r"_T[34]_[Ii]\d+", base_name, re.IGNORECASE):
            messagebox.showerror(
                "오류", 
                f"이 도구는 T3 또는 T4 버전의 엑셀 파일만 처리할 수 있습니다.\n\n"
                f"선택한 파일: {os.path.basename(input_path)}\n"
                f"파일명에 '_T3_I*' 또는 '_T4_I*' 패턴이 포함되어 있어야 합니다."
            )
            return

        try:
            self._log("--- 검사 시작 ---")
            df = pd.read_excel(input_path)
            
            target_col = 'ST3_결과상품명'
            if target_col not in df.columns:
                cols = [c for c in df.columns if "ST3" in c and "상품명" in c]
                if cols: target_col = cols[0]
                else: raise ValueError("컬럼을 찾을 수 없습니다.")

            if '상품코드' not in df.columns:
                if messagebox.askyesno("확인", "'상품코드' 없음. 행 번호 사용?"):
                    df['상품코드'] = df.index + 2
                else: return

            dropped_logs = []
            total = 0
            passed = 0

            for idx, row in df.iterrows():
                p_code = row['상품코드']
                raw = str(row.get(target_col, ""))
                if not raw.strip() or raw == "nan": continue

                cands = [c.strip() for c in raw.split('\n') if c.strip()]
                total += len(cands)
                valid = []
                
                for c in cands:
                    ok, reason = self.validator.validate(c)
                    if ok:
                        if c not in valid: valid.append(c)
                    else:
                        dropped_logs.append({
                            "입력파일": os.path.basename(input_path),
                            "시각": datetime.now().strftime("%Y-%m-%d %H:%M"),
                            "상품코드": p_code,
                            "탈락상품명": c,
                            "사유": reason
                        })
                
                if valid:
                    df.at[idx, target_col] = "\n".join(valid)
                    passed += len(valid)
                else:
                    df.at[idx, target_col] = "" 

            # T3 → T4, T4 → T4로 파일명 생성
            base_dir = os.path.dirname(input_path)
            base_name = os.path.splitext(os.path.basename(input_path))[0]
            
            # 입력 파일명에서 버전 정보 추출 (괄호 포함 가능, 예: _I5(업완))
            pattern = r"_T(\d+)_I(\d+)(\([^)]+\))?"
            match = re.search(pattern, base_name, re.IGNORECASE)
            if match:
                current_t = int(match.group(1))
                current_i = int(match.group(2))
                i_suffix = match.group(3) or ""  # 괄호 부분이 있으면 유지 (예: (업완))
                # 원본명 추출 (버전 정보 제거, 괄호 포함)
                original_name = re.sub(r"_T\d+_I\d+(\([^)]+\))?.*$", "", base_name, flags=re.IGNORECASE).rstrip("_")
                
                # T3이면 T4로, T4이면 T4 그대로 저장 (I 버전과 (완), (업완) 등 모든 후속 정보 유지)
                if current_t == 3:
                    new_t = 4
                    new_i = current_i
                    out_filename = f"{original_name}_T{new_t}_I{new_i}{i_suffix}.xlsx"
                elif current_t == 4:
                    # T4는 입력 파일명의 버전 부분을 그대로 유지 (I 버전, (완), (업완) 등 모든 후속 정보 포함)
                    # _T4_I 이후의 모든 부분을 추출하여 그대로 사용 (I 버전은 절대 변경하지 않음)
                    version_suffix_match = re.search(r"_T4_I\d+(\([^)]+\))?.*$", base_name, re.IGNORECASE)
                    if version_suffix_match:
                        version_suffix = version_suffix_match.group(0)  # 예: "_T4_I5(완)", "_T4_I5(업완)", "_T4_I3"
                        out_filename = f"{original_name}{version_suffix}.xlsx"
                    else:
                        # 패턴이 없으면 기본 형식으로 (이 경우는 거의 발생하지 않음)
                        out_filename = f"{original_name}_T{current_t}_I{current_i}{i_suffix}.xlsx"
                else:
                    # T3, T4가 아니면 T 버전만 +1 (I는 유지)
                    new_t = current_t + 1
                    new_i = current_i
                    out_filename = f"{original_name}_T{new_t}_I{new_i}.xlsx"
            else:
                # 버전 정보가 없으면 T4_I0으로 생성
                out_filename = f"{base_name}_T4_I0.xlsx"
            out_path = os.path.join(base_dir, out_filename)

            with pd.ExcelWriter(out_path, engine='openpyxl') as writer:
                df.to_excel(writer, sheet_name="정제결과", index=False)
                if dropped_logs:
                    pd.DataFrame(dropped_logs).to_excel(writer, sheet_name="ST4_탈락로그", index=False)
                else:
                    pd.DataFrame(columns=["내용없음"]).to_excel(writer, sheet_name="ST4_탈락로그", index=False)

            if dropped_logs:
                new_log = pd.DataFrame(dropped_logs)
                if os.path.exists(GLOBAL_REJECT_LOG):
                    try:
                        old = pd.read_csv(GLOBAL_REJECT_LOG, encoding="utf-8-sig")
                        combined = pd.concat([old, new_log]).drop_duplicates()
                        combined.to_csv(GLOBAL_REJECT_LOG, index=False, encoding="utf-8-sig")
                    except: new_log.to_csv(GLOBAL_REJECT_LOG, index=False, encoding="utf-8-sig")
                else:
                    new_log.to_csv(GLOBAL_REJECT_LOG, index=False, encoding="utf-8-sig")

            self._log(f"[완료] 총 {total}개 중 {passed}개 통과, {len(dropped_logs)}개 탈락.")
            self._log(f"[저장] {out_path}")
            
            # 메인 런처 현황판에 T4-1(금지어완료) 상태 기록 (img 상태는 변경하지 않음)
            try:
                root_name = get_root_filename(out_path)
                JobManager.update_status(root_name, text_msg="T4-1(금지어완료)")
                self._log(f"[Launcher] 상태 업데이트: {root_name} -> T4-1(금지어완료)")
            except Exception as e:
                self._log(f"[WARN] 런처 연동 실패: {e}")
            
            messagebox.showinfo("완료", f"처리 완료.\n탈락: {len(dropped_logs)}건")
            
            base_dir = os.path.dirname(input_path)
            if messagebox.askyesno("폴더 열기", "폴더를 여시겠습니까?"):
                if sys.platform=='win32': os.startfile(base_dir)
                else: subprocess.Popen(['open' if sys.platform=='darwin' else 'xdg-open', base_dir])

        except Exception as e:
            self._log(f"[오류] {e}")
            messagebox.showerror("에러", str(e))

if __name__ == "__main__":
    app = Stage4FilterGUI()
    app.mainloop()