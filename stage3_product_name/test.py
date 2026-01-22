import os 
import pandas as pd
import tkinter as tk
from tkinter import filedialog, messagebox
from tkinter.scrolledtext import ScrolledText
import threading
import traceback

# =========================
# Stage 3 프롬프트 헤더 (고정 규칙 설명)
# =========================

STAGE3_HEADER = """너는 “위탁판매 특화 상품명 크리에이터(Stage 3)”다.

이전 단계(Stage 2)에서 추출한 구조화 데이터(JSON)를 입력받아,
쿠팡/네이버/11번가/지마켓/옥션 등 오픈마켓에 적합한 최종 상품명 후보를 생성하는 것이 너의 역할이다.

====================
[1] 작업 맥락 (위탁판매 전용 룰)
====================
1. 위탁판매자는 브랜드 경쟁력이 없다.
   - 상품명에 브랜드가 들어가면 공식몰·대형 판매자와의 가격 비교에서 항상 불리하다.
   - 따라서 브랜드명은 상품명에서 무조건 제거한다.
   - meta나 naming_seeds에 브랜드가 있어도, 상품명에는 절대 넣지 않는다.

2. 우리의 목표
   - 도매처가 제공한 상품명보다
     - 더 정확하고,
     - 기능·용도·상황이 분명하고,
     - 감성과 키워드가 자연스럽게 녹아 있는,
     잘 팔리는 새 상품명을 만드는 것이다.
   - 근거는 항상 Stage 2 JSON(상세설명·이미지 기반 정보)이어야 하며,
     상세설명에 없는 정보는 절대 지어내지 않는다.

====================
[2] 입력 형식
====================
너는 항상 두 덩어리의 입력을 함께 받는다고 가정한다.

[설정]
- 마켓: "쿠팡" / "네이버" / "11번가" / "지마켓" / "옥션" / 기타
- 최대글자수: 정수 (예: 40, 50)
  - 별도 값이 없으면 기본 50자 이내로 맞춘다.
- 출력개수: 정수 (예: 5, 6, 7, 8)
  - 별도 값이 없으면 5~8개 정도의 서로 다른 스타일의 상품명을 생성한다.
- 명명전략:
  - "통합형"      → 옵션이 있어도 대표 상품명만 (색상/옵션명 미노출)
  - "옵션포함형"  → 색상/두께/용량 등 옵션 특성을 활용한 이름
  - 값이 없으면 기본값은 "통합형"으로 간주한다.

[상품데이터(JSON)]
- Stage 2에서 생성된 JSON 1개가 통째로 들어온다고 가정한다.
- JSON은 대략 아래와 같은 구조를 가진다.

{ 
  "meta": {
    "기본상품명": "...",
    "판매형태": "단품형" 또는 "옵션형",
    "옵션_원본": "...",
    "카테고리_경로": "대>중>소"
  },
  "core_attributes": {
    "상품타입": "...",
    "상위카테고리": "...",
    "사용대상": [...],
    "주요용도": [...],
    "재질": [...],
    "스타일/특징": [...],
    "사이즈": {
      "표기형식": "...",
      "주요_길이_1_cm": 숫자 또는 null,
      "주요_길이_2_cm": 숫자 또는 null,
      "기타": "..."
    },
    "색상/옵션_리스트": [...],
    "옵션구분방식": "...",
    "세트구성": "...",
    "기타기능": [...]
  },
  "usage_scenarios": [
    "... 실제 사용 상황 설명 문장들 ..."
  ],
  "search_keywords": [
    "... 검색용 키워드 리스트 ..."
  ],
  "naming_seeds": {
    "상품핵심명사": [...],
    "스타일형용사": [...],
    "기능/효과표현": [...],
    "상황/장소/계절": [...],
    "타깃고객": "... 한 줄 설명 ...",
    "차별화포인트": [...]
  }
}

- 너는 이 JSON 안의 모든 정보를 자유롭게 활용할 수 있지만,
  JSON에 없는 내용을 새로 지어내서는 안 된다.


====================
[3] 내부 점검 절차 (사용자에게는 보이지 않음)
====================
실제 출력 전에, 너는 내부적으로만 다음을 수행한다.

1. JSON에서 핵심을 요약한다.
   - 이 상품의 핵심 카테고리 한 줄
   - 고객 입장에서의 핵심 문제·욕구 1~2개
   - 그 문제를 해결하는 핵심 기능·효과 2~3개
   - 어떤 사람이 어떤 상황에서 쓰는지 대표 사용 시나리오 1~2개

2. 이 요약을 기반으로, 서로 다른 패턴의 이름 후보를 머릿속에서 먼저 설계한다.
   - 기능·효과 중심형
   - 상황·용도 중심형 (언제·어디서·무엇 할 때 쓰는지)
   - 감성·스타일 중심형 (느낌, 분위기, 인테리어 톤 등)

3. 각 후보에 대해 스스로 체크한다.
   - 글자수: 설정된 최대 글자수 이내인지
   - 위탁룰: 브랜드 없음, 과장·허위 없음
   - 기호 룰: 슬래시(/), 쉼표(,), 하이픈(-) 미사용
   - 후보 간 차별성: 구조·키워드가 너무 비슷하지 않은지
   - 검색성: search_keywords와 상품핵심명사에서 중요한 단어가 자연스럽게 포함되어 있는지

4. 이 자기 점검을 통과한 후보들만 최종 출력한다.
   내부 과정·초안·평가는 어떤 형식으로도 출력하지 않는다.

====================
[4] 공통 작성 규칙
====================
1. 기본 구조(권장 패턴)
   - [핵심 카테고리/타입] + [주요 기능·효과 1~2개] + [핵심 사용상황 혹은 차별포인트 1개]
   - 예시 패턴:
     - 겨울 방한 니트 귀마개 모자 넥워머 세트
     - 폼 단열 쿠션 포인트 벽지 50x250cm 셀프시공
     - 무소음 수능 시험용 학생 손목시계 우레탄밴드

2. 브랜드·상호 금지
   - 브랜드명, 상호명, 쇼핑몰명(○○몰, ○○샵, 마켓 등)은 전부 제거한다.
   - JSON에 브랜드가 있더라도, 상품명에는 절대 사용하지 않는다.

3. 광고/이벤트성 금지
   - 무료배송, 최저가, 특가, 행사, 1+1, 사은품, 인기템, MD추천, 한정수량 등
     모든 홍보·이벤트성 단어를 금지한다.
   - 느낌표, 이모티콘(♥, ★ 등), 과도한 강조 표현도 사용하지 않는다.

4. 기호/문장 부호 규칙
   - 슬래시(/) 절대 사용 금지
   - 쉼표(,) 사용 금지
   - 하이픈(-) 사용 금지
   - 가능한 한 한글+공백 중심으로만 구성한다.
   - 사이즈 표기 등에서 x는 사용 가능하다. (예: 50x250cm, 4x6 액자)
   - 괄호는 꼭 필요할 때만 최소 사용 가능하지만, 가급적 피한다.

5. 사실성·숫자
   - Stage 2 JSON에 없는 스펙(재질, 인증, 수량, 사이즈, 용량 등)을 새로 만들지 않는다.
   - 숫자·단위는 JSON에 있을 때만 사용하고, 값을 바꾸지 않는다.

6. 타깃 표현
   - 사용대상(남성/여성/아동/학생 등)이 JSON에 명확하게 있을 때만 자연스럽게 활용한다.
   - 타깃이 애매하면, 공용/데일리 같은 힘없는 표현 대신
     기능·상황·카테고리 중심으로만 이름을 구성한다.

7. 언어 스타일
   - 자연스러운 한국어 상품명, 명사구 형태로만 작성한다.
   - “합니다, 해요, 어떠세요?” 같은 문장형 표현은 사용하지 않는다.
   - DIY, camping, LED처럼 실제 시장에서 많이 쓰는 짧은 영문 키워드는
     자연스럽게 섞어 쓸 수 있으나, 과하게 늘어놓지 않는다.

====================
[5] 통합형 / 옵션포함형
====================
1) 통합형 (기본 모드)
   - 판매형태(단품/옵션형)에 상관없이,
     상품 전체를 대표하는 1개의 상품 단위로 이름을 만든다.
   - 개별 옵션(색상, 두께, 용량명 등)은 상품명에 직접 나열하지 않는다.

2) 옵션포함형
   - 이 모드가 설정된 경우에만, 옵션을 활용한 이름을 허용한다.
   - 원칙:
     - 옵션명 그대로 줄줄이 나열하지 말고,
     - “컬러 선택 가능”, “두께 선택 가능” 등으로 옵션 개념만 표현하거나,
     - {색상}, {두께} 같은 자리표시자 개념으로 처리할 수 있다.

====================
[6] 마켓/글자수 규칙
====================
1. 최대글자수
   - 최대글자수를 넘지 않게 작성한다.
   - 없으면 기본 50자 이내로 맞춘다.
   - 초과할 경우, 중요도 낮은 요소부터 순서대로 제거한다.
     감성어 → 상세 상황어 일부 → 부가 기능/효과 → 부가 스펙 순서.

2. 마켓별 톤
   - 네이버:
     - 검색 키워드 밀도는 의식하되, 키워드 나열처럼 보이지 않게 쓴다.
   - 쿠팡:
     - 카테고리 + 핵심 기능 + 사용 상황이 한눈에 들어오도록 구성한다.
   - 그 외 마켓:
     - 네이버/쿠팡 중간 느낌의 일반 쇼핑몰 톤으로 작성한다.

====================
[7] 출력 규칙
====================
1. 출력 형태
   - 각 줄마다 오직 하나의 상품명만 쓴다.
   - 번호, 불릿, 따옴표, “후보1:” 같은 라벨은 사용하지 않는다.
   - 설명, 해설, 이유, 평가는 어떤 형식으로도 출력하지 않는다.

2. 개수
   - 출력개수가 지정되어 있으면 그 개수 이내에서 품질이 충분한 후보만 생성한다.
   - 지정되지 않으면 5~8개 정도의 서로 다른 스타일의 상품명을 생성한다.
   - 단지 개수를 채우기 위해 비슷한 구조의 이름을 억지로 반복해서 만들지 않는다.

====================
[8] 최종 행동
====================
1. 설정 값과 Stage 2 JSON을 해석한다.
2. 내부 점검 절차에 따라 다양한 패턴의 후보를 설계하고, 품질을 검토한다.
3. 위 모든 규칙을 지키는 상품명 후보들만 여러 줄로 출력한다.
4. 그 외의 어떤 부가 설명도 출력하지 않는다.
"""


def safe_str(v):
    """NaN/None 안전하게 문자열로 변환 + strip."""
    if v is None:
        return ""
    try:
        if pd.isna(v):
            return ""
    except Exception:
        pass
    return str(v).strip()


def build_stage3_prompt(row) -> str:
    """
    한 행(row)에 대해 Stage3 프롬프트 1개 생성.
    - 설정값(마켓, 최대글자수, 출력개수, 명명전략)을 읽고
    - ST2_JSON 컬럼에 들어 있는 JSON을 그대로 붙인다.
    """
    json_payload = safe_str(row.get("ST2_JSON", ""))

    # ST2_JSON이 비어 있으면 프롬프트를 만들지 않고 빈 문자열 반환
    if not json_payload:
        return ""

    # 설정값들: 엑셀 컬럼(이미 UI에서 일괄 세팅된 값)을 사용
    market = safe_str(row.get("마켓", "쿠팡"))
    max_len = safe_str(row.get("최대글자수", "50"))
    strategy = safe_str(row.get("명명전략", "통합형"))
    output_count = safe_str(row.get("출력개수", ""))

    # [설정] 블록 구성
    lines = []
    lines.append(f'마켓: "{market}"')
    lines.append(f"최대글자수: {max_len}")
    if output_count:  # 값이 있으면만 사용
        lines.append(f"출력개수: {output_count}")
    lines.append(f'명명전략: "{strategy}"')
    settings_block = "\n".join(lines)

    # 최종 프롬프트 조립
    prompt = (
        STAGE3_HEADER
        + "\n\n====================\n[입력 데이터]\n====================\n\n"
        + "[설정]\n"
        + settings_block
        + "\n\n[상품데이터(JSON)]\n"
        + json_payload
    )
    return prompt


def process_excel_for_stage3(
    excel_path: str,
    log_func=print,
    max_len_value: int = 50,
    output_count_value: int | None = None,
):
    """
    ST2_JSON 컬럼이 포함된 엑셀을 입력으로 받아,
    각 행마다 ST3_프롬프트 컬럼을 생성한 뒤
    [원본파일명]_stage3_prompts.xlsx 로 저장.

    max_len_value / output_count_value 는 UI에서 선택한 값을 그대로 사용.
    - max_len_value: 전체 행 공통 최대글자수 (기본 50)
    - output_count_value: None이면 자동(5~8개), 아니면 지정 개수
    """
    log = log_func
    log(f"[INFO] 엑셀 읽는 중: {excel_path}")

    df = pd.read_excel(excel_path, header=0)

    # ST2_JSON 필수
    if "ST2_JSON" not in df.columns:
        raise ValueError("엑셀에 'ST2_JSON' 컬럼이 없습니다. Stage2 단계에서 ST2_JSON 컬럼을 포함해 주세요.")

    # 마켓 / 명명전략 기본값 세팅 (엑셀에 없어도 됨)
    if "마켓" not in df.columns:
        df["마켓"] = "쿠팡"
        log("[INFO] '마켓' 컬럼이 없어 기본값 '쿠팡'으로 생성했습니다.")
    if "명명전략" not in df.columns:
        df["명명전략"] = "통합형"
        log("[INFO] '명명전략' 컬럼이 없어 기본값 '통합형'으로 생성했습니다.")

    # UI에서 넘어온 값으로 전체 행 일괄 세팅
    df["최대글자수"] = max_len_value
    log(f"[INFO] 전체 행의 '최대글자수'를 {max_len_value}로 설정했습니다.")

    if output_count_value is None:
        df["출력개수"] = ""
        log("[INFO] '출력개수'는 자동 모드(5~8개)로 설정했습니다.")
    else:
        df["출력개수"] = output_count_value
        log(f"[INFO] 전체 행의 '출력개수'를 {output_count_value}로 설정했습니다.")

    log("[INFO] ST3_프롬프트 생성 중...")

    def make_prompt(row):
        p = build_stage3_prompt(row)
        if not p:
            if safe_str(row.get("ST2_JSON", "")) == "":
                log(f"[WARN] ST2_JSON 비어 있어 프롬프트 미생성 (상품코드: {row.get('상품코드', '')})")
            return ""
        return p

    df["ST3_프롬프트"] = df.apply(make_prompt, axis=1)

    # GPT Stage3 최종 결과 붙여넣을 컬럼: 없으면 새로 생성
    if "ST3_결과상품명" not in df.columns:
        df["ST3_결과상품명"] = ""

    base_dir = os.path.dirname(excel_path)
    base_name = os.path.splitext(os.path.basename(excel_path))[0]
    out_path = os.path.join(base_dir, f"{base_name}_stage3_prompts.xlsx")

    df.to_excel(out_path, index=False)
    log(f"[INFO] Stage3 프롬프트 포함 엑셀 저장 완료: {out_path}")
    return out_path


# =========================
# Tkinter GUI
# =========================

class Stage3PromptApp:
    def __init__(self, root):
        self.root = root
        root.title("Stage3 프롬프트 생성기 (최종 상품명 후보용)")
        root.geometry("880x600")

        # 설명
        desc = (
            "① 입력 파일\n"
            "   - Stage2에서 생성한 '*_stage2_prompts.xlsx' 파일에\n"
            "     ST2_JSON 컬럼에 LLM 응답(JSON)을 붙여 넣은 엑셀을 사용합니다.\n\n"
            "② 필요 컬럼\n"
            "   - 필수: ST2_JSON\n"
            "   - 선택(없으면 자동 생성): 마켓, 명명전략\n\n"
            "③ 옵션 (전체 행 공통 적용)\n"
            "   - 최대글자수: 아래에서 40/45/50/60 또는 직접입력\n"
            "   - 출력개수: 자동(5~8개) 또는 5/6/7/8개 중 선택\n"
        )
        lbl_desc = tk.Label(root, text=desc, justify="left", anchor="w")
        lbl_desc.pack(fill=tk.X, padx=10, pady=(8, 4))

        # 상단 컨트롤 영역
        ctrl_frame = tk.Frame(root)
        ctrl_frame.pack(pady=5, fill=tk.X, padx=10)

        # 파일 선택 버튼
        self.btn_select = tk.Button(
            ctrl_frame,
            text="Stage2 JSON 포함 엑셀 불러오기",
            command=self.on_select_excel
        )
        self.btn_select.grid(row=0, column=0, padx=(0, 15), pady=2, sticky="w")

        # ===== 최대글자수 선택 =====
        max_frame = tk.LabelFrame(ctrl_frame, text="최대글자수", padx=5, pady=3)
        max_frame.grid(row=0, column=1, padx=5, sticky="w")

        self.max_len_mode = tk.StringVar(value="50")  # 기본 50자
        self.max_len_custom = tk.StringVar()

        tk.Radiobutton(max_frame, text="40자", value="40", variable=self.max_len_mode).grid(row=0, column=0, sticky="w")
        tk.Radiobutton(max_frame, text="45자", value="45", variable=self.max_len_mode).grid(row=0, column=1, sticky="w")
        tk.Radiobutton(max_frame, text="50자(기본)", value="50", variable=self.max_len_mode).grid(row=0, column=2, sticky="w")
        tk.Radiobutton(max_frame, text="60자", value="60", variable=self.max_len_mode).grid(row=0, column=3, sticky="w")
        tk.Radiobutton(max_frame, text="직접입력", value="custom", variable=self.max_len_mode).grid(row=0, column=4, sticky="w")

        tk.Label(max_frame, text="숫자:").grid(row=1, column=0, sticky="e")
        tk.Entry(max_frame, textvariable=self.max_len_custom, width=6).grid(row=1, column=1, sticky="w")

        # ===== 출력개수 선택 =====
        out_frame = tk.LabelFrame(ctrl_frame, text="출력개수", padx=5, pady=3)
        out_frame.grid(row=0, column=2, padx=10, sticky="w")

        # 기본값: 자동(5~8개)
        self.out_count_mode = tk.StringVar(value="auto")

        tk.Radiobutton(out_frame, text="자동(5~8개)", value="auto", variable=self.out_count_mode).grid(row=0, column=0, sticky="w")
        tk.Radiobutton(out_frame, text="5개", value="5", variable=self.out_count_mode).grid(row=0, column=1, sticky="w")
        tk.Radiobutton(out_frame, text="6개", value="6", variable=self.out_count_mode).grid(row=0, column=2, sticky="w")
        tk.Radiobutton(out_frame, text="7개", value="7", variable=self.out_count_mode).grid(row=0, column=3, sticky="w")
        tk.Radiobutton(out_frame, text="8개", value="8", variable=self.out_count_mode).grid(row=0, column=4, sticky="w")

        self.log_box = ScrolledText(root, wrap=tk.WORD, height=22)
        self.log_box.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

    # 실제 로그 추가는 메인 스레드에서만 수행
    def _append_log(self, msg: str):
        self.log_box.insert(tk.END, msg + "\n")
        self.log_box.see(tk.END)

    def log(self, msg: str):
        self.root.after(0, self._append_log, msg)

    def on_select_excel(self):
        filetypes = [
            ("Excel files", "*.xlsx *.xls"),
            ("All files", "*.*"),
        ]
        filepath = filedialog.askopenfilename(
            title="Stage2 JSON이 들어 있는 엑셀 파일을 선택하세요",
            filetypes=filetypes
        )
        if not filepath:
            return

        # ===== 최대글자수 파싱 =====
        mode = self.max_len_mode.get()
        if mode == "custom":
            s = self.max_len_custom.get().strip()
            if not s.isdigit():
                messagebox.showerror("입력 오류", "최대글자수(직접입력)는 숫자만 입력해 주세요.")
                return
            max_len_value = int(s)
        else:
            max_len_value = int(mode)  # "40","45","50","60"

        # ===== 출력개수 파싱 =====
        out_mode = self.out_count_mode.get()
        if out_mode == "auto":
            output_count_value = None
        else:
            output_count_value = int(out_mode)

        # 작업 중에는 버튼 비활성화
        self.btn_select.config(state=tk.DISABLED)

        thread = threading.Thread(
            target=self.run_generation,
            args=(filepath, max_len_value, output_count_value)
        )
        thread.daemon = True
        thread.start()

    def run_generation(self, filepath: str, max_len_value: int, output_count_value: int | None):
        try:
            self.log(f"[INFO] 선택된 파일: {filepath}")
            self.log(f"[INFO] 설정 - 최대글자수: {max_len_value}")
            if output_count_value is None:
                self.log("[INFO] 설정 - 출력개수: 자동(5~8개)")
            else:
                self.log(f"[INFO] 설정 - 출력개수: {output_count_value}개")

            out_path = process_excel_for_stage3(
                filepath,
                log_func=self.log,
                max_len_value=max_len_value,
                output_count_value=output_count_value,
            )
            self.log("[INFO] 모든 작업 완료")

            self.root.after(
                0,
                lambda: messagebox.showinfo(
                    "완료",
                    f"Stage3 프롬프트 생성이 완료되었습니다.\n\n{out_path}"
                )
            )
        except Exception as e:
            self.log("[FATAL] 오류 발생:")
            self.log(str(e))
            self.log(traceback.format_exc())
            self.root.after(
                0,
                lambda: messagebox.showerror(
                    "오류",
                    f"작업 중 오류가 발생했습니다.\n\n{e}"
                )
            )
        finally:
            self.root.after(0, lambda: self.btn_select.config(state=tk.NORMAL))


def main():
    root = tk.Tk()
    app = Stage3PromptApp(root)
    root.mainloop()


if __name__ == "__main__":
    main()
