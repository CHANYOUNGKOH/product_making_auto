import os
import threading
from datetime import datetime

import pandas as pd
import tkinter as tk
from tkinter import filedialog, messagebox
from tkinter.scrolledtext import ScrolledText
from tkinter import ttk

# stage3_core에서 공통 로직/설정 import
from stage3_core import (
    safe_str,                      # NaN/None → "" 변환 + strip
    Stage3Settings,                # 전역/기본 설정 dataclass
    Stage3Request,                 # 한 행에 대한 Stage3 요청 정보 (프롬프트 등)
    build_stage3_request_from_row, # (row + 기본세팅) → Stage3Request
)


# =========================
# 엑셀 처리 로직 (Stage3 프롬프트 생성)
# =========================

def process_excel_for_stage3(
    excel_path: str,
    log_func=print,
    skip_existing: bool = True,
    default_settings: Stage3Settings | None = None,
):
    """
    ST2_JSON 컬럼이 포함된 엑셀을 입력으로 받아,
    각 행마다 ST3_프롬프트 컬럼을 생성/갱신한 뒤
    [원본파일명]_stage3_prompts.xlsx 로 저장.

    - Stage3 프롬프트 텍스트 자체는 stage3_core.STAGE3_PROMPT_TEMPLATE 를 사용.
    - 행별 설정값은 다음 우선순위로 적용됨:
        1) 행의 ST3_마켓 / ST3_최대글자수 / ST3_출력개수 / ST3_명명전략
        2) (호환용) 행의 마켓 / 최대글자수 / 출력개수 / 명명전략 → ST3_*로 복사
        3) default_settings (인수)
        4) Stage3Settings() 기본값

    반환값: (DataFrame, 저장된_엑셀_경로)
    """
    log = log_func
    log(f"[INFO] 엑셀 읽는 중: {excel_path}")

    df = pd.read_excel(excel_path, header=0)

    # ✅ ST2_JSON 컬럼 필수
    if "ST2_JSON" not in df.columns:
        raise ValueError("엑셀에 'ST2_JSON' 컬럼이 없습니다. Stage2 단계에서 ST2_JSON 컬럼을 포함해 주세요.")

    # ✅ Stage3 출력 컬럼들 없으면 생성
    if "ST3_프롬프트" not in df.columns:
        df["ST3_프롬프트"] = ""
    if "ST3_결과상품명" not in df.columns:
        df["ST3_결과상품명"] = ""  # 여러 줄일 수도 있으니, 나중에 그대로 붙여넣기 용

    # ✅ (호환용) 예전 컬럼명을 ST3_* 컬럼으로 매핑
    #   - 예전:  마켓 / 최대글자수 / 출력개수 / 명명전략
    #   - 신규: ST3_마켓 / ST3_최대글자수 / ST3_출력개수 / ST3_명명전략
    compat_map = [
        ("마켓",       "ST3_마켓"),
        ("최대글자수", "ST3_최대글자수"),
        ("출력개수",   "ST3_출력개수"),
        ("명명전략",   "ST3_명명전략"),
    ]
    for src, dst in compat_map:
        if src in df.columns and dst not in df.columns:
            df[dst] = df[src]
            log(f"[INFO] 호환용 컬럼 매핑: '{src}' → '{dst}'")

    total = len(df)
    log(f"[INFO] 총 행 수: {total}개")

    generated = 0
    skipped_json_empty = 0
    skipped_existing = 0
    failed = 0

    # ✅ 기본 Stage3Settings (GUI에서 안 넘겨주면 이 값 사용)
    if default_settings is None:
        default_settings = Stage3Settings(
            market="쿠팡",
            max_len=50,
            num_candidates=None,  # 자동 모드 → 프롬프트에서 "10개 정도"로 유도
            naming_strategy="통합형",
        )

    for idx, row in df.iterrows():
        # 1) ST2_JSON 비어 있으면 스킵
        json_payload = safe_str(row.get("ST2_JSON", ""))
        if not json_payload:
            skipped_json_empty += 1
            log(f"[SKIP] idx={idx} : ST2_JSON 이 비어 있어 Stage3 프롬프트를 생성하지 않음.")
            continue

        # 2) 이미 ST3_프롬프트가 있고, skip_existing 옵션이면 스킵
        existing_prompt = safe_str(row.get("ST3_프롬프트", ""))
        if skip_existing and existing_prompt:
            skipped_existing += 1
            log(f"[SKIP] idx={idx} : 이미 ST3_프롬프트가 있어 건너뜀.")
            continue

        # 3) stage3_core의 공통 로직 사용해 요청/프롬프트 생성
        try:
            req: Stage3Request = build_stage3_request_from_row(
                row=row,
                default_settings=default_settings,
                st2_col="ST2_JSON",
            )
        except Exception as e:
            failed += 1
            log(f"[ERROR] idx={idx} : Stage3Request 생성 중 예외 → 건너뜀. ({e})")
            continue

        # 프롬프트 기록
        df.at[idx, "ST3_프롬프트"] = req.prompt
        generated += 1

    base_dir = os.path.dirname(excel_path)
    base_name = os.path.splitext(os.path.basename(excel_path))[0]
    out_path = os.path.join(base_dir, f"{base_name}_stage3_prompts.xlsx")

    # 저장 (원본이 열려 있을 수 있으므로 백업 플랜)
    try:
        df.to_excel(out_path, index=False)
        log(f"[INFO] Stage3 프롬프트 포함 엑셀 저장 완료: {out_path}")
    except Exception as e:
        log(f"[WARN] 기본 경로로 저장 실패: {e}")
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_path = os.path.join(base_dir, f"{base_name}_stage3_prompts_partial_{ts}.xlsx")
        df.to_excel(backup_path, index=False)
        out_path = backup_path
        log(f"[INFO] 대신 임시 파일로 저장: {backup_path}")

    log(
        f"[SUMMARY] 전체={total}, 생성={generated}, "
        f"ST2_JSON_없음_스킵={skipped_json_empty}, "
        f"기존프롬프트_스킵={skipped_existing}, 실패={failed}"
    )

    return df, out_path


# =========================
# Tkinter GUI
# =========================

class Stage3PromptApp:
    def __init__(self, root: tk.Tk):
        self.root = root
        root.title("Stage3 프롬프트 생성기 (최종 상품명 후보용)")
        root.geometry("960x700")

        # 상태 변수
        self.excel_path_var = tk.StringVar(value="")
        self.progress_var = tk.DoubleVar(value=0.0)
        self.progress_text_var = tk.StringVar(value="진행률: -")
        self.status_text_var = tk.StringVar(value="Stage2 JSON 이 포함된 엑셀을 선택해 주세요.")
        self.skip_existing_var = tk.BooleanVar(value=True)

        # ✅ 마켓 + 최대 글자수 옵션
        #   - 네이버: 50자
        #   - 쿠팡:   100자
        #   - 지마켓/옥션: 45자
        self.market_maxlen_var = tk.StringVar(value="네이버 50자")

        # 미리보기 상태
        self.df: pd.DataFrame | None = None
        self.out_path: str | None = None
        self.current_index: int = 0

        self._worker_thread: threading.Thread | None = None

        self._build_widgets()

    # ---------- UI 구성 ----------
    def _build_widgets(self):
        style = ttk.Style(self.root)
        try:
            style.theme_use("clam")
        except Exception:
            pass

        style.configure("Title.TLabel", font=("맑은 고딕", 14, "bold"))
        style.configure("Small.TLabel", font=("맑은 고딕", 9), foreground="#666666")
        style.configure(
            "Green.Horizontal.TProgressbar",
            troughcolor="#f0f0f0",
            background="#4caf50",
        )

        # 상단 제목
        lbl_title = ttk.Label(
            self.root,
            text="Stage3 프롬프트 생성기",
            style="Title.TLabel",
        )
        lbl_title.pack(fill="x", padx=10, pady=(8, 0))

        lbl_sub = ttk.Label(
            self.root,
            text="Stage2 JSON → Stage3 프롬프트 엑셀 생성 + 행별 프롬프트 미리보기/복사",
            style="Small.TLabel",
        )
        lbl_sub.pack(fill="x", padx=10, pady=(0, 8))

        # 파일 선택 영역
        frame_file = ttk.LabelFrame(self.root, text="입력 엑셀 (Stage2 JSON 포함)")
        frame_file.pack(fill="x", padx=10, pady=5)

        ttk.Label(frame_file, text="엑셀 파일:").grid(row=0, column=0, sticky="w", padx=5, pady=5)

        entry_path = ttk.Entry(frame_file, textvariable=self.excel_path_var, width=70)
        entry_path.grid(row=0, column=1, sticky="w", padx=5, pady=5)

        btn_browse = ttk.Button(frame_file, text="찾기...", command=self.on_select_excel)
        btn_browse.grid(row=0, column=2, sticky="w", padx=5, pady=5)

        chk_skip = ttk.Checkbutton(
            frame_file,
            text="이미 ST3_프롬프트가 있는 행은 건너뛰기",
            variable=self.skip_existing_var,
        )
        chk_skip.grid(row=1, column=1, sticky="w", padx=5, pady=(0, 5))

        # ✅ 마켓 / 최대 글자수 옵션 박스
        ttk.Label(frame_file, text="마켓 / 최대 글자수:").grid(
            row=2, column=0, sticky="w", padx=5, pady=5
        )
        self.combo_market = ttk.Combobox(
            frame_file,
            textvariable=self.market_maxlen_var,
            values=[
                "네이버 50자",
                "쿠팡 100자",
                "지마켓/옥션 45자",
            ],
            state="readonly",
            width=20,
        )
        self.combo_market.grid(row=2, column=1, sticky="w", padx=5, pady=5)
        self.combo_market.current(0)

        # 실행 버튼
        self.btn_run = ttk.Button(
            frame_file,
            text="선택된 엑셀로 Stage3 프롬프트 생성",
            command=self.on_run_stage3,
        )
        self.btn_run.grid(row=3, column=1, sticky="w", padx=5, pady=5)

        # 진행 상태
        frame_progress = ttk.LabelFrame(self.root, text="진행 상태")
        frame_progress.pack(fill="x", padx=10, pady=5)

        self.progress_bar = ttk.Progressbar(
            frame_progress,
            variable=self.progress_var,
            maximum=100.0,
            mode="determinate",
            style="Green.Horizontal.TProgressbar",
        )
        self.progress_bar.grid(row=0, column=0, columnspan=2, sticky="we", padx=10, pady=5)

        lbl_prog = ttk.Label(frame_progress, textvariable=self.progress_text_var)
        lbl_prog.grid(row=1, column=0, sticky="w", padx=10, pady=2)

        lbl_status = ttk.Label(frame_progress, textvariable=self.status_text_var)
        lbl_status.grid(row=1, column=1, sticky="e", padx=10, pady=2)

        # 프롬프트 미리보기 & 복사
        frame_preview = ttk.LabelFrame(self.root, text="프롬프트 미리보기 / 클립보드 복사")
        frame_preview.pack(fill="both", expand=False, padx=10, pady=5)

        self.frame_preview = frame_preview  # 나중에 state 관리용

        ttk.Label(frame_preview, text="행 인덱스:").grid(row=0, column=0, sticky="w", padx=5, pady=5)

        self.spin_index = ttk.Spinbox(
            frame_preview,
            from_=0,
            to=0,
            width=7,
            command=self.on_change_index,
        )
        self.spin_index.grid(row=0, column=1, sticky="w", padx=5, pady=5)

        self.lbl_row_info_var = tk.StringVar(value="행 정보: -")
        lbl_row_info = ttk.Label(frame_preview, textvariable=self.lbl_row_info_var)
        lbl_row_info.grid(row=0, column=2, sticky="w", padx=5, pady=5)

        btn_prev = ttk.Button(frame_preview, text="◀ 이전", command=self.on_prev_row)
        btn_prev.grid(row=0, column=3, sticky="e", padx=5, pady=5)

        btn_next = ttk.Button(frame_preview, text="다음 ▶", command=self.on_next_row)
        btn_next.grid(row=0, column=4, sticky="e", padx=5, pady=5)

        self.btn_copy = ttk.Button(
            frame_preview,
            text="이 행 프롬프트 복사",
            command=self.on_copy_prompt,
        )
        self.btn_copy.grid(row=0, column=5, sticky="e", padx=10, pady=5)

        # 프롬프트 텍스트 박스
        self.preview_box = ScrolledText(frame_preview, wrap=tk.WORD, height=10)
        self.preview_box.grid(row=1, column=0, columnspan=6, sticky="nsew", padx=5, pady=5)

        frame_preview.grid_rowconfigure(1, weight=1)
        frame_preview.grid_columnconfigure(2, weight=1)

        # 로그 박스
        frame_log = ttk.LabelFrame(self.root, text="로그")
        frame_log.pack(fill="both", expand=True, padx=10, pady=5)

        self.log_box = ScrolledText(frame_log, wrap=tk.WORD, height=10)
        self.log_box.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

        # 처음엔 미리보기 비활성화
        self.set_preview_enabled(False)

    # ---------- 기본 유틸 ----------
    def log(self, msg: str):
        def _do():
            self.log_box.insert(tk.END, msg + "\n")
            self.log_box.see(tk.END)

        self.root.after(0, _do)

    def set_progress(self, done: int, total: int):
        if total <= 0:
            pct = 0.0
        else:
            pct = round(done / total * 100.0, 1)

        def _do():
            self.progress_var.set(pct)
            self.progress_text_var.set(f"진행률: {pct}% ({done} / {total})")

        self.root.after(0, _do)

    def set_status(self, text: str):
        self.root.after(0, lambda: self.status_text_var.set(text))

    def set_preview_enabled(self, enabled: bool):
        state = "normal" if enabled else "disabled"
        self.spin_index.config(state=state)
        self.btn_copy.config(state=state)
        # 이전/다음 버튼 (row=0 에 있는 버튼들) enable/disable
        for child in self.frame_preview.grid_slaves(row=0):
            if isinstance(child, ttk.Button) and child not in (self.btn_copy,):
                child.config(state=state)
        if not enabled:
            self.preview_box.config(state="normal")
            self.preview_box.delete("1.0", tk.END)
            self.preview_box.insert("1.0", "엑셀 처리 후 프롬프트를 미리보기할 수 있습니다.")
            self.preview_box.config(state="disabled")

    # ---------- 이벤트 핸들러 ----------
    def on_select_excel(self):
        filetypes = [
            ("Excel files", "*.xlsx *.xls"),
            ("All files", "*.*"),
        ]
        filepath = filedialog.askopenfilename(
            title="Stage2 JSON이 들어 있는 엑셀 파일을 선택하세요",
            filetypes=filetypes,
        )
        if not filepath:
            return
        self.excel_path_var.set(filepath)
        self.set_status("파일 선택됨. 이제 'Stage3 프롬프트 생성'을 눌러주세요.")

    def _get_default_settings_from_gui(self) -> Stage3Settings:
        """
        GUI에서 선택한 마켓/글자수 옵션을 Stage3Settings로 변환.
        - 네이버 50자
        - 쿠팡 100자
        - 지마켓/옥션 45자
        """
        choice = self.market_maxlen_var.get().strip()

        if "쿠팡" in choice:
            market = "쿠팡"
            max_len = 100
        elif "지마켓" in choice or "옥션" in choice:
            market = "지마켓/옥션"
            max_len = 45
        else:
            # 기본 네이버 50자
            market = "네이버"
            max_len = 50

        # 출력 개수 / 명명전략은 1차 버전에서는 고정:
        # - num_candidates=None  → 프롬프트에서 "10개 정도" 자동
        # - naming_strategy="통합형" → 우선 통합형만 운용
        return Stage3Settings(
            market=market,
            max_len=max_len,
            num_candidates=None,
            naming_strategy="통합형",
        )

    def on_run_stage3(self):
        if self._worker_thread and self._worker_thread.is_alive():
            messagebox.showwarning("알림", "이미 작업이 진행 중입니다.")
            return

        excel_path = self.excel_path_var.get().strip()
        if not excel_path:
            # 경로가 비어 있으면 먼저 파일 선택
            self.on_select_excel()
            excel_path = self.excel_path_var.get().strip()
            if not excel_path:
                return

        if not os.path.exists(excel_path):
            messagebox.showerror("오류", f"입력 엑셀을 찾을 수 없습니다.\n\n{excel_path}")
            return

        self.btn_run.config(state="disabled")
        self.set_preview_enabled(False)
        self.progress_var.set(0.0)
        self.progress_text_var.set("진행률: 0%")
        self.set_status("Stage3 프롬프트 생성 중...")

        skip_existing = self.skip_existing_var.get()
        default_settings = self._get_default_settings_from_gui()
        self.log(
            f"[INFO] GUI 기본 설정: market={default_settings.market}, "
            f"max_len={default_settings.max_len}, "
            f"num_candidates={default_settings.num_candidates}, "
            f"naming_strategy={default_settings.naming_strategy}"
        )

        def worker():
            try:
                # 1차: 전체 엑셀 처리 (프롬프트 생성)
                df, out_path = process_excel_for_stage3(
                    excel_path,
                    log_func=self.log,
                    skip_existing=skip_existing,
                    default_settings=default_settings,
                )

                # 진행률 100%로 맞추기
                self.set_progress(done=len(df), total=len(df))
                self.set_status("Stage3 프롬프트 생성 완료.")

                # 상태 업데이트
                self.df = df
                self.out_path = out_path
                self.current_index = 0

                # 미리보기 활성화 + 첫 행 로딩
                if len(df) > 0:
                    self.root.after(0, lambda: self.init_preview_widgets(df))
                else:
                    self.root.after(
                        0,
                        lambda: messagebox.showinfo(
                            "완료",
                            f"엑셀에 행이 없습니다.\n\n{out_path}",
                        ),
                    )

            except Exception as e:
                self.log("[FATAL] 오류 발생:")
                self.log(str(e))
                self.set_status("오류 발생")
                self.root.after(
                    0,
                    lambda: messagebox.showerror(
                        "오류",
                        f"작업 중 오류가 발생했습니다.\n\n{e}",
                    ),
                )
            finally:
                self.root.after(0, lambda: self.btn_run.config(state="normal"))

        th = threading.Thread(target=worker, daemon=True)
        th.start()
        self._worker_thread = th

    # ---------- 프롬프트 미리보기 관련 ----------
    def init_preview_widgets(self, df: pd.DataFrame):
        total = len(df)
        self.spin_index.config(from_=0, to=max(0, total - 1))
        self.spin_index.delete(0, tk.END)
        self.spin_index.insert(0, "0")

        self.set_preview_enabled(True)
        self.load_prompt_for_index(0)

        # 안내 팝업
        msg = (
            "Stage3 프롬프트 생성이 완료되었습니다.\n\n"
            f"저장된 엑셀:\n{self.out_path}\n\n"
            "상단 '행 인덱스'를 조절하거나 ◀ / ▶ 버튼을 눌러\n"
            "각 행의 프롬프트를 확인하고, '이 행 프롬프트 복사' 버튼으로\n"
            "GPT 채팅창에 바로 붙여넣어 사용할 수 있습니다."
        )
        messagebox.showinfo("완료", msg)

    def load_prompt_for_index(self, idx: int):
        if self.df is None:
            return
        total = len(self.df)
        if total == 0:
            return

        if idx < 0:
            idx = 0
        if idx >= total:
            idx = total - 1
        self.current_index = idx

        row = self.df.iloc[idx]
        prompt = safe_str(row.get("ST3_프롬프트", ""))
        상품코드 = safe_str(row.get("상품코드", ""))
        기본상품명 = safe_str(row.get("기본상품명", ""))

        info = f"행 {idx} / {total - 1}"
        if 상품코드:
            info += f" | 상품코드: {상품코드}"
        if 기본상품명:
            info += f" | 기본상품명: {기본상품명[:30]}"
        self.lbl_row_info_var.set(info)

        self.preview_box.config(state="normal")
        self.preview_box.delete("1.0", tk.END)
        if prompt:
            self.preview_box.insert("1.0", prompt)
        else:
            self.preview_box.insert(
                "1.0",
                "이 행에는 ST3_프롬프트가 없습니다.\n"
                "→ ST2_JSON 비어 있는 행이었거나, '기존 프롬프트 건너뛰기' 옵션에 의해 스킵되었을 수 있습니다.",
            )
        self.preview_box.config(state="disabled")

        # Spinbox 값도 맞춰줌
        self.spin_index.delete(0, tk.END)
        self.spin_index.insert(0, str(idx))

    def on_change_index(self):
        if self.df is None:
            return
        try:
            idx = int(self.spin_index.get())
        except ValueError:
            idx = 0
        self.load_prompt_for_index(idx)

    def on_prev_row(self):
        if self.df is None:
            return
        self.load_prompt_for_index(self.current_index - 1)

    def on_next_row(self):
        if self.df is None:
            return
        self.load_prompt_for_index(self.current_index + 1)

    def on_copy_prompt(self):
        if self.df is None:
            return
        prompt = self.preview_box.get("1.0", tk.END).strip()
        if not prompt:
            messagebox.showwarning("알림", "복사할 프롬프트가 없습니다.")
            return
        self.root.clipboard_clear()
        self.root.clipboard_append(prompt)
        self.root.update()  # 일부 OS에서 클립보드 유지용
        messagebox.showinfo(
            "복사 완료",
            "현재 행의 Stage3 프롬프트가 클립보드에 복사되었습니다.\nGPT 채팅창에 붙여넣어 사용하세요.",
        )


def main():
    root = tk.Tk()
    app = Stage3PromptApp(root)
    root.mainloop()


if __name__ == "__main__":
    main()
