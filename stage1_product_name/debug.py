import os
import re
import threading
from typing import Optional, Tuple

import pandas as pd
from openai import OpenAI

import tkinter as tk
from tkinter import ttk, filedialog, messagebox
from tkinter.scrolledtext import ScrolledText

# =========================
# 전역 설정
# =========================

client: Optional[OpenAI] = None
STOP_REQUESTED = False

# API 키 저장 파일 (스크립트와 같은 폴더)
CONFIG_API_KEY_PATH = os.path.join(os.path.dirname(__file__), ".openai_api_key")

# gpt-5 계열에서 Reasoning + 최종 답변까지 여유 있게 나오도록 출력 토큰 상한
DEFAULT_MAX_OUTPUT_TOKENS = 1024


def safe_str(v) -> str:
    """NaN/None 안전하게 문자열로 변환 + strip."""
    if v is None:
        return ""
    try:
        if pd.isna(v):
            return ""
    except Exception:
        pass
    return str(v).strip()


# =========================
# 응답 텍스트 추출 유틸
# =========================

def extract_text_from_response(response) -> str:
    """
    OpenAI responses.create 응답 객체에서 최종 텍스트를 추출.

    우선순위:
    1) response.output_text  (SDK 편의 속성: 최종 answer만 돌려줌)
    2) response.output[*] 중 type == "message" 인 것의 content[0].text.value
    """

    # 1) SDK 편의 속성 (최신 SDK에서 제공)
    ot = getattr(response, "output_text", None)
    if isinstance(ot, str) and ot.strip():
        return ot.strip()
    if ot is not None and not isinstance(ot, str):
        # pydantic 객체인 경우 .value
        v = getattr(ot, "value", None)
        if isinstance(v, str) and v.strip():
            return v.strip()

    # 2) output 리스트에서 message 타입 찾기
    outputs = getattr(response, "output", None)
    if not outputs:
        return ""

    for o in outputs:
        # type 확인 (reasoning, message 등)
        o_type = getattr(o, "type", None)
        if o_type is None and isinstance(o, dict):
            o_type = o.get("type")

        # reasoning 로그는 무시, message만 본다
        if o_type != "message":
            continue

        # content 꺼내기
        content = getattr(o, "content", None)
        if content is None and isinstance(o, dict):
            content = o.get("content")
        if not content:
            continue

        first_c = content[0]

        # text 필드 접근
        txt_obj = getattr(first_c, "text", None)
        if txt_obj is None and isinstance(first_c, dict):
            txt_obj = first_c.get("text")
        if txt_obj is None:
            continue

        # Text 객체이거나 str 일 수 있음
        if hasattr(txt_obj, "value"):
            return safe_str(txt_obj.value)
        return safe_str(txt_obj)

    # 그래도 못 찾으면 빈 문자열
    return ""


# =========================
# OpenAI 호출 함수
# =========================

def call_stage1_model(
    prompt: str,
    model: str = "gpt-5-mini",
    temperature: float = 0.2,
    max_output_tokens: int = DEFAULT_MAX_OUTPUT_TOKENS,
    max_retries: int = 3,
) -> Tuple[str, int, int]:
    """
    Stage1 프롬프트 1개를 OpenAI에 보내고,
    정제된 상품명(텍스트)과 토큰 사용량을 반환.

    반환: (result_text, input_tokens, output_tokens)
    """
    global client

    prompt = safe_str(prompt)
    if not prompt:
        return "", 0, 0

    if client is None:
        raise RuntimeError("OpenAI 클라이언트가 초기화되지 않았습니다.")

    last_err: Optional[Exception] = None

    for attempt in range(1, max_retries + 1):
        try:
            # responses API 호출 파라미터 구성
            kwargs = {
                "model": model,
                "input": prompt,
                "max_output_tokens": max_output_tokens,
            }

            # gpt-5 계열은 temperature 미지원 + reasoning 모델
            if model.startswith("gpt-5"):
                # reasoning 토큰 낭비 줄이기 위해 effort 낮게
                kwargs["reasoning"] = {"effort": "low"}
                # temperature는 보내지 않음
            else:
                kwargs["temperature"] = temperature

            response = client.responses.create(**kwargs)

            # 최종 텍스트 추출
            result_text = extract_text_from_response(response)

            usage = getattr(response, "usage", None)
            in_tokens = getattr(usage, "input_tokens", 0) if usage else 0
            out_tokens = getattr(usage, "output_tokens", 0) if usage else 0

            # 토큰은 썼는데 텍스트가 비어 있으면 status 살짝 찍어줌 (디버깅용)
            if (in_tokens or out_tokens) and not result_text:
                status = getattr(response, "status", None)
                incomplete = getattr(response, "incomplete_details", None)
                print("[WARN] 텍스트 추출 실패. status=", status, "incomplete_details=", incomplete)

            return result_text, in_tokens, out_tokens

        except Exception as e:
            last_err = e
            err_str = str(e)

            # 에러 문자열에서 status code 추출 (예: "Error code: 404 - {...}")
            m = re.search(r"Error code:\s*(\d+)", err_str)
            status_code = int(m.group(1)) if m else None

            print(f"[WARN] OpenAI 호출 실패({attempt}/{max_retries}) - {err_str}")

            # 모델 없음(404), quota 부족(429)은 재시도해도 의미 없음 → 바로 중단
            if status_code in (404, 429):
                break

    # 여기까지 왔으면 실패
    raise RuntimeError(f"OpenAI 호출 반복 실패: {last_err}")


# =========================
# 엑셀 처리 메인 로직
# =========================

def run_stage1_on_excel(
    excel_path: str,
    model: str,
    temperature: float,
    max_output_tokens: int,
    save_every: int,
    overwrite: bool,
    log_func=print,
) -> str:
    """
    Stage1 맵핑 엑셀을 읽어서 ST1_프롬프트 기준으로
    OpenAI를 호출하고, ST1_정제상품명을 채워 넣는다.

    - excel_path: 입력 엑셀 경로 (*_stage1_mapping.xlsx)
    - model: gpt-5-mini / gpt-5.1 / gpt-4.1-mini 등
    - temperature: 샘플링 온도 (gpt-5 계열에서는 내부적으로 무시)
    - max_output_tokens: 모델 최대 출력 토큰 수
    - save_every: N행마다 중간 저장
    - overwrite: True면 기존 ST1_정제상품명 있어도 덮어씀
    """
    global STOP_REQUESTED

    log = log_func
    log(f"[INFO] 엑셀 로드: {excel_path}")

    df = pd.read_excel(excel_path, dtype=str)

    # 필수 컬럼 체크
    required_cols = ["ST1_프롬프트", "ST1_정제상품명"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"엑셀에 필수 컬럼이 없습니다: {missing}")

    # 출력 파일 경로
    base_dir = os.path.dirname(excel_path)
    base_name = os.path.splitext(os.path.basename(excel_path))[0]
    out_path = os.path.join(base_dir, f"{base_name}_stage1_completed.xlsx")

    total_rows = len(df)
    log(f"[INFO] 총 행 수: {total_rows}")
    if model.startswith("gpt-5"):
        log(f"[INFO] 사용할 모델: {model} (temperature는 모델 특성상 무시됨)")
    else:
        log(f"[INFO] 사용할 모델: {model}")
        log(f"[INFO] temperature: {temperature:.2f}")
    log(f"[INFO] max_output_tokens: {max_output_tokens}")
    log(f"[INFO] save_every: {save_every}, overwrite: {overwrite}")

    processed = 0
    total_in_tokens = 0
    total_out_tokens = 0

    for idx, row in df.iterrows():
        # 사용자 중단 요청 확인
        if STOP_REQUESTED:
            log("[INFO] 사용자 중단 요청 감지. 현재까지 진행된 결과만 저장하고 종료합니다.")
            break

        prompt = safe_str(row.get("ST1_프롬프트", ""))
        if not prompt:
            continue

        current_result = safe_str(row.get("ST1_정제상품명", ""))
        if current_result and not overwrite:
            # 이미 결과가 있고 덮어쓰기 옵션이 아니면 스킵
            continue

        log(f"\n[INFO] 행 {idx} 처리 중...")

        try:
            result_text, in_tok, out_tok = call_stage1_model(
                prompt=prompt,
                model=model,
                temperature=temperature,
                max_output_tokens=max_output_tokens,
            )
        except Exception as e:
            msg = str(e)
            log(f"[ERROR] 행 {idx} 처리 실패: {msg}")

            # quota 부족 → 더 진행해도 전부 실패하므로 즉시 중단
            if "insufficient_quota" in msg or "You exceeded your current quota" in msg:
                log("[ERROR] OpenAI 크레딧/요금 한도를 초과했습니다.")
                log("[ERROR] platform.openai.com에서 결제/한도를 확인한 후 다시 시도해 주세요.")
                break

            # 모델 없음 / 권한 없음 → 즉시 중단
            if "model_not_found" in msg or "does not exist" in msg:
                log("[ERROR] 선택한 모델이 존재하지 않거나 접근 권한이 없습니다.")
                log("[ERROR] 다른 모델을 선택한 후 다시 실행해 주세요.")
                break

            # 그 외 에러는 해당 행만 건너뛰고 다음 행으로 진행
            continue

        # 결과 기록
        df.at[idx, "ST1_정제상품명"] = result_text
        processed += 1
        total_in_tokens += in_tok
        total_out_tokens += out_tok

        log(f"[OK] 행 {idx} 완료")
        log(f"     결과: {result_text}")
        if in_tok or out_tok:
            log(f"     tokens in/out = {in_tok}/{out_tok}")

        # 중간 저장
        if processed > 0 and processed % save_every == 0:
            log(f"[INFO] {processed}개 처리, 중간 저장: {out_path}")
            df.to_excel(out_path, index=False)

    # 최종 저장
    log(f"\n[INFO] 최종 저장: {out_path}")
    df.to_excel(out_path, index=False)

    log(f"[INFO] 처리 완료. 새로 처리된 행 수: {processed}")
    log(f"[INFO] 총 토큰 사용량: input={total_in_tokens}, output={total_out_tokens}")

    return out_path


# =========================
# Tkinter GUI
# =========================

class Stage1APIRunnerApp:
    def __init__(self, root: tk.Tk):
        self.root = root
        root.title("Stage1 상품명 정제 API 실행기")
        root.geometry("920x650")

        self.selected_file: Optional[str] = None

        # ----- 상단 설명 -----
        desc = (
            "① 대상 파일\n"
            "   - Stage1 맵핑 툴로 생성한 '*_stage1_mapping.xlsx' 파일을 사용합니다.\n\n"
            "② 필수 컬럼\n"
            "   - ST1_프롬프트, ST1_정제상품명\n\n"
            "③ 동작 방식\n"
            "   - 각 행의 ST1_프롬프트를 OpenAI API로 보내고,\n"
            "     결과를 ST1_정제상품명에 채운 뒤 '*_stage1_completed.xlsx'로 저장합니다.\n"
            "   - '기존 결과 덮어쓰기'를 끄면 이미 채워진 행은 자동으로 건너뜁니다."
        )
        lbl_desc = tk.Label(root, text=desc, justify="left", anchor="w")
        lbl_desc.pack(fill=tk.X, padx=10, pady=(8, 4))

        # ----- 설정 영역 -----
        cfg_frame = tk.Frame(root)
        cfg_frame.pack(fill=tk.X, padx=10, pady=5)

        # API 키
        tk.Label(cfg_frame, text="OpenAI API 키:").grid(row=0, column=0, sticky="e", padx=5, pady=2)
        self.api_entry = tk.Entry(cfg_frame, width=55, show="*")
        self.api_entry.grid(row=0, column=1, columnspan=3, sticky="w", padx=5, pady=2)

        # 저장된 API 키 로드
        self.load_api_key()

        # 모델 선택
        tk.Label(cfg_frame, text="모델:").grid(row=1, column=0, sticky="e", padx=5, pady=2)
        self.model_var = tk.StringVar(value="gpt-5-mini")
        self.cmb_model = ttk.Combobox(
            cfg_frame,
            textvariable=self.model_var,
            values=["gpt-5-mini", "gpt-5.1", "gpt-4.1-mini"],
            width=15,
            state="readonly",
        )
        self.cmb_model.grid(row=1, column=1, sticky="w", padx=5, pady=2)

        lbl_model_hint = tk.Label(
            cfg_frame,
            text="gpt-5-mini: 빠르고 저렴(Reasoning 모델) / gpt-5.1: 최고 품질 / gpt-4.1-mini: 온도 조절 가능",
            fg="#555",
            anchor="w",
        )
        lbl_model_hint.grid(row=1, column=2, columnspan=2, sticky="w", padx=5, pady=2)

        # temperature 슬라이더
        tk.Label(cfg_frame, text="temperature:").grid(row=2, column=0, sticky="e", padx=5, pady=2)
        self.temp_var = tk.DoubleVar(value=0.2)
        self.temp_scale = tk.Scale(
            cfg_frame,
            variable=self.temp_var,
            from_=0.0,
            to=1.0,
            resolution=0.05,
            orient=tk.HORIZONTAL,
            length=200,
        )
        self.temp_scale.grid(row=2, column=1, sticky="w", padx=5, pady=2)

        lbl_temp_hint = tk.Label(
            cfg_frame,
            text="낮을수록 결정적, 높을수록 랜덤성↑ (gpt-5 계열에서는 내부적으로 무시됨)",
            fg="#555",
            anchor="w",
        )
        lbl_temp_hint.grid(row=2, column=2, columnspan=2, sticky="w", padx=5, pady=2)

        # 중간 저장 간격
        tk.Label(cfg_frame, text="중간 저장 간격(행):").grid(row=3, column=0, sticky="e", padx=5, pady=2)
        self.save_every_var = tk.IntVar(value=10)
        self.spin_save_every = tk.Spinbox(
            cfg_frame,
            from_=1,
            to=1000,
            textvariable=self.save_every_var,
            width=6,
        )
        self.spin_save_every.grid(row=3, column=1, sticky="w", padx=5, pady=2)

        # 덮어쓰기 여부
        self.overwrite_var = tk.BooleanVar(value=False)
        chk_overwrite = tk.Checkbutton(
            cfg_frame,
            text="기존 ST1_정제상품명 덮어쓰기",
            variable=self.overwrite_var,
        )
        chk_overwrite.grid(row=3, column=2, sticky="w", padx=5, pady=2)

        # ----- 파일/실행 버튼 영역 -----
        btn_frame = tk.Frame(root)
        btn_frame.pack(fill=tk.X, padx=10, pady=5)

        self.btn_select = tk.Button(
            btn_frame,
            text="Stage1 맵핑 엑셀 선택",
            command=self.on_select_file,
        )
        self.btn_select.pack(side=tk.LEFT, padx=5)

        self.btn_run = tk.Button(
            btn_frame,
            text="실행",
            command=self.on_run_click,
        )
        self.btn_run.pack(side=tk.LEFT, padx=5)

        self.btn_stop = tk.Button(
            btn_frame,
            text="중단 요청",
            command=self.on_stop_click,
        )
        self.btn_stop.pack(side=tk.LEFT, padx=5)

        self.lbl_file = tk.Label(btn_frame, text="선택된 파일: (없음)", anchor="w")
        self.lbl_file.pack(side=tk.LEFT, padx=10)

        # ----- 로그 영역 -----
        self.log_box = ScrolledText(root, wrap=tk.WORD, height=20)
        self.log_box.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

    # 로그 출력 (UI 스레드에서 실행)
    def _append_log(self, msg: str):
        self.log_box.insert(tk.END, msg + "\n")
        self.log_box.see(tk.END)

    def log(self, msg: str):
        self.root.after(0, self._append_log, msg)

    # API 키 저장/로드
    def load_api_key(self):
        if os.path.exists(CONFIG_API_KEY_PATH):
            try:
                with open(CONFIG_API_KEY_PATH, "r", encoding="utf-8") as f:
                    key = f.read().strip()
                    if key:
                        self.api_entry.insert(0, key)
            except Exception:
                pass

    def save_api_key(self, api_key: str):
        api_key = api_key.strip()
        if not api_key:
            return
        try:
            with open(CONFIG_API_KEY_PATH, "w", encoding="utf-8") as f:
                f.write(api_key)
        except Exception as e:
            self.log(f"[WARN] API 키 저장 중 오류: {e}")

    # 파일 선택
    def on_select_file(self):
        filepath = filedialog.askopenfilename(
            title="Stage1 맵핑 엑셀 파일 선택 (*_stage1_mapping.xlsx)",
            filetypes=[("Excel files", "*.xlsx *.xls"), ("All files", "*.*")],
        )
        if not filepath:
            return

        base_name = os.path.basename(filepath)
        # 파일명 검사: '_stage1_mapping' 포함 여부
        if "_stage1_mapping" not in base_name:
            messagebox.showerror(
                "파일 이름 오류",
                "파일 이름에 '_stage1_mapping' 이 포함된 Stage1 맵핑 파일만 사용할 수 있습니다.",
            )
            return

        self.selected_file = filepath
        self.lbl_file.config(text=f"선택된 파일: {filepath}")

    # 실행 버튼
    def on_run_click(self):
        global STOP_REQUESTED

        api_key = self.api_entry.get().strip()
        if not api_key:
            messagebox.showerror("입력 오류", "OpenAI API 키를 입력해 주세요.")
            return

        if not self.selected_file:
            messagebox.showerror("입력 오류", "Stage1 맵핑 엑셀 파일을 먼저 선택해 주세요.")
            return

        # 파일명 한 번 더 검증
        base_name = os.path.basename(self.selected_file)
        if "_stage1_mapping" not in base_name:
            messagebox.showerror(
                "파일 이름 오류",
                "파일 이름에 '_stage1_mapping' 이 포함된 Stage1 맵핑 파일만 사용할 수 있습니다.",
            )
            return

        model = self.model_var.get()
        temperature = float(self.temp_var.get())
        save_every = int(self.save_every_var.get())
        overwrite = bool(self.overwrite_var.get())

        # 중단 플래그 초기화
        STOP_REQUESTED = False

        # 실행 버튼 잠금
        self.btn_run.config(state=tk.DISABLED)

        # 백그라운드 스레드에서 API 작업 수행
        thread = threading.Thread(
            target=self.run_task,
            args=(api_key, self.selected_file, model, temperature, save_every, overwrite),
            daemon=True,
        )
        thread.start()

    # 중단 버튼
    def on_stop_click(self):
        global STOP_REQUESTED
        STOP_REQUESTED = True
        self.log("[INFO] 중단 요청 플래그 설정됨. 현재 처리 중인 행 이후부터 중단됩니다.")

    # 백그라운드 작업
    def run_task(
        self,
        api_key: str,
        filepath: str,
        model: str,
        temperature: float,
        save_every: int,
        overwrite: bool,
    ):
        global client
        try:
            # 클라이언트 초기화
            client = OpenAI(api_key=api_key)

            # 초기화 성공 시 키 저장
            self.save_api_key(api_key)

            self.log(f"[INFO] 선택된 파일: {filepath}")
            self.log(f"[INFO] 선택 모델: {model}")
            if model.startswith("gpt-5"):
                self.log("[INFO] temperature 설정값: %.2f (gpt-5 계열은 무시됨)" % temperature)
            else:
                self.log("[INFO] temperature 설정값: %.2f" % temperature)
            self.log(f"[INFO] 중간 저장 간격: {save_every}행")
            self.log(f"[INFO] 기존 결과 덮어쓰기: {overwrite}")

            out_path = run_stage1_on_excel(
                excel_path=filepath,
                model=model,
                temperature=temperature,
                max_output_tokens=DEFAULT_MAX_OUTPUT_TOKENS,
                save_every=save_every,
                overwrite=overwrite,
                log_func=self.log,
            )

            self.log("[INFO] 모든 작업 완료")
            self.root.after(
                0,
                lambda: messagebox.showinfo(
                    "완료",
                    f"Stage1 API 실행이 완료되었습니다.\n\n{out_path}",
                ),
            )
        except Exception as e:
            self.log("[FATAL] 오류 발생:")
            self.log(str(e))
            self.root.after(
                0,
                lambda: messagebox.showerror("오류", f"작업 중 오류가 발생했습니다.\n\n{e}"),
            )
        finally:
            self.root.after(0, lambda: self.btn_run.config(state=tk.NORMAL))


def main():
    root = tk.Tk()
    app = Stage1APIRunnerApp(root)
    root.mainloop()


if __name__ == "__main__":
    main()
