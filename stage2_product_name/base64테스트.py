# vision_image_test_gui.py
import os
import base64
import mimetypes
import threading
from datetime import datetime

import tkinter as tk
from tkinter import ttk, filedialog, messagebox
from tkinter.scrolledtext import ScrolledText

from openai import OpenAI

API_KEY_FILE = ".openai_api_key_vision_test"


# =======================
#  유틸: API 키 파일 저장/로드
# =======================
def load_api_key_from_file() -> str:
    if os.path.exists(API_KEY_FILE):
        try:
            with open(API_KEY_FILE, "r", encoding="utf-8") as f:
                return f.read().strip()
        except Exception:
            return ""
    return ""


def save_api_key_to_file(key: str) -> None:
    try:
        with open(API_KEY_FILE, "w", encoding="utf-8") as f:
            f.write(key.strip())
    except Exception as e:
        print(f"[WARN] API 키 저장 실패: {e}")


# =======================
#  유틸: 로컬 이미지 → data URL
# =======================
def encode_image_to_data_url(path: str) -> str:
    """
    로컬 이미지 파일을 data:[mime];base64,... 형태 문자열로 변환.
    """
    mime, _ = mimetypes.guess_type(path)
    if mime is None:
        mime = "image/jpeg"
    with open(path, "rb") as f:
        b64 = base64.b64encode(f.read()).decode("ascii")
    return f"data:{mime};base64,{b64}"


# =======================
#  메인 GUI
# =======================
class VisionTestApp(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Vision Test (base64 data URL)")
        self.geometry("900x650")

        # 상태 변수
        self.api_key_var = tk.StringVar(value=load_api_key_from_file())
        self.model_var = tk.StringVar(value="gpt-5-mini")
        self.image_path_var = tk.StringVar(value="")
        self.prompt_var = tk.StringVar(value="이 이미지에 무엇이 보이는지 자세히 설명해줘.")
        self.status_var = tk.StringVar(value="대기 중")

        self._client: Client | None = None  # type: ignore
        self._worker_thread: threading.Thread | None = None

        self._build_widgets()

    # ---------------- UI 구성 ----------------
    def _build_widgets(self):
        # API 설정
        frame_api = ttk.LabelFrame(self, text="OpenAI 설정")
        frame_api.pack(fill="x", padx=10, pady=5)

        ttk.Label(frame_api, text="API Key:").grid(row=0, column=0, sticky="w", padx=5, pady=3)
        entry_key = ttk.Entry(frame_api, textvariable=self.api_key_var, width=50, show="*")
        entry_key.grid(row=0, column=1, sticky="w", padx=5, pady=3)
        btn_save = ttk.Button(frame_api, text="키 저장", command=self.on_save_api_key)
        btn_save.grid(row=0, column=2, sticky="w", padx=5, pady=3)

        ttk.Label(frame_api, text="모델:").grid(row=1, column=0, sticky="w", padx=5, pady=3)
        combo_model = ttk.Combobox(
            frame_api,
            textvariable=self.model_var,
            values=["gpt-5", "gpt-5-mini", "gpt-5-nano"],
            state="readonly",
            width=20,
        )
        combo_model.grid(row=1, column=1, sticky="w", padx=5, pady=3)

        # 이미지 선택
        frame_img = ttk.LabelFrame(self, text="이미지 선택")
        frame_img.pack(fill="x", padx=10, pady=5)

        ttk.Label(frame_img, text="이미지 파일:").grid(row=0, column=0, sticky="w", padx=5, pady=5)
        entry_img = ttk.Entry(frame_img, textvariable=self.image_path_var, width=70)
        entry_img.grid(row=0, column=1, sticky="w", padx=5, pady=5)
        btn_browse = ttk.Button(frame_img, text="찾기...", command=self.on_browse_image)
        btn_browse.grid(row=0, column=2, sticky="w", padx=5, pady=5)

        # 프롬프트
        frame_prompt = ttk.LabelFrame(self, text="프롬프트")
        frame_prompt.pack(fill="x", padx=10, pady=5)

        entry_prompt = ttk.Entry(frame_prompt, textvariable=self.prompt_var, width=100)
        entry_prompt.pack(fill="x", padx=5, pady=5)

        # 실행/상태
        frame_run = ttk.Frame(self)
        frame_run.pack(fill="x", padx=10, pady=5)

        btn_run = ttk.Button(frame_run, text="Vision 테스트 실행", command=self.on_run)
        btn_run.pack(side="left", padx=5, pady=3)

        lbl_status = ttk.Label(frame_run, textvariable=self.status_var)
        lbl_status.pack(side="left", padx=10, pady=3)

        # 결과/로그
        frame_out = ttk.LabelFrame(self, text="응답 / 로그")
        frame_out.pack(fill="both", expand=True, padx=10, pady=5)

        self.txt_out = ScrolledText(frame_out, height=20)
        self.txt_out.pack(fill="both", expand=True, padx=5, pady=5)

        # 안내
        frame_help = ttk.LabelFrame(self, text="설명")
        frame_help.pack(fill="x", padx=10, pady=5)

        help_text = (
            "1) API Key 입력 후 [키 저장]\n"
            "2) [이미지 파일]에서 테스트할 로컬 이미지를 선택\n"
            "3) 프롬프트를 원하는 문장으로 수정\n"
            "4) [Vision 테스트 실행] 클릭\n\n"
            "이 코드는 이미지를 base64 data URL (data:image/jpeg;base64,...)로 변환해서\n"
            "Responses API에 다음과 같이 보냅니다:\n"
            "  content = [\n"
            "    {type: 'input_text', text: <프롬프트>},\n"
            "    {type: 'input_image', image_url: <data URL 문자열>}\n"
            "  ]\n"
            "→ 모델이 이미지를 실제로 이해하고 설명하는지 확인용 테스트입니다."
        )
        ttk.Label(frame_help, text=help_text, justify="left").pack(anchor="w", padx=5, pady=5)

    # ---------------- 유틸 ----------------
    def append_out(self, msg: str):
        def _do():
            self.txt_out.insert("end", msg + "\n")
            self.txt_out.see("end")

        self.after(0, _do)

    # ---------------- 이벤트 핸들러 ----------------
    def on_save_api_key(self):
        key = self.api_key_var.get().strip()
        if not key:
            messagebox.showwarning("경고", "API Key를 입력하세요.")
            return
        save_api_key_to_file(key)
        self.append_out("[INFO] API 키 저장 완료.")

    def on_browse_image(self):
        path = filedialog.askopenfilename(
            title="테스트용 이미지 선택",
            filetypes=[
                ("Image files", "*.jpg;*.jpeg;*.png;*.webp;*.bmp"),
                ("All files", "*.*"),
            ],
        )
        if path:
            self.image_path_var.set(path)

    def on_run(self):
        api_key = self.api_key_var.get().strip()
        model_name = self.model_var.get().strip()
        img_path = self.image_path_var.get().strip()
        prompt = self.prompt_var.get().strip()

        if not api_key:
            messagebox.showwarning("경고", "API Key를 입력하세요.")
            return
        if not model_name:
            messagebox.showwarning("경고", "모델을 선택하세요.")
            return
        if not img_path:
            messagebox.showwarning("경고", "이미지 파일을 선택하세요.")
            return
        if not os.path.exists(img_path):
            messagebox.showwarning("경고", f"이미지 파일을 찾을 수 없습니다:\n{img_path}")
            return
        if not prompt:
            messagebox.showwarning("경고", "프롬프트를 입력하세요.")
            return

        self.status_var.set("실행 중...")
        self.append_out("=" * 80)
        self.append_out(f"[RUN] {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}  모델={model_name}")
        self.append_out(f"[RUN] 이미지: {img_path}")
        self.append_out(f"[RUN] 프롬프트: {prompt}")

        def worker():
            try:
                client = OpenAI(api_key=api_key)

                # 1) 이미지 → data URL
                try:
                    data_url = encode_image_to_data_url(img_path)
                    self.append_out("[INFO] 이미지 base64 data URL 생성 완료.")
                except Exception as e:
                    self.append_out(f"[ERROR] 이미지 인코딩 실패: {e}")
                    self.after(0, lambda: self.status_var.set("에러 (이미지 인코딩 실패)"))
                    return

                content = [
                    {"type": "input_text", "text": prompt},
                    {"type": "input_image", "image_url": data_url},
                ]

                t0 = datetime.now()
                try:
                    resp = client.responses.create(
                        model=model_name,
                        input=[{"role": "user", "content": content}],
                    )
                except Exception as e:
                    self.append_out(f"[ERROR] API 호출 실패: {e}")
                    self.after(0, lambda: self.status_var.set("에러 (API 호출 실패)"))
                    return

                elapsed = (datetime.now() - t0).total_seconds()
                self.append_out(f"[OK] 응답 수신 완료 ({elapsed:.1f}초)\n")

                # 응답 텍스트 뽑기 (간단 버전)
                text_out = self._extract_text_from_response(resp)
                self.append_out("===== 응답 텍스트 =====")
                self.append_out(text_out)
                self.append_out("=======================")

                # usage 로그
                in_tok, out_tok = self._extract_usage_tokens(resp)
                if in_tok or out_tok:
                    self.append_out(f"[TOKENS] input={in_tok}, output={out_tok}")

                self.after(0, lambda: self.status_var.set("완료"))

            except Exception as e:
                self.append_out(f"[FATAL] 처리 중 예외 발생: {e}")
                self.after(0, lambda: self.status_var.set("에러 (예외 발생)"))

        th = threading.Thread(target=worker, daemon=True)
        th.start()
        self._worker_thread = th

    # ---------------- 응답 텍스트 뽑기 (간단 버전) ----------------
    def _extract_text_from_response(self, resp) -> str:
        text_chunks = []

        outputs = getattr(resp, "output", None)
        if outputs:
            try:
                for out in outputs:
                    content_list = getattr(out, "content", None)
                    if not content_list:
                        continue
                    for item in content_list:
                        txt_obj = getattr(item, "text", None)
                        if txt_obj is None:
                            continue
                        val = getattr(txt_obj, "value", None)
                        if isinstance(val, str) and val.strip():
                            text_chunks.append(val.strip())
                        elif isinstance(txt_obj, str) and txt_obj.strip():
                            text_chunks.append(txt_obj.strip())
            except TypeError:
                pass

        if not text_chunks:
            try:
                data = resp.model_dump()
            except Exception:
                data = None

            if isinstance(data, dict):
                out_list = data.get("output") or []
                if isinstance(out_list, list):
                    for out in out_list:
                        if not isinstance(out, dict):
                            continue
                        content_list = out.get("content") or []
                        if not isinstance(content_list, list):
                            continue
                        for c in content_list:
                            if not isinstance(c, dict):
                                continue
                            txt_obj = c.get("text")
                            if isinstance(txt_obj, str) and txt_obj.strip():
                                text_chunks.append(txt_obj.strip())
                            elif isinstance(txt_obj, dict):
                                val = txt_obj.get("value")
                                if isinstance(val, str) and val.strip():
                                    text_chunks.append(val.strip())

        full_text = "\n".join(text_chunks).strip()
        if not full_text:
            return "(응답에서 텍스트를 찾지 못했습니다.)"
        return full_text

    # ---------------- usage 추출 (간단 버전) ----------------
    def _extract_usage_tokens(self, resp):
        in_tok = 0
        out_tok = 0
        try:
            if hasattr(resp, "model_dump"):
                data = resp.model_dump()
            else:
                data = resp

            if isinstance(data, dict):
                usage = data.get("usage")
            else:
                usage = getattr(data, "usage", None)

            if isinstance(usage, dict):
                in_tok = int(usage.get("input_tokens") or usage.get("prompt_tokens") or 0)
                out_tok = int(usage.get("output_tokens") or usage.get("completion_tokens") or 0)
        except Exception:
            pass

        return in_tok, out_tok


def main():
    app = VisionTestApp()
    app.mainloop()


if __name__ == "__main__":
    main()

# 노력정도는 없는 모드이고, 그럼에도 불구하고, 이미지를 이해하는능력뛰어남, base64완벽하네