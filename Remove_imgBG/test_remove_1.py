#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
remove_background_presets_gui.py

- rembg + birefnet-general 기반 프리셋 비교용 GUI
- 단일 파일 / 폴더를 선택하면, 정의된 여러 프리셋으로
  배경제거 결과를 각각 생성
- 출력 파일명: 원본파일명_프리셋이름.jpg

사용 예시:
    python remove_background_presets_gui.py
"""

import os
import io
import time
import tkinter as tk
from tkinter import filedialog, messagebox, scrolledtext

import numpy as np
import cv2
from PIL import Image
from rembg import remove, new_session
import onnxruntime as ort

# ---------------------------------------------------------------------
# onnxruntime provider 설정 (GPU 가능하면 CUDA, 아니면 CPU)
# ---------------------------------------------------------------------
_AVAILABLE_PROVIDERS = ort.get_available_providers()
if "CUDAExecutionProvider" in _AVAILABLE_PROVIDERS:
    PROVIDERS = ["CUDAExecutionProvider", "CPUExecutionProvider"]
else:
    PROVIDERS = ["CPUExecutionProvider"]

# ---------------------------------------------------------------------
# 모델 세션
# ---------------------------------------------------------------------
SESSION = new_session("birefnet-general", providers=PROVIDERS)

# 최종 출력 사이즈 (필요 없으면 1000,1000 대신 None 쓰도록 수정 가능)
OUTPUT_SIZE = (1000, 1000)

# ---------------------------------------------------------------------
# 프리셋 정의
#   - 여기 숫자를 바꿔가면서 실험하면 됨
#   - 나중에 마음에 드는 프리셋의 값을 메인 GUI 코드에 옮겨 심으면 됨
# ---------------------------------------------------------------------
PRESETS = {
    # 기본형 (지금 메인 코드와 비슷한 느낌)
    "std": dict(
        fg_t=240,
        bg_t=10,
        erode_size=10,
        base_size=1000,
        soft_thr=10,
        alpha_thr=200,
        erode_iter=2,
        dilate_iter=3,
    ),
    # 디테일 중시 (얇은 프레임/선 같은 것 살리기 쪽)
    "detail": dict(
        fg_t=230,
        bg_t=20,
        erode_size=5,
        base_size=1500,
        soft_thr=5,
        alpha_thr=180,
        erode_iter=1,
        dilate_iter=2,
    ),
    # 공격적으로 깨끗하게 (배경 노이즈 최대 제거, 대신 잘릴 수도 있음)
    "aggressive": dict(
        fg_t=250,
        bg_t=5,
        erode_size=15,
        base_size=1200,
        soft_thr=15,
        alpha_thr=220,
        erode_iter=3,
        dilate_iter=4,
    ),
}


# ---------------------------------------------------------------------
#  유틸: 폴더 안 이미지 나열
# ---------------------------------------------------------------------
def iter_image_files(input_dir: str):
    supported_exts = {".jpg", ".jpeg", ".png", ".webp"}
    for root, dirs, files in os.walk(input_dir):
        for fname in files:
            ext = os.path.splitext(fname)[1].lower()
            if ext in supported_exts:
                yield root, fname


# ---------------------------------------------------------------------
#  프리셋 한 개를 적용해서 이미지 처리
# ---------------------------------------------------------------------
def process_image_with_preset(img_bytes: bytes, preset: dict) -> Image.Image:
    """
    - rembg + alpha_matting (preset 값 사용)
    - 큰 커널로 '덩어리 분리'만 하고, 모양은 원래 alpha로 유지
    - 가장 큰 덩어리(주 제품)만 남기고 1000x1000 흰 배경에 올림
    """
    # 1) rembg 호출 (birefnet-general)
    res_bytes = remove(
        img_bytes,
        session=SESSION,
        alpha_matting=True,
        alpha_matting_foreground_threshold=preset["fg_t"],
        alpha_matting_background_threshold=preset["bg_t"],
        alpha_matting_erode_size=preset["erode_size"],
        alpha_matting_base_size=preset["base_size"],
        force_return_bytes=True,
    )

    img = Image.open(io.BytesIO(res_bytes)).convert("RGBA")
    r, g, b, a = img.split()
    alpha = np.array(a, dtype=np.uint8)  # 원래 알파 (디테일 유지용)

    # ---------------------------------------------------------
    # 2) soft mask (rembg 알파 → 0/1)
    # ---------------------------------------------------------
    soft_mask = (alpha > preset["soft_thr"]).astype(np.uint8)

    # ---------------------------------------------------------
    # 3) '연결 끊기' 전용 큰 커널 erosion
    #    - 여기서만 강하게 깎아서 의자/테이블 분리 시도
    # ---------------------------------------------------------
    # 큰 커널로 얇은 연결부를 끊는다 (예: 7x7, 1~2회)
    kernel_sep = np.ones((7, 7), np.uint8)
    eroded_for_labels = cv2.erode(soft_mask, kernel_sep, iterations=1)

    # 소형 노이즈를 조금 줄이기 위해 한 번 더 3x3 opening 정도 해도 됨
    # kernel_small = np.ones((3, 3), np.uint8)
    # eroded_for_labels = cv2.erode(eroded_for_labels, kernel_small, iterations=1)

    # ---------------------------------------------------------
    # 4) connected components로 '덩어리' 라벨링
    # ---------------------------------------------------------
    num_labels, labels = cv2.connectedComponents(eroded_for_labels)

    if num_labels <= 1:
        # 전경이 한 덩어리뿐이면 그냥 alpha 그대로 사용
        final_alpha = alpha.copy()
    else:
        # label 0 = 배경, 1.. = 덩어리들
        areas = np.bincount(labels.flatten())
        areas[0] = 0
        largest_label = areas.argmax()

        # 이 라벨(의자라고 가정)만 남기는 마스크 (0/1)
        component_mask = (labels == largest_label).astype(np.uint8)

        # 너무 많이 깎였을 수 있으니 3x3로 살짝만 다시 키우기
        kernel_small = np.ones((3, 3), np.uint8)
        component_mask = cv2.dilate(
            component_mask, kernel_small, iterations=preset["dilate_iter"]
        )

        # ---------------------------------------------------------
        # 5) '모양'은 원래 alpha, '어디를 남길지'만 component_mask로 결정
        # ---------------------------------------------------------
        final_alpha = alpha * component_mask

    # ---------------------------------------------------------
    # 6) 잔상 제거용 alpha threshold
    # ---------------------------------------------------------
    final_alpha[final_alpha < preset["alpha_thr"]] = 0

    final_a = Image.fromarray(final_alpha, mode="L")
    final_img = Image.merge("RGBA", (r, g, b, final_a))

    # ---------------------------------------------------------
    # 7) 1000x1000 흰 배경에 비율 유지 리사이즈
    # ---------------------------------------------------------
    if OUTPUT_SIZE is not None:
        target_w, target_h = OUTPUT_SIZE
        src_w, src_h = final_img.size
        scale = min(target_w / src_w, target_h / src_h)
        new_w = max(1, int(src_w * scale))
        new_h = max(1, int(src_h * scale))

        resized = final_img.resize((new_w, new_h), Image.LANCZOS)
        canvas = Image.new("RGBA", (target_w, target_h), (255, 255, 255, 255))
        offset_x = (target_w - new_w) // 2
        offset_y = (target_h - new_h) // 2
        canvas.paste(resized, (offset_x, offset_y), resized)
        final_rgb = canvas.convert("RGB")
    else:
        final_rgb = final_img.convert("RGB")

    return final_rgb



# ---------------------------------------------------------------------
#  메인 처리 로직 (파일/폴더 공통)
# ---------------------------------------------------------------------
def process_single_file(input_path: str, output_dir: str, log_func=None):
    """단일 파일에 대해 모든 프리셋 결과를 생성."""
    with open(input_path, "rb") as f:
        img_bytes = f.read()

    base_name, _ = os.path.splitext(os.path.basename(input_path))

    for preset_name, preset in PRESETS.items():
        out_name = f"{base_name}_{preset_name}.jpg"
        out_path = os.path.join(output_dir, out_name)
        if log_func:
            log_func(f"  - preset '{preset_name}' -> {out_path}")
        out_img = process_image_with_preset(img_bytes, preset)
        os.makedirs(output_dir, exist_ok=True)
        out_img.save(out_path, "JPEG", quality=90, optimize=True)


def process_folder(input_dir: str, output_dir: str, log_func=None):
    """폴더 내 모든 이미지에 대해 모든 프리셋 결과를 생성."""
    files = list(iter_image_files(input_dir))
    total_files = len(files)

    if total_files == 0:
        if log_func:
            log_func("처리할 이미지가 없습니다.")
        return

    if log_func:
        log_func(f"총 이미지 파일 수: {total_files}개")
        log_func(f"프리셋 수: {len(PRESETS)}개")
        log_func(f"ONNX providers: {PROVIDERS}")

    for idx, (root, fname) in enumerate(files, start=1):
        in_path = os.path.join(root, fname)
        rel_root = os.path.relpath(root, input_dir)
        out_root = os.path.join(output_dir, rel_root)

        if log_func:
            log_func(f"[{idx}/{total_files}] {in_path}")

        process_single_file(in_path, out_root, log_func)


# ---------------------------------------------------------------------
#  Tkinter GUI
# ---------------------------------------------------------------------
def run_gui():
    root = tk.Tk()
    root.title("배경제거 프리셋 테스트 (rembg)")
    root.geometry("700x450")

    # 상태 변수
    input_path_var = tk.StringVar()
    output_path_var = tk.StringVar()
    mode_var = tk.StringVar(value="file")  # 기본은 단일 파일

    # 로그창
    log_text = scrolledtext.ScrolledText(root, height=18, state="disabled")
    log_text.pack(fill="both", expand=True, padx=10, pady=5)

    def log(msg: str):
        log_text.configure(state="normal")
        log_text.insert(tk.END, msg + "\n")
        log_text.see(tk.END)
        log_text.configure(state="disabled")
        root.update_idletasks()

    # 시작 안내
    log(f"ONNX providers: {PROVIDERS}")
    log("프리셋 목록:")
    for name, p in PRESETS.items():
        log(f"  - {name}: {p}")
    log("출력 파일명: 원본이름_프리셋이름.jpg (예: chair_std.jpg, chair_detail.jpg)")
    log("")

    # 입력 선택
    def select_input():
        if mode_var.get() == "file":
            path = filedialog.askopenfilename(
                title="이미지 파일 선택",
                filetypes=[
                    ("Image files", "*.jpg *.jpeg *.png *.webp"),
                    ("All files", "*.*"),
                ],
            )
        else:
            path = filedialog.askdirectory(title="입력 폴더 선택")
        if path:
            input_path_var.set(path)

    # 출력 폴더 선택
    def select_output():
        path = filedialog.askdirectory(title="출력 폴더 선택")
        if path:
            output_path_var.set(path)

    # 실행
    def start():
        in_path = input_path_var.get().strip()
        out_dir = output_path_var.get().strip()

        if not in_path:
            messagebox.showwarning("주의", "입력 파일/폴더를 선택해주세요.")
            return
        if not out_dir:
            messagebox.showwarning("주의", "출력 폴더를 선택해주세요.")
            return
        if not os.path.exists(in_path):
            messagebox.showerror("오류", f"입력 경로가 존재하지 않습니다:\n{in_path}")
            return

        try:
            os.makedirs(out_dir, exist_ok=True)
            log("=== 작업 시작 ===")
            start_time = time.time()

            if os.path.isdir(in_path) and mode_var.get() == "folder":
                process_folder(in_path, out_dir, log)
            else:
                log(f"입력 파일: {in_path}")
                process_single_file(in_path, out_dir, log)

            elapsed = time.time() - start_time
            log(f"=== 작업 완료 (경과: {int(elapsed)}초) ===")
            messagebox.showinfo("완료", "프리셋 테스트 이미지 생성이 완료되었습니다.")
        except Exception as e:
            log(f"[ERROR] {e}")
            messagebox.showerror("오류", f"작업 중 오류 발생:\n{e}")

    # --- UI 구성 ---

    # 모드 선택
    mode_frame = tk.Frame(root)
    mode_frame.pack(fill="x", padx=10, pady=5)
    tk.Label(mode_frame, text="모드:").pack(side="left")
    tk.Radiobutton(mode_frame, text="단일 파일", variable=mode_var, value="file").pack(
        side="left"
    )
    tk.Radiobutton(mode_frame, text="폴더 전체", variable=mode_var, value="folder").pack(
        side="left"
    )

    # 입력 경로
    in_frame = tk.Frame(root)
    in_frame.pack(fill="x", padx=10, pady=5)
    tk.Label(in_frame, text="입력:").pack(side="left")
    tk.Entry(in_frame, textvariable=input_path_var, width=55).pack(
        side="left", padx=5
    )
    tk.Button(in_frame, text="찾기...", command=select_input).pack(side="left")

    # 출력 폴더
    out_frame = tk.Frame(root)
    out_frame.pack(fill="x", padx=10, pady=5)
    tk.Label(out_frame, text="출력 폴더:").pack(side="left")
    tk.Entry(out_frame, textvariable=output_path_var, width=55).pack(
        side="left", padx=5
    )
    tk.Button(out_frame, text="찾기...", command=select_output).pack(side="left")

    # 실행 버튼
    btn_frame = tk.Frame(root)
    btn_frame.pack(fill="x", padx=10, pady=5)
    tk.Button(
        btn_frame,
        text="배경제거 시작",
        command=start,
        bg="#4caf50",
        fg="white",
    ).pack(side="right")

    root.mainloop()


if __name__ == "__main__":
    run_gui()
