#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
remove_background_gui.py

- rembg(birefnet-general)을 이용해 이미지 배경 제거
- 단일 이미지 또는 폴더 전체 처리
- Tkinter GUI로 폴더/파일 선택 후 버튼 한 번으로 실행
- rembg가 만든 알파를 최대한 보존(모폴로지 X)
  + 가장 큰 객체만 남기고, 아주 얇은 노이즈만 제거
- 결과 이미지는 1000x1000 흰 배경 JPG
- 품질 분석을 통해
    _auto_ok       : 자동으로 써도 될 것 같은 컷
    _need_manual   : 사람이 다시 확인해야 할 컷
  두 폴더로 자동 분리 저장

필수 패키지 (GPU 사용 가능):
    pip install "rembg[gpu]" pillow opencv-python numpy onnxruntime
또는 CPU 전용:
    pip install "rembg[cpu]" pillow opencv-python numpy onnxruntime
"""

import os
import io
import argparse
import time
from datetime import datetime, timedelta

from rembg import remove, new_session
from PIL import Image
import numpy as np
import cv2
import onnxruntime as ort  # GPU / CPU provider 확인용

# ---------------------------------------------------------------------
#  onnxruntime provider 설정 (GPU 가능하면 CUDA, 아니면 CPU)
# ---------------------------------------------------------------------

_AVAILABLE_PROVIDERS = ort.get_available_providers()

if "CUDAExecutionProvider" in _AVAILABLE_PROVIDERS:
    PROVIDERS = ["CUDAExecutionProvider", "CPUExecutionProvider"]
else:
    PROVIDERS = ["CPUExecutionProvider"]

# ---------------------------------------------------------------------
#  고급 설정: 모델 세션 & 마스크 파라미터
# ---------------------------------------------------------------------

# 고품질 일반용 모델
SESSION = new_session("birefnet-general", providers=PROVIDERS)

# 마스크/알파 관련 파라미터
SOFT_MASK_THRESHOLD = 5    # 알파가 이 값(0~255)보다 크면 전경으로 간주
ALPHA_CUTOFF = 20          # 이 값보다 낮은 알파는 완전 0으로 (잔털만 제거)

# 최종 출력 사이즈
OUTPUT_SIZE = (1000, 1000)  # (width, height)

# 예상 처리 속도(초/이미지) - 실제 속도 보고 여기 숫자만 조절
EST_SECONDS_PER_IMAGE = 6.6

# 품질 분류용 폴더 이름
AUTO_OK_DIR = "_auto_ok"
NEED_MANUAL_DIR = "_need_manual"


# --- 유틸 함수 -------------------------------------------------------------


def format_time(seconds: float) -> str:
    """초 단위를 'H시간 M분 S초' 형태로 포맷."""
    seconds = int(max(seconds, 0))
    m, s = divmod(seconds, 60)
    h, m = divmod(m, 60)
    if h > 0:
        return f"{h}시간 {m}분 {s}초"
    elif m > 0:
        return f"{m}분 {s}초"
    else:
        return f"{s}초"


# --- 품질 분석 로직 --------------------------------------------------------


def analyze_mask(final_alpha: np.ndarray) -> bool:
    """
    알파 마스크를 보고 '수상한' 이미지인지 판별.
    True  = 사람이 다시 확인해야 할 이미지 (_need_manual)
    False = 자동으로 사용해도 될 것 같은 이미지 (_auto_ok)

    ※ 기준은 굉장히 완화해서,
       - 거의 아무것도 없거나 (fg_ratio 너무 작음)
       - 화면을 다 뒤덮었거나 (fg_ratio 너무 큼)
       - 큰 덩어리가 2개 이상일 때만 수동으로 보냄
    """
    h, w = final_alpha.shape
    bin_mask = (final_alpha > ALPHA_CUTOFF).astype(np.uint8)

    total_pixels = h * w
    fg_pixels = int(bin_mask.sum())

    if fg_pixels == 0:
        return True  # 완전 비어 있으면 무조건 수동

    fg_ratio = fg_pixels / total_pixels

    # bounding box
    ys, xs = np.where(bin_mask > 0)
    y_min, y_max = ys.min(), ys.max()
    x_min, x_max = xs.min(), xs.max()

    touch_left = x_min == 0
    touch_right = x_max == w - 1
    touch_top = y_min == 0
    touch_bottom = y_max == h - 1
    touches = sum([touch_left, touch_right, touch_top, touch_bottom])

    # 연결 컴포넌트 통계
    num_labels, labels, stats, _ = cv2.connectedComponentsWithStats(bin_mask)
    if num_labels > 1:
        areas = stats[1:, cv2.CC_STAT_AREA]  # 배경 제외
        large_areas = [a for a in areas if a > (total_pixels * 0.08)]
    else:
        large_areas = []

    suspicious = False

    # 전경 비율이 너무 작거나 너무 크면 수동
    if fg_ratio < 0.03 or fg_ratio > 0.98:
        suspicious = True

    # 큰 덩어리가 2개 이상이면 수동 (제품 + 다른 큰 물체 같이 남았을 가능성)
    if len(large_areas) >= 2:
        suspicious = True

    # 화면 양쪽/위아래를 동시에 건드리고 있으면(2면 이상) 수동
    if touches >= 2:
        suspicious = True

    return suspicious


# --- 핵심 처리 로직 -------------------------------------------------------


def remove_bg_to_rgba(input_path: str):
    """
    rembg로 배경제거 후,
    - 가장 큰 객체만 남기고
    - 알파 컷으로 노이즈만 제거
    한 RGBA 이미지와 알파 마스크를 반환.
    (모폴로지 연산 X, 디테일 최대 보존)
    """
    with open(input_path, "rb") as infile:
        input_data = infile.read()

    # rembg로 배경제거 수행 (birefnet-general 사용)
    result_bytes = remove(
        input_data,
        session=SESSION,
        # alpha_matting 옵션은 그대로 유지 (품질 보정용)
        alpha_matting=True,
        alpha_matting_foreground_threshold=240,
        alpha_matting_background_threshold=10,
        alpha_matting_erode_size=5,
        alpha_matting_base_size=1500,
        force_return_bytes=True,
    )

    # PNG 바이트 → PIL 이미지 (RGBA)
    img = Image.open(io.BytesIO(result_bytes)).convert("RGBA")
    r, g, b, a = img.split()
    alpha_raw = np.array(a, dtype=np.uint8)  # (H, W)

    # 1) 얇은 마스크 생성 (알파 값 거의 0인 부분만 배경)
    soft_mask = (alpha_raw > SOFT_MASK_THRESHOLD).astype(np.uint8)

    # 2) 가장 큰 객체만 남김 (추가 erosion/dilation 없음)
    num_labels, labels = cv2.connectedComponents(soft_mask)
    if num_labels <= 1:
        keep_mask = soft_mask
    else:
        areas = np.bincount(labels.flatten())
        areas[0] = 0  # 배경 무시
        largest_label = areas.argmax()
        keep_mask = (labels == largest_label).astype(np.uint8)

    # 3) 최종 알파: 원래 알파에 keep_mask만 곱해서 디테일 보존
    final_alpha = alpha_raw * keep_mask

    # 4) 아주 낮은 알파만 0으로 잘라서 잔털 제거
    final_alpha[final_alpha < ALPHA_CUTOFF] = 0

    final_a = Image.fromarray(final_alpha, mode="L")
    final_img = Image.merge("RGBA", (r, g, b, final_a))

    return final_img, final_alpha


def render_to_1000x1000_rgb(img_rgba: Image.Image) -> Image.Image:
    """RGBA 이미지를 1000x1000 흰 배경 위에 비율 유지하여 가운데 배치 후 RGB로."""
    target_w, target_h = OUTPUT_SIZE
    src_w, src_h = img_rgba.size

    scale = min(target_w / src_w, target_h / src_h)
    new_w = max(1, int(src_w * scale))
    new_h = max(1, int(src_h * scale))

    resized = img_rgba.resize((new_w, new_h), Image.LANCZOS)

    canvas = Image.new("RGBA", (target_w, target_h), (255, 255, 255, 255))
    offset_x = (target_w - new_w) // 2
    offset_y = (target_h - new_h) // 2
    canvas.paste(resized, (offset_x, offset_y), resized)

    return canvas.convert("RGB")


def iter_image_files(input_dir: str):
    """폴더 안의 지원되는 이미지 경로들을 generator로 반환"""
    supported_exts = {".jpg", ".jpeg", ".png", ".webp"}
    for root, dirs, files in os.walk(input_dir):
        for fname in files:
            ext = os.path.splitext(fname)[1].lower()
            if ext in supported_exts:
                yield root, fname


def process_directory(input_dir: str, output_dir: str, log_func=None) -> None:
    """폴더 전체 배경제거 + 품질 분류 + 진행률/ETA 로그"""
    files = list(iter_image_files(input_dir))
    total = len(files)

    if total == 0:
        if log_func:
            log_func("처리할 이미지가 없습니다.")
        return

    if log_func:
        log_func(f"전체 이미지 수: {total}개")
        log_func(f"ONNX providers: {PROVIDERS}")
        log_func(f"결과 폴더: {output_dir}")
        log_func(f"  - 자동 통과: {os.path.join(output_dir, AUTO_OK_DIR)}")
        log_func(f"  - 수동 확인: {os.path.join(output_dir, NEED_MANUAL_DIR)}")

    start_time = time.time()

    for idx, (root, fname) in enumerate(files, start=1):
        relative_root = os.path.relpath(root, input_dir)

        base_name, _ = os.path.splitext(fname)
        output_name = f"{base_name}.jpg"
        input_path = os.path.join(root, fname)

        # rembg + 후처리
        final_rgba, final_alpha = remove_bg_to_rgba(input_path)

        # 품질 분석
        suspicious = analyze_mask(final_alpha)
        category = "수동확인" if suspicious else "자동OK"
        subdir = NEED_MANUAL_DIR if suspicious else AUTO_OK_DIR

        target_root = os.path.join(output_dir, subdir, relative_root)
        os.makedirs(target_root, exist_ok=True)
        output_path = os.path.join(target_root, output_name)

        # 렌더링 + 저장
        output_img = render_to_1000x1000_rgb(final_rgba)
        output_img.save(output_path, "JPEG", quality=90, optimize=True)

        # 진행률/ETA 로그
        elapsed = time.time() - start_time
        avg = elapsed / idx
        remaining = avg * (total - idx)

        if log_func:
            log_func(
                f"[{idx}/{total}] {input_path} -> {output_path} "
                f"[{category}] "
                f"(경과: {format_time(elapsed)}, 예상 남은: {format_time(remaining)})"
            )


def process_single_file(input_path: str, output_dir: str, log_func=None) -> None:
    """단일 파일 처리 (품질 분류 + 저장)"""
    if log_func:
        log_func(f"[1/1] {input_path}")
        log_func(f"ONNX providers: {PROVIDERS}")
        log_func(f"결과 폴더: {output_dir}")
        log_func(f"  - 자동 통과: {os.path.join(output_dir, AUTO_OK_DIR)}")
        log_func(f"  - 수동 확인: {os.path.join(output_dir, NEED_MANUAL_DIR)}")

    base_name, _ = os.path.splitext(os.path.basename(input_path))
    output_name = f"{base_name}.jpg"

    final_rgba, final_alpha = remove_bg_to_rgba(input_path)

    suspicious = analyze_mask(final_alpha)
    category = "수동확인" if suspicious else "자동OK"
    subdir = NEED_MANUAL_DIR if suspicious else AUTO_OK_DIR

    target_root = os.path.join(output_dir, subdir)
    os.makedirs(target_root, exist_ok=True)
    output_path = os.path.join(target_root, output_name)

    output_img = render_to_1000x1000_rgb(final_rgba)
    output_img.save(output_path, "JPEG", quality=90, optimize=True)

    if log_func:
        log_func(f"저장 위치: {output_path} [{category}]")


# --- CLI 모드 ---------------------------------------------------------------


def cli_main():
    parser = argparse.ArgumentParser(
        description="Remove backgrounds from images using rembg (CLI/GUI)."
    )
    parser.add_argument("--input", help="(선택) 이미지 파일 또는 디렉토리. 생략하면 GUI 실행.")
    parser.add_argument("--output", help="(선택) 출력 디렉토리. 생략하면 GUI 실행.")
    args = parser.parse_args()

    if not args.input or not args.output:
        run_gui()
        return

    print(f"[INFO] Using ONNX providers: {PROVIDERS}")

    input_path = args.input
    output_path = args.output

    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Input path does not exist: {input_path}")

    if os.path.isdir(input_path):
        files = list(iter_image_files(input_path))
        total = len(files)
        if total == 0:
            print("처리할 이미지가 없습니다.")
            return

        est_total = EST_SECONDS_PER_IMAGE * total
        est_end = datetime.now() + timedelta(seconds=est_total)
        print(f"전체 이미지 수: {total}개")
        print(
            f"한 장당 {EST_SECONDS_PER_IMAGE:.1f}초 기준, "
            f"예상 소요: {format_time(est_total)}"
        )
        print(f"예상 종료 시각: {est_end.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"결과 폴더: {output_path}")
        print(f"  - 자동 통과: {os.path.join(output_path, AUTO_OK_DIR)}")
        print(f"  - 수동 확인: {os.path.join(output_path, NEED_MANUAL_DIR)}")

        os.makedirs(output_path, exist_ok=True)
        process_directory(input_path, output_path, print)
    else:
        os.makedirs(output_path, exist_ok=True)
        process_single_file(input_path, output_path, print)


# --- Tkinter GUI -------------------------------------------------------------

import tkinter as tk
from tkinter import filedialog, messagebox, scrolledtext


def run_gui():
    root = tk.Tk()
    root.title("배경제거 자동화 (rembg)")
    root.geometry("700x450")

    input_path_var = tk.StringVar()
    output_path_var = tk.StringVar()
    mode_var = tk.StringVar(value="folder")  # 'file' or 'folder'

    log_text = scrolledtext.ScrolledText(root, height=18, state="disabled")
    log_text.pack(fill="both", expand=True, padx=10, pady=5)

    def log(message: str):
        log_text.configure(state="normal")
        log_text.insert(tk.END, message + "\n")
        log_text.see(tk.END)
        log_text.configure(state="disabled")
        root.update_idletasks()

    # 초기 안내
    log(f"ONNX providers: {PROVIDERS}")
    log("결과는 1000x1000 흰 배경 JPG로 저장됩니다.")
    log(f"현재 설정된 예상 속도: {EST_SECONDS_PER_IMAGE:.1f}초/이미지")
    log(f"자동 분류 폴더: '{AUTO_OK_DIR}' (자동 통과), '{NEED_MANUAL_DIR}' (수동 확인)")

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

    def select_output():
        path = filedialog.askdirectory(title="출력 폴더 선택")
        if path:
            output_path_var.set(path)

    def start_processing():
        in_path = input_path_var.get().strip()
        out_path = output_path_var.get().strip()

        if not in_path:
            messagebox.showwarning("주의", "입력 파일/폴더를 선택해주세요.")
            return
        if not out_path:
            messagebox.showwarning("주의", "출력 폴더를 선택해주세요.")
            return
        if not os.path.exists(in_path):
            messagebox.showerror("오류", f"입력 경로가 존재하지 않습니다:\n{in_path}")
            return

        try:
            if os.path.isdir(in_path) and mode_var.get() == "folder":
                files = list(iter_image_files(in_path))
                total = len(files)
                if total == 0:
                    messagebox.showwarning("주의", "처리할 이미지가 없습니다.")
                    return
                est_total = EST_SECONDS_PER_IMAGE * total
                est_end = datetime.now() + timedelta(seconds=est_total)

                log("=== 작업 설정 ===")
                log(f"전체 이미지 수: {total}개")
                log(
                    f"한 장당 {EST_SECONDS_PER_IMAGE:.1f}초 기준, "
                    f"예상 소요: {format_time(est_total)}"
                )
                log(f"예상 종료 시각: {est_end.strftime('%Y-%m-%d %H:%M:%S')}")
                log(f"결과 폴더: {out_path}")
                log(f"  - 자동 통과: {os.path.join(out_path, AUTO_OK_DIR)}")
                log(f"  - 수동 확인: {os.path.join(out_path, NEED_MANUAL_DIR)}")
            else:
                log("단일 파일 모드: 1개 이미지 처리 예정")
                log(f"결과 폴더: {out_path}")
                log(f"  - 자동 통과: {os.path.join(out_path, AUTO_OK_DIR)}")
                log(f"  - 수동 확인: {os.path.join(out_path, NEED_MANUAL_DIR)}")

            log("=== 작업 시작 ===")
            if os.path.isdir(in_path) and mode_var.get() == "folder":
                process_directory(in_path, out_path, log)
            else:
                os.makedirs(out_path, exist_ok=True)
                process_single_file(in_path, out_path, log)
            log("=== 작업 완료 ===")
            messagebox.showinfo("완료", "배경제거 작업이 완료되었습니다.")
        except Exception as e:
            log(f"[ERROR] {e}")
            messagebox.showerror("오류", f"작업 중 오류 발생:\n{e}")

    # 모드 선택
    mode_frame = tk.Frame(root)
    mode_frame.pack(fill="x", padx=10, pady=5)
    tk.Label(mode_frame, text="모드:").pack(side="left")
    tk.Radiobutton(
        mode_frame, text="폴더 전체", variable=mode_var, value="folder"
    ).pack(side="left")
    tk.Radiobutton(
        mode_frame, text="단일 파일", variable=mode_var, value="file"
    ).pack(side="left")

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
        command=start_processing,
        bg="#4caf50",
        fg="white",
    ).pack(side="right")

    root.mainloop()


if __name__ == "__main__":
    cli_main()
