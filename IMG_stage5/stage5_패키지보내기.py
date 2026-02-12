"""
stage5_export_tool.py

Stage5 외부 PC 작업용 패키지 생성 도구 (Export Tool)
- 기능: I4/I5 엑셀 파일과 필요한 이미지 파일을 패키지로 묶어 외부로 보내기
- 출력: 상대 경로로 변환된 엑셀 + images 폴더 + manifest.json + ZIP 압축

사용법:
1. 엑셀 파일 선택 (I4 또는 I5 버전)
2. 패키지 생성 클릭
3. 생성된 ZIP 파일을 클라우드 드라이브/이메일로 전송
"""

import os
import re
import json
import shutil
import zipfile
import threading
from datetime import datetime
from typing import Optional, Dict, Any, List
from pathlib import Path

import pandas as pd
import tkinter as tk
from tkinter import ttk, filedialog, messagebox
from tkinter.scrolledtext import ScrolledText

try:
    from PIL import Image
    PIL_AVAILABLE = True
except ImportError:
    PIL_AVAILABLE = False


def resize_image_for_review(src_path: str, dest_path: str, size: int = 350) -> bool:
    """
    리뷰용으로 이미지를 리사이즈하여 저장
    - 350x350으로 축소 (비율 유지, 중앙 맞춤)
    - JPG로 저장 (품질 85%)
    """
    if not PIL_AVAILABLE:
        # PIL 없으면 그냥 복사
        import shutil
        shutil.copy2(src_path, dest_path)
        return True

    try:
        img = Image.open(src_path)

        # RGBA → RGB 변환 (JPG 저장용)
        if img.mode in ('RGBA', 'LA', 'P'):
            background = Image.new('RGB', img.size, (255, 255, 255))
            if img.mode == 'P':
                img = img.convert('RGBA')
            if img.mode == 'RGBA':
                background.paste(img, mask=img.split()[-1])
            else:
                background.paste(img)
            img = background
        elif img.mode != 'RGB':
            img = img.convert('RGB')

        # 비율 유지하며 리사이즈
        img.thumbnail((size, size), Image.Resampling.LANCZOS)

        # JPG로 저장 (품질 85%)
        dest_path_jpg = os.path.splitext(dest_path)[0] + ".jpg"
        img.save(dest_path_jpg, "JPEG", quality=85, optimize=True)
        return True
    except Exception as e:
        # 실패 시 원본 복사 시도
        try:
            import shutil
            shutil.copy2(src_path, dest_path)
            return True
        except:
            return False


def get_root_filename(filename: str) -> str:
    """파일명에서 버전 정보와 꼬리표를 제거하고 원본명만 추출"""
    name = os.path.basename(filename)
    base, ext = os.path.splitext(name)

    # 버전 패턴 제거
    while True:
        new_base = re.sub(r"_[Tt]\d+\([^)]*\)_[Ii]\d+", "", base, flags=re.IGNORECASE)
        new_base = re.sub(r"_[Tt]\d+_[Ii]\d+", "", new_base, flags=re.IGNORECASE)
        if new_base == base:
            break
        base = new_base

    base = re.sub(r"\([^)]*\)", "", base)
    base = base.rstrip("_")
    return base + ext


class Stage5ExportToolGUI(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Stage5 Export Tool - 외부 작업용 패키지 생성")
        self.geometry("900x700")

        # 스타일 설정
        self._configure_styles()

        # 변수 초기화
        self.input_file_path = tk.StringVar()
        self.output_dir_path = tk.StringVar()
        self.create_zip = tk.BooleanVar(value=True)

        # 상태 변수
        self.is_running = False
        self.df = None

        # UI 구성
        self._init_ui()

    def _configure_styles(self):
        """스타일 설정"""
        style = ttk.Style()
        try:
            style.theme_use('clam')
        except:
            pass

        bg_color = "#f5f5f5"
        self.configure(background=bg_color)

        style.configure("TFrame", background=bg_color)
        style.configure("TLabelframe", background=bg_color, font=("맑은 고딕", 10, "bold"))
        style.configure("TLabelframe.Label", background=bg_color, foreground="#333333")
        style.configure("TLabel", background=bg_color, font=("맑은 고딕", 10))
        style.configure("Header.TLabel", font=("맑은 고딕", 11, "bold"), foreground="#444")
        style.configure("Action.TButton", font=("맑은 고딕", 11, "bold"), padding=5)

    def _init_ui(self):
        """UI 초기화"""
        main_frame = ttk.Frame(self, padding=20)
        main_frame.pack(fill='both', expand=True)

        # 제목
        title_label = ttk.Label(
            main_frame,
            text="Stage5 외부 작업용 패키지 생성 도구",
            font=("맑은 고딕", 14, "bold"),
            foreground="#0052cc"
        )
        title_label.pack(pady=(0, 10))

        # 설명
        desc_label = ttk.Label(
            main_frame,
            text="I4/I5 엑셀 파일과 관련 이미지를 패키지로 만들어 외부 작업자에게 전송합니다.",
            font=("맑은 고딕", 9),
            foreground="#666"
        )
        desc_label.pack(pady=(0, 15))

        # 1. 입력 파일 선택
        frame_input = ttk.LabelFrame(main_frame, text="1. 입력 파일 선택", padding=15)
        frame_input.pack(fill='x', pady=(0, 10))

        rf1 = ttk.Frame(frame_input)
        rf1.pack(fill='x', pady=5)
        ttk.Label(rf1, text="엑셀 파일:", width=12).pack(side='left')
        ttk.Entry(rf1, textvariable=self.input_file_path, width=50).pack(side='left', fill='x', expand=True, padx=5)
        ttk.Button(rf1, text="파일 선택", command=self._select_input_file).pack(side='right')

        # 2. 출력 위치 선택
        frame_output = ttk.LabelFrame(main_frame, text="2. 패키지 출력 위치", padding=15)
        frame_output.pack(fill='x', pady=(0, 10))

        rf2 = ttk.Frame(frame_output)
        rf2.pack(fill='x', pady=5)
        ttk.Label(rf2, text="출력 폴더:", width=12).pack(side='left')
        ttk.Entry(rf2, textvariable=self.output_dir_path, width=50).pack(side='left', fill='x', expand=True, padx=5)
        ttk.Button(rf2, text="폴더 선택", command=self._select_output_dir).pack(side='right')

        # 옵션
        opt_frame = ttk.Frame(frame_output)
        opt_frame.pack(fill='x', pady=(10, 0))
        ttk.Checkbutton(opt_frame, text="ZIP 파일로 압축", variable=self.create_zip).pack(side='left')

        # 3. 패키지 정보 미리보기
        frame_preview = ttk.LabelFrame(main_frame, text="3. 패키지 정보 미리보기", padding=15)
        frame_preview.pack(fill='x', pady=(0, 10))

        self.preview_text = tk.Text(frame_preview, height=6, state='disabled', font=("Consolas", 9))
        self.preview_text.pack(fill='x')

        # 4. 액션 버튼
        btn_frame = ttk.Frame(main_frame)
        btn_frame.pack(fill='x', pady=(0, 10))

        self.btn_preview = ttk.Button(btn_frame, text="미리보기", style="Action.TButton", command=self._preview_package)
        self.btn_preview.pack(side='left', fill='x', expand=True, padx=(0, 5))

        self.btn_create = ttk.Button(btn_frame, text="패키지 생성", style="Action.TButton", command=self._create_package)
        self.btn_create.pack(side='right', fill='x', expand=True, padx=(5, 0))

        # 5. 진행 상황
        frame_progress = ttk.LabelFrame(main_frame, text="진행 상황", padding=10)
        frame_progress.pack(fill='x', pady=(0, 10))

        self.progress_bar = ttk.Progressbar(frame_progress, mode='determinate', length=400)
        self.progress_bar.pack(fill='x', pady=5)

        self.progress_label = ttk.Label(frame_progress, text="대기 중...")
        self.progress_label.pack(anchor='w')

        # 6. 로그
        frame_log = ttk.LabelFrame(main_frame, text="상세 로그", padding=10)
        frame_log.pack(fill='both', expand=True)

        self.log_widget = ScrolledText(frame_log, height=10, state='disabled', font=("Consolas", 9))
        self.log_widget.pack(fill='both', expand=True)

    def _log(self, msg: str):
        """로그 출력"""
        ts = datetime.now().strftime("%H:%M:%S")
        self.log_widget.config(state='normal')
        self.log_widget.insert('end', f"[{ts}] {msg}\n")
        self.log_widget.see('end')
        self.log_widget.config(state='disabled')

    def _update_preview(self, text: str):
        """미리보기 텍스트 업데이트"""
        self.preview_text.config(state='normal')
        self.preview_text.delete('1.0', 'end')
        self.preview_text.insert('end', text)
        self.preview_text.config(state='disabled')

    def _select_input_file(self):
        """입력 파일 선택"""
        path = filedialog.askopenfilename(
            title="엑셀 파일 선택 (I4 또는 I5)",
            filetypes=[("Excel files", "*.xlsx"), ("All files", "*.*")]
        )
        if path:
            base_name = os.path.basename(path)

            # I4 또는 I5 파일만 허용
            i_match = re.search(r"_I(\d+)", base_name, re.IGNORECASE)
            if not i_match:
                messagebox.showerror("오류", "파일명에 버전 정보(_T*_I*)가 없습니다.")
                return

            i_version = int(i_match.group(1))
            if i_version not in [4, 5]:
                messagebox.showerror("오류", f"I4 또는 I5 파일만 선택 가능합니다. (현재: I{i_version})")
                return

            self.input_file_path.set(path)
            self._log(f"입력 파일 선택됨: {base_name}")

            # 출력 폴더 자동 설정
            excel_dir = os.path.dirname(path)
            job_name = get_root_filename(base_name).replace(".xlsx", "")
            default_output = os.path.join(excel_dir, f"stage5_package_{job_name}")

            if not self.output_dir_path.get():
                self.output_dir_path.set(default_output)
                self._log(f"출력 폴더 자동 설정: {default_output}")

            # 자동 미리보기
            self._preview_package()

    def _select_output_dir(self):
        """출력 폴더 선택"""
        path = filedialog.askdirectory(title="패키지 출력 폴더 선택")
        if path:
            self.output_dir_path.set(path)
            self._log(f"출력 폴더 선택됨: {path}")

    def _preview_package(self):
        """패키지 정보 미리보기"""
        if not self.input_file_path.get():
            self._update_preview("엑셀 파일을 먼저 선택해주세요.")
            return

        try:
            df = pd.read_excel(self.input_file_path.get())
            self.df = df

            # 이미지 경로 수집
            image_cols = ["썸네일경로", "IMG_S1_누끼", "IMG_S4_mix_생성경로"]
            total_images = 0
            existing_images = 0
            missing_images = []

            for col in image_cols:
                if col not in df.columns:
                    continue
                for idx, val in df[col].items():
                    path_str = str(val).strip()
                    if path_str and path_str != "nan":
                        total_images += 1
                        if os.path.exists(path_str):
                            existing_images += 1
                        else:
                            if len(missing_images) < 5:
                                missing_images.append(f"  - {path_str[:60]}...")

            # 파일 크기 추정 (350x350 JPG 기준, 평균 ~15KB)
            estimated_size_mb = existing_images * 0.015  # 평균 15KB 가정

            job_name = get_root_filename(os.path.basename(self.input_file_path.get())).replace(".xlsx", "")

            preview_text = f"""작업명: {job_name}
엑셀 행 수: {len(df)}행
총 이미지 수: {total_images}개
존재하는 이미지: {existing_images}개
누락된 이미지: {total_images - existing_images}개
예상 패키지 크기: ~{estimated_size_mb:.1f} MB"""

            if missing_images:
                preview_text += f"\n\n누락된 파일 (최대 5개):\n" + "\n".join(missing_images)

            self._update_preview(preview_text)
            self._log("미리보기 완료")

        except Exception as e:
            self._update_preview(f"미리보기 실패: {e}")
            self._log(f"[오류] 미리보기 실패: {e}")

    def _create_package(self):
        """패키지 생성"""
        if self.is_running:
            return

        if not self.input_file_path.get():
            messagebox.showwarning("오류", "엑셀 파일을 선택해주세요.")
            return

        if not self.output_dir_path.get():
            messagebox.showwarning("오류", "출력 폴더를 선택해주세요.")
            return

        self.is_running = True
        self.btn_create.config(state='disabled')

        # 백그라운드 스레드에서 실행
        thread = threading.Thread(target=self._create_package_thread)
        thread.daemon = True
        thread.start()

    def _create_package_thread(self):
        """패키지 생성 스레드"""
        try:
            input_path = self.input_file_path.get()
            output_base = self.output_dir_path.get()

            excel_name = os.path.basename(input_path)
            job_name = get_root_filename(excel_name).replace(".xlsx", "")

            # 패키지 폴더 생성
            package_dir = output_base
            if os.path.exists(package_dir):
                self.after(0, lambda: self._log(f"기존 폴더 삭제 중: {package_dir}"))
                shutil.rmtree(package_dir)

            os.makedirs(package_dir, exist_ok=True)
            self.after(0, lambda: self._log(f"패키지 폴더 생성: {package_dir}"))

            # 이미지 폴더 생성
            images_dir = os.path.join(package_dir, "images")
            nukki_dir = os.path.join(images_dir, "nukki")
            mix_dir = os.path.join(images_dir, "mix")
            thumbnail_dir = os.path.join(images_dir, "thumbnail")

            os.makedirs(nukki_dir, exist_ok=True)
            os.makedirs(mix_dir, exist_ok=True)
            os.makedirs(thumbnail_dir, exist_ok=True)

            # 엑셀 로드
            df = pd.read_excel(input_path)
            original_base_dir = os.path.dirname(input_path)

            # 이미지 파일 복사 및 경로 변환
            col_mappings = {
                "썸네일경로": ("thumbnail", thumbnail_dir),
                "IMG_S1_누끼": ("nukki", nukki_dir),
                "IMG_S4_mix_생성경로": ("mix", mix_dir)
            }

            total_images = 0
            copied_images = 0
            failed_images = []

            # 총 이미지 수 계산
            for col in col_mappings.keys():
                if col in df.columns:
                    total_images += int(df[col].notna().sum())

            self.after(0, lambda: self.progress_bar.config(maximum=total_images if total_images > 0 else 1))

            processed = 0
            for col, (subdir, dest_dir) in col_mappings.items():
                if col not in df.columns:
                    continue

                for idx, val in df[col].items():
                    path_str = str(val).strip()
                    if not path_str or path_str == "nan":
                        continue

                    processed += 1
                    self.after(0, lambda p=processed: self.progress_bar.config(value=p))

                    if os.path.exists(path_str):
                        # 파일명 생성 (충돌 방지, JPG로 통일)
                        orig_name = os.path.basename(path_str)
                        base_name = os.path.splitext(orig_name)[0]
                        new_name = f"row{idx}_{base_name}.jpg"
                        dest_path = os.path.join(dest_dir, new_name)

                        try:
                            # 350x350으로 리사이즈하여 저장
                            if resize_image_for_review(path_str, dest_path, size=350):
                                # 상대 경로로 변환
                                rel_path = os.path.join("images", subdir, new_name)
                                df.at[idx, col] = rel_path
                                copied_images += 1
                            else:
                                failed_images.append(f"{path_str}: 리사이즈 실패")
                        except Exception as e:
                            failed_images.append(f"{path_str}: {e}")
                            self.after(0, lambda p=path_str, e=e: self._log(f"[오류] 복사 실패: {p} - {e}"))
                    else:
                        failed_images.append(f"{path_str}: 파일 없음")

                    # UI 업데이트
                    if processed % 50 == 0:
                        self.after(0, lambda p=processed, t=total_images:
                            self.progress_label.config(text=f"이미지 복사 중... {p}/{t}"))

            self.after(0, lambda: self._log(f"이미지 복사 완료: {copied_images}/{total_images}개"))

            # 엑셀 파일 저장 (경로 변환됨)
            excel_output_path = os.path.join(package_dir, excel_name)
            df.to_excel(excel_output_path, index=False)
            self.after(0, lambda: self._log(f"엑셀 파일 저장: {excel_name}"))

            # manifest.json 생성
            manifest = {
                "version": "1.0",
                "job_name": job_name,
                "excel_file": excel_name,
                "original_base_dir": original_base_dir,
                "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "total_rows": len(df),
                "total_images": total_images,
                "copied_images": copied_images,
                "failed_images": len(failed_images),
                "columns_converted": list(col_mappings.keys())
            }

            manifest_path = os.path.join(package_dir, "manifest.json")
            with open(manifest_path, 'w', encoding='utf-8') as f:
                json.dump(manifest, f, ensure_ascii=False, indent=2)
            self.after(0, lambda: self._log("manifest.json 생성 완료"))

            # README 파일 생성
            readme_content = f"""Stage5 외부 작업 패키지
========================

작업명: {job_name}
생성일: {manifest['created_at']}

사용 방법:
1. Stage5_Review.exe 파일을 이 폴더에 복사하세요.
2. Stage5_Review.exe 실행
3. 엑셀 파일 선택: {excel_name}
4. 품질 검증 수행
5. 완료 후 폴더 전체를 ZIP으로 압축하여 회신

포함된 파일:
- {excel_name} (경로가 상대 경로로 변환됨)
- images/ (이미지 파일들)
- manifest.json (패키지 정보)
- README.txt (이 파일)

이미지 통계:
- 총 이미지: {total_images}개
- 복사 완료: {copied_images}개
- 복사 실패: {len(failed_images)}개
"""

            readme_path = os.path.join(package_dir, "README.txt")
            with open(readme_path, 'w', encoding='utf-8') as f:
                f.write(readme_content)
            self.after(0, lambda: self._log("README.txt 생성 완료"))

            # ZIP 압축
            zip_path = None
            if self.create_zip.get():
                self.after(0, lambda: self.progress_label.config(text="ZIP 파일 생성 중..."))
                zip_path = f"{package_dir}.zip"

                with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
                    for root, dirs, files in os.walk(package_dir):
                        for file in files:
                            file_path = os.path.join(root, file)
                            arcname = os.path.relpath(file_path, os.path.dirname(package_dir))
                            zf.write(file_path, arcname)

                zip_size_mb = os.path.getsize(zip_path) / (1024 * 1024)
                self.after(0, lambda: self._log(f"ZIP 파일 생성 완료: {os.path.basename(zip_path)} ({zip_size_mb:.1f} MB)"))

            # 완료
            self.after(0, lambda: self.progress_label.config(text="패키지 생성 완료!"))
            self.after(0, lambda: self.progress_bar.config(value=self.progress_bar['maximum']))

            # 결과 메시지
            result_msg = f"""패키지 생성이 완료되었습니다.

작업명: {job_name}
이미지: {copied_images}/{total_images}개 복사됨

패키지 폴더: {package_dir}"""

            if zip_path:
                result_msg += f"\nZIP 파일: {zip_path}"

            if failed_images:
                result_msg += f"\n\n경고: {len(failed_images)}개 이미지 복사 실패"

            self.after(0, lambda: messagebox.showinfo("완료", result_msg))

            # 폴더 열기
            self.after(0, lambda: os.startfile(package_dir) if os.name == 'nt' else None)

        except Exception as e:
            self.after(0, lambda: self._log(f"[오류] 패키지 생성 실패: {e}"))
            self.after(0, lambda: messagebox.showerror("오류", f"패키지 생성 실패:\n{e}"))
        finally:
            self.is_running = False
            self.after(0, lambda: self.btn_create.config(state='normal'))


def main():
    app = Stage5ExportToolGUI()
    app.mainloop()


if __name__ == "__main__":
    main()
