"""
stage5_import_tool.py

Stage5 외부 PC 작업 결과 가져오기 도구 (Import Tool)
- 기능: 외부 작업자가 완료한 패키지를 가져와서 경로를 복원
- 출력: 절대 경로로 변환된 엑셀 파일 + 복사된 이미지 파일

사용법:
1. 외부에서 받은 패키지 폴더 또는 ZIP 파일 선택
2. 원본 이미지 경로 지정 (선택사항)
3. Import 실행
4. cloudflare_upload_gui로 업로드
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


def get_root_filename(filename: str) -> str:
    """파일명에서 버전 정보와 꼬리표를 제거하고 원본명만 추출"""
    name = os.path.basename(filename)
    base, ext = os.path.splitext(name)

    while True:
        new_base = re.sub(r"_[Tt]\d+\([^)]*\)_[Ii]\d+", "", base, flags=re.IGNORECASE)
        new_base = re.sub(r"_[Tt]\d+_[Ii]\d+", "", new_base, flags=re.IGNORECASE)
        if new_base == base:
            break
        base = new_base

    base = re.sub(r"\([^)]*\)", "", base)
    base = base.rstrip("_")
    return base + ext


class Stage5ImportToolGUI(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Stage5 Import Tool - 외부 작업 결과 가져오기")
        self.geometry("950x750")

        # 스타일 설정
        self._configure_styles()

        # 변수 초기화
        self.package_path = tk.StringVar()  # 패키지 폴더 또는 ZIP 경로
        self.output_dir_path = tk.StringVar()  # 결과 저장 위치
        self.restore_original_paths = tk.BooleanVar(value=False)  # 원본 경로 복원 여부
        self.copy_final_images = tk.BooleanVar(value=True)  # 최종 이미지 복사 여부

        # 상태 변수
        self.is_running = False
        self.manifest = None
        self.package_dir = None  # 실제 패키지 디렉토리 (ZIP 압축 해제 후)

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
            text="Stage5 외부 작업 결과 가져오기 도구",
            font=("맑은 고딕", 14, "bold"),
            foreground="#28a745"
        )
        title_label.pack(pady=(0, 10))

        # 설명
        desc_label = ttk.Label(
            main_frame,
            text="외부 작업자가 완료한 패키지를 가져와서 절대 경로로 복원하고 cloudflare_upload_gui와 연동합니다.",
            font=("맑은 고딕", 9),
            foreground="#666"
        )
        desc_label.pack(pady=(0, 15))

        # 1. 패키지 선택
        frame_input = ttk.LabelFrame(main_frame, text="1. 완료된 패키지 선택", padding=15)
        frame_input.pack(fill='x', pady=(0, 10))

        rf1 = ttk.Frame(frame_input)
        rf1.pack(fill='x', pady=5)
        ttk.Label(rf1, text="패키지:", width=12).pack(side='left')
        ttk.Entry(rf1, textvariable=self.package_path, width=45).pack(side='left', fill='x', expand=True, padx=5)
        ttk.Button(rf1, text="폴더 선택", command=self._select_package_dir).pack(side='right', padx=2)
        ttk.Button(rf1, text="ZIP 선택", command=self._select_package_zip).pack(side='right', padx=2)

        # 2. 출력 위치 설정
        frame_output = ttk.LabelFrame(main_frame, text="2. 결과 저장 위치", padding=15)
        frame_output.pack(fill='x', pady=(0, 10))

        rf2 = ttk.Frame(frame_output)
        rf2.pack(fill='x', pady=5)
        ttk.Label(rf2, text="출력 폴더:", width=12).pack(side='left')
        ttk.Entry(rf2, textvariable=self.output_dir_path, width=45).pack(side='left', fill='x', expand=True, padx=5)
        ttk.Button(rf2, text="폴더 선택", command=self._select_output_dir).pack(side='right')

        # 옵션
        opt_frame = ttk.Frame(frame_output)
        opt_frame.pack(fill='x', pady=(10, 0))
        ttk.Checkbutton(
            opt_frame,
            text="최종 이미지(IMG_S5_*) 복사 (cloudflare_upload_gui용)",
            variable=self.copy_final_images
        ).pack(side='left')

        # 3. 패키지 정보
        frame_info = ttk.LabelFrame(main_frame, text="3. 패키지 정보", padding=15)
        frame_info.pack(fill='x', pady=(0, 10))

        self.info_text = tk.Text(frame_info, height=8, state='disabled', font=("Consolas", 9))
        self.info_text.pack(fill='x')

        # 4. 액션 버튼
        btn_frame = ttk.Frame(main_frame)
        btn_frame.pack(fill='x', pady=(0, 10))

        self.btn_analyze = ttk.Button(btn_frame, text="패키지 분석", style="Action.TButton", command=self._analyze_package)
        self.btn_analyze.pack(side='left', fill='x', expand=True, padx=(0, 5))

        self.btn_import = ttk.Button(btn_frame, text="Import 실행", style="Action.TButton", command=self._import_package)
        self.btn_import.pack(side='left', fill='x', expand=True, padx=5)

        self.btn_open_upload = ttk.Button(btn_frame, text="cloudflare_upload_gui 열기", style="Action.TButton", command=self._open_upload_gui)
        self.btn_open_upload.pack(side='right', fill='x', expand=True, padx=(5, 0))

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

        self.log_widget = ScrolledText(frame_log, height=8, state='disabled', font=("Consolas", 9))
        self.log_widget.pack(fill='both', expand=True)

    def _log(self, msg: str):
        """로그 출력"""
        ts = datetime.now().strftime("%H:%M:%S")
        self.log_widget.config(state='normal')
        self.log_widget.insert('end', f"[{ts}] {msg}\n")
        self.log_widget.see('end')
        self.log_widget.config(state='disabled')

    def _update_info(self, text: str):
        """정보 텍스트 업데이트"""
        self.info_text.config(state='normal')
        self.info_text.delete('1.0', 'end')
        self.info_text.insert('end', text)
        self.info_text.config(state='disabled')

    def _select_package_dir(self):
        """패키지 폴더 선택"""
        path = filedialog.askdirectory(title="완료된 패키지 폴더 선택")
        if path:
            self.package_path.set(path)
            self._log(f"패키지 폴더 선택됨: {path}")
            self._analyze_package()

    def _select_package_zip(self):
        """패키지 ZIP 파일 선택"""
        path = filedialog.askopenfilename(
            title="완료된 패키지 ZIP 파일 선택",
            filetypes=[("ZIP files", "*.zip"), ("All files", "*.*")]
        )
        if path:
            self.package_path.set(path)
            self._log(f"패키지 ZIP 선택됨: {path}")
            self._analyze_package()

    def _select_output_dir(self):
        """출력 폴더 선택"""
        path = filedialog.askdirectory(title="결과 저장 폴더 선택")
        if path:
            self.output_dir_path.set(path)
            self._log(f"출력 폴더 선택됨: {path}")

    def _analyze_package(self):
        """패키지 분석"""
        pkg_path = self.package_path.get()
        if not pkg_path:
            self._update_info("패키지를 먼저 선택해주세요.")
            return

        try:
            # ZIP 파일인 경우 임시 디렉토리에 압축 해제
            if pkg_path.lower().endswith('.zip'):
                temp_dir = os.path.join(os.path.dirname(pkg_path), "_temp_import_" + datetime.now().strftime("%Y%m%d_%H%M%S"))
                os.makedirs(temp_dir, exist_ok=True)

                self._log(f"ZIP 파일 압축 해제 중: {os.path.basename(pkg_path)}")
                with zipfile.ZipFile(pkg_path, 'r') as zf:
                    zf.extractall(temp_dir)

                # 압축 해제된 폴더에서 실제 패키지 폴더 찾기
                items = os.listdir(temp_dir)
                if len(items) == 1 and os.path.isdir(os.path.join(temp_dir, items[0])):
                    self.package_dir = os.path.join(temp_dir, items[0])
                else:
                    self.package_dir = temp_dir

                self._log(f"압축 해제 완료: {self.package_dir}")
            else:
                self.package_dir = pkg_path

            # manifest.json 확인
            manifest_path = os.path.join(self.package_dir, "manifest.json")
            if not os.path.exists(manifest_path):
                self._update_info("오류: manifest.json 파일이 없습니다.\n이 폴더는 유효한 패키지가 아닙니다.")
                self._log("[오류] manifest.json 파일을 찾을 수 없습니다.")
                return

            with open(manifest_path, 'r', encoding='utf-8') as f:
                self.manifest = json.load(f)

            # 엑셀 파일 확인
            excel_file = self.manifest.get('excel_file', '')
            excel_path = os.path.join(self.package_dir, excel_file)

            if not os.path.exists(excel_path):
                self._update_info(f"오류: 엑셀 파일을 찾을 수 없습니다: {excel_file}")
                return

            # 엑셀 파일 분석
            df = pd.read_excel(excel_path)

            # 최종 이미지 컬럼 확인
            nukki_col = "IMG_S5_누끼_최종경로"
            mix_col = "IMG_S5_믹스_최종경로"

            final_nukki_count = 0
            final_mix_count = 0

            if nukki_col in df.columns:
                final_nukki_count = df[nukki_col].notna().sum() - (df[nukki_col] == "").sum()

            if mix_col in df.columns:
                final_mix_count = df[mix_col].notna().sum() - (df[mix_col] == "").sum()

            # 정보 표시
            info_text = f"""패키지 정보
============

작업명: {self.manifest.get('job_name', '-')}
생성일: {self.manifest.get('created_at', '-')}
원본 경로: {self.manifest.get('original_base_dir', '-')}

엑셀 파일: {excel_file}
총 행 수: {len(df)}행

이미지 통계 (Export 시):
- 총 이미지: {self.manifest.get('total_images', 0)}개
- 복사된 이미지: {self.manifest.get('copied_images', 0)}개

최종 결과 (Stage5 완료 후):
- 누끼 최종경로: {final_nukki_count}개
- 믹스 최종경로: {final_mix_count}개

상태: {'Stage5 작업 완료' if final_nukki_count > 0 else 'Stage5 작업 미완료'}"""

            self._update_info(info_text)
            self._log("패키지 분석 완료")

            # 출력 폴더 자동 설정
            if not self.output_dir_path.get():
                original_base = self.manifest.get('original_base_dir', '')
                if original_base and os.path.exists(original_base):
                    self.output_dir_path.set(original_base)
                    self._log(f"출력 폴더 자동 설정 (원본 경로): {original_base}")
                else:
                    default_output = os.path.join(os.path.dirname(pkg_path), f"imported_{self.manifest.get('job_name', 'package')}")
                    self.output_dir_path.set(default_output)
                    self._log(f"출력 폴더 자동 설정: {default_output}")

        except Exception as e:
            self._update_info(f"패키지 분석 실패: {e}")
            self._log(f"[오류] 패키지 분석 실패: {e}")

    def _import_package(self):
        """패키지 Import 실행"""
        if self.is_running:
            return

        if not self.package_dir or not self.manifest:
            messagebox.showwarning("오류", "먼저 패키지를 분석해주세요.")
            return

        if not self.output_dir_path.get():
            messagebox.showwarning("오류", "출력 폴더를 선택해주세요.")
            return

        self.is_running = True
        self.btn_import.config(state='disabled')

        thread = threading.Thread(target=self._import_package_thread)
        thread.daemon = True
        thread.start()

    def _import_package_thread(self):
        """Import 실행 스레드"""
        try:
            output_dir = self.output_dir_path.get()
            os.makedirs(output_dir, exist_ok=True)

            excel_file = self.manifest.get('excel_file', '')
            excel_path = os.path.join(self.package_dir, excel_file)

            df = pd.read_excel(excel_path)
            self.after(0, lambda: self._log(f"엑셀 파일 로드: {excel_file}"))

            # 상대 경로를 절대 경로로 변환
            path_cols = ["썸네일경로", "IMG_S1_누끼", "IMG_S4_mix_생성경로", "IMG_S5_누끼_최종경로", "IMG_S5_믹스_최종경로"]

            total_paths = 0
            converted_paths = 0

            for col in path_cols:
                if col not in df.columns:
                    continue

                for idx, val in df[col].items():
                    path_str = str(val).strip()
                    if not path_str or path_str == "nan" or path_str == "":
                        continue

                    total_paths += 1

                    # 상대 경로인 경우 절대 경로로 변환
                    if not os.path.isabs(path_str):
                        abs_path = os.path.normpath(os.path.join(self.package_dir, path_str))
                        if os.path.exists(abs_path):
                            df.at[idx, col] = abs_path
                            converted_paths += 1
                        else:
                            self.after(0, lambda p=path_str: self._log(f"[경고] 파일 없음: {p}"))

            self.after(0, lambda: self._log(f"경로 변환 완료: {converted_paths}/{total_paths}개"))

            # 최종 이미지 복사 (cloudflare_upload_gui용)
            if self.copy_final_images.get():
                self.after(0, lambda: self.progress_label.config(text="최종 이미지 복사 중..."))

                final_images_dir = os.path.join(output_dir, f"최종이미지_{self.manifest.get('job_name', 'imported')}")
                os.makedirs(final_images_dir, exist_ok=True)

                nukki_col = "IMG_S5_누끼_최종경로"
                mix_col = "IMG_S5_믹스_최종경로"

                copied_count = 0
                for col in [nukki_col, mix_col]:
                    if col not in df.columns:
                        continue

                    for idx, val in df[col].items():
                        path_str = str(val).strip()
                        if not path_str or path_str == "nan" or path_str == "":
                            continue

                        if os.path.exists(path_str):
                            dest_path = os.path.join(final_images_dir, os.path.basename(path_str))
                            try:
                                shutil.copy2(path_str, dest_path)
                                df.at[idx, col] = dest_path
                                copied_count += 1
                            except Exception as e:
                                self.after(0, lambda p=path_str, e=e: self._log(f"[오류] 복사 실패: {p}"))

                self.after(0, lambda: self._log(f"최종 이미지 복사 완료: {copied_count}개 -> {final_images_dir}"))

            # 변환된 엑셀 파일 저장
            output_excel = os.path.join(output_dir, excel_file)
            df.to_excel(output_excel, index=False)
            self.after(0, lambda: self._log(f"엑셀 파일 저장: {output_excel}"))

            # import_info.json 저장 (cloudflare_upload_gui 연동용)
            import_info = {
                "imported_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "original_package": self.package_path.get(),
                "manifest": self.manifest,
                "output_excel": output_excel,
                "final_images_dir": final_images_dir if self.copy_final_images.get() else None,
                "total_rows": len(df),
                "converted_paths": converted_paths
            }

            import_info_path = os.path.join(output_dir, "import_info.json")
            with open(import_info_path, 'w', encoding='utf-8') as f:
                json.dump(import_info, f, ensure_ascii=False, indent=2)

            # 완료
            self.after(0, lambda: self.progress_label.config(text="Import 완료!"))
            self.after(0, lambda: self.progress_bar.config(value=100))

            result_msg = f"""Import가 완료되었습니다.

작업명: {self.manifest.get('job_name', '-')}
출력 폴더: {output_dir}
엑셀 파일: {excel_file}

다음 단계:
1. cloudflare_upload_gui.py 실행
2. '{output_excel}' 파일 선택
3. 업로드 진행"""

            self.after(0, lambda: messagebox.showinfo("완료", result_msg))

            # 출력 폴더 열기
            self.after(0, lambda: os.startfile(output_dir) if os.name == 'nt' else None)

        except Exception as e:
            self.after(0, lambda: self._log(f"[오류] Import 실패: {e}"))
            self.after(0, lambda: messagebox.showerror("오류", f"Import 실패:\n{e}"))
        finally:
            self.is_running = False
            self.after(0, lambda: self.btn_import.config(state='normal'))

    def _open_upload_gui(self):
        """cloudflare_upload_gui 실행"""
        try:
            # 현재 디렉토리 기준으로 cloudflare_upload_gui.py 찾기
            current_dir = os.path.dirname(os.path.abspath(__file__))
            upload_gui_paths = [
                os.path.join(current_dir, "cloudflare_upload_gui.py"),
                os.path.join(current_dir, "..", "cloudflare_upload_gui.py"),
                os.path.join(current_dir, "..", "IMG_upload", "cloudflare_upload_gui.py"),
            ]

            upload_gui_path = None
            for path in upload_gui_paths:
                if os.path.exists(path):
                    upload_gui_path = os.path.abspath(path)
                    break

            if upload_gui_path:
                import subprocess
                import sys
                subprocess.Popen([sys.executable, upload_gui_path], cwd=os.path.dirname(upload_gui_path))
                self._log(f"cloudflare_upload_gui 실행: {upload_gui_path}")
            else:
                messagebox.showwarning(
                    "파일 없음",
                    "cloudflare_upload_gui.py 파일을 찾을 수 없습니다.\n수동으로 실행해주세요."
                )

        except Exception as e:
            self._log(f"[오류] cloudflare_upload_gui 실행 실패: {e}")
            messagebox.showerror("오류", f"cloudflare_upload_gui 실행 실패:\n{e}")


def main():
    app = Stage5ImportToolGUI()
    app.mainloop()


if __name__ == "__main__":
    main()
