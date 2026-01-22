import os
import threading
import traceback
import subprocess
from datetime import datetime

import pandas as pd
import tkinter as tk
from tkinter import filedialog, messagebox
from tkinter.scrolledtext import ScrolledText
from tkinter import ttk  # ttk 위젯 / 진행바

# 1단계: 상세이미지 추출 함수 (기존 g.py 재사용)
from Product_detaildescription import process_excel as run_step1_excel

# 2단계: Stage2 프롬프트 생성 함수 (기존 stage2_prompt_builder.py 재사용)
from stage2_prompt_builder import process_excel_for_stage2 as run_step2_excel

# =========================
# 서울 시간 헬퍼
# =========================
try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None


def get_seoul_now() -> datetime:
    """가능하면 Asia/Seoul 기준 현재 시각, 실패하면 로컬 현재 시각."""
    if ZoneInfo is not None:
        try:
            return datetime.now(ZoneInfo("Asia/Seoul"))
        except Exception:
            pass
    return datetime.now()


class Stage2PipelineApp:
    """
    1단계(상세이미지 추출) + 2단계(Stage2 프롬프트 생성)를
    - 개별로도 실행할 수 있고
    - 한 번에(전체 파이프라인)도 실행할 수 있는 통합 GUI
    """

    def __init__(self, root: tk.Tk):
        self.root = root
        root.title("Stage2 상품명 파이프라인 (1단계 + 2단계 통합)")
        root.geometry("950x700")

        self.current_thread: threading.Thread | None = None
        self.pipeline_start_time: datetime | None = None
        self.pipeline_end_time: datetime | None = None

        # ---------- 사용자 옵션 ----------
        # 1단계 완료 후 *_with_detail_images 엑셀 자동 열기 여부
        self.auto_open_detail = tk.BooleanVar(value=False)
        # 2단계 / 전체 완료 후 *_stage2_prompts 엑셀 자동 열기 여부
        self.auto_open_stage2 = tk.BooleanVar(value=True)

        # ================= 스타일 =================
        style = ttk.Style(root)
        try:
            style.theme_use("clam")
        except Exception:
            pass

        style.configure("Title.TLabel", font=("맑은 고딕", 14, "bold"))
        style.configure("SubTitle.TLabel", font=("맑은 고딕", 10), foreground="#555555")
        style.configure("SmallGray.TLabel", font=("맑은 고딕", 9), foreground="#777777")

        # 초록 진행바 스타일
        style.configure(
            "Green.Horizontal.TProgressbar",
            troughcolor="#f0f0f0",
            background="#4caf50",
        )

        # ================= 상단 타이틀 =================
        header = ttk.Label(
            root,
            text="Stage2 상품명 파이프라인",
            style="Title.TLabel",
        )
        header.pack(fill="x", padx=10, pady=(8, 0))

        sub_header = ttk.Label(
            root,
            text="1단계: 상세이미지 추출  →  2단계: LLM 입력용 Stage2 프롬프트 생성",
            style="SubTitle.TLabel",
        )
        sub_header.pack(fill="x", padx=10, pady=(0, 4))

        # ================= 설명 =================
        desc = (
            "▶ 입력 파일 (권장)\n"
            "   - Stage1 맵핑 및 정제 완료 엑셀: *_stage1_mapping_stage1_completed.xlsx\n"
            "   - 필수 컬럼: 상품코드 / 상세설명 또는 본문상세설명\n\n"
            "▶ 출력 파일\n"
            "   - 1단계: [원본파일명]_with_detail_images.xlsx\n"
            "            → 상세이미지_1 ~ N 컬럼에 실제 저장된 상세이미지 경로 매핑\n"
            "   - 2단계: [원본파일명]_stage2_prompts.xlsx\n"
            "            → ST2_프롬프트, ST2_JSON 컬럼 포함 (Stage2 LLM 입력용)\n\n"
            "▶ 사용 순서 (권장)\n"
            "   1) 1단계: HTML 상세설명에서 상세이미지를 로컬로 추출 + 엑셀에 경로 매핑\n"
            "   2) 2단계: 1단계 결과 엑셀(*_with_detail_images.xlsx)을 이용해\n"
            "             Stage2 LLM 입력용 프롬프트(ST2_프롬프트) 생성\n\n"
            "※ '전체 2단계 한 번에 실행' 버튼을 사용하면\n"
            "   1단계 + 2단계를 순차적으로 자동 수행합니다."
        )
        lbl_desc = ttk.Label(
            root,
            text=desc,
            justify="left",
            anchor="w",
            style="SmallGray.TLabel",
        )
        lbl_desc.pack(fill=tk.X, padx=10, pady=(4, 4))

        # ================= 통합 실행 버튼 =================
        frame_all = ttk.LabelFrame(root, text="통합 실행 (추천)", padding=(8, 6))
        frame_all.pack(fill=tk.X, padx=10, pady=(4, 4))

        self.btn_run_all = ttk.Button(
            frame_all,
            text="✅ [전체] 1단계 + 2단계 한 번에 실행",
            command=self.on_click_run_all,
            width=40,
        )
        self.btn_run_all.pack(padx=5, pady=2)

        # ================= 개별 실행 버튼 =================
        frame_each = ttk.LabelFrame(root, text="개별 실행 (필요 시만 사용)", padding=(8, 6))
        frame_each.pack(fill=tk.X, padx=10, pady=(4, 4))

        self.btn_run_step1 = ttk.Button(
            frame_each,
            text="① 1단계만 실행 (상세이미지 추출)",
            command=self.on_click_run_step1,
            width=35,
        )
        self.btn_run_step1.pack(side=tk.LEFT, padx=5, pady=2)

        self.btn_run_step2 = ttk.Button(
            frame_each,
            text="② 2단계만 실행 (Stage2 프롬프트 생성)",
            command=self.on_click_run_step2,
            width=40,
        )
        self.btn_run_step2.pack(side=tk.LEFT, padx=5, pady=2)

        # ================= 옵션 체크박스 =================
        frame_opts = ttk.LabelFrame(root, text="추가 옵션", padding=(8, 6))
        frame_opts.pack(fill=tk.X, padx=10, pady=(0, 4))

        chk_detail = ttk.Checkbutton(
            frame_opts,
            text="1단계 완료 후 상세이미지 포함 엑셀 자동 열기",
            variable=self.auto_open_detail,
        )
        chk_detail.pack(side=tk.LEFT, padx=5)

        chk_stage2 = ttk.Checkbutton(
            frame_opts,
            text="2단계/전체 완료 후 Stage2 프롬프트 엑셀 자동 열기",
            variable=self.auto_open_stage2,
        )
        chk_stage2.pack(side=tk.LEFT, padx=5)

        # ================= 진행률 / 시간 표시 =================
        frame_status = ttk.Frame(root)
        frame_status.pack(fill=tk.X, padx=10, pady=(4, 4))

        self.progress_var = tk.DoubleVar(value=0.0)
        self.progress_bar = ttk.Progressbar(
            frame_status,
            maximum=100,
            variable=self.progress_var,
            mode="determinate",
            style="Green.Horizontal.TProgressbar",
        )
        self.progress_bar.pack(fill=tk.X, padx=5, pady=(0, 3))

        self.lbl_progress = ttk.Label(
            frame_status, text="진행률: -", anchor="w", style="SmallGray.TLabel"
        )
        self.lbl_progress.pack(fill=tk.X, padx=5)

        self.lbl_time = ttk.Label(
            frame_status, text="시작: - / 종료: -", anchor="w", style="SmallGray.TLabel"
        )
        self.lbl_time.pack(fill=tk.X, padx=5)

        # ================= 로그 창 =================
        self.log_box = ScrolledText(root, wrap=tk.WORD, height=22)
        self.log_box.pack(fill=tk.BOTH, expand=True, padx=10, pady=(4, 4))

        # ================= 로그 저장 버튼 =================
        bottom_frame = ttk.Frame(root)
        bottom_frame.pack(fill=tk.X, padx=10, pady=(0, 10))

        self.btn_save_log = ttk.Button(
            bottom_frame,
            text="로그 저장...",
            command=self.on_save_log,
            width=14,
        )
        self.btn_save_log.pack(side=tk.RIGHT, padx=2)

    # ---------- 공통 유틸 ----------

    def _append_log(self, msg: str) -> None:
        self.log_box.insert(tk.END, msg + "\n")
        self.log_box.see(tk.END)

    def log(self, msg: str) -> None:
        self.root.after(0, self._append_log, msg)

    def set_progress(self, percent: float, text: str) -> None:
        """
        진행바(0~100) + 진행률 라벨을 동시에 업데이트.
        """

        def _update():
            p = max(0.0, min(100.0, float(percent)))
            self.progress_var.set(p)
            self.lbl_progress.config(text=f"진행률: {text}")

        self.root.after(0, _update)

    def set_time_info(self, start: datetime | None = None, end: datetime | None = None) -> None:
        if start is not None:
            self.pipeline_start_time = start
        if end is not None:
            self.pipeline_end_time = end

        start_str = (
            self.pipeline_start_time.strftime("%Y-%m-%d %H:%M:%S")
            if self.pipeline_start_time
            else "-"
        )
        end_str = (
            self.pipeline_end_time.strftime("%Y-%m-%d %H:%M:%S")
            if self.pipeline_end_time
            else "-"
        )
        text = f"시작: {start_str} / 종료: {end_str}"
        self.root.after(0, lambda: self.lbl_time.config(text=text))

    def disable_buttons(self) -> None:
        for btn in (self.btn_run_all, self.btn_run_step1, self.btn_run_step2):
            btn.config(state=tk.DISABLED)

    def enable_buttons(self) -> None:
        for btn in (self.btn_run_all, self.btn_run_step1, self.btn_run_step2):
            btn.config(state=tk.NORMAL)

    # ---- 파일 열기 헬퍼 (완료 후 '예' 선택 시) ----
    def open_path(self, path: str) -> None:
        if not path or not os.path.exists(path):
            messagebox.showwarning("경고", f"파일을 찾을 수 없습니다:\n{path}")
            return

        try:
            if os.name == "nt":
                os.startfile(path)  # type: ignore[attr-defined]
            elif os.name == "posix":
                if "darwin" in os.sys.platform:
                    subprocess.run(["open", path], check=False)
                else:
                    subprocess.run(["xdg-open", path], check=False)
            else:
                messagebox.showinfo("알림", f"이 OS에서는 자동 열기가 지원되지 않을 수 있습니다.\n\n{path}")
        except Exception as e:
            messagebox.showerror("오류", f"파일을 여는 중 오류가 발생했습니다.\n\n{e}")

    def _show_complete_message(
        self,
        title: str,
        base_message: str,
        out_path: str,
        auto_open_var: tk.BooleanVar | None = None,
    ) -> None:
        """
        완료 메시지를 띄우고, 필요 시 파일 열지 여부를 묻거나 자동으로 연다.
        auto_open_var가 주어지고 True이면 질문 없이 바로 파일을 연다.
        """

        def _inner():
            if out_path and os.path.exists(out_path):
                file_name = os.path.basename(out_path)
                if auto_open_var is not None and auto_open_var.get():
                    # 자동 열기 모드
                    msg = (
                        base_message
                        + f"\n\n출력 파일: {file_name}\n\n"
                        + "설정에 따라 파일을 자동으로 엽니다."
                    )
                    messagebox.showinfo(title, msg)
                    self.open_path(out_path)
                else:
                    # 묻고 열기
                    msg = (
                        base_message
                        + f"\n\n'{file_name}' 파일을 지금 여시겠습니까?"
                    )
                    if messagebox.askyesno(title, msg):
                        self.open_path(out_path)
            else:
                messagebox.showinfo(title, base_message)

        self.root.after(0, _inner)

    # ---------- 로그 저장 ----------

    def on_save_log(self) -> None:
        log_text = self.log_box.get("1.0", tk.END).strip()
        if not log_text:
            messagebox.showinfo("로그 저장", "저장할 로그가 없습니다.")
            return

        file_path = filedialog.asksaveasfilename(
            title="로그를 저장할 위치를 선택하세요",
            defaultextension=".txt",
            filetypes=[("Text files", "*.txt"), ("All files", "*.*")],
        )
        if not file_path:
            return

        try:
            with open(file_path, "w", encoding="utf-8") as f:
                f.write(log_text)
            messagebox.showinfo("로그 저장", f"로그가 저장되었습니다.\n\n{file_path}")
        except Exception as e:
            messagebox.showerror("오류", f"로그 저장 중 오류가 발생했습니다.\n\n{e}")

    # ---------- 버튼 핸들러 ----------

    def on_click_run_all(self) -> None:
        """
        1단계 + 2단계를 순차적으로 실행.
        입력: *_stage1_mapping_stage1_completed.xlsx
        """
        filetypes = [
            ("Excel files", "*.xlsx *.xls"),
            ("All files", "*.*"),
        ]
        excel_path = filedialog.askopenfilename(
            title="1단계 입력용 엑셀(*_stage1_mapping_stage1_completed.xlsx)을 선택하세요",
            filetypes=filetypes,
        )
        if not excel_path:
            return

        self.disable_buttons()
        self.set_progress(0.0, "대기 중...")
        self.set_time_info(start=None, end=None)

        self.current_thread = threading.Thread(
            target=self.worker_run_all,
            args=(excel_path,),
            daemon=True,
        )
        self.current_thread.start()

    def on_click_run_step1(self) -> None:
        filetypes = [
            ("Excel files", "*.xlsx *.xls"),
            ("All files", "*.*"),
        ]
        excel_path = filedialog.askopenfilename(
            title="1단계만 실행할 엑셀(*_stage1_mapping_stage1_completed.xlsx)을 선택하세요",
            filetypes=filetypes,
        )
        if not excel_path:
            return

        self.disable_buttons()
        self.set_progress(0.0, "1단계만 실행 준비 중...")
        self.set_time_info(start=None, end=None)

        self.current_thread = threading.Thread(
            target=self.worker_run_step1,
            args=(excel_path,),
            daemon=True,
        )
        self.current_thread.start()

    def on_click_run_step2(self) -> None:
        filetypes = [
            ("Excel files", "*with_detail_images*.xlsx"),
            ("Excel files", "*.xlsx *.xls"),
            ("All files", "*.*"),
        ]
        excel_path = filedialog.askopenfilename(
            title="2단계만 실행할 엑셀(*_with_detail_images.xlsx)을 선택하세요",
            filetypes=filetypes,
        )
        if not excel_path:
            return

        self.disable_buttons()
        self.set_progress(0.0, "2단계만 실행 준비 중...")
        self.set_time_info(start=None, end=None)

        self.current_thread = threading.Thread(
            target=self.worker_run_step2,
            args=(excel_path,),
            daemon=True,
        )
        self.current_thread.start()

    # ---------- 실제 작업 쓰레드 ----------

    def worker_run_step1(self, excel_path: str) -> None:
        start = get_seoul_now()
        self.set_time_info(start=start, end=None)
        try:
            self.log(f"[RUN] 1단계 시작: {excel_path}")
            self.set_progress(5.0, "1단계 실행 중...")

            # 대상 행 개수 (가능하면)
            try:
                df = pd.read_excel(excel_path)
                total = len(df.index)
                self.log(f"[INFO] 1단계 대상 상품 수: {total}개")
            except Exception as e:
                self.log(f"[WARN] 1단계 대상 상품 수 계산 실패: {e}")

            # 1단계 실행
            run_step1_excel(excel_path, log_func=self.log)

            base_dir = os.path.dirname(excel_path)
            base_name = os.path.splitext(os.path.basename(excel_path))[0]
            out1 = os.path.join(base_dir, f"{base_name}_with_detail_images.xlsx")

            self.log(f"[DONE] 1단계 완료. 출력: {out1}")
            self.set_progress(100.0, "1단계 완료 (1/1, 100%)")

            end = get_seoul_now()
            self.set_time_info(start=start, end=end)

            self._show_complete_message(
                "완료",
                f"1단계 작업이 완료되었습니다.\n\n출력 파일:\n{out1}",
                out1,
                auto_open_var=self.auto_open_detail,
            )
        except Exception as e:
            self.log("[FATAL] 1단계 수행 중 오류 발생:")
            self.log(str(e))
            self.log(traceback.format_exc())
            end = get_seoul_now()
            self.set_time_info(start=start, end=end)

            self.set_progress(self.progress_var.get(), "에러로 중단됨 (1단계)")
            self.root.after(
                0,
                lambda e=e: messagebox.showerror(
                    "오류",
                    f"1단계 수행 중 오류가 발생했습니다.\n\n{e}",
                ),
            )
        finally:
            self.root.after(0, self.enable_buttons)

    def worker_run_step2(self, excel_path: str) -> None:
        start = get_seoul_now()
        self.set_time_info(start=start, end=None)
        try:
            self.log(f"[RUN] 2단계 시작: {excel_path}")
            self.set_progress(5.0, "2단계 실행 중...")

            # 대상 행 개수 (가능하면)
            try:
                df = pd.read_excel(excel_path)
                total = len(df.index)
                self.log(f"[INFO] 2단계 대상 상품 수: {total}개")
            except Exception as e:
                self.log(f"[WARN] 2단계 대상 상품 수 계산 실패: {e}")

            out2 = run_step2_excel(excel_path, log_func=self.log)

            skipped_info = ""
            if isinstance(out2, tuple):
                out2_path, skipped = out2
                if isinstance(skipped, int) and skipped > 0:
                    skipped_info = f"\n(참고: ST1_결과상품명이 비어 있어 건너뛴 상품 {skipped}개)"
            else:
                out2_path, skipped = out2, None

            self.log(f"[DONE] 2단계 완료. 출력: {out2_path}{skipped_info}")
            self.set_progress(100.0, "2단계 완료 (1/1, 100%)")

            end = get_seoul_now()
            self.set_time_info(start=start, end=end)

            self._show_complete_message(
                "완료",
                f"2단계 작업이 완료되었습니다.\n\n출력 파일:\n{out2_path}{skipped_info}",
                out2_path,
                auto_open_var=self.auto_open_stage2,
            )
        except Exception as e:
            self.log("[FATAL] 2단계 수행 중 오류 발생:")
            self.log(str(e))
            self.log(traceback.format_exc())
            end = get_seoul_now()
            self.set_time_info(start=start, end=end)

            self.set_progress(self.progress_var.get(), "에러로 중단됨 (2단계)")
            self.root.after(
                0,
                lambda e=e: messagebox.showerror(
                    "오류",
                    f"2단계 수행 중 오류가 발생했습니다.\n\n{e}",
                ),
            )
        finally:
            self.root.after(0, self.enable_buttons)

    def worker_run_all(self, excel_path: str) -> None:
        pipeline_start = get_seoul_now()
        self.set_time_info(start=pipeline_start, end=None)
        try:
            # ===== 1단계 =====
            self.set_progress(0.0, "1단계 진행 중 (0/2, 0%)")
            self.log(f"[RUN] [전체] 1단계 시작: {excel_path}")

            # 대상 행 개수 (가능하면)
            try:
                df1 = pd.read_excel(excel_path)
                total1 = len(df1.index)
                self.log(f"[INFO] 1단계 대상 상품 수: {total1}개")
            except Exception as e:
                self.log(f"[WARN] 1단계 대상 상품 수 계산 실패: {e}")

            run_step1_excel(excel_path, log_func=self.log)

            base_dir = os.path.dirname(excel_path)
            base_name = os.path.splitext(os.path.basename(excel_path))[0]
            out1 = os.path.join(base_dir, f"{base_name}_with_detail_images.xlsx")

            self.log(f"[DONE] 1단계 완료. 출력: {out1}")
            self.set_progress(50.0, "1단계 완료 (1/2, 50%)")

            # ===== 2단계 =====
            self.log(f"[RUN] [전체] 2단계 시작: {out1}")
            self.set_progress(50.0, "2단계 진행 중 (1/2, 50%)")

            try:
                df2 = pd.read_excel(out1)
                total2 = len(df2.index)
                self.log(f"[INFO] 2단계 대상 상품 수: {total2}개")
            except Exception as e:
                self.log(f"[WARN] 2단계 대상 상품 수 계산 실패: {e}")

            out2 = run_step2_excel(out1, log_func=self.log)

            skipped_info = ""
            if isinstance(out2, tuple):
                out2_path, skipped = out2
                if isinstance(skipped, int) and skipped > 0:
                    skipped_info = f"\n(참고: ST1_결과상품명이 비어 있어 건너뛴 상품 {skipped}개)"
            else:
                out2_path, skipped = out2, None

            self.log(f"[DONE] 2단계 완료. 출력: {out2_path}{skipped_info}")
            self.set_progress(100.0, "전체 파이프라인 완료 (2/2, 100%)")

            pipeline_end = get_seoul_now()
            self.set_time_info(start=pipeline_start, end=pipeline_end)

            self._show_complete_message(
                "완료",
                (
                    "전체 파이프라인 완료!\n\n"
                    f"1단계 출력: {out1}\n"
                    f"2단계 출력: {out2_path}{skipped_info}"
                ),
                out2_path,
                auto_open_var=self.auto_open_stage2,
            )
        except Exception as e:
            self.log("[FATAL] 전체 파이프라인 수행 중 오류 발생:")
            self.log(str(e))
            self.log(traceback.format_exc())
            pipeline_end = get_seoul_now()
            self.set_time_info(start=pipeline_start, end=pipeline_end)

            self.set_progress(self.progress_var.get(), "에러로 중단됨 (전체)")
            self.root.after(
                0,
                lambda e=e: messagebox.showerror(
                    "오류",
                    f"전체 파이프라인 수행 중 오류가 발생했습니다.\n\n{e}",
                ),
            )
        finally:
            self.root.after(0, self.enable_buttons)


def main() -> None:
    root = tk.Tk()
    app = Stage2PipelineApp(root)
    root.mainloop()


if __name__ == "__main__":
    main()
