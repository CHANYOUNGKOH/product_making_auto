"""
stage3_LLM_gui.py

- Stage 3: LLM 기반 최종 상품명 생성 GUI (Final Complete Version)
- 원본 로직(백업 저장, 응답 파싱) 100% 유지
- Stage 4-2 스타일의 디자인(대시보드, 스마트 이어하기) 적용
"""

import os
import time
import threading
import subprocess
from datetime import datetime
import pytz

import pandas as pd
import tkinter as tk
from tkinter import ttk, filedialog, messagebox
from tkinter.scrolledtext import ScrolledText

from openai import OpenAI
from stage3_core import (
    safe_str,
    Stage3Settings,
    Stage3Request,
    build_stage3_request_from_row,
    MODEL_PRICING_USD_PER_MTOK,
    load_api_key_from_file,
    save_api_key_to_file,
    API_KEY_FILE,
)
from stage3_run_history import append_run_history

def get_seoul_now() -> datetime:
    try:
        return datetime.now(pytz.timezone("Asia/Seoul"))
    except:
        return datetime.now()

class Stage3LLMGUI(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Stage 3: Product Naming Generator (Complete Pro)")
        self.geometry("1000x850")
        
        self._configure_styles()

        # --- 변수 초기화 ---
        self.api_key_var = tk.StringVar()
        self.input_file_path = tk.StringVar()
        self.output_file_path = ""
        
        # API 옵션
        self.model_var = tk.StringVar(value="gpt-5-mini")
        self.effort_var = tk.StringVar(value="medium") # none/low/medium/high

        # Stage 3 전용 옵션
        self.market_var = tk.StringVar(value="네이버 50자")
        self.num_cand_var = tk.IntVar(value=10)
        self.naming_strategy_var = tk.StringVar(value="통합형")

        # 상태 및 통계 변수
        self.is_running = False
        self.stop_requested = False
        
        self.stat_progress = tk.StringVar(value="0.0%")
        self.stat_count = tk.StringVar(value="0 / 0")
        self.stat_success = tk.StringVar(value="0")
        self.stat_fail = tk.StringVar(value="0")
        self.stat_cost = tk.StringVar(value="$0.0000")
        self.stat_time = tk.StringVar(value="00:00:00")
        self.status_msg = tk.StringVar(value="파일을 선택하고 작업을 시작하세요.")

        self._init_ui()
        self._load_key()

    def _configure_styles(self):
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
        style.configure("Stat.TLabel", font=("맑은 고딕", 12, "bold"), foreground="#0052cc")
        style.configure("Cost.TLabel", font=("맑은 고딕", 12, "bold"), foreground="#d32f2f")
        
        style.configure("Action.TButton", font=("맑은 고딕", 11, "bold"), padding=5)
        style.configure("Stop.TButton", font=("맑은 고딕", 11, "bold"), foreground="red", padding=5)

    def _init_ui(self):
        main_frame = ttk.Frame(self, padding=20)
        main_frame.pack(fill='both', expand=True)

        # 1. API & Model 설정
        frame_top = ttk.LabelFrame(main_frame, text="API 및 모델 설정 (Configuration)", padding=15)
        frame_top.pack(fill='x', pady=(0, 10))

        # Row 1: API Key
        r1 = ttk.Frame(frame_top)
        r1.pack(fill='x', pady=2)
        ttk.Label(r1, text="OpenAI Key:", width=12).pack(side='left')
        entry_key = ttk.Entry(r1, textvariable=self.api_key_var, show="*", width=50)
        entry_key.pack(side='left', padx=5)
        ttk.Button(r1, text="저장", command=self._save_key, width=8).pack(side='left')

        # Row 2: Model & Effort
        r2 = ttk.Frame(frame_top)
        r2.pack(fill='x', pady=5)
        ttk.Label(r2, text="Model:", width=12).pack(side='left')
        models = list(MODEL_PRICING_USD_PER_MTOK.keys())
        ttk.Combobox(r2, textvariable=self.model_var, values=models, state="readonly", width=18).pack(side='left', padx=5)
        
        ttk.Label(r2, text="Effort:", width=8).pack(side='left', padx=(20,0))
        ttk.Combobox(r2, textvariable=self.effort_var, values=["none", "low", "medium", "high"], state="readonly", width=12).pack(side='left', padx=5)

        # 2. Stage3 상세 설정
        frame_set = ttk.LabelFrame(main_frame, text="Stage 3 생성 옵션", padding=15)
        frame_set.pack(fill='x', pady=(0, 10))

        rs = ttk.Frame(frame_set)
        rs.pack(fill='x')

        ttk.Label(rs, text="마켓/길이:").pack(side='left')
        market_opts = ["네이버 50자", "쿠팡 100자", "지마켓/옥션 45자", "기타"]
        ttk.Combobox(rs, textvariable=self.market_var, values=market_opts, state="readonly", width=15).pack(side='left', padx=5)

        ttk.Label(rs, text="출력개수:").pack(side='left', padx=(15, 0))
        ttk.Spinbox(rs, from_=1, to=30, textvariable=self.num_cand_var, width=5).pack(side='left', padx=5)

        ttk.Label(rs, text="명명전략:").pack(side='left', padx=(15, 0))
        st_opts = ["통합형", "옵션포함형", "키워드형"]
        ttk.Combobox(rs, textvariable=self.naming_strategy_var, values=st_opts, state="readonly", width=12).pack(side='left', padx=5)

        # 3. 파일 선택
        frame_file = ttk.LabelFrame(main_frame, text="작업 대상 (Target File)", padding=15)
        frame_file.pack(fill='x', pady=(0, 10))
        
        rf = ttk.Frame(frame_file)
        rf.pack(fill='x')
        ttk.Entry(rf, textvariable=self.input_file_path).pack(side='left', fill='x', expand=True, padx=(0, 5))
        ttk.Button(rf, text="📂 파일 선택", command=self._select_file).pack(side='right')

        # 4. 대시보드
        dash_frame = ttk.LabelFrame(main_frame, text="실시간 현황 (Dashboard)", padding=15)
        dash_frame.pack(fill='x', pady=(0, 10))

        # 1행: 진행률
        d1 = ttk.Frame(dash_frame)
        d1.pack(fill='x', pady=5)
        ttk.Label(d1, text="진행률:", style="Header.TLabel", width=10).pack(side='left')
        self.pb = ttk.Progressbar(d1, maximum=100, mode='determinate')
        self.pb.pack(side='left', fill='x', expand=True, padx=5)
        ttk.Label(d1, textvariable=self.stat_progress, style="Stat.TLabel", width=8).pack(side='right')

        # 2행: 통계
        d2 = ttk.Frame(dash_frame)
        d2.pack(fill='x', pady=5)
        ttk.Label(d2, text="처리 건수:", width=10).pack(side='left')
        ttk.Label(d2, textvariable=self.stat_count, width=15, foreground="blue", font=("맑은 고딕", 10, "bold")).pack(side='left')
        
        ttk.Label(d2, text="성공/실패:", width=10).pack(side='left')
        lbl_res = ttk.Label(d2, textvariable=self.stat_success, foreground="green", font=("맑은 고딕", 10, "bold"))
        lbl_res.pack(side='left')
        ttk.Label(d2, text=" / ").pack(side='left')
        lbl_fail = ttk.Label(d2, textvariable=self.stat_fail, foreground="red", font=("맑은 고딕", 10, "bold"))
        lbl_fail.pack(side='left')

        # 3행: 비용/시간
        d3 = ttk.Frame(dash_frame)
        d3.pack(fill='x', pady=5)
        ttk.Label(d3, text="예상 비용:", width=10).pack(side='left')
        ttk.Label(d3, textvariable=self.stat_cost, style="Cost.TLabel", width=15).pack(side='left')
        
        ttk.Label(d3, text="경과 시간:", width=10).pack(side='left')
        ttk.Label(d3, textvariable=self.stat_time).pack(side='left')

        # 5. 버튼
        btn_frame = ttk.Frame(main_frame)
        btn_frame.pack(fill='x', pady=(0, 10))
        
        self.btn_start = ttk.Button(btn_frame, text="▶ 작업 시작 (이어하기)", style="Action.TButton", command=self._start_thread)
        self.btn_start.pack(side='left', fill='x', expand=True, padx=(0, 5))
        
        self.btn_stop = ttk.Button(btn_frame, text="⏹ 저장 후 중단", style="Stop.TButton", command=self._request_stop, state='disabled')
        self.btn_stop.pack(side='right', fill='x', expand=True, padx=(5, 0))

        ttk.Label(main_frame, textvariable=self.status_msg, foreground="#555", anchor='center').pack(fill='x', pady=(0, 5))

        # 6. 로그
        log_frame = ttk.LabelFrame(main_frame, text="상세 로그", padding=10)
        log_frame.pack(fill='both', expand=True)
        self.log_widget = ScrolledText(log_frame, height=10, state='disabled', font=("Consolas", 9))
        self.log_widget.pack(fill='both', expand=True)

    # --- 유틸 메서드 (원본 기능 복원) ---
    def _save_key(self):
        k = self.api_key_var.get().strip()
        if k:
            save_api_key_to_file(k)
            messagebox.showinfo("저장", "API Key가 저장되었습니다.")

    def _load_key(self):
        k = load_api_key_from_file()
        if k:
            self.api_key_var.set(k)

    def _select_file(self):
        p = filedialog.askopenfilename(filetypes=[("Excel Files", "*.xlsx;*.xls")])
        if p:
            # 스마트 이어하기 로직
            dir_name = os.path.dirname(p)
            base, ext = os.path.splitext(os.path.basename(p))
            
            if "_stage3_done" in base:
                self.input_file_path.set(p)
                self._log(f"결과 파일 선택됨: {os.path.basename(p)}")
                self.status_msg.set("이어서 작업을 진행합니다.")
                return

            done_file = f"{base}_stage3_done{ext}"
            done_path = os.path.join(dir_name, done_file)
            
            if os.path.exists(done_path):
                if messagebox.askyesno("이어하기", f"작업 중이던 파일이 발견되었습니다.\n\n{done_file}\n\n이어서 하시겠습니까?"):
                    self.input_file_path.set(done_path)
                    self._log(f"작업 중이던 파일 로드: {done_file}")
                    self.status_msg.set("작업 재개 준비 완료")
                else:
                    self.input_file_path.set(p)
                    self._log(f"새 원본 파일 선택: {os.path.basename(p)}")
                    self.status_msg.set("새 작업 준비 완료")
            else:
                self.input_file_path.set(p)
                self._log(f"파일 선택됨: {os.path.basename(p)}")
                self.status_msg.set("준비 완료.")

    def _log(self, msg):
        self.log_widget.after(0, self._append_log, msg)

    def _append_log(self, msg):
        t = datetime.now().strftime("%H:%M:%S")
        self.log_widget.config(state='normal')
        self.log_widget.insert("end", f"[{t}] {msg}\n")
        self.log_widget.see("end")
        self.log_widget.config(state='disabled')

    def _save_df_with_backup(self, df: pd.DataFrame, excel_path: str) -> str:
        """[중요] 원본 기능 복원: 엑셀 저장 실패 시 백업 생성"""
        try:
            df.to_excel(excel_path, index=False)
            return excel_path
        except Exception as e:
            base, ext = os.path.splitext(excel_path)
            ts = get_seoul_now().strftime("%Y%m%d_%H%M%S")
            backup_path = f"{base}_stage3_partial_{ts}{ext}"
            try:
                df.to_excel(backup_path, index=False)
                self._log(f"⚠️ 원본 저장 실패(열림 등). 백업 저장: {os.path.basename(backup_path)}")
                return backup_path
            except Exception as e2:
                self._log(f"🔥 백업 저장도 실패: {e2}")
                return excel_path

    def _open_file(self, path: str):
        if not path or not os.path.exists(path):
            return
        try:
            os.startfile(path)
        except:
            pass

    # --- LLM 처리 관련 헬퍼 (원본 로직 복원) ---
    def _get_stage3_settings(self) -> Stage3Settings:
        choice = self.market_var.get()
        max_len = 50
        market = "네이버"
        
        if "쿠팡" in choice:
            market = "쿠팡"
            max_len = 100
        elif "지마켓" in choice:
            market = "지마켓/옥션"
            max_len = 45
        
        # 사용자가 직접 입력한 값이 있으면 우선 (spinbox)
        try:
            user_len = self.max_len_var.get()
            if user_len > 0: max_len = user_len
        except: pass

        return Stage3Settings(
            market=market,
            max_len=max_len,
            num_candidates=self.num_cand_var.get(),
            naming_strategy=self.naming_strategy_var.get(),
            model_name=self.model_var.get(),
            reasoning_effort=self.effort_var.get()
        )

    def _extract_text_from_response(self, resp) -> str:
        """[중요] 원본 기능 복원: 다양한 API 응답 구조 안전 파싱"""
        text_chunks = []
        
        # 1. 객체 접근 방식
        outputs = getattr(resp, "output", None) or getattr(resp, "choices", None)
        
        if outputs:
            try:
                for out in outputs:
                    # choices 구조인 경우 (gpt-4o 등)
                    if hasattr(out, "message"):
                        content = out.message.content
                        if content: text_chunks.append(content)
                        continue
                        
                    # output 구조인 경우 (일부 o1 베타 등)
                    content_list = getattr(out, "content", None)
                    if content_list:
                        for item in content_list:
                            txt = getattr(item, "text", None)
                            if txt:
                                val = getattr(txt, "value", None)
                                if val: text_chunks.append(val)
            except:
                pass

        full_text = "\n".join(text_chunks).strip()
        if not full_text:
            # Fallback: 그냥 문자열 변환 시도
            try:
                return str(resp.choices[0].message.content).strip()
            except:
                return ""
        return full_text

    def _extract_usage_tokens(self, resp):
        """[중요] 원본 기능 복원: 토큰 계산 로직"""
        usage = getattr(resp, "usage", None)
        if not usage: return 0, 0, 0
        
        i = getattr(usage, "prompt_tokens", 0) or 0
        o = getattr(usage, "completion_tokens", 0) or 0
        r = 0
        
        # reasoning details
        details = getattr(usage, "completion_tokens_details", None)
        if details:
            r = getattr(details, "reasoning_tokens", 0) or 0
            
        return i, o, r

    def _calc_cost(self, model, i, o, r):
        price = MODEL_PRICING_USD_PER_MTOK.get(model, {"input":0, "output":0})
        i_cost = (i / 1_000_000) * price["input"]
        o_cost = ((o + r) / 1_000_000) * price["output"] # reasoning은 output에 포함
        return i_cost + o_cost

    # --- 메인 작업 스레드 ---
    def _start_thread(self):
        if self.is_running: return
        
        key = self.api_key_var.get().strip()
        path = self.input_file_path.get().strip()
        
        if not key:
            messagebox.showwarning("오류", "API Key가 없습니다.")
            return
        if not path or not os.path.exists(path):
            messagebox.showwarning("오류", "파일이 없습니다.")
            return
            
        self.is_running = True
        self.stop_requested = False
        self.btn_start.config(state='disabled')
        self.btn_stop.config(state='normal')
        self.status_msg.set("작업 초기화 중...")
        
        t = threading.Thread(target=self._run_process, args=(key, path))
        t.daemon = True
        t.start()

    def _request_stop(self):
        if self.is_running:
            self.stop_requested = True
            self.status_msg.set("⚠️ 중단 요청됨! 현재 행 처리 후 멈춥니다.")
            self.btn_stop.config(state='disabled')

    def _run_process(self, api_key, input_path):
        try:
            # 설정 준비
            client = OpenAI(api_key=api_key)
            settings = self._get_stage3_settings()
            
            df = pd.read_excel(input_path)
            
            if "ST2_JSON" not in df.columns:
                raise ValueError("ST2_JSON 컬럼이 없습니다.")
            
            # 컬럼 준비
            if "ST3_결과상품명" not in df.columns: df["ST3_결과상품명"] = ""
            if "ST3_프롬프트" not in df.columns: df["ST3_프롬프트"] = ""
            
            df["ST3_결과상품명"] = df["ST3_결과상품명"].astype(str)

            # 저장 경로
            base, ext = os.path.splitext(input_path)
            if "_stage3_done" in input_path:
                out_path = input_path
            else:
                out_path = f"{base}_stage3_done{ext}"
            self.output_file_path = out_path

            total_rows = len(df)
            start_dt = get_seoul_now()
            self._update_timer(start_dt)

            stats = {
                "in": 0, "out": 0, "reason": 0, "cost": 0.0,
                "success": 0, "fail": 0, "skip": 0, "api": 0
            }
            
            processed_now = 0

            self._log(f"▶ 시작: {len(df)}행, 모델={settings.model_name}")

            for idx, row in df.iterrows():
                if self.stop_requested:
                    self._log("⛔ 사용자 중단 요청.")
                    break

                # Resume Check
                val = safe_str(row.get("ST3_결과상품명", ""))
                if val and val != "nan":
                    stats["skip"] += 1
                    self._update_ui_stats(idx+1, total_rows, stats)
                    continue

                # 1. Prompt 생성
                try:
                    req = build_stage3_request_from_row(row, settings)
                    prompt = req.prompt
                    df.at[idx, "ST3_프롬프트"] = prompt
                except Exception as e:
                    self._log(f"[Row {idx+1}] 프롬프트 생성 오류: {e}")
                    stats["fail"] += 1
                    continue

                # 2. API Call
                try:
                    params = {
                        "model": settings.model_name,
                        "messages": [{"role": "user", "content": prompt}],
                    }
                    if "gpt-5" in settings.model_name or "o1" in settings.model_name:
                        if settings.reasoning_effort != "none":
                            params["reasoning_effort"] = settings.reasoning_effort
                    else:
                        params["temperature"] = 0.7

                    resp = client.chat.completions.create(**params)
                    
                    # 3. 결과 파싱
                    res_text = self._extract_text_from_response(resp)
                    df.at[idx, "ST3_결과상품명"] = res_text
                    
                    # 4. 비용 계산
                    i, o, r = self._extract_usage_tokens(resp)
                    cost = self._calc_cost(settings.model_name, i, o, r)
                    
                    stats["in"] += i
                    stats["out"] += o
                    stats["reason"] += r
                    stats["cost"] += cost
                    stats["api"] += 1
                    stats["success"] += 1
                    
                except Exception as e:
                    self._log(f"[Row {idx+1}] API 오류: {e}")
                    stats["fail"] += 1

                processed_now += 1
                self._update_ui_stats(idx+1, total_rows, stats)

                # 자동 저장
                if processed_now % 10 == 0:
                    self._save_df_with_backup(df, out_path)
            
            # 최종 저장
            self._save_df_with_backup(df, out_path)
            finish_dt = get_seoul_now()

            # 히스토리
            if stats["api"] > 0:
                append_run_history(
                    stage="Stage 3",
                    model_name=settings.model_name,
                    reasoning_effort=settings.reasoning_effort,
                    src_file=input_path,
                    out_file=out_path,
                    total_rows=total_rows,
                    api_rows=stats["api"],
                    elapsed_seconds=(finish_dt - start_dt).total_seconds(),
                    total_in_tok=stats["in"],
                    total_out_tok=stats["out"],
                    total_reasoning_tok=stats["reason"],
                    input_cost_usd=0, # 약식
                    output_cost_usd=0, 
                    total_cost_usd=stats["cost"],
                    start_dt=start_dt,
                    finish_dt=finish_dt,
                    success_rows=stats["success"],
                    fail_rows=stats["fail"]
                )

            msg = "작업이 중단되었습니다." if self.stop_requested else "모든 작업이 완료되었습니다."
            self.status_msg.set(msg)
            self._show_completion(msg, stats, out_path)

        except Exception as e:
            self._log(f"🔥 치명적 오류: {e}")
            messagebox.showerror("오류", str(e))
        finally:
            self.is_running = False
            self.stop_requested = False
            self.btn_start.config(state='normal')
            self.btn_stop.config(state='disabled')

    def _update_ui_stats(self, curr, total, stats):
        pct = (curr / total) * 100
        self.pb['value'] = pct
        self.stat_progress.set(f"{pct:.1f}%")
        self.stat_count.set(f"{curr} / {total}")
        self.stat_success.set(str(stats['success']))
        self.stat_fail.set(str(stats['fail']))
        self.stat_cost.set(f"${stats['cost']:.4f}")
        
        msg = f"진행 중... {curr}/{total}"
        if stats['skip'] > 0: msg += f" (Skip: {stats['skip']})"
        self.status_msg.set(msg)
        self.update_idletasks()

    def _update_timer(self, start_dt):
        if not self.is_running: return
        now = get_seoul_now()
        diff = int((now - start_dt).total_seconds())
        h, r = divmod(diff, 3600)
        m, s = divmod(r, 60)
        self.stat_time.set(f"{h:02}:{m:02}:{s:02}")
        self.after(500, lambda: self._update_timer(start_dt))

    def _show_completion(self, title, stats, path):
        msg = (
            f"[{title}]\n\n"
            f"성공: {stats['success']}\n"
            f"실패: {stats['fail']}\n"
            f"건너뜀: {stats['skip']}\n"
            f"총 비용: ${stats['cost']:.4f}\n\n"
            f"파일:\n{os.path.basename(path)}"
        )
        if messagebox.askyesno("완료", msg + "\n\n결과 파일을 여시겠습니까?"):
            self._open_file(path)

if __name__ == "__main__":
    app = Stage3LLMGUI()
    app.mainloop()