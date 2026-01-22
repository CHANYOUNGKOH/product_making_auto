import os
import sys
import json
import threading
import traceback
from datetime import datetime
import pandas as pd
import tkinter as tk
from tkinter import ttk, filedialog, messagebox, scrolledtext

# =============================================================================
# [모듈 1] 설정 및 프롬프트 관리 (Config)
# =============================================================================
class Config:
    # API 설정 (사용자 입력 또는 환경변수)
    DEFAULT_MODEL = "gpt-4o"  # 또는 gpt-3.5-turbo, gpt-4-turbo 등
    
    # -------------------------------------------------------------------------
    # 사용자님이 확정하신 최종 프롬프트 (Safety Net 포함)
    # -------------------------------------------------------------------------
    PROMPT_TEMPLATE = """
당신은 한국 이커머스 시장의 **상품명 최적화 전문가(SEO & Conversion Specialist)**입니다.
입력된 후보 리스트를 검증 데이터(JSON)와 대조하여 **거짓·부적절한 상품명을 제거**하고,
살아남은 후보를 **구매 전환율(CTR)이 높을 것 같은 순서대로 재정렬**하십시오.

[입력 정보]
- 기준 상품명(ST1): {st1_refined_name}
- 상세 속성(ST2 JSON, 사실 정보): {st2_json}
- 후보 상품명 목록(ST3 Result, 줄바꿈으로 구분):
---
{candidate_list}
---

[1단계: 제거 규칙 (Filtering)]
아래 기준 중 하나라도 위반하면 해당 후보는 최종 결과에서 **즉시 탈락**시킵니다.

1. 팩트 오류(Hallucination)
   - ST2 JSON에 없는 수량·용량·재질·구성·대상·기능을 포함하거나, 정보를 왜곡한 경우.
   - 예: 1개를 1+1로 표기, 일반형을 국산/정품으로 단정.
2. 허위 마케팅
   - ST2 JSON에 명시되지 않은 '무료배송, 공식몰, 정품 보증, 파격 세일, 단독 특가' 등의 문구를
     사실처럼 단언하는 경우.
   - 단, ST2 JSON에 명시된 혜택·특징이라면 사용할 수 있습니다.
3. 가독성 미달 / 정체 불명
   - 무엇을 파는지 직관적으로 이해하기 어렵거나,
   - 자연스러운 상품명이 아니라 키워드·태그만 나열한 것처럼 보이는 경우.
4. 품질 열위
   - 어색한 한국어 어순, 잘못된 띄어쓰기, 의미 중복(예: "겨울 겨울 장갑") 등으로
     다른 후보 대비 명확히 품질이 떨어지는 경우.
5. 중복 제거
   - 의미가 거의 같은 후보가 여러 개라면, 가장 자연스럽고 읽기 좋은 1개만 남기고 삭제합니다.

[2단계: 정렬 기준 (Ranking)]
살아남은 후보들 중 **가장 잘 팔릴 상품명**이 맨 위에 오도록 순서를 정하십시오.

1. [1순위] 매력도 / 클릭률
   - 고객의 니즈를 자극하여 클릭을 유도하는가?
2. [2순위] 직관성
   - 카테고리와 핵심 혜택이 한눈에 이해되는가?
3. [3순위] 안정성
   - 과장되지 않고 깔끔하며 신뢰감을 주는가?

[3단계: 중요 행동 수칙 (Strict Rules)]

상황 A. 후보가 1개 이상 살아남은 경우
- **수정 금지:** 후보 텍스트를 고치거나 새로운 단어를 추가하지 마십시오.
- 오직 살아남은 후보들의 **순서만** 변경하여 출력하십시오.

상황 B. 모든 후보가 탈락한 경우 (비상 대책)
- **빈칸 금지:** 절대 빈 결과를 출력하지 마십시오.
- 대신, ST1(기준 상품명)과 ST2(JSON)의 사실 정보만을 조합하여
  **가장 안전하고 완성도 높은 상품명 1개를 새로 작성하여 출력**하십시오.
- 이때도 ST2 JSON에 없는 허위 혜택·과장 표현은 사용하지 마십시오.

[최종 출력 형식]
- 선택된(또는 생성된) 상품명만 줄바꿈으로 나열하십시오.
- 각 줄에는 **오직 상품명 텍스트**만 쓰고, 번호·따옴표·점수·설명 등 다른 내용은 절대 포함하지 마십시오.
"""

# =============================================================================
# [모듈 2] LLM API 클라이언트 (LLMClient)
# =============================================================================
class LLMClient:
    def __init__(self, api_key):
        self.api_key = api_key
        try:
            import openai
            self.client = openai.OpenAI(api_key=self.api_key)
            self.available = True
        except ImportError:
            self.available = False
            print("OpenAI 모듈이 설치되지 않았습니다. pip install openai")

    def call_llm(self, prompt, model=Config.DEFAULT_MODEL):
        if not self.available:
            return "Error: OpenAI module not installed."
        
        try:
            response = self.client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": "당신은 이커머스 상품명 최적화 전문가입니다."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.3, # 검수 및 정렬이므로 창의성보다는 정확성 중시
                top_p=0.9
            )
            return response.choices[0].message.content.strip()
        except Exception as e:
            return f"API Error: {str(e)}"

# =============================================================================
# [모듈 3] 데이터 처리 및 프롬프트 조립 (CoreProcessor)
# =============================================================================
class CoreProcessor:
    def __init__(self, llm_client: LLMClient):
        self.llm = llm_client

    def process_row(self, row):
        """
        엑셀의 한 행(Row)을 받아 LLM 처리를 수행하고 결과를 반환
        """
        try:
            # 1. 데이터 추출
            st1_name = str(row.get('ST1_정제상품명', '')).strip()
            st2_json = str(row.get('ST2_JSON', '{}')).strip()
            st3_result = str(row.get('ST3_결과상품명', '')).strip() # 이전 단계 결과

            # 후보가 없으면 즉시 ST1 반환 (혹은 빈값)
            if not st3_result or st3_result == "nan":
                return "SKIPPED_NO_CANDIDATES"

            # 2. 프롬프트 생성
            prompt = Config.PROMPT_TEMPLATE.format(
                st1_refined_name=st1_name,
                st2_json=st2_json,
                candidate_list=st3_result
            )

            # 3. LLM 호출
            result_text = self.llm.call_llm(prompt)

            return result_text

        except Exception as e:
            return f"Process Error: {str(e)}"

# =============================================================================
# [모듈 4] 메인 GUI (Stage4_2_GUI)
# =============================================================================
class Stage4_2_GUI(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Stage 4-2: LLM 최종 심사 및 정렬 (Final Ranker)")
        self.geometry("900x700")
        
        # 스타일 설정
        style = ttk.Style()
        if 'clam' in style.theme_names():
            style.theme_use('clam')

        self.input_file_path = tk.StringVar()
        self.api_key_var = tk.StringVar()
        self.status_var = tk.StringVar(value="대기 중...")
        self.is_running = False

        self._init_ui()

    def _init_ui(self):
        # --- 상단: API 키 설정 ---
        api_frame = ttk.LabelFrame(self, text="API 설정 (OpenAI)", padding=10)
        api_frame.pack(fill='x', padx=10, pady=5)
        
        ttk.Label(api_frame, text="API Key:").pack(side='left')
        entry_api = ttk.Entry(api_frame, textvariable=self.api_key_var, show="*", width=50)
        entry_api.pack(side='left', padx=5)
        
        # 팁: 환경변수에서 불러오기 시도
        env_key = os.getenv("OPENAI_API_KEY")
        if env_key:
            self.api_key_var.set(env_key)

        # --- 중단: 파일 선택 ---
        file_frame = ttk.LabelFrame(self, text="입력 파일 (Stage 3/4-1 완료 엑셀)", padding=10)
        file_frame.pack(fill='x', padx=10, pady=5)

        entry_file = ttk.Entry(file_frame, textvariable=self.input_file_path)
        entry_file.pack(side='left', fill='x', expand=True, padx=5)
        ttk.Button(file_frame, text="파일 찾기", command=self._select_file).pack(side='right')

        # --- 중단: 실행 버튼 및 진행바 ---
        action_frame = ttk.Frame(self, padding=10)
        action_frame.pack(fill='x', padx=10)

        self.btn_run = ttk.Button(action_frame, text="▶ LLM 검수 및 정렬 시작", command=self._start_thread)
        self.btn_run.pack(fill='x', ipady=5)

        self.progress = ttk.Progressbar(action_frame, orient="horizontal", mode="determinate")
        self.progress.pack(fill='x', pady=5)
        
        ttk.Label(action_frame, textvariable=self.status_var, foreground="blue").pack(anchor='w')

        # --- 하단: 로그 ---
        log_frame = ttk.LabelFrame(self, text="진행 로그", padding=10)
        log_frame.pack(fill='both', expand=True, padx=10, pady=5)

        self.log_widget = scrolledtext.ScrolledText(log_frame, state='disabled', height=15)
        self.log_widget.pack(fill='both', expand=True)

    def _log(self, msg):
        timestamp = datetime.now().strftime("%H:%M:%S")
        self.log_widget.config(state='normal')
        self.log_widget.insert(tk.END, f"[{timestamp}] {msg}\n")
        self.log_widget.see(tk.END)
        self.log_widget.config(state='disabled')

    def _select_file(self):
        path = filedialog.askopenfilename(filetypes=[("Excel Files", "*.xlsx;*.xls")])
        if path:
            self.input_file_path.set(path)
            self._log(f"파일 선택됨: {os.path.basename(path)}")

    def _start_thread(self):
        if self.is_running: return
        
        api_key = self.api_key_var.get().strip()
        file_path = self.input_file_path.get().strip()

        if not api_key:
            messagebox.showwarning("경고", "OpenAI API Key를 입력해주세요.")
            return
        if not file_path:
            messagebox.showwarning("경고", "엑셀 파일을 선택해주세요.")
            return

        self.is_running = True
        self.btn_run.config(state='disabled')
        
        thread = threading.Thread(target=self._run_process, args=(api_key, file_path))
        thread.daemon = True
        thread.start()

    def _run_process(self, api_key, file_path):
        try:
            self._log("=== Stage 4-2 작업 시작 ===")
            
            # 1. 초기화
            llm_client = LLMClient(api_key)
            if not llm_client.available:
                raise ImportError("OpenAI 라이브러리 설치 필요")
                
            processor = CoreProcessor(llm_client)

            # 2. 엑셀 로드
            df = pd.read_excel(file_path)
            self._log(f"데이터 로드 완료: {len(df)}행")

            # 필요한 컬럼 확인
            required_cols = ['ST1_정제상품명', 'ST2_JSON', 'ST3_결과상품명'] # 4-1이 있다면 'ST4_필터결과' 등 사용 가능
            
            # (옵션) 4-1 결과가 있다면 그것을 우선 사용, 없으면 ST3 사용
            target_col = 'ST3_결과상품명'
            for col in df.columns:
                if 'filtered' in col or '정제결과' in col: # Stage 4-1 결과 컬럼명 추정
                     if '상품명' in col:
                         target_col = col
                         self._log(f"입력 소스 컬럼 자동 감지: {target_col}")
                         break
            
            # 결과 저장할 컬럼
            result_col = 'ST4_최종확정'
            df[result_col] = ""

            total_rows = len(df)
            processed_cnt = 0

            # 3. 반복 처리
            for idx, row in df.iterrows():
                try:
                    result_text = processor.process_row(row)
                    
                    # 결과 처리
                    if "API Error" in result_text:
                        self._log(f"[Row {idx+1}] API 오류 발생")
                    elif result_text == "SKIPPED_NO_CANDIDATES":
                        # 후보가 아예 없으면 ST1을 안전망으로 사용하거나 비워둠
                        # 여기서는 프롬프트 논리에 따라 비워두지 않고 ST1을 넣는 로직 추가 가능
                        # 하지만 프롬프트가 Safety Net을 가지고 있으므로, 입력값이 아예 Nan인 경우만 여기 해당됨.
                        pass 
                    else:
                        df.at[idx, result_col] = result_text
                    
                except Exception as e:
                    self._log(f"[Row {idx+1}] 치명적 오류: {e}")

                processed_cnt += 1
                progress_val = (processed_cnt / total_rows) * 100
                self.progress['value'] = progress_val
                self.status_var.set(f"처리 중... {processed_cnt}/{total_rows}")
                self.update_idletasks()

            # 4. 저장
            base, ext = os.path.splitext(file_path)
            output_path = f"{base}_final_ranked{ext}"
            df.to_excel(output_path, index=False)
            
            self._log(f"=== 작업 완료 ===")
            self._log(f"저장 경로: {output_path}")
            messagebox.showinfo("완료", f"처리가 완료되었습니다.\n저장 파일: {os.path.basename(output_path)}")

        except Exception as e:
            traceback.print_exc()
            self._log(f"작업 중지: {str(e)}")
            messagebox.showerror("에러", f"작업 중 오류가 발생했습니다.\n{e}")
        finally:
            self.is_running = False
            self.btn_run.config(state='normal')
            self.status_var.set("대기 중...")

if __name__ == "__main__":
    app = Stage4_2_GUI()
    app.mainloop()