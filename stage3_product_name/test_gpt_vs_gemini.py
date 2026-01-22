"""
test_gpt_vs_gemini.py

GPT-5 vs Gemini 2.5 Flash-Lite 품질/비용 비교 테스트 스크립트

사용법:
    python test_gpt_vs_gemini.py [테스트_엑셀_파일.xlsx]

    또는 직접 실행하면 GUI가 열립니다.
"""

import os
import sys
import json
import time
from datetime import datetime
from dataclasses import dataclass, asdict
from typing import Optional, List, Dict, Any

import pandas as pd

# ============================================================
# API Clients
# ============================================================
# OpenAI
try:
    from openai import OpenAI
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False
    print("[WARN] openai 패키지가 설치되지 않았습니다.")

# Gemini
try:
    from google import genai
    from google.genai import types
    GEMINI_AVAILABLE = True
except ImportError:
    GEMINI_AVAILABLE = False
    print("[WARN] google-genai 패키지가 설치되지 않았습니다.")

# ============================================================
# Core imports (Stage 3)
# ============================================================
try:
    from stage3_core_Casche import (
        STAGE3_SYSTEM_PROMPT as GPT_SYSTEM_PROMPT,
        STAGE3_USER_PROMPT_TEMPLATE as GPT_USER_TEMPLATE,
        Stage3Settings,
        build_stage3_request_from_row as build_gpt_request,
        safe_str, fmt_safe,
    )
    GPT_CORE_AVAILABLE = True
except ImportError:
    GPT_CORE_AVAILABLE = False
    print("[WARN] stage3_core_Casche.py를 찾을 수 없습니다.")

try:
    from stage3_core_gemini import (
        STAGE3_SYSTEM_INSTRUCTION as GEMINI_SYSTEM_INSTRUCTION,
        STAGE3_USER_PROMPT_TEMPLATE as GEMINI_USER_TEMPLATE,
        Stage3Settings as GeminiSettings,
        build_stage3_request_from_row as build_gemini_request,
    )
    GEMINI_CORE_AVAILABLE = True
except ImportError:
    GEMINI_CORE_AVAILABLE = False
    print("[WARN] stage3_core_gemini.py를 찾을 수 없습니다.")

# ============================================================
# API Keys
# ============================================================
OPENAI_API_KEY_FILE = ".openai_api_key"
GEMINI_API_KEY_FILE = ".gemini_api_key_stage3_batch"

def load_api_key(filename: str) -> str:
    """API 키 파일에서 키 로드"""
    if os.path.exists(filename):
        try:
            with open(filename, "r", encoding="utf-8") as f:
                return f.read().strip()
        except:
            pass
    return ""

def save_api_key(filename: str, key: str):
    """API 키 파일에 저장"""
    try:
        with open(filename, "w", encoding="utf-8") as f:
            f.write(key.strip())
    except Exception as e:
        print(f"[WARN] API 키 저장 실패: {e}")

# ============================================================
# Pricing (USD per 1M tokens)
# ============================================================
PRICING = {
    "gpt-5-mini": {
        "input": 0.25,    # $0.25/1M (Batch: 50% = $0.125)
        "output": 2.00,   # $2.00/1M (Batch: 50% = $1.00)
        "cached_input": 0.025,  # 90% discount
    },
    "gemini-2.5-flash-lite": {
        "input": 0.10,    # $0.10/1M (Batch: 50% = $0.05)
        "output": 0.40,   # $0.40/1M (Batch: 50% = $0.20)
        "cached_input": 0.01,  # 90% discount (implicit caching)
    },
}

# ============================================================
# Test Result Dataclass
# ============================================================
@dataclass
class TestResult:
    model: str
    row_index: int
    input_tokens: int
    output_tokens: int
    cached_tokens: int
    response_time_ms: float
    cost_usd: float
    result_text: str
    error: Optional[str] = None

# ============================================================
# GPT-5 API Call
# ============================================================
def call_gpt5(client: OpenAI, system_prompt: str, user_prompt: str, model: str = "gpt-5-mini") -> TestResult:
    """GPT-5 API 호출"""
    start_time = time.time()

    try:
        response = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            temperature=0.7,
            max_tokens=2048,
        )

        elapsed_ms = (time.time() - start_time) * 1000

        # 토큰 사용량
        usage = response.usage
        input_tokens = usage.prompt_tokens
        output_tokens = usage.completion_tokens
        cached_tokens = getattr(usage, 'prompt_tokens_details', {})
        if hasattr(cached_tokens, 'cached_tokens'):
            cached_tokens = cached_tokens.cached_tokens or 0
        else:
            cached_tokens = 0

        # 비용 계산
        pricing = PRICING.get(model, PRICING["gpt-5-mini"])
        non_cached = input_tokens - cached_tokens
        cost = (non_cached / 1_000_000) * pricing["input"]
        cost += (cached_tokens / 1_000_000) * pricing["cached_input"]
        cost += (output_tokens / 1_000_000) * pricing["output"]

        result_text = response.choices[0].message.content

        return TestResult(
            model=model,
            row_index=-1,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            cached_tokens=cached_tokens,
            response_time_ms=elapsed_ms,
            cost_usd=cost,
            result_text=result_text,
            error=None
        )

    except Exception as e:
        elapsed_ms = (time.time() - start_time) * 1000
        return TestResult(
            model=model,
            row_index=-1,
            input_tokens=0,
            output_tokens=0,
            cached_tokens=0,
            response_time_ms=elapsed_ms,
            cost_usd=0,
            result_text="",
            error=str(e)
        )

# ============================================================
# Gemini API Call
# ============================================================
def call_gemini(client, system_instruction: str, user_prompt: str,
                model: str = "gemini-2.5-flash-lite") -> TestResult:
    """Gemini API 호출"""
    start_time = time.time()

    try:
        response = client.models.generate_content(
            model=model,
            contents=user_prompt,
            config=types.GenerateContentConfig(
                system_instruction=system_instruction,
                temperature=0.7,
                max_output_tokens=2048,
            )
        )

        elapsed_ms = (time.time() - start_time) * 1000

        # 토큰 사용량
        usage = response.usage_metadata
        input_tokens = getattr(usage, 'prompt_token_count', 0)
        output_tokens = getattr(usage, 'candidates_token_count', 0)
        cached_tokens = getattr(usage, 'cached_content_token_count', 0)

        # 비용 계산
        pricing = PRICING.get(model, PRICING["gemini-2.5-flash-lite"])
        non_cached = input_tokens - cached_tokens
        cost = (non_cached / 1_000_000) * pricing["input"]
        cost += (cached_tokens / 1_000_000) * pricing["cached_input"]
        cost += (output_tokens / 1_000_000) * pricing["output"]

        result_text = response.text

        return TestResult(
            model=model,
            row_index=-1,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            cached_tokens=cached_tokens,
            response_time_ms=elapsed_ms,
            cost_usd=cost,
            result_text=result_text,
            error=None
        )

    except Exception as e:
        elapsed_ms = (time.time() - start_time) * 1000
        return TestResult(
            model=model,
            row_index=-1,
            input_tokens=0,
            output_tokens=0,
            cached_tokens=0,
            response_time_ms=elapsed_ms,
            cost_usd=0,
            result_text="",
            error=str(e)
        )

# ============================================================
# Comparison Test
# ============================================================
def run_comparison_test(
    excel_path: str,
    openai_key: str,
    gemini_key: str,
    max_rows: int = 10,
    market: str = "네이버",
    max_len: int = 50,
    num_candidates: int = 10,
) -> Dict[str, Any]:
    """
    GPT-5 vs Gemini 비교 테스트 실행

    Returns:
        {
            "gpt_results": [TestResult, ...],
            "gemini_results": [TestResult, ...],
            "summary": {...}
        }
    """
    print(f"\n{'='*60}")
    print(f"GPT-5 vs Gemini 2.5 Flash-Lite 비교 테스트")
    print(f"{'='*60}")
    print(f"파일: {excel_path}")
    print(f"최대 행 수: {max_rows}")
    print(f"마켓: {market}, 글자수: {max_len}, 후보수: {num_candidates}")
    print(f"{'='*60}\n")

    # 엑셀 로드
    df = pd.read_excel(excel_path)
    if "ST2_JSON" not in df.columns:
        raise ValueError("ST2_JSON 컬럼이 없습니다. Stage 2 처리가 완료된 파일을 사용하세요.")

    # ST2_JSON이 있는 행만 필터링
    df_valid = df[df["ST2_JSON"].notna() & (df["ST2_JSON"].str.strip() != "")]
    df_valid = df_valid.head(max_rows)

    print(f"테스트 대상 행 수: {len(df_valid)}\n")

    # Settings
    settings = Stage3Settings(
        market=market,
        max_len=max_len,
        num_candidates=num_candidates,
        naming_strategy="통합형"
    )

    gpt_results = []
    gemini_results = []

    # OpenAI Client
    openai_client = None
    if OPENAI_AVAILABLE and openai_key:
        openai_client = OpenAI(api_key=openai_key)

    # Gemini Client
    gemini_client = None
    if GEMINI_AVAILABLE and gemini_key:
        gemini_client = genai.Client(api_key=gemini_key)

    for idx, (_, row) in enumerate(df_valid.iterrows()):
        print(f"\n[Row {idx+1}/{len(df_valid)}] 처리 중...")

        # GPT-5 테스트
        if openai_client and GPT_CORE_AVAILABLE:
            try:
                gpt_req = build_gpt_request(row, settings)
                system_prompt = gpt_req.system_prompt if hasattr(gpt_req, 'system_prompt') else GPT_SYSTEM_PROMPT
                user_prompt = gpt_req.user_prompt if hasattr(gpt_req, 'user_prompt') else ""

                print(f"  [GPT-5] 호출 중...", end=" ")
                result = call_gpt5(openai_client, system_prompt, user_prompt)
                result.row_index = idx
                gpt_results.append(result)

                if result.error:
                    print(f"❌ 오류: {result.error[:50]}")
                else:
                    print(f"✅ {result.response_time_ms:.0f}ms, ${result.cost_usd:.6f}")

            except Exception as e:
                print(f"❌ 빌드 오류: {e}")

        # Gemini 테스트
        if gemini_client and GEMINI_CORE_AVAILABLE:
            try:
                gemini_req = build_gemini_request(row, settings)
                system_instruction = gemini_req.system_instruction
                user_prompt = gemini_req.user_prompt

                print(f"  [Gemini] 호출 중...", end=" ")
                result = call_gemini(gemini_client, system_instruction, user_prompt)
                result.row_index = idx
                gemini_results.append(result)

                if result.error:
                    print(f"❌ 오류: {result.error[:50]}")
                else:
                    print(f"✅ {result.response_time_ms:.0f}ms, ${result.cost_usd:.6f}")
                    # 캐싱 히트 표시
                    if result.cached_tokens > 0:
                        cache_rate = (result.cached_tokens / result.input_tokens) * 100 if result.input_tokens > 0 else 0
                        print(f"       💾 캐시 히트: {result.cached_tokens} tokens ({cache_rate:.1f}%)")

            except Exception as e:
                print(f"❌ 빌드 오류: {e}")

        # Rate limiting
        time.sleep(0.2)

    # 결과 요약
    summary = calculate_summary(gpt_results, gemini_results)

    return {
        "gpt_results": gpt_results,
        "gemini_results": gemini_results,
        "summary": summary
    }

def calculate_summary(gpt_results: List[TestResult], gemini_results: List[TestResult]) -> Dict:
    """결과 요약 계산"""
    summary = {
        "gpt": {
            "count": len([r for r in gpt_results if not r.error]),
            "total_input_tokens": sum(r.input_tokens for r in gpt_results),
            "total_output_tokens": sum(r.output_tokens for r in gpt_results),
            "total_cached_tokens": sum(r.cached_tokens for r in gpt_results),
            "total_cost_usd": sum(r.cost_usd for r in gpt_results),
            "avg_response_ms": 0,
            "errors": len([r for r in gpt_results if r.error]),
        },
        "gemini": {
            "count": len([r for r in gemini_results if not r.error]),
            "total_input_tokens": sum(r.input_tokens for r in gemini_results),
            "total_output_tokens": sum(r.output_tokens for r in gemini_results),
            "total_cached_tokens": sum(r.cached_tokens for r in gemini_results),
            "total_cost_usd": sum(r.cost_usd for r in gemini_results),
            "avg_response_ms": 0,
            "errors": len([r for r in gemini_results if r.error]),
        },
    }

    # 평균 응답 시간
    gpt_success = [r for r in gpt_results if not r.error]
    gemini_success = [r for r in gemini_results if not r.error]

    if gpt_success:
        summary["gpt"]["avg_response_ms"] = sum(r.response_time_ms for r in gpt_success) / len(gpt_success)
    if gemini_success:
        summary["gemini"]["avg_response_ms"] = sum(r.response_time_ms for r in gemini_success) / len(gemini_success)

    # 비용 비교
    if summary["gpt"]["total_cost_usd"] > 0:
        summary["cost_ratio"] = summary["gemini"]["total_cost_usd"] / summary["gpt"]["total_cost_usd"]
    else:
        summary["cost_ratio"] = 0

    return summary

def print_summary(summary: Dict):
    """결과 요약 출력"""
    print(f"\n{'='*60}")
    print("테스트 결과 요약")
    print(f"{'='*60}")

    print(f"\n[GPT-5]")
    gpt = summary["gpt"]
    print(f"  성공/오류: {gpt['count']}/{gpt['errors']}")
    print(f"  총 Input 토큰: {gpt['total_input_tokens']:,}")
    print(f"  총 Output 토큰: {gpt['total_output_tokens']:,}")
    print(f"  캐시 히트 토큰: {gpt['total_cached_tokens']:,}")
    print(f"  평균 응답 시간: {gpt['avg_response_ms']:.0f}ms")
    print(f"  총 비용: ${gpt['total_cost_usd']:.6f}")

    print(f"\n[Gemini 2.5 Flash-Lite]")
    gemini = summary["gemini"]
    print(f"  성공/오류: {gemini['count']}/{gemini['errors']}")
    print(f"  총 Input 토큰: {gemini['total_input_tokens']:,}")
    print(f"  총 Output 토큰: {gemini['total_output_tokens']:,}")
    print(f"  캐시 히트 토큰: {gemini['total_cached_tokens']:,}")
    print(f"  평균 응답 시간: {gemini['avg_response_ms']:.0f}ms")
    print(f"  총 비용: ${gemini['total_cost_usd']:.6f}")

    print(f"\n[비교]")
    if summary.get("cost_ratio"):
        if summary["cost_ratio"] < 1:
            savings = (1 - summary["cost_ratio"]) * 100
            print(f"  💰 Gemini가 GPT 대비 {savings:.1f}% 저렴")
        else:
            extra = (summary["cost_ratio"] - 1) * 100
            print(f"  💸 Gemini가 GPT 대비 {extra:.1f}% 비쌈")

    if gpt["avg_response_ms"] > 0 and gemini["avg_response_ms"] > 0:
        speed_ratio = gpt["avg_response_ms"] / gemini["avg_response_ms"]
        if speed_ratio > 1:
            print(f"  ⚡ Gemini가 GPT 대비 {speed_ratio:.1f}배 빠름")
        else:
            print(f"  🐢 Gemini가 GPT 대비 {1/speed_ratio:.1f}배 느림")

def save_results(results: Dict, output_path: str):
    """결과를 JSON/Excel로 저장"""
    # JSON 저장
    json_path = output_path.replace(".xlsx", "_comparison.json")

    output_data = {
        "timestamp": datetime.now().isoformat(),
        "summary": results["summary"],
        "gpt_results": [asdict(r) for r in results["gpt_results"]],
        "gemini_results": [asdict(r) for r in results["gemini_results"]],
    }

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(output_data, f, ensure_ascii=False, indent=2)

    print(f"\n결과 저장됨: {json_path}")

    # Excel 저장 (결과 비교표)
    excel_path = output_path.replace(".xlsx", "_comparison.xlsx")

    comparison_data = []
    max_len = max(len(results["gpt_results"]), len(results["gemini_results"]))

    for i in range(max_len):
        row_data = {"row_index": i}

        if i < len(results["gpt_results"]):
            gpt = results["gpt_results"][i]
            row_data["GPT_결과"] = gpt.result_text[:500] if gpt.result_text else gpt.error
            row_data["GPT_비용"] = gpt.cost_usd
            row_data["GPT_시간ms"] = gpt.response_time_ms
            row_data["GPT_입력토큰"] = gpt.input_tokens
            row_data["GPT_출력토큰"] = gpt.output_tokens

        if i < len(results["gemini_results"]):
            gemini = results["gemini_results"][i]
            row_data["Gemini_결과"] = gemini.result_text[:500] if gemini.result_text else gemini.error
            row_data["Gemini_비용"] = gemini.cost_usd
            row_data["Gemini_시간ms"] = gemini.response_time_ms
            row_data["Gemini_입력토큰"] = gemini.input_tokens
            row_data["Gemini_출력토큰"] = gemini.output_tokens
            row_data["Gemini_캐시토큰"] = gemini.cached_tokens

        comparison_data.append(row_data)

    df_result = pd.DataFrame(comparison_data)
    df_result.to_excel(excel_path, index=False)

    print(f"결과 저장됨: {excel_path}")

# ============================================================
# GUI (Tkinter)
# ============================================================
def run_gui():
    """GUI 실행"""
    import tkinter as tk
    from tkinter import ttk, filedialog, messagebox
    from tkinter.scrolledtext import ScrolledText

    class TestGUI(tk.Tk):
        def __init__(self):
            super().__init__()
            self.title("GPT-5 vs Gemini 비교 테스트")
            self.geometry("800x700")

            self.openai_key_var = tk.StringVar(value=load_api_key(OPENAI_API_KEY_FILE))
            self.gemini_key_var = tk.StringVar(value=load_api_key(GEMINI_API_KEY_FILE))
            self.file_var = tk.StringVar()
            self.max_rows_var = tk.IntVar(value=5)
            self.market_var = tk.StringVar(value="네이버")
            self.max_len_var = tk.IntVar(value=50)
            self.num_cand_var = tk.IntVar(value=10)

            self._init_ui()

        def _init_ui(self):
            # API Keys
            f_keys = ttk.LabelFrame(self, text="API Keys", padding=10)
            f_keys.pack(fill='x', padx=10, pady=5)

            ttk.Label(f_keys, text="OpenAI:").grid(row=0, column=0, sticky='w')
            ttk.Entry(f_keys, textvariable=self.openai_key_var, show="*", width=50).grid(row=0, column=1, padx=5)

            ttk.Label(f_keys, text="Gemini:").grid(row=1, column=0, sticky='w')
            ttk.Entry(f_keys, textvariable=self.gemini_key_var, show="*", width=50).grid(row=1, column=1, padx=5)

            # File Selection
            f_file = ttk.LabelFrame(self, text="테스트 파일", padding=10)
            f_file.pack(fill='x', padx=10, pady=5)

            ttk.Entry(f_file, textvariable=self.file_var, width=60).pack(side='left', fill='x', expand=True)
            ttk.Button(f_file, text="찾기", command=self._select_file).pack(side='right')

            # Options
            f_opt = ttk.LabelFrame(self, text="테스트 옵션", padding=10)
            f_opt.pack(fill='x', padx=10, pady=5)

            ttk.Label(f_opt, text="최대 행 수:").grid(row=0, column=0)
            ttk.Spinbox(f_opt, from_=1, to=100, textvariable=self.max_rows_var, width=5).grid(row=0, column=1)

            ttk.Label(f_opt, text="마켓:").grid(row=0, column=2, padx=(20,0))
            ttk.Combobox(f_opt, textvariable=self.market_var, values=["네이버", "쿠팡", "기타"], width=10).grid(row=0, column=3)

            ttk.Label(f_opt, text="글자수:").grid(row=0, column=4, padx=(20,0))
            ttk.Spinbox(f_opt, from_=20, to=150, textvariable=self.max_len_var, width=5).grid(row=0, column=5)

            ttk.Label(f_opt, text="후보수:").grid(row=0, column=6, padx=(20,0))
            ttk.Spinbox(f_opt, from_=1, to=20, textvariable=self.num_cand_var, width=5).grid(row=0, column=7)

            # Run Button
            ttk.Button(self, text="🚀 테스트 실행", command=self._run_test).pack(pady=10)

            # Log
            f_log = ttk.LabelFrame(self, text="로그", padding=10)
            f_log.pack(fill='both', expand=True, padx=10, pady=5)

            self.log_widget = ScrolledText(f_log, height=20, font=("Consolas", 9))
            self.log_widget.pack(fill='both', expand=True)

        def _select_file(self):
            path = filedialog.askopenfilename(filetypes=[("Excel", "*.xlsx")])
            if path:
                self.file_var.set(path)

        def _log(self, msg):
            self.log_widget.insert('end', f"{msg}\n")
            self.log_widget.see('end')
            self.update()

        def _run_test(self):
            openai_key = self.openai_key_var.get().strip()
            gemini_key = self.gemini_key_var.get().strip()
            file_path = self.file_var.get().strip()

            if not file_path or not os.path.exists(file_path):
                messagebox.showerror("오류", "테스트 파일을 선택하세요.")
                return

            if not openai_key and not gemini_key:
                messagebox.showerror("오류", "최소 하나의 API 키가 필요합니다.")
                return

            # 키 저장
            if openai_key:
                save_api_key(OPENAI_API_KEY_FILE, openai_key)
            if gemini_key:
                save_api_key(GEMINI_API_KEY_FILE, gemini_key)

            self.log_widget.delete('1.0', 'end')
            self._log("테스트 시작...")

            try:
                results = run_comparison_test(
                    excel_path=file_path,
                    openai_key=openai_key,
                    gemini_key=gemini_key,
                    max_rows=self.max_rows_var.get(),
                    market=self.market_var.get(),
                    max_len=self.max_len_var.get(),
                    num_candidates=self.num_cand_var.get(),
                )

                # 결과 출력
                print_summary(results["summary"])

                # 결과 저장
                save_results(results, file_path)

                self._log("\n테스트 완료!")
                messagebox.showinfo("완료", "테스트가 완료되었습니다. 결과 파일을 확인하세요.")

            except Exception as e:
                self._log(f"\n오류: {e}")
                import traceback
                self._log(traceback.format_exc())
                messagebox.showerror("오류", str(e))

    app = TestGUI()
    app.mainloop()

# ============================================================
# Main
# ============================================================
if __name__ == "__main__":
    if len(sys.argv) > 1:
        # CLI 모드
        excel_path = sys.argv[1]

        openai_key = load_api_key(OPENAI_API_KEY_FILE) or os.environ.get("OPENAI_API_KEY", "")
        gemini_key = load_api_key(GEMINI_API_KEY_FILE) or os.environ.get("GOOGLE_API_KEY", "")

        results = run_comparison_test(
            excel_path=excel_path,
            openai_key=openai_key,
            gemini_key=gemini_key,
            max_rows=10,
        )

        print_summary(results["summary"])
        save_results(results, excel_path)
    else:
        # GUI 모드
        run_gui()
