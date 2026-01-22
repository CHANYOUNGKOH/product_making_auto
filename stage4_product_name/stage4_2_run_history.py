"""
stage4_2_run_history.py

Stage 4-2 실행 이력(Run Summary)을 엑셀로 저장하는 모듈
Stage 3와 포맷 및 인터페이스 통일
"""

import os
import pandas as pd
from datetime import datetime
from typing import Optional

# 실행 이력 저장 엑셀 경로
RUN_LOG_PATH = os.path.join(os.path.dirname(__file__), "stage4_2_run_history.xlsx")

def append_run_history(
    stage: str,
    model_name: str,
    reasoning_effort: str,
    src_file: str,
    total_rows: int,
    api_rows: int,
    elapsed_seconds: float,
    total_in_tok: int,
    total_out_tok: int,
    total_reasoning_tok: int,
    input_cost_usd: float,
    output_cost_usd: float,
    total_cost_usd: float,
    start_dt: datetime,
    finish_dt: datetime,
    api_type: str = "per_call",
    batch_id: Optional[str] = None,
    out_file: Optional[str] = None,
    success_rows: Optional[int] = None,
    fail_rows: Optional[int] = None,
) -> None:
    
    # 데이터 구성
    row_data = {
        "start_time": start_dt.strftime("%Y-%m-%d %H:%M:%S"),
        "finish_time": finish_dt.strftime("%Y-%m-%d %H:%M:%S"),
        "elapsed_seconds": round(elapsed_seconds, 2),
        "stage": stage,
        "api_type": api_type,
        "batch_id": batch_id,
        "model_name": model_name,
        "reasoning_effort": reasoning_effort,
        "src_file": src_file,
        "out_file": out_file,
        "total_rows": total_rows,
        "api_rows": api_rows,
        "success_rows": success_rows,
        "fail_rows": fail_rows,
        "input_tokens": total_in_tok,
        "output_tokens": total_out_tok,
        "reasoning_tokens": total_reasoning_tok,
        "input_cost_usd": round(input_cost_usd, 6),
        "output_cost_usd": round(output_cost_usd, 6),
        "total_cost_usd": round(total_cost_usd, 6),
    }

    new_df = pd.DataFrame([row_data])

    if os.path.exists(RUN_LOG_PATH):
        try:
            # openpyxl 엔진 사용 명시
            old_df = pd.read_excel(RUN_LOG_PATH, engine="openpyxl")
            combined_df = pd.concat([old_df, new_df], ignore_index=True)
            combined_df.to_excel(RUN_LOG_PATH, index=False)
        except Exception as e:
            print(f"[History Error] 엑셀 저장 실패: {e}")
            # 백업 시도
            backup = RUN_LOG_PATH.replace(".xlsx", f"_{int(datetime.now().timestamp())}.xlsx")
            new_df.to_excel(backup, index=False)
    else:
        try:
            new_df.to_excel(RUN_LOG_PATH, index=False)
        except Exception as e:
            print(f"[History Error] 엑셀 생성 실패: {e}")