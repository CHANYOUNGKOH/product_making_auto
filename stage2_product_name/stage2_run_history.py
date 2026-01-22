# stage2_run_history.py
import os
from datetime import datetime
from typing import Optional

import pandas as pd

# 실행 이력 저장 엑셀 경로 (같은 폴더에 생성)
RUN_LOG_PATH = os.path.join(os.path.dirname(__file__), "stage2_run_log.xlsx")


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
    input_cost_usd: Optional[float],
    output_cost_usd: Optional[float],
    total_cost_usd: Optional[float],
    start_dt: datetime,
    finish_dt: datetime,
    # ----- 아래부터 옵션 -----
    api_type: str = "per_call",                 # "per_call" / "batch"
    batch_id: Optional[str] = None,             # 배치일 때만 값, 건바이건이면 None
    out_file: str = "",                         # 결과 엑셀 경로 (있으면 기록)
    success_rows: Optional[int] = None,         # 성공 행 수
    fail_rows: Optional[int] = None,            # 실패 행 수
) -> None:
    """
    Stage2 실행 1회에 대한 요약 정보를 stage2_run_log.xlsx에 한 줄 추가.
    실패해도 메인 작업에는 영향 주지 않도록 try/except로 감싼다.
    """
    try:
        total_tokens = total_in_tok + total_out_tok + total_reasoning_tok

        # 파생값 계산
        sec_per_all_row = (elapsed_seconds / total_rows) if total_rows > 0 else None
        sec_per_api_row = (elapsed_seconds / api_rows) if api_rows > 0 else None

        cost_per_1k = None
        if total_cost_usd is not None and total_tokens > 0:
            cost_per_1k = total_cost_usd / (total_tokens / 1000.0)

        cost_per_api_row = None
        if total_cost_usd is not None and api_rows > 0:
            cost_per_api_row = total_cost_usd / api_rows

        row = {
            "stage": stage,
            "api_type": api_type,
            "batch_id": batch_id,
            "start_time": start_dt.strftime("%Y-%m-%d %H:%M:%S"),
            "finish_time": finish_dt.strftime("%Y-%m-%d %H:%M:%S"),
            "elapsed_seconds": elapsed_seconds,
            "src_file": src_file,
            "out_file": out_file,
            "total_rows": total_rows,
            "api_rows": api_rows,
            "success_rows": success_rows,
            "fail_rows": fail_rows,
            "model_name": model_name,
            "reasoning_effort": reasoning_effort,
            "input_tokens": total_in_tok,
            "output_tokens": total_out_tok,
            "reasoning_tokens": total_reasoning_tok,
            "total_tokens": total_tokens,
            "input_cost_usd": input_cost_usd,
            "output_cost_usd": output_cost_usd,
            "total_cost_usd": total_cost_usd,
            "cost_per_1k_tokens": cost_per_1k,
            "cost_per_api_row": cost_per_api_row,
            "sec_per_all_row": sec_per_all_row,
            "sec_per_api_row": sec_per_api_row,
        }

        new_df = pd.DataFrame([row])

        if os.path.exists(RUN_LOG_PATH):
            try:
                old_df = pd.read_excel(RUN_LOG_PATH)
                df_all = pd.concat([old_df, new_df], ignore_index=True)
            except Exception:
                # 기존 로그 파일이 깨져 있으면 새로 시작
                df_all = new_df
        else:
            df_all = new_df

        df_all.to_excel(RUN_LOG_PATH, index=False)
    except Exception as e:
        print(f"[WARN] Stage2 실행 이력 저장 실패: {e}")
