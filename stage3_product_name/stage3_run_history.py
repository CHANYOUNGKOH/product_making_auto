# stage3_run_history.py
"""
Stage3 LLM 실행 이력 기록 모듈

- stage2_run_history.append_run_history 와 동일한 인터페이스 및 형식
- 단, 로그 파일은 stage3 전용: stage3_run_history.xlsx
"""

import os
from datetime import datetime
from typing import Optional

import pandas as pd

# 실행 이력 저장 엑셀 경로 (같은 폴더에 생성)
RUN_LOG_PATH = os.path.join(os.path.dirname(__file__), "stage3_run_history.xlsx")


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
) -> bool:
    """
    Stage3 실행 1회에 대한 요약 정보를 stage3_run_history.xlsx에 한 줄 추가.
    실패해도 메인 작업에는 영향 주지 않도록 try/except로 감싼다.
    
    Returns:
        bool: 기록 성공 여부 (중복 기록으로 건너뛴 경우 False)
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

        # 배치 ID가 있는 경우 중복 체크
        if batch_id:
            if os.path.exists(RUN_LOG_PATH):
                try:
                    old_df = pd.read_excel(RUN_LOG_PATH)
                    # batch_id 컬럼이 있고, 같은 batch_id가 이미 기록되어 있는지 확인
                    if "batch_id" in old_df.columns:
                        existing = old_df[old_df["batch_id"] == batch_id]
                        if len(existing) > 0:
                            print(f"[stage3_run_history] 배치 {batch_id}는 이미 기록되어 있습니다. 중복 기록을 건너뜁니다.")
                            return False  # 중복 기록 방지 (False 반환)
                except Exception as e:
                    print(f"[stage3_run_history] 중복 체크 중 오류 (계속 진행): {e}")
                    import traceback
                    print(traceback.format_exc())
                    # 파일 읽기 실패 시 계속 진행
        
        new_df = pd.DataFrame([row])

        if os.path.exists(RUN_LOG_PATH):
            try:
                old_df = pd.read_excel(RUN_LOG_PATH)
                df_all = pd.concat([old_df, new_df], ignore_index=True)
            except Exception as e:
                print(f"[stage3_run_history] 기존 파일 읽기 실패, 새로 시작: {e}")
                # 기존 로그 파일이 깨져 있으면 새로 시작
                df_all = new_df
        else:
            df_all = new_df

        # 파일 저장 시도 (PermissionError 처리)
        max_retries = 3
        for attempt in range(max_retries):
            try:
                df_all.to_excel(RUN_LOG_PATH, index=False)
                # 저장 성공 확인
                if os.path.exists(RUN_LOG_PATH):
                    print(f"[stage3_run_history] 로그 저장 완료: {RUN_LOG_PATH} (배치 ID: {batch_id or 'N/A'})")
                    return True  # 기록 성공
                else:
                    print(f"[WARN] stage3_run_history 파일이 저장되지 않았습니다: {RUN_LOG_PATH}")
                    return False
            except PermissionError:
                if attempt < max_retries - 1:
                    print(f"[WARN] stage3_run_history 파일이 열려있습니다. 재시도 중... ({attempt + 1}/{max_retries})")
                    import time
                    time.sleep(1)
                else:
                    print(f"[ERROR] stage3_run_history 파일 저장 실패: 파일이 열려있습니다. {RUN_LOG_PATH}")
                    return False
            except Exception as e:
                print(f"[ERROR] stage3_run_history 파일 저장 실패: {e}")
                return False
        
        return False
    except Exception as e:
        print(f"[WARN] Stage3 실행 이력 저장 실패: {e}")
        import traceback
        print(traceback.format_exc())
        return False  # 기록 실패
