# batch_stage1_core.py
"""
STAGE1 Batch API + 엑셀 병합 핵심 로직
"""

import os
import json
import time
import threading
from typing import Any, Dict, List, Tuple, Optional

import pandas as pd
from openai import OpenAI

from prompts_stage1 import build_stage1_prompt, safe_str
from stage1_run_history import append_run_history

# API 키 파일 경로 (GUI와 공유)
API_KEY_FILE = ".openai_api_key_batch"

# =======================
# 시간/타임존 유틸
# =======================
try:
    from zoneinfo import ZoneInfo  # Python 3.9+
except ImportError:  # pragma: no cover
    ZoneInfo = None  # type: ignore


def get_seoul_now():
    """
    Asia/Seoul 기준 현재 시각을 datetime으로 반환.
    zoneinfo 가 없으면 naive datetime 으로 fallback.
    """
    from datetime import datetime, timezone, timedelta

    if ZoneInfo is not None:
        return datetime.now(ZoneInfo("Asia/Seoul"))
    # fallback: UTC+9 고정
    return datetime.now(timezone(timedelta(hours=9)))


# =======================
# API 키 로드/저장
# =======================

def load_api_key_from_file() -> str:
    if os.path.exists(API_KEY_FILE):
        try:
            with open(API_KEY_FILE, "r", encoding="utf-8") as f:
                return f.read().strip()
        except Exception:
            return ""
    return ""


def save_api_key_to_file(key: str) -> None:
    try:
        with open(API_KEY_FILE, "w", encoding="utf-8") as f:
            f.write(key.strip())
    except Exception as e:
        print(f"[WARN] API 키 저장 실패: {e}")


# =======================
# 토큰 단가 & 비용 계산 (ST1 전용)
# =======================

# 모델별 100만 토큰당 단가 (USD) - ST1 러너와 동일하게 맞춤
MODEL_PRICING: Dict[str, Dict[str, float]] = {
    # 참고: 실제 가격은 OpenAI 공식 문서 기준으로 필요 시 수정
    "gpt-5": {
        "input_per_million": 1.250,
        "cached_input_per_million": 0.125,
        "output_per_million": 10.000,
    },
    "gpt-5-mini": {
        "input_per_million": 0.250,
        "cached_input_per_million": 0.025,
        "output_per_million": 1.250,
    },
    "gpt-5-nano": {
        "input_per_million": 0.050,
        "cached_input_per_million": 0.005,
        "output_per_million": 0.300,
    },
}


def compute_cost_usd(
    model_name: str,
    total_input_tokens: int,
    total_output_tokens: int,
) -> Optional[Dict[str, float]]:
    """
    모델별 토큰 단가를 이용해 대략적인 비용(USD) 계산.
    - 캐시 입력 토큰은 아직 구분하지 않으므로 일반 입력 단가만 사용.
    - 모델 정보가 없으면 None 반환.
    """
    pricing = MODEL_PRICING.get(model_name)
    if not pricing:
        return None

    in_million = total_input_tokens / 1_000_000.0
    out_million = total_output_tokens / 1_000_000.0

    input_cost = in_million * pricing["input_per_million"]
    output_cost = out_million * pricing["output_per_million"]
    total_cost = input_cost + output_cost

    return {
        "input_cost": input_cost,
        "output_cost": output_cost,
        "total_cost": total_cost,
    }


# =====================================
# 응답 텍스트 / 사용량 추출
# =====================================

def extract_text_from_response_dict(resp: Dict[str, Any]) -> str:
    """
    Batch 결과 JSONL 안의 'response' 딕셔너리에서
    사람이 읽을 텍스트만 뽑아내는 함수.

    ⚠️ 주의:
    Batch API에서는 한 줄이 이런 형태다.
      {
        "custom_id": "row-0",
        "response": {
          "status_code": 200,
          "request_id": "res_xxx",
          "body": { ...responses.create 결과... }
        },
        "error": null
      }

    그래서 먼저 resp["body"] 를 꺼내서 그 안에서 output 을 찾아야 한다.
    """
    try:
        # 1) Batch 응답 envelope 풀기 (status_code / body 구조)
        body = resp.get("body") if isinstance(resp, dict) and "body" in resp else resp

        chunks: List[str] = []

        # 2) Responses API 표준 구조: body["output"][..]["content"][..]["text"]
        output_list = body.get("output") or []
        for out in output_list:
            o_type = out.get("type")
            # type 이 따로 안 붙거나 "message" 인 경우만 사용
            if o_type not in (None, "message"):
                continue

            content_list = out.get("content") or []
            for c in content_list:
                t_obj = c.get("text")
                if isinstance(t_obj, str):
                    # text 가 그냥 문자열일 때
                    chunks.append(t_obj)
                elif isinstance(t_obj, dict):
                    # {"value": "..."} 형태일 때
                    val = t_obj.get("value")
                    if isinstance(val, str):
                        chunks.append(val)

        if chunks:
            full_text = "\n".join(chunks).strip()
            # 우리는 "정제된 상품명 한 줄"만 필요하니까 첫 줄만 사용
            first_line = full_text.splitlines()[0].strip()
            return first_line

    except Exception:
        # 여기서 에러 나더라도 아래 fallback 으로 넘어가도록 조용히 무시
        pass

    # 3) 혹시 body 에 output_text 필드만 있는 경우 (미래 호환용)
    maybe = resp.get("output_text") if isinstance(resp, dict) else None
    if isinstance(maybe, str) and maybe.strip():
        return maybe.strip()

    return ""


def extract_usage_from_response_dict(resp: Dict[str, Any]) -> Tuple[int, int, int]:
    """
    Batch 결과 JSONL 안의 'response' 딕셔너리에서
    토큰 사용량 (input, output, reasoning)을 추출.
    """
    try:
        body = resp.get("body") if isinstance(resp, dict) and "body" in resp else resp
        usage = body.get("usage") or {}
        in_tok = int(usage.get("input_tokens") or 0)
        out_tok = int(usage.get("output_tokens") or 0)

        reasoning_tok = 0
        details = usage.get("output_tokens_details") or {}
        if isinstance(details, dict):
            reasoning_tok = int(details.get("reasoning_tokens") or 0)

        return in_tok, out_tok, reasoning_tok
    except Exception:
        return 0, 0, 0


# =====================================
# Batch API 핵심 로직
# =====================================

def create_batch_input_jsonl(
    excel_path: str,
    jsonl_path: str,
    model_name: str = "gpt-5-mini",
    reasoning_effort: str = "low",
):
    """
    엑셀 파일(원본상품명, 카테고리명, 판매형태) → Batch API용 JSONL 생성.
    - 카테고리명 / 판매형태 / 원본상품명 중 하나라도 비어 있으면 그 행은 JSONL에서 제외.
    - 제외된 행은 별도 엑셀 파일(<원본명>_stage1_skipped_rows.xlsx)에 저장.
    - 반환값(info_dict)으로 전체/변환/제외 개수와 제외파일 경로를 돌려줌.
    """
    df = pd.read_excel(excel_path)

    required_cols = ["원본상품명", "카테고리명", "판매형태"]
    for col in required_cols:
        if col not in df.columns:
            raise ValueError(f"엑셀에 필수 컬럼이 없습니다: {col}")

    total_rows = len(df)
    written_count = 0
    skipped_rows: List[Dict[str, Any]] = []

    with open(jsonl_path, "w", encoding="utf-8") as f:
        for idx, row in df.iterrows():
            raw_name = safe_str(row["원본상품명"])
            category = safe_str(row["카테고리명"])
            sale_type = safe_str(row["판매형태"])

            missing_fields = []
            if not category:
                missing_fields.append("카테고리명")
            if not sale_type:
                missing_fields.append("판매형태")
            if not raw_name:
                missing_fields.append("원본상품명")

            # 하나라도 비어 있으면 JSONL에는 안 쓰고, 스킵 목록에만 저장
            if missing_fields:
                skipped_rows.append({
                    "엑셀_인덱스": idx,
                    "누락항목": ", ".join(missing_fields),
                    "카테고리명": category,
                    "판매형태": sale_type,
                    "원본상품명": raw_name,
                })
                continue

            prompt_text = build_stage1_prompt(category, sale_type, raw_name)

            body: Dict[str, Any] = {
                "model": model_name,
                "input": [
                    {"role": "user", "content": prompt_text}
                ],
                "reasoning": {"effort": reasoning_effort},
            }

            item = {
                "custom_id": f"row-{idx}",
                "method": "POST",
                "url": "/v1/responses",
                "body": body,
            }

            f.write(json.dumps(item, ensure_ascii=False) + "\n")
            written_count += 1

    # 스킵된 행 요약 엑셀 저장
    skipped_path = ""
    if skipped_rows:
        base_dir = os.path.dirname(excel_path)
        base_name = os.path.splitext(os.path.basename(excel_path))[0]
        skipped_path = os.path.join(base_dir, f"{base_name}_stage1_skipped_rows.xlsx")
        skipped_df = pd.DataFrame(skipped_rows)
        skipped_df.to_excel(skipped_path, index=False)

    # 간단 요약 반환
    info = {
        "total_rows": total_rows,
        "written_count": written_count,
        "skipped_count": len(skipped_rows),
        "skipped_path": skipped_path,
    }
    return info


def submit_batch(jsonl_path: str, client: OpenAI, completion_window: str = "24h") -> str:
    """
    JSONL 파일 업로드 후 Batch 생성, batch_id 반환
    """
    with open(jsonl_path, "rb") as f:
        file_obj = client.files.create(
            file=f,
            purpose="batch",
        )

    batch = client.batches.create(
        input_file_id=file_obj.id,
        endpoint="/v1/responses",
        completion_window=completion_window,
    )
    return batch.id


def wait_and_collect_batch(
    batch_id: str,
    excel_path: str,
    output_excel_path: str,
    client: OpenAI,
    poll_interval_sec: int = 30,
    log_fn=None,
    stop_event: Optional[threading.Event] = None,
    model_name: Optional[str] = None,
    reasoning_effort: Optional[str] = None,
) -> None:
    """
    - batch_id 상태를 폴링해서 completed 되면
    - output JSONL을 다운로드하고
    - custom_id(row-0, row-1, ...) 기준으로 ST1_정제상품명 컬럼에 병합
    - stop_event 가 set 되면 수집 중단
    - 수집이 정상 완료되면 stage1_run_history 에 토큰/비용 로그 남김
    """
    def log(msg: str):
        if log_fn:
            log_fn(msg)
        else:
            print(msg)

    def check_stop():
        if stop_event is not None and stop_event.is_set():
            log("[COLLECT] 사용자 요청으로 수집 중단.")
            raise RuntimeError("사용자가 결과 수집을 중단했습니다.")

    start_dt = get_seoul_now()
    start_time = time.time()

    log(f"[COLLECT] batch_id={batch_id} 상태 조회 시작...")

    # 1) Batch 상태 폴링
    while True:
        check_stop()

        batch = client.batches.retrieve(batch_id)
        log(f"  - status={batch.status}, request_counts={getattr(batch, 'request_counts', None)}")
        if batch.status in ("completed", "failed", "cancelled", "expired"):
            break

        # poll_interval_sec 동안 1초 단위로 끊어서 중단 여부 체크
        for _ in range(poll_interval_sec):
            check_stop()
            time.sleep(1)

    check_stop()

    if batch.status != "completed":
        raise RuntimeError(f"배치가 완료 상태가 아닙니다: status={batch.status}")

    output_file_id = getattr(batch, "output_file_id", None)
    if not output_file_id:
        # 신버전에서 output_file_ids 배열일 수도 있으므로 보조 처리
        output_ids = getattr(batch, "output_file_ids", None)
        if output_ids and isinstance(output_ids, (list, tuple)) and len(output_ids) > 0:
            output_file_id = output_ids[0]

    if not output_file_id:
        raise RuntimeError("batch.output_file_id 를 찾을 수 없습니다.")

    log(f"[COLLECT] output_file_id={output_file_id} 다운로드 중...")
    file_content = client.files.content(output_file_id)

    if hasattr(file_content, "read"):
        data_bytes = file_content.read()
    elif hasattr(file_content, "iter_bytes"):
        # 일부 클라이언트 구현에서는 iter_bytes() 로 chunk 가 올 수 있음
        chunks = []
        for ch in file_content.iter_bytes():
            chunks.append(ch)
        data_bytes = b"".join(chunks)
    else:
        data_bytes = file_content  # type: ignore

    text = data_bytes.decode("utf-8")
    lines = [ln for ln in text.splitlines() if ln.strip()]

    # 2) JSONL 한 줄씩 파싱 → 결과/토큰 집계
    result_map: Dict[str, str] = {}
    total_in_tok = 0
    total_out_tok = 0
    total_reasoning_tok = 0
    api_rows = 0

    for ln in lines:
        obj = json.loads(ln)
        custom_id = obj.get("custom_id")
        resp = obj.get("response")
        error = obj.get("error")

        if error is not None:
            log(f"[ERROR] custom_id={custom_id} 에러 발생: {error}")
            continue
        if not resp:
            continue

        refined = extract_text_from_response_dict(resp)
        result_map[custom_id] = refined

        in_tok, out_tok, reasoning_tok = extract_usage_from_response_dict(resp)
        total_in_tok += in_tok
        total_out_tok += out_tok
        total_reasoning_tok += reasoning_tok
        api_rows += 1

    log(f"[COLLECT] 결과 매핑 개수: {len(result_map)}")
    log(
        f"[USAGE] API 호출 수(api_rows)={api_rows}, "
        f"input_tokens={total_in_tok}, output_tokens={total_out_tok}, "
        f"reasoning_tokens={total_reasoning_tok}"
    )

    # 3) 엑셀 병합
    df = pd.read_excel(excel_path)
    total_rows = len(df)

    if "ST1_정제상품명" not in df.columns:
        df["ST1_정제상품명"] = ""
    if "ST1_판매형태" not in df.columns:
        df["ST1_판매형태"] = ""

    for idx in range(len(df)):
        cid = f"row-{idx}"
        if cid in result_map:
            df.at[idx, "ST1_정제상품명"] = result_map[cid]
            df.at[idx, "ST1_판매형태"] = safe_str(df.at[idx, "판매형태"])

    df.to_excel(output_excel_path, index=False)
    log(f"[COLLECT] 엑셀 병합 완료: {output_excel_path}")

    # 4) 비용 계산 + 러닝 타임/히스토리 기록
    elapsed_seconds = time.time() - start_time
    finish_dt = get_seoul_now()

    input_cost_usd = None
    output_cost_usd = None
    total_cost_usd = None

    if model_name:
        cost_info = compute_cost_usd(model_name, total_in_tok, total_out_tok)
        if cost_info:
            input_cost_usd = cost_info["input_cost"]
            output_cost_usd = cost_info["output_cost"]
            total_cost_usd = cost_info["total_cost"]
            log(
                f"[COST] model={model_name}, "
                f"input=${input_cost_usd:.6f}, output=${output_cost_usd:.6f}, "
                f"total=${total_cost_usd:.6f}"
            )

    # stage1_run_history.xlsx 에 한 줄 추가 (ST1-BATCH)
    try:
        append_run_history(
            stage="ST1-BATCH",
            model_name=model_name or "(unknown)",
            reasoning_effort=reasoning_effort or "(unknown)",
            src_file=excel_path,
            total_rows=total_rows,
            api_rows=api_rows,
            elapsed_seconds=elapsed_seconds,
            total_in_tok=total_in_tok,
            total_out_tok=total_out_tok,
            total_reasoning_tok=total_reasoning_tok,
            input_cost_usd=input_cost_usd,
            output_cost_usd=output_cost_usd,
            total_cost_usd=total_cost_usd,
            start_dt=start_dt,
            finish_dt=finish_dt,
        )
        log("[INFO] stage1_run_history.xlsx 에 ST1-BATCH 실행 기록 추가 완료.")
    except Exception as e:
        log(f"[WARN] 실행 이력 기록(stage1_run_history) 중 예외 발생: {e}")
