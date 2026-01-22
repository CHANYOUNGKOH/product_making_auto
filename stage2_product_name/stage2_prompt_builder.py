# stage2_prompt_builder.py
import os
import pandas as pd

from stage2_core import (
    build_stage2_request_from_row,
    safe_str,
)


def process_excel_for_stage2(excel_path: str, log_func=print):
    """
    '*_with_detail_images.xlsx' 를 입력으로 받아,
    각 행마다 Stage2Request(prompt, image_paths)를 생성하고
    프롬프트만 ST2_프롬프트 컬럼에 저장한다.

    반환값: (out_path, skipped_cnt)
      - out_path: 생성된 엑셀 경로
      - skipped_cnt: ST1_결과상품명이 비어 있어서 건너뛴 행 수
    """
    log = log_func
    log(f"[INFO] 엑셀 읽는 중: {excel_path}")

    df = pd.read_excel(excel_path, header=0)

    required_cols = [
        "상품코드",
        "카테고리명",
        "원본상품명",
        "옵션1값",
        "이미지대",
        "본문상세설명",
    ]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"다음 필수 컬럼이 엑셀에 없습니다: {missing}")

    has_stage1 = "ST1_결과상품명" in df.columns
    if not has_stage1:
        log("[WARN] 'ST1_결과상품명' 컬럼이 없습니다. 기본상품명으로 원본상품명을 사용합니다.")
    if "키워드" not in df.columns:
        log("[WARN] '키워드' 컬럼이 없습니다. 키워드 입력은 빈 값으로 처리됩니다.")
    if "판매형태" not in df.columns:
        log("[WARN] '판매형태' 컬럼이 없습니다. 옵션1값+상품명으로 판매형태를 추론합니다.")

    # 상세이미지 컬럼 탐색
    detail_cols = [c for c in df.columns if str(c).startswith("상세이미지_")]
    if detail_cols:
        def sort_key(c):
            try:
                return int(str(c).split("_")[1])
            except Exception:
                return 9999
        detail_cols.sort(key=sort_key)
    log(f"[INFO] 상세이미지 컬럼들: {detail_cols}")

    # ST2 컬럼 준비
    if "ST2_프롬프트" not in df.columns:
        df["ST2_프롬프트"] = ""
    if "ST2_JSON" not in df.columns:
        df["ST2_JSON"] = ""

    # Stage1 비진행 행 체크
    skipped_cnt = 0
    if has_stage1:
        log("[INFO] ST1_결과상품명이 비어 있는 행은 Stage2에서 건너뜁니다.")

    log("[INFO] ST2_프롬프트 생성 중...")
    for idx, row in df.iterrows():
        # ST1_결과상품명이 비어 있으면 스킵
        if has_stage1 and not safe_str(row.get("ST1_결과상품명", "")):
            skipped_cnt += 1
            continue

        try:
            req = build_stage2_request_from_row(row, detail_cols)
            df.at[idx, "ST2_프롬프트"] = req.prompt
            # 필요하면 나중에 image_paths를 따로 컬럼에 기록 가능:
            # df.at[idx, "ST2_이미지리스트"] = "\n".join(req.image_paths)
        except Exception as e:
            log(f"[WARN] idx={idx} 프롬프트 생성 실패: {e}")

    base_dir = os.path.dirname(excel_path)
    base_name = os.path.splitext(os.path.basename(excel_path))[0]
    out_path = os.path.join(base_dir, f"{base_name}_stage2_prompts.xlsx")

    df.to_excel(out_path, index=False)
    log(f"[INFO] Stage2 프롬프트 포함 엑셀 저장 완료: {out_path}")
    if has_stage1 and skipped_cnt:
        log(f"[INFO] ST1_결과상품명 비어 있어 건너뛴 행 수: {skipped_cnt}개")

    return out_path, skipped_cnt
