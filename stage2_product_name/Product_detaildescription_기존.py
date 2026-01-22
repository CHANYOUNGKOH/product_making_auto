# Product_detaildescription.py
import os
import re
import math
import threading
import traceback
from requests.utils import requote_uri
from requests.exceptions import ConnectTimeout, ReadTimeout, SSLError

import pandas as pd
import requests
from bs4 import BeautifulSoup
from PIL import Image, ImageStat
from io import BytesIO

import tkinter as tk
from tkinter import filedialog, messagebox
from tkinter.scrolledtext import ScrolledText


# =========================
# 설정값
# =========================

# URL에 이 단어들이 들어가면 "공지/배송/이벤트/안내" 이미지로 보고 제외
NOISE_URL_KEYWORDS = [
    "notice", "delivery", "ship", "shipping", "배송", "교환", "반품", "환불",
    "exchange", "return", "event", "banner", "coupon", "gift", "guide", "info"
]

# 세로 픽셀 기준 (품질용)
MAX_HEIGHT_1 = 4000   # 이하면 그대로 1장
MAX_HEIGHT_2 = 8000   # 이 사이면 2장, 그 이상이면 3장

# 한 상품당 상세이미지 최대 개수
# - None 으로 두면 제한 없음
# - 9 로 설정하면 detail_01 ~ detail_09 까지만 저장
MAX_DETAIL_IMAGES = None  # 또는 9 로 바꿔도 됨

# ★ 추가: 엑셀 컬럼명 후보들
PRODUCT_CODE_COL_CANDIDATES = ["상품코드", "판매자관리코드", "판매자관리코드1"]
DETAIL_DESC_COL_CANDIDATES = ["상세설명", "본문상세설명"]


# =========================
# 유틸 함수
# =========================

def sanitize_code(code):
    """파일명 / 폴더명으로 쓰기 안전하게 상품코드를 정리."""
    if code is None:
        return "unknown"
    code = str(code).strip()
    # 파일명에 문제될 수 있는 문자 제거
    code = re.sub(r"[\\/*?:\"<>|]", "_", code)
    if not code:
        code = "unknown"
    return code


def is_noise_image(url: str) -> bool:
    """URL에 노이즈 키워드가 있으면 True."""
    lower = url.lower()
    return any(k in lower for k in NOISE_URL_KEYWORDS)


def extract_image_urls_from_html(html: str):
    """상세설명 HTML에서 <img src=...> 전부 추출."""
    if not isinstance(html, str) or not html.strip():
        return []

    soup = BeautifulSoup(html, "html.parser")
    urls = []
    for img in soup.find_all("img"):
        src = img.get("src")
        if not src:
            continue
        src = src.strip()
        if not src:
            continue
        urls.append(src)
    return urls


HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}


def download_image(url: str) -> Image.Image:
    """
    이미지 URL을 다운로드해서 PIL Image로 반환.
    - 한글/특수문자는 requote_uri로 인코딩
    - HTTPS 기준으로 최대 3번까지 재시도
    - 그래도 실패하면 HTTP로 한 번 더 시도(verify=False)
    """

    fixed_url = requote_uri(url)

    def bytes_to_image(data: bytes) -> Image.Image:
        img = Image.open(BytesIO(data))
        if img.mode in ("P", "RGBA"):
            img = img.convert("RGB")
        return img

    last_err = None

    # 1) HTTPS로 3회까지 재시도 (connect 30초, read 120초)
    for attempt in range(3):
        try:
            resp = requests.get(
                fixed_url,
                timeout=(30, 120),   # (connect timeout, read timeout)
                headers=HEADERS,
            )
            resp.raise_for_status()
            return bytes_to_image(resp.content)

        except (ConnectTimeout, ReadTimeout) as e:
            # 연결/읽기 시간 초과 → 한 번 더 시도
            last_err = e
            continue
        except SSLError as e:
            # SSL 문제는 나중에 HTTP fallback에서 다시 시도
            last_err = e
            break
        except Exception as e:
            # 기타 에러는 재시도해도 의미 없을 수 있으니 바로 break
            last_err = e
            break

    # 2) 마지막 시도: HTTP + verify=False (보안은 떨어지지만, 다운로드 우선)
    #    designliving 같은 서버가 HTTPS 핸드셰이크에 예민한 경우 대비
    fallback_url = fixed_url.replace("https://", "http://")

    try:
        resp = requests.get(
            fallback_url,
            timeout=(30, 120),
            headers=HEADERS,
            verify=False,  # SSL 검증 비활성 (http라 의미 거의 없음)
        )
        resp.raise_for_status()
        return bytes_to_image(resp.content)
    except Exception as e:
        # 여기까지 실패하면 최종 에러를 다시 던짐
        raise last_err or e


def split_long_image(img: Image.Image):
    """
    세로가 너무 긴 이미지는 2~3조각으로 나눈다.
    - height <= MAX_HEIGHT_1: 1장 그대로
    - MAX_HEIGHT_1 < height <= MAX_HEIGHT_2: 2장
    - height > MAX_HEIGHT_2: 3장
    반환: [Image, Image, ...]
    """
    width, height = img.size

    if height <= MAX_HEIGHT_1:
        return [img]

    if height <= MAX_HEIGHT_2:
        num_splits = 2
    else:
        num_splits = 3

    split_height = math.ceil(height / num_splits)
    parts = []

    for i in range(num_splits):
        upper = i * split_height
        lower = min((i + 1) * split_height, height)
        if upper >= lower:
            continue
        crop_box = (0, upper, width, lower)
        part_img = img.crop(crop_box)
        parts.append(part_img)

    return parts

def is_small_or_blank_image(img: Image.Image) -> bool:
    """
    1) 너무 얇은 라인 이미지 (가로/세로 10px 이하)
    2) 아이콘 수준으로 작으면서(양변 < 200) 거의 단색인 이미지
    → 노이즈로 판단해서 True 리턴
    """
    w, h = img.size

    # 1) 한쪽이라도 10px 이하인 얇은 라인/점 이미지는 무조건 노이즈
    if w <= 10 or h <= 10:
        return True

    # 2) 둘 다 작은데(아이콘급) 거의 단색이면 노이즈
    if w < 200 and h < 200:
        stat = ImageStat.Stat(img.convert("L"))
        if stat.stddev[0] < 5:  # 픽셀 밝기 변화가 거의 없음
            return True

    return False

# =========================
# 메인 처리 로직
# =========================

def find_existing_column(df: pd.DataFrame, candidates, logical_name: str) -> str:
    """
    df 안에서 candidates 중 실제 존재하는 첫 번째 컬럼명을 리턴.
    하나도 없으면 에러.
    """
    for col in candidates:
        if col in df.columns:
            return col
    raise ValueError(
        f"엑셀에서 '{logical_name}'에 해당하는 컬럼을 찾을 수 없습니다. "
        f"다음 중 하나의 컬럼명이 필요합니다: {candidates} / 실제 컬럼: {list(df.columns)}"
    )
def process_excel(excel_path: str, log_func=print):
    """
    엑셀을 읽어서 상품코드/상세설명 컬럼을 기준으로
    상세 이미지들을 로컬에 저장한다.

    + 추가 기능:
      - 각 행(상품코드)별로 실제 '사용'된 상세이미지 경로를 모아서
        엑셀 DF에 '상세이미지_1' ~ '상세이미지_N' 컬럼으로 붙인다.
      - 결과 엑셀은 원본 엑셀 이름 뒤에 '_with_detail_images'를 붙여 저장한다.
      - 기존처럼 detail_image_mapping.xlsx 도 별도 저장(에러/노이즈 확인용).
    """
    log = log_func

    base_dir = os.path.dirname(excel_path)
    excel_name = os.path.splitext(os.path.basename(excel_path))[0]

    # 상세이미지 저장 루트 폴더
    output_root = os.path.join(base_dir, f"{excel_name}_detail_images")
    os.makedirs(output_root, exist_ok=True)

    log(f"[INFO] 엑셀 읽는 중: {excel_path}")
    df = pd.read_excel(excel_path, sheet_name=0, header=0)

    # --- 컬럼명 자동 매핑 ---
    product_code_col = find_existing_column(
        df, PRODUCT_CODE_COL_CANDIDATES, "상품코드/판매자관리코드"
    )
    detail_desc_col = find_existing_column(
        df, DETAIL_DESC_COL_CANDIDATES, "상세설명/본문상세설명"
    )

    log(f"[INFO] 상품코드 컬럼: '{product_code_col}', 상세설명 컬럼: '{detail_desc_col}' 사용")

    mapping_records = []          # detail_image_mapping.xlsx용
    detail_paths_per_row = {}     # 각 DF row index별 사용 이미지 경로 리스트

    total_rows = len(df)
    log(f"[INFO] 총 상품 수: {total_rows}")

    for idx, row in df.iterrows():
        raw_code = row[product_code_col]
        html = row[detail_desc_col]

        safe_code = sanitize_code(str(raw_code))
        product_dir = os.path.join(output_root, safe_code)
        os.makedirs(product_dir, exist_ok=True)

        # 엑셀 상 실제 행번호 (1행 = 헤더라고 가정)
        excel_row_no = idx + 2

        # 이 행에서 실제로 저장된 상세이미지 경로들
        saved_paths = []

        # 상세설명이 비어 있으면 스킵
        if not isinstance(html, str) or not html.strip():
            log(f"[WARN] ({safe_code}) 상세설명 비어 있음 → 스킵")
            detail_paths_per_row[idx] = []
            continue

        log(f"[INFO] ({safe_code}) 상세 이미지 추출 시작 ({idx+1}/{total_rows})")

        # 1) HTML에서 이미지 URL 추출
        urls = extract_image_urls_from_html(html)
        if not urls:
            log(f"[WARN] ({safe_code}) 이미지 URL 없음 → 스킵")
            detail_paths_per_row[idx] = []
            continue

        detail_count = 0  # 이 상품에서 실제 저장한 상세이미지 개수

        # 원본 URL 순서대로 처리
        for img_idx, url in enumerate(urls, start=1):
            # 1-1) 노이즈 URL인지 먼저 체크
            if is_noise_image(url):
                mapping_records.append({
                    "엑셀행번호": excel_row_no,
                    "상품코드": raw_code,
                    "이미지순번": img_idx,
                    "분할index": 0,
                    "사용여부": "제외",
                    "제외사유": "노이즈URL",
                    "저장폴더": "",
                    "저장파일명": "",
                    "원본URL": url
                })
                continue

            # 최대 개수 제한 (이미지 자체 개수 제한 옵션)
            if MAX_DETAIL_IMAGES is not None and detail_count >= MAX_DETAIL_IMAGES:
                mapping_records.append({
                    "엑셀행번호": excel_row_no,
                    "상품코드": raw_code,
                    "이미지순번": img_idx,
                    "분할index": 0,
                    "사용여부": "제외",
                    "제외사유": "최대이미지개수초과",
                    "저장폴더": "",
                    "저장파일명": "",
                    "원본URL": url
                })
                continue

            # 1-2) 다운로드 시도
            try:
                img = download_image(url)
            except ConnectTimeout as e:
                log(f"[ERROR] ({safe_code}) 연결시간초과: {url} / {e}")
                mapping_records.append({
                    "엑셀행번호": excel_row_no,
                    "상품코드": raw_code,
                    "이미지순번": img_idx,
                    "분할index": 0,
                    "사용여부": "제외",
                    "제외사유": "연결시간초과",
                    "저장폴더": "",
                    "저장파일명": "",
                    "원본URL": url
                })
                continue
            except Exception as e:
                log(f"[ERROR] ({safe_code}) 이미지 다운로드 실패: {url} / {e}")
                mapping_records.append({
                    "엑셀행번호": excel_row_no,
                    "상품코드": raw_code,
                    "이미지순번": img_idx,
                    "분할index": 0,
                    "사용여부": "제외",
                    "제외사유": "다운로드실패",
                    "저장폴더": "",
                    "저장파일명": "",
                    "원본URL": url
                })
                continue

            # 1-3) 너무 작거나, 거의 단색이면 노이즈로 제외
            if is_small_or_blank_image(img):
                log(f"[INFO] ({safe_code}) 너무 작은/빈 이미지 {img.size} → 스킵")
                mapping_records.append({
                    "엑셀행번호": excel_row_no,
                    "상품코드": raw_code,
                    "이미지순번": img_idx,
                    "분할index": 0,
                    "사용여부": "제외",
                    "제외사유": "이미지너무작음",
                    "저장폴더": "",
                    "저장파일명": "",
                    "원본URL": url
                })
                continue

            # 2) 긴 이미지면 분할
            sub_images = split_long_image(img)

            for part_idx, sub_img in enumerate(sub_images, start=1):
                # 이미지 개수 제한 체크
                if MAX_DETAIL_IMAGES is not None and detail_count >= MAX_DETAIL_IMAGES:
                    mapping_records.append({
                        "엑셀행번호": excel_row_no,
                        "상품코드": raw_code,
                        "이미지순번": img_idx,
                        "분할index": part_idx,
                        "사용여부": "제외",
                        "제외사유": "최대이미지개수초과",
                        "저장폴더": "",
                        "저장파일명": "",
                        "원본URL": url
                    })
                    break

                detail_num = detail_count + 1
                filename = f"{safe_code}_detail_{detail_num:02d}.jpg"
                save_path = os.path.join(product_dir, filename)

                try:
                    sub_img.save(save_path, format="JPEG", quality=90)
                    used_flag = "사용"
                    reason = ""
                except Exception as e:
                    log(f"[ERROR] ({safe_code}) 이미지 저장 실패: {save_path} / {e}")
                    used_flag = "제외"
                    reason = "저장실패"
                    save_path = ""
                    filename = ""

                mapping_records.append({
                    "엑셀행번호": excel_row_no,
                    "상품코드": raw_code,
                    "이미지순번": img_idx,
                    "분할index": part_idx,
                    "사용여부": used_flag,
                    "제외사유": reason,
                    "저장폴더": product_dir if used_flag == "사용" else "",
                    "저장파일명": filename,
                    "원본URL": url
                })

                if used_flag == "사용":
                    detail_count += 1
                    # 엑셀에 넣을 때는 절대경로가 편하니 abs 경로로 저장
                    saved_paths.append(os.path.abspath(save_path))

        # 이 행에서 실제 사용된 이미지 경로 리스트 저장
        detail_paths_per_row[idx] = saved_paths

        log(f"[INFO] ({safe_code}) 저장된 상세이미지 수: {detail_count}")

    # --- 2차: 엑셀 DF에 상세이미지 경로 컬럼 붙이기 ---
    if detail_paths_per_row:
        # 1) 이 엑셀에서 필요한 최대 컬럼 개수 계산
        max_cols = max(len(paths) for paths in detail_paths_per_row.values())

        # 2) 컬럼 생성 (상세이미지_1 ~ 상세이미지_max_cols)
        for i in range(max_cols):
            col_name = f"상세이미지_{i+1}"
            if col_name not in df.columns:
                df[col_name] = ""

        # 3) 각 행별로 경로 채우기 (순서 그대로)
        for row_idx, paths in detail_paths_per_row.items():
            for i, p in enumerate(paths):
                col_name = f"상세이미지_{i+1}"
                df.at[row_idx, col_name] = p

        # 4) 새 엑셀로 저장
        out_excel_path = os.path.join(base_dir, f"{excel_name}_with_detail_images.xlsx")
        df.to_excel(out_excel_path, index=False)
        log(f"[INFO] 상세이미지 경로 포함 엑셀 저장: {out_excel_path}")
    else:
        log("[INFO] 어떤 행에서도 상세이미지가 저장되지 않아 엑셀 갱신 없음.")

    # --- 이미지 매핑 정보 엑셀 저장 (기존 기능 유지) ---
    if mapping_records:
        mapping_df = pd.DataFrame(mapping_records)
        mapping_path = os.path.join(output_root, "detail_image_mapping.xlsx")
        mapping_df.to_excel(mapping_path, index=False)
        log(f"[INFO] 이미지 매핑 정보 저장 완료: {mapping_path}")
    else:
        log("[INFO] 저장된 이미지 매핑 정보가 없습니다.")


# =========================
# Tkinter GUI
# =========================

class DetailExtractorApp:
    def __init__(self, root):
        self.root = root
        root.title("상세설명 이미지 추출기 2/5단계")

        root.geometry("760x520")

        # 안내 라벨 (Stage1 결과 파일 사용 안내)
        desc = (
            "① 사용 엑셀\n"
            "   - Stage1 맵핑 결과 파일(*_stage1_mapping.xlsx)을 사용합니다.\n"
            "   - 필수 컬럼 : 상품코드, 본문상세설명 또는 상세설명\n\n"
            "② 기능\n"
            "   - 상세설명 HTML에서 유효한 이미지 URL만 추출\n"
            "   - 공지/배송/이벤트/배너 이미지는 자동 제외\n"
            "   - 세로로 너무 긴 이미지는 2~3조각으로 자동 분할 저장\n"
            "   - 각 상품별 상세이미지 경로를 엑셀 컬럼(상세이미지_1~N)에 추가\n"
        )
        lbl_desc = tk.Label(root, text=desc, justify="left", anchor="w")
        lbl_desc.pack(fill=tk.X, padx=10, pady=(8, 4))

        top_frame = tk.Frame(root)
        top_frame.pack(pady=5)

        self.btn_select = tk.Button(
            top_frame,
            text="Stage1 맵핑 엑셀 불러오기",
            command=self.on_select_excel
        )

        self.btn_select.pack(side=tk.LEFT, padx=5)

        self.log_box = ScrolledText(root, wrap=tk.WORD, height=18)
        self.log_box.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

    # 내부에서만 사용하는 실제 로그 출력 함수 (항상 메인 스레드에서만 호출)
    def _append_log(self, msg: str):
        self.log_box.insert(tk.END, msg + "\n")
        self.log_box.see(tk.END)

    # 외부에서 부르는 log는 어떤 스레드에서 호출돼도 안전하게 after로 보냄
    def log(self, msg: str):
        self.root.after(0, self._append_log, msg)

    def on_select_excel(self):
        filetypes = [
            ("Excel files", "*.xlsx *.xls"),
            ("All files", "*.*"),
        ]
        filepath = filedialog.askopenfilename(
            title="상세설명 Stage1 맵핑 결과 엑셀(*_stage1_mapping.xlsx) 파일을 선택하세요",
            filetypes=filetypes
        )
        if not filepath:
            return

        # 작업 중에는 버튼 비활성화
        self.btn_select.config(state=tk.DISABLED)

        thread = threading.Thread(target=self.run_extraction, args=(filepath,))
        thread.daemon = True
        thread.start()

    def run_extraction(self, filepath: str):
        try:
            self.log(f"[INFO] 선택된 파일: {filepath}")
            process_excel(filepath, log_func=self.log)
            self.log("[INFO] 모든 작업 완료")
            
            # 메시지박스도 메인 스레드에서 띄우도록 after 사용
            self.root.after(
                0,
                lambda: messagebox.showinfo(
                    "완료",
                    "상세설명 이미지 추출이 완료되었습니다."
                )
            )            
        except Exception as e:
            self.log("[FATAL] 오류 발생:")
            self.log(str(e))
            self.log(traceback.format_exc())
            self.root.after(
                0,
                lambda: messagebox.showerror(
                    "오류",
                    f"작업 중 오류가 발생했습니다.\n\n{e}"
                )
            )
        finally:
            # 버튼 다시 활성화
            self.root.after(0, lambda: self.btn_select.config(state=tk.NORMAL))


def main():
    root = tk.Tk()
    app = DetailExtractorApp(root)
    root.mainloop()


if __name__ == "__main__":
    main()
