"""
Product_detaildescription.py

[기능]
1. 엑셀 파일 읽기 (Stage 1 결과물)
2. 상세페이지(HTML) 파싱 및 이미지 URL 추출
3. 노이즈(배송/공지 등) 필터링
4. [중요] 긴 이미지는 자동으로 분할(Split)하여 다운로드 (품질 유지)
5. 엑셀에 '상세이미지_X' 컬럼으로 경로 기록 및 저장
6. [NEW] Main Launcher 연동 (JobManager 추가)

"""

import os
import re
import time
import math
import threading
import traceback
import requests
from io import BytesIO
import json
from datetime import datetime
from urllib.parse import quote, unquote
import pandas as pd
from bs4 import BeautifulSoup
from PIL import Image, ImageStat

import tkinter as tk
from tkinter import ttk, filedialog, messagebox
from tkinter.scrolledtext import ScrolledText

# =============================================================================
# [런처 연동] JobManager & 유틸 (추가됨)
# =============================================================================
def get_root_filename(filename):
    """
    파일명에서 버전 정보(_T*_I* 또는 _t*_i*, _I*(업완) 포함) 및 기타 꼬리표를 떼고 원본명(Key)만 추출
    예: 나이키_T0_I0.xlsx -> 나이키.xlsx
    예: 아디다스_T2_I1.xlsx -> 아디다스.xlsx
    예: 나이키_T1_I0(업완).xlsx -> 나이키.xlsx
    예: 나이키_T1_I0_T2_I1.xlsx -> 나이키.xlsx (여러 버전 패턴 제거)
    예: 나이키_T1_I5(업완).xlsx -> 나이키.xlsx
    """
    name = os.path.basename(filename)
    base, ext = os.path.splitext(name)
    
    # 1. 버전 패턴 (_T숫자_I숫자(괄호)? 또는 _t숫자_i숫자(괄호)?) 반복 제거 (대소문자 구분 없음)
    # 패턴이 여러 번 나올 수 있으므로 반복 제거, 괄호가 붙은 경우도 포함
    while True:
        new_base = re.sub(r"_[Tt]\d+_[Ii]\d+(\([^)]+\))?", "", base, flags=re.IGNORECASE)
        if new_base == base:
            break
        base = new_base
    
    # 2. 괄호 안의 텍스트 제거 (예: (업완), (완료) 등) - 버전 패턴의 괄호는 이미 제거됨
    base = re.sub(r"\([^)]*\)", "", base)
    
    # 3. 기타 구형 꼬리표 제거 (호환성 유지)
    suffixes = ["_stage1_mapping", "_stage1_img_mapping", "_stage1_batch_done", 
                "_stage2_analysis", "_stage3_done", "_stage4_final", "_with_images"]
    for s in suffixes:
        base = base.replace(s, "")
    
    # 4. 끝에 남은 언더스코어 제거
    base = base.rstrip("_")
        
    return base + ext

class JobManager:
    DB_FILE = None

    @classmethod
    def find_db_path(cls):
        """런처의 job_history.json 위치를 자동으로 찾습니다."""
        if cls.DB_FILE and os.path.exists(cls.DB_FILE): return cls.DB_FILE
        
        current_dir = os.path.dirname(os.path.abspath(__file__))
        # 현재 폴더, 상위, 상위의 상위까지 탐색
        search_dirs = [
            current_dir,
            os.path.abspath(os.path.join(current_dir, "..")), 
            os.path.abspath(os.path.join(current_dir, "..", ".."))
        ]
        
        for d in search_dirs:
            target = os.path.join(d, "job_history.json")
            if os.path.exists(target):
                cls.DB_FILE = target
                return target
        
        # 못 찾으면 상위 폴더에 기본 설정
        default_path = os.path.abspath(os.path.join(current_dir, "..", "job_history.json"))
        cls.DB_FILE = default_path
        return default_path

    @classmethod
    def load_jobs(cls):
        db_path = cls.find_db_path()
        if not os.path.exists(db_path): return {}
        try:
            with open(db_path, 'r', encoding='utf-8') as f: return json.load(f)
        except: return {}

    @classmethod
    def update_status(cls, filename, text_msg=None, img_msg=None):
        """
        filename: 꼬리표가 제거된 원본 파일명 (get_root_filename 사용 권장)
        text_msg: 텍스트 파트 상태 메시지
        img_msg: 이미지 파트 상태 메시지
        """
        db_path = cls.find_db_path()
        data = cls.load_jobs()
        now = datetime.now().strftime("%m-%d %H:%M")
        
        if filename not in data:
            # 신규 파일이면 초기화
            data[filename] = {
                "start_time": now,
                "text_status": "대기", "text_time": "-",
                "image_status": "대기", "image_time": "-", "memo": ""
            }

        if text_msg:
            data[filename]["text_status"] = text_msg
            data[filename]["text_time"] = now
        if img_msg:
            data[filename]["image_status"] = img_msg
            data[filename]["image_time"] = now
            
        data[filename]["last_update"] = now
        
        try:
            with open(db_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=4)
        except Exception as e:
            print(f"[JobManager Error] {e}")
            
# =============================================================================
# [CORE] 설정 및 로직
# =============================================================================

# 제외할 키워드 (광고/공지사항 등) - 기본값
DEFAULT_NOISE_URL_KEYWORDS = [
    "notice", "delivery", "ship", "shipping", "배송", "교환", "반품", "환불",
    "exchange", "return", "event", "banner", "coupon", "gift", "guide", "info",
    "kakao", "consult", "size", "washing", "model"
]

# 금지 이미지 URL 저장 파일 경로
BLOCKED_URLS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".blocked_image_urls.json")

# 금지 키워드 저장 파일 경로
BLOCKED_KEYWORDS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".blocked_keywords.json")

def load_blocked_keywords() -> list:
    """저장된 금지 키워드 리스트를 로드 (기본값 포함)"""
    if not os.path.exists(BLOCKED_KEYWORDS_FILE):
        # 기본값 저장
        save_blocked_keywords(DEFAULT_NOISE_URL_KEYWORDS)
        return DEFAULT_NOISE_URL_KEYWORDS.copy()
    try:
        with open(BLOCKED_KEYWORDS_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
            keywords = data.get('blocked_keywords', [])
            # 기본값이 없으면 추가
            if not keywords:
                keywords = DEFAULT_NOISE_URL_KEYWORDS.copy()
                save_blocked_keywords(keywords)
            return keywords
    except Exception:
        return DEFAULT_NOISE_URL_KEYWORDS.copy()

def save_blocked_keywords(keywords: list):
    """금지 키워드 리스트를 저장"""
    try:
        data = {'blocked_keywords': keywords}
        with open(BLOCKED_KEYWORDS_FILE, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception:
        pass

def add_blocked_keyword(keyword: str) -> bool:
    """금지 키워드 추가 (중복 체크)"""
    keyword = keyword.strip().lower()
    if not keyword:
        return False
    keywords = load_blocked_keywords()
    if keyword not in keywords:
        keywords.append(keyword)
        save_blocked_keywords(keywords)
        return True
    return False

def remove_blocked_keyword(keyword: str):
    """금지 키워드 제거"""
    keywords = load_blocked_keywords()
    if keyword in keywords:
        keywords.remove(keyword)
        save_blocked_keywords(keywords)

def get_noise_url_keywords() -> list:
    """현재 사용할 금지 키워드 리스트 반환"""
    return load_blocked_keywords()

def load_blocked_urls() -> list:
    """저장된 금지 URL 리스트를 로드"""
    if not os.path.exists(BLOCKED_URLS_FILE):
        return []
    try:
        with open(BLOCKED_URLS_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
            return data.get('blocked_urls', [])
    except Exception:
        return []

def save_blocked_urls(urls: list):
    """금지 URL 리스트를 저장"""
    try:
        data = {'blocked_urls': urls}
        with open(BLOCKED_URLS_FILE, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception:
        pass

def add_blocked_url(url: str) -> bool:
    """금지 URL 추가 (중복 체크)"""
    url = url.strip()
    if not url:
        return False
    urls = load_blocked_urls()
    if url not in urls:
        urls.append(url)
        save_blocked_urls(urls)
        return True
    return False

def remove_blocked_url(url: str):
    """금지 URL 제거"""
    urls = load_blocked_urls()
    if url in urls:
        urls.remove(url)
        save_blocked_urls(urls)

def is_blocked_url(img_url: str) -> bool:
    """이미지 URL이 금지 목록에 있는지 확인"""
    blocked_urls = load_blocked_urls()
    for blocked in blocked_urls:
        if blocked in img_url:
            return True
    return False

# 엑셀 컬럼명 후보
COL_PRODUCT_CODE = [
    "상품코드",
    "판매자관리코드",
    "판매자관리코드1",
    "자사상품코드",
    "ProductCode",
    "code",
]
COL_DETAIL_HTML = ["본문상세설명", "상세설명", "Description", "detail", "상세페이지"]
COL_DETAIL_IMG_PREFIX = "상세이미지"

# 이미지 분할 기준 (세로 픽셀)
MAX_HEIGHT_1 = 4000  # 이하면 1장
MAX_HEIGHT_2 = 8000  # 이하면 2장, 초과면 3장

# 전체 다운로드 제한 (None = 제한없음)
MAX_DETAIL_IMAGES = None


def get_valid_filename(name: str) -> str:
    """파일명으로 쓸 수 없는 특수문자 제거"""
    return re.sub(r'[\\/*?:"<>|]', "", str(name)).strip()


def is_small_or_blank_image(img: Image.Image) -> bool:
    """
    너무 작은 아이콘/로고/구분선 또는 거의 단색 이미지는 제외하기 위한 필터.

    - 가로/세로가 10px 이하 → 무조건 제외
    - 가로가 600px 미만이면 → 제외 (대부분 860px 고정이므로 상세설명 이미지로 적합하지 않음)
    - 세로가 650px 미만이면 → 제외 (500px 미만에서 650px 미만까지 제외, 안내문구나 불필요한 이미지일 확률이 높음)
    - 1000px ≥ 가로 ≥ 600px AND 1000px ≥ 세로 ≥ 650px이지만 밝기 표준편차가 매우 낮으면(거의 단색) → 제외
      (애매한 사이즈의 단색 이미지 제거, 1000px 이상의 큰 이미지는 상세설명일 수 있으므로 제외하지 않음)
    """
    try:
        w, h = img.size
        if w <= 10 or h <= 10:
            return True

        # 가로가 600px 미만이면 제외
        if w < 600:
            return True

        # 세로가 650px 미만이면 제외 (500px 미만에서 650px 미만까지)
        if h < 650:
            return True

        # 중간 크기 이미지(600~1000px 범위)에서 거의 단색인 경우 제외
        # 1000px 이상의 큰 이미지는 실제 상세설명일 수 있으므로 밝기 체크 제외
        if 600 <= w <= 1000 and 650 <= h <= 1000:
            try:
                stat = ImageStat.Stat(img.convert("L"))
                if stat.stddev[0] < 5:  # 표준편차가 매우 낮으면 단색에 가깝다
                    return True
            except Exception:
                # 이미지 변환 실패 시 필터링하지 않음 (통과)
                pass

        return False
    except Exception:
        # 이미지 처리 중 오류 발생 시 필터링하지 않음 (통과)
        return False


def download_and_split_image(
    url: str,
    save_folder: str,
    base_filename: str,
    max_retries: int = 3,
):
    """
    이미지를 다운로드하고, 너무 길면 분할하여 저장함.
    Returns: 저장된 파일명 리스트 (List[str])
    """
    headers = {
        'User-Agent': (
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
            'AppleWebKit/537.36 (KHTML, like Gecko) '
            'Chrome/91.0.4472.124 Safari/537.36'
        )
    }

    img = None
    # 1. 다운로드 (재시도 로직)
    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=headers, timeout=15)
            response.raise_for_status()
            img = Image.open(BytesIO(response.content))
            break
        except requests.exceptions.RequestException:
            # 네트워크 오류는 재시도
            if attempt < max_retries - 1:
                time.sleep(1)
            else:
                return []  # 다운로드 실패
        except Exception:
            # 기타 오류 (이미지 파싱 실패 등)는 재시도하지 않음
            return []

    if img is None:
        return []

    try:
        # 2. 품질/노이즈 체크 (작은 아이콘/단색 이미지 제외)
        if is_small_or_blank_image(img):
            return []

        # RGB 변환
        if img.mode in ("RGBA", "P"):
            try:
                img = img.convert("RGB")
            except Exception:
                return []  # 이미지 변환 실패

        width, height = img.size

        # 3. 분할 개수 결정
        if height <= MAX_HEIGHT_1:
            split_count = 1
        elif height <= MAX_HEIGHT_2:
            split_count = 2
        else:
            split_count = 3

        saved_files = []

        # 4. 분할 및 저장
        if split_count == 1:
            # 분할 없음
            save_path = os.path.join(save_folder, base_filename)
            try:
                img.save(save_path, "JPEG", quality=90)
                saved_files.append(base_filename)
            except (IOError, OSError) as e:
                # 디스크 공간 부족, 권한 오류 등
                return []
            except Exception:
                return []
        else:
            # N등분
            unit_height = math.ceil(height / split_count)
            name_stem, name_ext = os.path.splitext(base_filename)

            for i in range(split_count):
                try:
                    top = i * unit_height
                    bottom = min((i + 1) * unit_height, height)

                    if top >= height:
                        break

                    crop_img = img.crop((0, top, width, bottom))

                    # 분할 파일명: 원본_1.jpg, 원본_2.jpg ...
                    split_filename = f"{name_stem}_{i + 1}{name_ext}"
                    save_path = os.path.join(save_folder, split_filename)

                    try:
                        crop_img.save(save_path, "JPEG", quality=90)
                        saved_files.append(split_filename)
                    except (IOError, OSError):
                        # 저장 실패 시 해당 분할만 건너뛰고 계속 진행
                        continue
                    except Exception:
                        continue
                except Exception:
                    # 분할 중 오류 발생 시 해당 분할만 건너뛰고 계속 진행
                    continue

        return saved_files
    except Exception:
        # 이미지 처리 중 예상치 못한 오류
        return []


def process_excel_logic(filepath: str, log_func, progress_func=None):
    """
    실제 엑셀 처리 로직 (스레드 내부 실행)

    progress_func(current, total) 형태의 콜백을 받아
    진행률 업데이트에 사용한다.
    """
    base_dir = os.path.dirname(filepath)
    file_name_only = os.path.splitext(os.path.basename(filepath))[0]
    
    # [런처 연동] 시작 상태 업데이트
    root_name = get_root_filename(filepath)
    try:
        JobManager.update_status(root_name, text_msg="T2 전처리중")
        log_func(f"[Launcher] 상태 업데이트: {root_name} -> T2 전처리중")
    except Exception as e:
        log_func(f"[Launcher] 상태 업데이트 실패: {e}")

    # 이미지 저장용 폴더
    save_folder_name = f"{file_name_only}_detail"
    save_folder_path = os.path.join(base_dir, save_folder_name)
    os.makedirs(save_folder_path, exist_ok=True)

    log_func(f"[시작] 파일 로드: {os.path.basename(filepath)}")
    log_func(f"[폴더] 이미지 저장 경로: {save_folder_path}")

    try:
        df = pd.read_excel(filepath)
    except Exception as e:
        log_func(f"[에러] 엑셀 파일을 읽을 수 없습니다: {e}")
        return None

    # 상세설명 컬럼 찾기
    desc_col = None
    for c in COL_DETAIL_HTML:
        if c in df.columns:
            desc_col = c
            break

    # 상품코드 컬럼 찾기 (자동 인식)
    code_col = None
    for c in COL_PRODUCT_CODE:
        if c in df.columns:
            code_col = c
            break

    if not desc_col:
        log_func(f"[오류] 상세설명 컬럼({COL_DETAIL_HTML})을 찾을 수 없습니다.")
        return None

    if not code_col:
        log_func(f"[오류] 상품코드 컬럼({COL_PRODUCT_CODE})을 찾을 수 없습니다.")
        return None

    total_rows = len(df)
    log_func(f"[정보] 총 {total_rows}개 상품 처리를 시작합니다.")

    # 시작 시 진행률 0으로 초기화
    if progress_func and total_rows > 0:
        progress_func(0, total_rows)

    success_count = 0

    for idx, row in df.iterrows():
        try:
            # 상품코드(또는 판매자코드 등) 가져오기
            p_code_raw = row.get(code_col, f'Row_{idx}')
            p_code = get_valid_filename(p_code_raw)

            html_content = str(row.get(desc_col, ''))

            if not html_content or html_content.lower() == 'nan':
                # 빈 상세설명일 때도 진행률은 올라가야 함
                if progress_func:
                    progress_func(idx + 1, total_rows)
                continue

            # HTML 파싱
            try:
                soup = BeautifulSoup(html_content, 'html.parser')
                img_tags = soup.find_all('img')
            except Exception as e:
                log_func(f"[경고] 행 {idx + 1} ({p_code}): HTML 파싱 실패 - {str(e)}")
                if progress_func:
                    progress_func(idx + 1, total_rows)
                continue

            # URL 추출
            img_urls = []
            try:
                for img in img_tags:
                    src = img.get('src')
                    if src:
                        src = src.strip()
                        # 노이즈 필터링 (키워드 기반) - 동적으로 로드
                        noise_keywords = get_noise_url_keywords()
                        # URL 인코딩된 형태도 체크 (한글 키워드 대응)
                        src_lower = src.lower()
                        src_decoded = unquote(src_lower)  # URL 디코딩
                        
                        keyword_matched = False
                        for k in noise_keywords:
                            k_lower = k.lower()
                            # 원본 키워드 체크
                            if k_lower in src_lower or k_lower in src_decoded:
                                keyword_matched = True
                                break
                            # URL 인코딩된 키워드 체크 (한글 키워드의 경우)
                            try:
                                k_encoded = quote(k, safe='')  # URL 인코딩
                                if k_encoded.lower() in src_lower:
                                    keyword_matched = True
                                    break
                            except:
                                pass
                        
                        if keyword_matched:
                            continue
                        # 금지 URL 필터링 (사용자 추가)
                        if is_blocked_url(src):
                            continue
                        if not src.startswith('http'):
                            continue
                        img_urls.append(src)
            except Exception as e:
                log_func(f"[경고] 행 {idx + 1} ({p_code}): URL 추출 중 오류 - {str(e)}")

            img_urls = list(dict.fromkeys(img_urls))  # 중복 제거

            # 이번 상품의 모든 이미지 경로(분할 포함)를 담을 리스트
            all_saved_rel_paths = []

            # 다운로드
            for i, url in enumerate(img_urls):
                try:
                    if MAX_DETAIL_IMAGES and len(all_saved_rel_paths) >= MAX_DETAIL_IMAGES:
                        break

                    # 기본 파일명 (분할 시 뒤에 _1, _2 붙음)
                    base_filename = f"{p_code}_{i + 1:02d}.jpg"

                    # 다운로드 및 분할 저장
                    created_files = download_and_split_image(
                        url,
                        save_folder_path,
                        base_filename,
                    )

                    for fname in created_files:
                        try:
                            # ✅ 엑셀에 "절대경로"로 기록하도록 변경
                            abs_path = os.path.abspath(os.path.join(save_folder_path, fname))
                            all_saved_rel_paths.append(abs_path)
                        except Exception as e:
                            log_func(f"[경고] 행 {idx + 1} ({p_code}): 경로 생성 실패 - {str(e)}")
                except Exception as e:
                    log_func(f"[경고] 행 {idx + 1} ({p_code}): 이미지 다운로드 실패 (URL: {url[:50]}...) - {str(e)}")
                    continue

            # 엑셀 컬럼 업데이트
            # (분할된 것까지 포함해서 상세이미지_1, 2, 3... 순서대로 기입)
            try:
                for i, rel_path in enumerate(all_saved_rel_paths):
                    col_name = f"{COL_DETAIL_IMG_PREFIX}_{i + 1}"
                    if col_name not in df.columns:
                        df[col_name] = None
                    df.at[idx, col_name] = rel_path
            except Exception as e:
                log_func(f"[경고] 행 {idx + 1} ({p_code}): 엑셀 업데이트 실패 - {str(e)}")

            if all_saved_rel_paths:
                success_count += 1

        except Exception as e:
            # 행 처리 중 예상치 못한 오류 발생 시 로그만 남기고 계속 진행
            log_func(f"[오류] 행 {idx + 1} 처리 중 예외 발생: {str(e)}")
            log_func(f"[상세] {traceback.format_exc()}")
        finally:
            # 진행률 업데이트 (오류 발생 여부와 관계없이)
            if progress_func:
                progress_func(idx + 1, total_rows)

    # 이미지가 있는 행과 없는 행 분리
    col_detail_img_1 = f"{COL_DETAIL_IMG_PREFIX}_1"
    
    # '상세이미지_1' 컬럼이 비어있거나 None인 행 찾기
    if col_detail_img_1 in df.columns:
        # 이미지가 있는 행: '상세이미지_1'에 값이 있는 행
        df_with_images = df[df[col_detail_img_1].notna() & (df[col_detail_img_1] != '')].copy()
        # 이미지가 없는 행: '상세이미지_1'이 비어있는 행
        df_no_images = df[df[col_detail_img_1].isna() | (df[col_detail_img_1] == '')].copy()
    else:
        # 컬럼이 없으면 모든 행이 이미지 없음으로 처리
        df_with_images = pd.DataFrame()
        df_no_images = df.copy()
    
    # 결과 저장
    out_path = filepath
    no_images_path = None
    
    try:
        # 1. 이미지가 있는 행들만 원본 파일에 저장
        if len(df_with_images) > 0:
            try:
                df_with_images.to_excel(out_path, index=False)
                log_func("=" * 40)
                log_func(f"[완료] 이미지 추출 종료.")
                log_func(f" - 이미지 저장 성공: {success_count}개 상품")
                log_func(f" - 결과 파일: {os.path.basename(out_path)} ({len(df_with_images)}개 행)")
                
                # [런처 연동] 완료 상태 업데이트 (img 상태는 변경하지 않음)
                try:
                    JobManager.update_status(root_name, text_msg="T2-1(상세다운완료)")
                    log_func(f"[Launcher] 상태 업데이트: {root_name} -> T2-1(상세다운완료)")
                except Exception as e:
                    log_func(f"[Launcher] 상태 업데이트 실패: {e}")
            except PermissionError:
                log_func("[에러] 엑셀 파일이 열려있습니다. 닫고 다시 시도하세요.")
                return None
            except IOError as e:
                log_func(f"[에러] 파일 저장 실패 (디스크 공간 부족 또는 권한 오류): {e}")
                return None
            except Exception as e:
                log_func(f"[에러] 파일 저장 중 예외 발생: {e}")
                log_func(f"[상세] {traceback.format_exc()}")
                return None
        else:
            log_func("=" * 40)
            log_func(f"[경고] 이미지가 있는 상품이 없습니다.")
            # 모든 행이 이미지 없음이면 원본 파일은 그대로 유지하되 분리 파일로만 저장
        
        # 2. 이미지가 없는 행들을 T2-1(실패) 버전으로 별도 파일 저장
        if len(df_no_images) > 0:
            try:
                base_dir = os.path.dirname(filepath)
                base_name, ext = os.path.splitext(os.path.basename(filepath))
                
                # 현재 파일명에서 버전 정보 추출 (예: _T1_I0 또는 _T1_I5(업완))
                # T2-1(실패) 버전으로 변경
                # 괄호가 붙은 경우도 인식 (예: _I5(업완))
                all_matches = list(re.finditer(r"_([Tt])(\d+)_([Ii])(\d+)(\([^)]+\))?", base_name, re.IGNORECASE))
                
                if all_matches:
                    # 마지막 버전 패턴 사용
                    match = all_matches[-1]
                    original_name = base_name[: match.start()].rstrip("_")
                    current_i = int(match.group(4))
                    # T2-1(실패) 버전으로 생성 (기존 괄호는 제거하고 (실패)로 교체)
                    new_filename = f"{original_name}_T2-1(실패)_I{current_i}{ext}"
                else:
                    # 버전 패턴이 없으면 기본적으로 T2-1(실패)_I0로 생성
                    new_filename = f"{base_name}_T2-1(실패)_I0{ext}"
                
                no_images_path = os.path.join(base_dir, new_filename)
                df_no_images.to_excel(no_images_path, index=False)
                
                log_func(f" - T2-1(실패) 분리 파일: {os.path.basename(no_images_path)} ({len(df_no_images)}개 행)")
                log_func(f"   ※ 이 파일은 T1 단계(정제상품명)까지만 작업 가능합니다.")
                
                # 분리된 파일의 런처 상태 업데이트
                try:
                    no_images_root_name = get_root_filename(no_images_path)
                    JobManager.update_status(no_images_root_name, text_msg="T2-1(실패)")
                    log_func(f"[Launcher] 분리 파일 상태 업데이트: {no_images_root_name} -> T2-1(실패)")
                except Exception as e:
                    log_func(f"[Launcher] 분리 파일 상태 업데이트 실패: {e}")
            except PermissionError:
                log_func("[경고] T2-1(실패) 분리 파일 저장 실패: 파일이 열려있습니다.")
            except IOError as e:
                log_func(f"[경고] T2-1(실패) 분리 파일 저장 실패 (디스크 공간 부족 또는 권한 오류): {e}")
            except Exception as e:
                log_func(f"[경고] T2-1(실패) 분리 파일 저장 중 예외 발생: {e}")
                log_func(f"[상세] {traceback.format_exc()}")
        else:
            log_func(" - 이미지가 없는 행이 없습니다.")

        # 분리된 파일 경로도 함께 반환 (없으면 None)
        return (out_path, no_images_path)
    except Exception as e:
        log_func(f"[에러] 저장 프로세스 중 예상치 못한 오류: {e}")
        log_func(f"[상세] {traceback.format_exc()}")
        return None


# =============================================================================
# [GUI] UI/UX Implementation
# =============================================================================

COLOR_BG = "#F8F9FA"
COLOR_WHITE = "#FFFFFF"
COLOR_PRIMARY = "#4A90E2"
COLOR_PRIMARY_HOVER = "#357ABD"
COLOR_TEXT = "#333333"
COLOR_HEADER = "#2C3E50"


class ToolTip:
    def __init__(self, widget, text):
        self.widget = widget
        self.text = text
        self.tipwindow = None
        self.widget.bind("<Enter>", self.show_tip)
        self.widget.bind("<Leave>", self.hide_tip)

    def show_tip(self, event=None):
        if self.tipwindow or not self.text:
            return
        x = self.widget.winfo_rootx() + 20
        y = self.widget.winfo_rooty() + 30
        self.tipwindow = tw = tk.Toplevel(self.widget)
        tw.wm_overrideredirect(True)
        tw.wm_geometry(f"+{x}+{y}")
        label = tk.Label(
            tw,
            text=self.text,
            justify='left',
            background="#ffffe0",
            relief='solid',
            borderwidth=1,
            font=("맑은 고딕", 9),
        )
        label.pack(ipadx=4, ipady=2)

    def hide_tip(self, event=None):
        if self.tipwindow:
            self.tipwindow.destroy()
            self.tipwindow = None


class ImageExtractorApp(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("상세페이지 이미지 추출기 (Stage 2-0)")
        self.geometry("650x900")
        self.configure(bg=COLOR_BG)

        self.file_path = tk.StringVar()
        self.is_running = False

        self._setup_styles()
        self._init_ui()

    def _setup_styles(self):
        style = ttk.Style()
        try:
            style.theme_use('clam')
        except Exception:
            pass

        style.configure("TFrame", background=COLOR_BG)
        style.configure("TLabel", background=COLOR_BG, foreground=COLOR_TEXT, font=("맑은 고딕", 10))

        style.configure(
            "Primary.TButton",
            background=COLOR_PRIMARY,
            foreground="white",
            font=("맑은 고딕", 10, "bold"),
            borderwidth=0,
            focuscolor=COLOR_PRIMARY,
        )
        style.map("Primary.TButton", background=[("active", COLOR_PRIMARY_HOVER)])

        style.configure("Card.TLabelframe", background=COLOR_WHITE, bordercolor="#E0E0E0")
        style.configure(
            "Card.TLabelframe.Label",
            background=COLOR_WHITE,
            foreground=COLOR_PRIMARY,
            font=("맑은 고딕", 11, "bold"),
        )

        # ProgressBar 스타일 (초록색 바)
        style.configure(
            "Green.Horizontal.TProgressbar",
            troughcolor=COLOR_BG,
            bordercolor=COLOR_BG,
            background="#4CAF50",
            lightcolor="#4CAF50",
            darkcolor="#4CAF50",
        )

    def _init_ui(self):
        # 헤더
        header = tk.Frame(self, bg=COLOR_HEADER, height=70)
        header.pack(fill="x")
        header.pack_propagate(False)
        tk.Label(
            header,
            text="🖼️ 상세페이지 이미지 다운로더",
            font=("맑은 고딕", 18, "bold"),
            bg=COLOR_HEADER,
            fg="white",
        ).pack(expand=True)

        main_container = tk.Frame(self, bg=COLOR_BG, padx=20, pady=20)
        main_container.pack(fill="both", expand=True)

        # 파일 선택
        card_file = ttk.LabelFrame(
            main_container,
            text=" 1. 대상 파일 선택 ",
            style="Card.TLabelframe",
            padding=20,
        )
        card_file.pack(fill="x", pady=(0, 20))

        f_inner = tk.Frame(card_file, bg=COLOR_WHITE)
        f_inner.pack(fill="x")

        entry = ttk.Entry(f_inner, textvariable=self.file_path, font=("맑은 고딕", 10))
        entry.pack(side="left", fill="x", expand=True, padx=(0, 5))

        btn_find = ttk.Button(f_inner, text="📂 파일 찾기", command=self.open_file)
        btn_find.pack(side="right")

        ToolTip(btn_find, "Stage 1이 완료된 엑셀 파일을 선택하세요.")

        # 실행 버튼
        self.btn_run = ttk.Button(
            main_container,
            text="🚀 이미지 추출 및 다운로드 시작",
            style="Primary.TButton",
            command=self.start_extraction,
        )
        self.btn_run.pack(fill="x", pady=(0, 10), ipady=8)
        ToolTip(self.btn_run, "긴 이미지는 자동으로 분할되어 다운로드됩니다.")

        # 금지 URL 관리 섹션
        card_blocked = ttk.LabelFrame(
            main_container,
            text=" 2. 금지 이미지 URL 관리 ",
            style="Card.TLabelframe",
            padding=15,
        )
        card_blocked.pack(fill="x", pady=(0, 15))

        blocked_inner = tk.Frame(card_blocked, bg=COLOR_WHITE)
        blocked_inner.pack(fill="both", expand=True)

        # 입력 필드와 추가 버튼
        input_frame = tk.Frame(blocked_inner, bg=COLOR_WHITE)
        input_frame.pack(fill="x", pady=(0, 10))

        tk.Label(
            input_frame,
            text="금지할 이미지 URL:",
            font=("맑은 고딕", 9),
            bg=COLOR_WHITE,
            fg="#555",
        ).pack(anchor="w", pady=(0, 3))

        url_input_frame = tk.Frame(input_frame, bg=COLOR_WHITE)
        url_input_frame.pack(fill="x")

        self.blocked_url_var = tk.StringVar()
        entry_blocked = ttk.Entry(
            url_input_frame,
            textvariable=self.blocked_url_var,
            font=("맑은 고딕", 9),
        )
        entry_blocked.pack(side="left", fill="x", expand=True, padx=(0, 5))
        
        btn_add_blocked = ttk.Button(
            input_frame,
            text="➕ 추가",
            command=self.add_blocked_url,
            width=8,
        )
        btn_add_blocked.pack(side="right", padx=(0, 5))
        
        btn_remove_blocked = ttk.Button(
            input_frame,
            text="➖ 삭제",
            command=self.remove_blocked_url,
            width=8,
        )
        btn_remove_blocked.pack(side="right")

        # 금지 URL 리스트
        list_frame = tk.Frame(blocked_inner, bg=COLOR_WHITE)
        list_frame.pack(fill="both", expand=True)

        tk.Label(
            list_frame,
            text="금지 URL 목록:",
            font=("맑은 고딕", 9),
            bg=COLOR_WHITE,
            fg="#555",
        ).pack(anchor="w", pady=(0, 5))

        # 리스트박스와 스크롤바
        listbox_frame = tk.Frame(list_frame, bg=COLOR_WHITE)
        listbox_frame.pack(fill="both", expand=True)

        scrollbar = ttk.Scrollbar(listbox_frame)
        scrollbar.pack(side="right", fill="y")

        self.blocked_listbox = tk.Listbox(
            listbox_frame,
            font=("Consolas", 8),
            bg="white",
            selectmode=tk.SINGLE,
            yscrollcommand=scrollbar.set,
            height=4,
        )
        self.blocked_listbox.pack(side="left", fill="both", expand=True)
        scrollbar.config(command=self.blocked_listbox.yview)

        # 금지 URL 목록 로드
        self.refresh_blocked_list()

        ToolTip(entry_blocked, "금지할 이미지 URL을 입력하고 추가 버튼을 클릭하세요.\n예: https://gi.esmplus.com/khch124/...")
        ToolTip(btn_add_blocked, "입력한 URL을 금지 목록에 추가합니다.\n중복된 URL은 추가되지 않습니다.")
        ToolTip(btn_remove_blocked, "선택한 URL을 금지 목록에서 제거합니다.")

        # 금지 키워드 관리 섹션
        card_keywords = ttk.LabelFrame(
            main_container,
            text=" 3. 금지 키워드 관리 (URL에 포함된 단어로 필터링) ",
            style="Card.TLabelframe",
            padding=15,
        )
        card_keywords.pack(fill="x", pady=(0, 15))

        keywords_inner = tk.Frame(card_keywords, bg=COLOR_WHITE)
        keywords_inner.pack(fill="both", expand=True)

        # 입력 필드와 추가 버튼
        keyword_input_frame = tk.Frame(keywords_inner, bg=COLOR_WHITE)
        keyword_input_frame.pack(fill="x", pady=(0, 10))

        tk.Label(
            keyword_input_frame,
            text="금지할 키워드 (URL에 포함된 단어):",
            font=("맑은 고딕", 9),
            bg=COLOR_WHITE,
            fg="#555",
        ).pack(anchor="w", pady=(0, 3))

        keyword_entry_frame = tk.Frame(keyword_input_frame, bg=COLOR_WHITE)
        keyword_entry_frame.pack(fill="x")

        self.blocked_keyword_var = tk.StringVar()
        entry_keyword = ttk.Entry(
            keyword_entry_frame,
            textvariable=self.blocked_keyword_var,
            font=("맑은 고딕", 9),
        )
        entry_keyword.pack(side="left", fill="x", expand=True, padx=(0, 5))
        
        btn_add_keyword = ttk.Button(
            keyword_input_frame,
            text="➕ 추가",
            command=self.add_blocked_keyword,
            width=8,
        )
        btn_add_keyword.pack(side="right", padx=(0, 5))
        
        btn_remove_keyword = ttk.Button(
            keyword_input_frame,
            text="➖ 삭제",
            command=self.remove_blocked_keyword,
            width=8,
        )
        btn_remove_keyword.pack(side="right")

        # 금지 키워드 리스트
        keyword_list_frame = tk.Frame(keywords_inner, bg=COLOR_WHITE)
        keyword_list_frame.pack(fill="both", expand=True)

        tk.Label(
            keyword_list_frame,
            text="금지 키워드 목록:",
            font=("맑은 고딕", 9),
            bg=COLOR_WHITE,
            fg="#555",
        ).pack(anchor="w", pady=(0, 5))

        # 리스트박스와 스크롤바
        keyword_listbox_frame = tk.Frame(keyword_list_frame, bg=COLOR_WHITE)
        keyword_listbox_frame.pack(fill="both", expand=True)

        keyword_scrollbar = ttk.Scrollbar(keyword_listbox_frame)
        keyword_scrollbar.pack(side="right", fill="y")

        self.blocked_keyword_listbox = tk.Listbox(
            keyword_listbox_frame,
            font=("Consolas", 8),
            bg="white",
            selectmode=tk.SINGLE,
            yscrollcommand=keyword_scrollbar.set,
            height=4,
        )
        self.blocked_keyword_listbox.pack(side="left", fill="both", expand=True)
        keyword_scrollbar.config(command=self.blocked_keyword_listbox.yview)

        # 금지 키워드 목록 로드
        self.refresh_keyword_list()

        ToolTip(entry_keyword, "URL에 포함된 단어를 입력하고 추가 버튼을 클릭하세요.\n예: notice, delivery, 배송, 교환 등")
        ToolTip(btn_add_keyword, "입력한 키워드를 금지 목록에 추가합니다.\n중복된 키워드는 추가되지 않습니다.")
        ToolTip(btn_remove_keyword, "선택한 키워드를 금지 목록에서 제거합니다.")

        # 진행률 ProgressBar + 라벨
        progress_frame = tk.Frame(main_container, bg=COLOR_BG)
        progress_frame.pack(fill="x", pady=(0, 15))

        self.progress_bar = ttk.Progressbar(
            progress_frame,
            style="Green.Horizontal.TProgressbar",
            orient="horizontal",
            mode="determinate",
            maximum=100,
        )
        self.progress_bar.pack(fill="x")

        self.progress_label = tk.Label(
            progress_frame,
            text="0/0 (0%)",
            font=("맑은 고딕", 9),
            bg=COLOR_BG,
            fg="#555",
            anchor="e",
        )
        self.progress_label.pack(fill="x", pady=(2, 0))

        # 로그
        lbl_log = tk.Label(
            main_container,
            text="진행 로그",
            font=("맑은 고딕", 10, "bold"),
            bg=COLOR_BG,
            fg="#555",
        )
        lbl_log.pack(anchor="w", pady=(0, 5))

        self.log_widget = ScrolledText(
            main_container,
            height=15,
            state='disabled',
            font=("Consolas", 9),
            bg="white",
            bd=1,
            relief="solid",
        )
        self.log_widget.pack(fill="both", expand=True)

    # 진행률 업데이트 (current: 현재 처리한 개수, total: 전체 개수)
    def update_progress(self, current: int, total: int):
        if total <= 0:
            return
        percent = int(current * 100 / total)

        def _update():
            try:
                self.progress_bar['value'] = percent
                self.progress_label.config(
                    text=f"{current}/{total} ({percent}%)"
                )
            except Exception:
                pass

        self.after(0, _update)

    def log(self, msg: str):
        ts = time.strftime("%H:%M:%S")
        full_msg = f"[{ts}] {msg}\n"

        def _update():
            try:
                self.log_widget.config(state='normal')
                self.log_widget.insert('end', full_msg)
                self.log_widget.see('end')
                self.log_widget.config(state='disabled')
            except Exception:
                pass

        self.after(0, _update)

    def refresh_blocked_list(self):
        """금지 URL 리스트 새로고침"""
        self.blocked_listbox.delete(0, tk.END)
        blocked_urls = load_blocked_urls()
        for url in blocked_urls:
            self.blocked_listbox.insert(tk.END, url)

    def add_blocked_url(self):
        """금지 URL 추가"""
        url = self.blocked_url_var.get().strip()
        if not url:
            messagebox.showwarning("경고", "URL을 입력해주세요.")
            return
        
        if add_blocked_url(url):
            self.blocked_url_var.set("")
            self.refresh_blocked_list()
            self.log(f"✅ 금지 URL 추가됨: {url}")
            messagebox.showinfo("완료", f"금지 URL이 추가되었습니다:\n{url}")
        else:
            messagebox.showwarning("경고", "이미 등록된 URL이거나 유효하지 않은 URL입니다.")

    def remove_blocked_url(self):
        """금지 URL 제거"""
        selection = self.blocked_listbox.curselection()
        if not selection:
            messagebox.showwarning("경고", "삭제할 URL을 선택해주세요.")
            return
        
        url = self.blocked_listbox.get(selection[0])
        remove_blocked_url(url)
        self.refresh_blocked_list()
        self.log(f"🗑️ 금지 URL 제거됨: {url}")
        messagebox.showinfo("완료", f"금지 URL이 제거되었습니다:\n{url}")

    def refresh_keyword_list(self):
        """금지 키워드 리스트 새로고침"""
        self.blocked_keyword_listbox.delete(0, tk.END)
        blocked_keywords = load_blocked_keywords()
        for keyword in blocked_keywords:
            self.blocked_keyword_listbox.insert(tk.END, keyword)

    def add_blocked_keyword(self):
        """금지 키워드 추가"""
        keyword = self.blocked_keyword_var.get().strip()
        if not keyword:
            messagebox.showwarning("경고", "키워드를 입력해주세요.")
            return
        
        if add_blocked_keyword(keyword):
            self.blocked_keyword_var.set("")
            self.refresh_keyword_list()
            self.log(f"✅ 금지 키워드 추가됨: {keyword}")
            messagebox.showinfo("완료", f"금지 키워드가 추가되었습니다:\n{keyword}")
        else:
            messagebox.showwarning("경고", "이미 등록된 키워드입니다.")

    def remove_blocked_keyword(self):
        """금지 키워드 제거"""
        selection = self.blocked_keyword_listbox.curselection()
        if not selection:
            messagebox.showwarning("경고", "삭제할 키워드를 선택해주세요.")
            return
        
        keyword = self.blocked_keyword_listbox.get(selection[0])
        remove_blocked_keyword(keyword)
        self.refresh_keyword_list()
        self.log(f"🗑️ 금지 키워드 제거됨: {keyword}")
        messagebox.showinfo("완료", f"금지 키워드가 제거되었습니다:\n{keyword}")

    def open_file(self):
        file_path = filedialog.askopenfilename(
            title="엑셀 파일 선택",
            filetypes=[("Excel Files", "*.xlsx;*.xls")],
        )
        if file_path:
            self.file_path.set(file_path)
            self.log(f"파일 선택됨: {os.path.basename(file_path)}")

    def start_extraction(self):
        if self.is_running:
            return

        path = self.file_path.get().strip()
        if not path:
            messagebox.showwarning("경고", "먼저 엑셀 파일을 선택해주세요.")
            return

        if not os.path.exists(path):
            messagebox.showerror("오류", "파일을 찾을 수 없습니다.")
            return
        
        # T1 포함 여부 검증
        base_name = os.path.splitext(os.path.basename(path))[0]
        if not re.search(r"_T1_[Ii]\d+", base_name, re.IGNORECASE):
            messagebox.showerror(
                "오류", 
                f"이 도구는 T1 버전의 엑셀 파일만 처리할 수 있습니다.\n\n"
                f"선택한 파일: {os.path.basename(path)}\n"
                f"파일명에 '_T1_I*' 패턴이 포함되어 있어야 합니다."
            )
            return

        # 진행률 초기화
        self.progress_bar['value'] = 0
        self.progress_label.config(text="0/0 (0%)")

        self.is_running = True
        self.btn_run.config(state="disabled", text="⏳ 작업 진행 중...")
        self.log("작업을 시작합니다...")

        t = threading.Thread(target=self._run_thread, args=(path,))
        t.daemon = True
        t.start()

    def _run_thread(self, filepath: str):
        try:
            result = process_excel_logic(
                filepath,
                log_func=self.log,
                progress_func=self.update_progress,
            )
            # result는 (out_path, no_images_path) 튜플 또는 단일 경로일 수 있음
            if isinstance(result, tuple):
                out_path, no_images_path = result
            else:
                out_path = result
                no_images_path = None
            self.after(0, lambda: self._on_complete(out_path, no_images_path))
        except Exception as e:
            self.log(f"❌ 오류 발생: {e}")
            self.log(traceback.format_exc())
            self.after(
                0,
                lambda: messagebox.showerror(
                    "오류",
                    f"작업 중 오류가 발생했습니다:\n{e}",
                ),
            )
        finally:
            self.is_running = False
            self.after(
                0,
                lambda: self.btn_run.config(
                    state="normal",
                    text="🚀 이미지 추출 및 다운로드 시작",
                ),
            )

    def _on_complete(self, out_path: str, no_images_path: str = None):
        if out_path and os.path.exists(out_path):
            # 혹시 100%가 아닐 수도 있으니 강제로 100으로 맞춰줌
            self.update_progress(1, 1)
            
            # 완료 메시지 구성
            msg = f"작업이 완료되었습니다.\n\n"
            msg += f"✅ 결과 파일: {os.path.basename(out_path)}\n"
            
            if no_images_path and os.path.exists(no_images_path):
                msg += f"\n⚠️ T2-1(실패) 분리 파일: {os.path.basename(no_images_path)}\n"
                msg += f"   (T1 단계까지만 작업 가능)\n"
            
            msg += f"\n파일을 여시겠습니까?"
            
            if messagebox.askyesno("완료", msg):
                try:
                    os.startfile(out_path)
                    # 분리된 파일도 함께 열기
                    if no_images_path and os.path.exists(no_images_path):
                        time.sleep(0.5)  # 첫 번째 파일이 열릴 시간을 줌
                        os.startfile(no_images_path)
                except Exception:
                    pass
        else:
            self.log("[안내] 결과 파일이 생성되지 않았습니다.")


if __name__ == "__main__":
    app = ImageExtractorApp()
    app.mainloop()
