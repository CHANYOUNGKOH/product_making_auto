"""
naver_ads_api.py

네이버 API 3종 통합 래퍼:
  1) 검색광고 키워드도구 API — 검색량 절대값 + 경쟁도
  2) 데이터랩 검색어트렌드 API — 시간별 상대 추이
  3) 데이터랩 쇼핑인사이트 API — 쇼핑 카테고리 트렌드

API 키는 프로젝트 루트 .env 에서 python-dotenv로 로딩.
여러 네이버 오픈API 어플리케이션 키를 등록하면 쿼터가 키 수만큼 배증.
"""
from __future__ import annotations

import hashlib
import hmac
import base64
import time
import os
import json
import threading
from dataclasses import dataclass, field, asdict
from typing import Any

import requests
from dotenv import load_dotenv

# ── .env 로딩 (프로젝트 루트 기준) ──────────────────────────────
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_ENV_PATH = os.path.join(_PROJECT_ROOT, ".env")
load_dotenv(_ENV_PATH)

# ── 환경 변수 ─────────────────────────────────────────────────
NAVER_AD_CUSTOMER_ID = os.getenv("NAVER_AD_CUSTOMER_ID", "")
NAVER_AD_API_KEY = os.getenv("NAVER_AD_API_KEY", "")
NAVER_AD_SECRET_KEY = os.getenv("NAVER_AD_SECRET_KEY", "")

NAVER_DATALAB_CLIENT_ID = os.getenv("NAVER_DATALAB_CLIENT_ID", "")
NAVER_DATALAB_CLIENT_SECRET = os.getenv("NAVER_DATALAB_CLIENT_SECRET", "")


# ================================================================
#  검색광고 API 멀티키 풀
# ================================================================
class NaverAdAPIKeyPool:
    """검색광고 API 멀티키 풀.

    .env 형식:
      NAVER_AD_CUSTOMER_ID=xxx      (1번째)
      NAVER_AD_API_KEY=yyy
      NAVER_AD_SECRET_KEY=zzz
      NAVER_AD_CUSTOMER_ID_2=aaa    (2번째)
      NAVER_AD_API_KEY_2=bbb
      NAVER_AD_SECRET_KEY_2=ccc
    """

    def __init__(self):
        self._keys: list[tuple[str, str, str]] = []  # [(customer_id, api_key, secret_key)]
        self._idx = 0
        self._lock = threading.Lock()
        self._load_keys()

    def _load_keys(self):
        cid = os.getenv("NAVER_AD_CUSTOMER_ID", "")
        akey = os.getenv("NAVER_AD_API_KEY", "")
        skey = os.getenv("NAVER_AD_SECRET_KEY", "")
        if cid and akey and skey:
            self._keys.append((cid, akey, skey))
        for i in range(2, 100):
            cid = os.getenv(f"NAVER_AD_CUSTOMER_ID_{i}", "")
            akey = os.getenv(f"NAVER_AD_API_KEY_{i}", "")
            skey = os.getenv(f"NAVER_AD_SECRET_KEY_{i}", "")
            if cid and akey and skey:
                self._keys.append((cid, akey, skey))
            else:
                break

    @property
    def key_count(self) -> int:
        return len(self._keys)

    def next_key(self) -> tuple[str, str, str]:
        """라운드로빈으로 다음 키셋 반환."""
        with self._lock:
            if not self._keys:
                return ("", "", "")
            idx = self._idx % len(self._keys)
            self._idx += 1
            return self._keys[idx]


ad_key_pool = NaverAdAPIKeyPool()


# ================================================================
#  네이버 오픈API 멀티키 풀
# ================================================================
class NaverOpenAPIKeyPool:
    """
    여러 네이버 오픈API 어플리케이션 키를 라운드로빈으로 로테이션.

    .env 형식:
      NAVER_DATALAB_CLIENT_ID=xxx          # 1번째 (기존 호환)
      NAVER_DATALAB_CLIENT_SECRET=yyy
      NAVER_DATALAB_CLIENT_ID_2=aaa        # 2번째
      NAVER_DATALAB_CLIENT_SECRET_2=bbb
      NAVER_DATALAB_CLIENT_ID_3=ccc        # 3번째
      NAVER_DATALAB_CLIENT_SECRET_3=ddd
      ...

    사용:
      pool = NaverOpenAPIKeyPool()
      cid, secret = pool.next_key()        # 라운드로빈
      pool.mark_exhausted(cid)             # 429 에러 시 해당 키 제외
    """

    def __init__(self):
        self._keys: list[tuple[str, str]] = []  # [(client_id, client_secret), ...]
        self._exhausted: set[str] = set()        # 429로 소진된 client_id
        self._idx = 0
        self._lock = threading.Lock()
        self._load_keys()

    def _load_keys(self):
        # 1번째 키 (기존 호환)
        cid = os.getenv("NAVER_DATALAB_CLIENT_ID", "")
        csec = os.getenv("NAVER_DATALAB_CLIENT_SECRET", "")
        if cid and csec:
            self._keys.append((cid, csec))

        # 2번째부터: NAVER_DATALAB_CLIENT_ID_2, _3, ...
        for i in range(2, 100):
            cid = os.getenv(f"NAVER_DATALAB_CLIENT_ID_{i}", "")
            csec = os.getenv(f"NAVER_DATALAB_CLIENT_SECRET_{i}", "")
            if cid and csec:
                self._keys.append((cid, csec))
            else:
                break

    @property
    def key_count(self) -> int:
        return len(self._keys)

    @property
    def available_count(self) -> int:
        with self._lock:
            return len(self._keys) - len(self._exhausted)

    def next_key(self) -> tuple[str, str]:
        """라운드로빈으로 다음 키 반환. 소진된 키는 건너뜀."""
        with self._lock:
            if not self._keys:
                return ("", "")
            # 최대 key_count번 시도 (모두 소진이면 첫 번째 반환)
            for _ in range(len(self._keys)):
                idx = self._idx % len(self._keys)
                self._idx += 1
                cid, csec = self._keys[idx]
                if cid not in self._exhausted:
                    return (cid, csec)
            # 모두 소진 → 첫 번째 반환 (재시도 가능하도록)
            return self._keys[0]

    def mark_exhausted(self, client_id: str):
        """429 에러 등으로 해당 키를 소진 상태로 표시."""
        with self._lock:
            self._exhausted.add(client_id)

    def reset_exhausted(self):
        """일일 리셋 시 소진 상태 초기화."""
        with self._lock:
            self._exhausted.clear()

    def all_exhausted(self) -> bool:
        with self._lock:
            return len(self._exhausted) >= len(self._keys)

    def get_all_keys(self) -> list[tuple[str, str]]:
        return list(self._keys)


# 싱글톤 인스턴스 (모듈 로드 시 1회 생성)
naver_key_pool = NaverOpenAPIKeyPool()


# ================================================================
#  1) 검색광고 키워드도구 API
# ================================================================
_AD_BASE_URL = "https://api.searchad.naver.com"


def generate_signature(timestamp: str, method: str, uri: str, secret_key: str) -> str:
    """네이버 검색광고 API 인증용 HMAC-SHA256 서명 생성."""
    message = f"{timestamp}.{method}.{uri}"
    sign = hmac.new(
        secret_key.encode("utf-8"),
        message.encode("utf-8"),
        hashlib.sha256,
    ).digest()
    return base64.b64encode(sign).decode("utf-8")


def _ad_headers(method: str, uri: str) -> dict[str, str]:
    if ad_key_pool.key_count > 0:
        cid, akey, skey = ad_key_pool.next_key()
    else:
        cid, akey, skey = NAVER_AD_CUSTOMER_ID, NAVER_AD_API_KEY, NAVER_AD_SECRET_KEY
    ts = str(int(time.time() * 1000))
    sig = generate_signature(ts, method, uri, skey)
    return {
        "X-Timestamp": ts,
        "X-API-KEY": akey,
        "X-Customer": cid,
        "X-Signature": sig,
        "Content-Type": "application/json; charset=UTF-8",
    }


@dataclass
class KeywordStat:
    keyword: str
    pc_volume: int = 0
    mobile_volume: int = 0
    total_volume: int = 0
    comp_idx: str = ""          # "높음" / "중간" / "낮음"
    monthly_avg_click: int = 0
    monthly_avg_click_rate: float = 0.0


def get_keyword_stats(keywords: list[str], *, batch_size: int = 5) -> list[KeywordStat]:
    """
    검색광고 키워드도구 API 호출.
    hintKeywords 파라미터로 5개씩 배치 호출하여 검색량/경쟁도 조회.
    """
    all_stats: list[KeywordStat] = []
    uri = "/keywordstool"

    for i in range(0, len(keywords), batch_size):
        chunk = keywords[i : i + batch_size]
        hint = ",".join(chunk)
        params = {
            "hintKeywords": hint,
            "showDetail": "1",
        }
        headers = _ad_headers("GET", uri)
        resp = requests.get(
            f"{_AD_BASE_URL}{uri}",
            params=params,
            headers=headers,
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()

        for item in data.get("keywordList", []):
            pc_vol = _to_int(item.get("monthlyPcQcCnt", 0))
            mob_vol = _to_int(item.get("monthlyMobileQcCnt", 0))
            all_stats.append(
                KeywordStat(
                    keyword=item.get("relKeyword", ""),
                    pc_volume=pc_vol,
                    mobile_volume=mob_vol,
                    total_volume=pc_vol + mob_vol,
                    comp_idx=item.get("compIdx", ""),
                    monthly_avg_click=_to_int(item.get("monthlyAvePcClkCnt", 0))
                    + _to_int(item.get("monthlyAveMobileClkCnt", 0)),
                    monthly_avg_click_rate=float(item.get("monthlyAvePcCtr", 0) or 0),
                )
            )

        # API rate limit 방지
        if i + batch_size < len(keywords):
            time.sleep(0.3)

    return all_stats


def get_related_keywords(
    hint_keyword: str,
    min_volume: int = 50,
) -> list[KeywordStat]:
    """
    단일 키워드의 연관 키워드 조회.

    hintKeywords에 1개만 전달하여 해당 키워드의 연관어 추출.
    입력 키워드 자체는 결과에서 제외.

    Args:
        hint_keyword: 시드 키워드 (1개)
        min_volume: 최소 검색량 필터 (기본 50)

    Returns:
        검색량 >= min_volume인 연관 키워드 목록 (검색량 내림차순)
    """
    uri = "/keywordstool"
    params = {"hintKeywords": hint_keyword, "showDetail": "1"}
    headers = _ad_headers("GET", uri)

    resp = requests.get(
        f"{_AD_BASE_URL}{uri}",
        params=params,
        headers=headers,
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()

    hint_lower = hint_keyword.strip().lower()
    results: list[KeywordStat] = []

    for item in data.get("keywordList", []):
        rel_kw = item.get("relKeyword", "").strip()
        if not rel_kw or rel_kw.lower() == hint_lower:
            continue
        pc_vol = _to_int(item.get("monthlyPcQcCnt", 0))
        mob_vol = _to_int(item.get("monthlyMobileQcCnt", 0))
        total = pc_vol + mob_vol
        if total < min_volume:
            continue
        results.append(
            KeywordStat(
                keyword=rel_kw,
                pc_volume=pc_vol,
                mobile_volume=mob_vol,
                total_volume=total,
                comp_idx=item.get("compIdx", ""),
                monthly_avg_click=_to_int(item.get("monthlyAvePcClkCnt", 0))
                + _to_int(item.get("monthlyAveMobileClkCnt", 0)),
                monthly_avg_click_rate=float(item.get("monthlyAvePcCtr", 0) or 0),
            )
        )

    results.sort(key=lambda s: s.total_volume, reverse=True)
    return results


def validate_keywords(keywords: list[str]) -> dict[str, Any]:
    """
    키워드 목록을 검색광고 API로 검증.

    Returns:
        {
            "verified": [{"keyword": str, "pc_volume": int, "mobile_volume": int, "total_volume": int, "comp": str}, ...],
            "unrecognized": [str, ...]
        }
    """
    stats = get_keyword_stats(keywords)

    # API가 반환한 키워드 세트 (relKeyword 기준)
    returned_kws: dict[str, KeywordStat] = {}
    for s in stats:
        returned_kws[s.keyword] = s

    verified: list[dict] = []
    unrecognized: list[str] = []

    for kw in keywords:
        stat = returned_kws.get(kw)
        if stat and stat.total_volume > 0:
            verified.append({
                "keyword": stat.keyword,
                "pc_volume": stat.pc_volume,
                "mobile_volume": stat.mobile_volume,
                "total_volume": stat.total_volume,
                "comp": stat.comp_idx,
            })
        else:
            unrecognized.append(kw)

    return {"verified": verified, "unrecognized": unrecognized}


# ================================================================
#  2) 데이터랩 검색어트렌드 API
# ================================================================
_DATALAB_SEARCH_URL = "https://openapi.naver.com/v1/datalab/search"


def _datalab_headers() -> dict[str, str]:
    cid, csec = naver_key_pool.next_key()
    return {
        "X-Naver-Client-Id": cid,
        "X-Naver-Client-Secret": csec,
        "Content-Type": "application/json",
    }


def shopping_search_headers() -> dict[str, str]:
    """쇼핑검색 API용 헤더 (키 풀 라운드로빈)."""
    cid, csec = naver_key_pool.next_key()
    return {
        "X-Naver-Client-Id": cid,
        "X-Naver-Client-Secret": csec,
    }


def get_search_trend(
    keywords: list[str],
    start_date: str,
    end_date: str,
    time_unit: str = "month",
) -> dict[str, Any]:
    """
    데이터랩 검색어트렌드 API.

    Args:
        keywords: 키워드 목록 (최대 5개 그룹, 각 그룹 최대 20개 키워드)
        start_date: 시작일 (YYYY-MM-DD)
        end_date: 종료일 (YYYY-MM-DD)
        time_unit: "date" / "week" / "month"

    Returns:
        API 응답 JSON (results 배열에 시계열 데이터)
    """
    # 키워드를 개별 그룹으로 구성 (최대 5개)
    keyword_groups = []
    for kw in keywords[:5]:
        keyword_groups.append({
            "groupName": kw,
            "keywords": [kw],
        })

    payload = {
        "startDate": start_date,
        "endDate": end_date,
        "timeUnit": time_unit,
        "keywordGroups": keyword_groups,
    }

    resp = requests.post(
        _DATALAB_SEARCH_URL,
        headers=_datalab_headers(),
        json=payload,
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json()


# ================================================================
#  3) 데이터랩 쇼핑인사이트 API
# ================================================================
_DATALAB_SHOPPING_URL = "https://openapi.naver.com/v1/datalab/shopping/categories"
_DATALAB_SHOPPING_KW_URL = "https://openapi.naver.com/v1/datalab/shopping/category/keywords"


def get_shopping_keyword_trend(
    category_id: str,
    keywords: list[str],
    start_date: str,
    end_date: str,
    time_unit: str = "month",
) -> dict[str, Any]:
    """
    데이터랩 쇼핑인사이트 — 카테고리 내 키워드 검색 추이.

    Args:
        category_id: 네이버 쇼핑 카테고리 ID
        keywords: 키워드 목록 (최대 5개)
        start_date: YYYY-MM-DD
        end_date: YYYY-MM-DD
        time_unit: "date" / "week" / "month"

    Returns:
        API 응답 JSON (results 배열에 키워드별 시계열 데이터)
    """
    keyword_groups = []
    for kw in keywords[:5]:
        keyword_groups.append({
            "name": kw,
            "param": [kw],
        })

    payload = {
        "startDate": start_date,
        "endDate": end_date,
        "timeUnit": time_unit,
        "category": category_id,
        "keyword": keyword_groups,
    }

    resp = requests.post(
        _DATALAB_SHOPPING_KW_URL,
        headers=_datalab_headers(),
        json=payload,
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json()


def get_shopping_trend(
    category_id: str,
    start_date: str,
    end_date: str,
    time_unit: str = "month",
) -> dict[str, Any]:
    """
    데이터랩 쇼핑인사이트 — 카테고리 트렌드 조회.

    Args:
        category_id: 네이버 쇼핑 카테고리 ID
        start_date: YYYY-MM-DD
        end_date: YYYY-MM-DD
        time_unit: "date" / "week" / "month"

    Returns:
        API 응답 JSON
    """
    payload = {
        "startDate": start_date,
        "endDate": end_date,
        "timeUnit": time_unit,
        "category": [
            {"name": f"cat_{category_id}", "param": [category_id]},
        ],
    }

    resp = requests.post(
        _DATALAB_SHOPPING_URL,
        headers=_datalab_headers(),
        json=payload,
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


# ================================================================
#  유틸리티
# ================================================================
def _to_int(v: Any) -> int:
    """'< 10' 같은 문자열도 안전하게 정수로 변환."""
    if v is None:
        return 0
    if isinstance(v, (int, float)):
        return int(v)
    s = str(v).strip().replace(",", "")
    if s.startswith("< ") or s.startswith("<"):
        s = s.replace("<", "").strip()
    if not s or s == "-":
        return 0
    try:
        return int(float(s))
    except (ValueError, TypeError):
        return 0


COMP_MULTIPLIER = {"낮음": 3, "중간": 2, "높음": 1, "": 0}


def opportunity_score(stat: KeywordStat) -> int:
    """검색량 * 경쟁도 가중치. 높을수록 노릴 만한 키워드."""
    return stat.total_volume * COMP_MULTIPLIER.get(stat.comp_idx, 0)


def check_api_keys() -> dict[str, bool]:
    """현재 로딩된 API 키 상태를 확인."""
    return {
        "ad_api": ad_key_pool.key_count > 0,
        "ad_key_count": ad_key_pool.key_count,
        "datalab": naver_key_pool.key_count > 0,
        "datalab_key_count": naver_key_pool.key_count,
    }
