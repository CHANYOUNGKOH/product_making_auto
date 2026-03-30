"""
keyword_enricher.py

ST2→ST3 키워드 보강 모듈.
  Phase 1:   검색광고 API -> 검색량/경쟁도
  Phase 1.5: 연관키워드 확장 -> 시드 키워드 기반 연관어 추출
  Phase 2:   검색어트렌드 API -> 12개월 추이 수집 + 판별 (^/v/-/~)
  Phase 3:   쇼핑인사이트 API -> 카테고리 내 쇼핑 키워드 추이

흐름:
  1. DataFrame에서 search_keywords 추출 -> 유니크 키워드 목록
  2. 검색광고 API -> 검색량/경쟁도/기회점수
  2.5. 행별 상위 시드 → 연관키워드 API → 키워드 풀 확장
  3. 검색어트렌드 API -> 12개월 시계열 + 추이 판별
  4. 쇼핑인사이트 API -> 카테고리별 쇼핑 키워드 추이
  5. 행별 간결 요약 생성 -> df["keyword_enriched"]

간결 요약 형식:
  TOP: 단열테이프(16260/H-) 보온테이프(1530/H^) 방수테이프(590/Hv)
  OK: 털귀마개(890~) 귀도리(450-)
  X: 이어머프캡 모자귀마개
  REL: 보온필름(3200/M^) 창문테이프(1200/L-)
  SHOP: 보온테이프^ 단열테이프- 방수테이프v

트렌드 인디케이터:
  ^ 상승  v 하락  - 유지  ~ 시즌  (없음) 데이터부족
"""
from __future__ import annotations

import json
import os
import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from typing import Any, Callable, Optional

import pandas as pd

from naver_ads_api import (
    KeywordStat,
    get_keyword_stats,
    get_related_keywords,
    get_search_trend,
    get_shopping_keyword_trend,
    opportunity_score,
    COMP_MULTIPLIER,
    check_api_keys,
    naver_key_pool,
    ad_key_pool,
)

# ── 상수 ──────────────────────────────────────────────────────
_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
_DEFAULT_DB_PATH = os.path.join(_THIS_DIR, "keyword_cache.db")
_CATEGORY_MAP_PATH = os.path.join(_THIS_DIR, "category_id_map.json")

CACHE_TTL_DAYS = 7
SHOP_TREND_CACHE_TTL_DAYS = 7  # 쇼핑 추이 캐시 TTL (7일)
TREND_CACHE_TTL_DAYS = 1   # 트렌드 캐시 TTL (1일)
TOP_COUNT = 5              # TOP 키워드 최대 개수
MAX_RETRIES = 3            # 429 에러 시 최대 재시도
BATCH_SIZE = 5             # API 호출 시 키워드 배치 크기
BATCH_DELAY = 0.5          # 배치 간 대기 (초)
_DATALAB_QUOTA_PER_KEY = 1000  # DataLab API 앱 1개당 일일 호출 제한
DATALAB_DAILY_QUOTA = _DATALAB_QUOTA_PER_KEY * max(naver_key_pool.key_count, 1)
RELATED_SEED_COUNT = 3     # 행별 연관키워드 확장용 시드 개수
RELATED_MAX_PER_SEED = 15  # 시드당 최대 연관키워드 수
RELATED_MIN_VOLUME = 50    # 연관키워드 최소 검색량

# 경쟁도 한글 → 약어
COMP_SHORT = {"낮음": "L", "중간": "M", "높음": "H", "": ""}


# ================================================================
#  KeywordCache -SQLite 기반 키워드 통계 캐시
# ================================================================
class KeywordCache:
    """검색광고 API 결과를 SQLite에 캐싱. 7일 TTL."""

    def __init__(self, db_path: str = _DEFAULT_DB_PATH):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS keyword_stats (
                keyword      TEXT PRIMARY KEY,
                pc_volume    INTEGER NOT NULL DEFAULT 0,
                mobile_volume INTEGER NOT NULL DEFAULT 0,
                total_volume INTEGER NOT NULL DEFAULT 0,
                comp_idx     TEXT NOT NULL DEFAULT '',
                score        INTEGER NOT NULL DEFAULT 0,
                fetched_at   TEXT NOT NULL
            )
            """
        )
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS keyword_trends (
                keyword     TEXT PRIMARY KEY,
                trend_data  TEXT NOT NULL,
                fetched_at  TEXT NOT NULL
            )
            """
        )
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS shopping_keyword_trends (
                category_id TEXT NOT NULL,
                keyword     TEXT NOT NULL,
                trend_data  TEXT NOT NULL,
                fetched_at  TEXT NOT NULL,
                PRIMARY KEY (category_id, keyword)
            )
            """
        )
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS daily_quota (
                api_name    TEXT NOT NULL,
                call_date   TEXT NOT NULL,
                call_count  INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (api_name, call_date)
            )
            """
        )
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS related_keywords (
                seed_keyword TEXT PRIMARY KEY,
                related_json TEXT NOT NULL,
                fetched_at   TEXT NOT NULL
            )
            """
        )
        self.conn.commit()

    # ── 조회 ──────────────────────────────────────────────
    def get(self, keyword: str) -> Optional[dict]:
        """캐시에서 키워드 조회. TTL 만료면 None 반환."""
        cur = self.conn.execute(
            "SELECT pc_volume, mobile_volume, total_volume, comp_idx, score, fetched_at "
            "FROM keyword_stats WHERE keyword = ?",
            (keyword,),
        )
        row = cur.fetchone()
        if row is None:
            return None

        fetched_at = datetime.fromisoformat(row[5])
        if datetime.now() - fetched_at > timedelta(days=CACHE_TTL_DAYS):
            return None  # 만료

        return {
            "keyword": keyword,
            "pc_volume": row[0],
            "mobile_volume": row[1],
            "total_volume": row[2],
            "comp_idx": row[3],
            "score": row[4],
        }

    def get_many(self, keywords: list[str]) -> dict[str, dict]:
        """여러 키워드 일괄 조회. {keyword: stats_dict} 반환."""
        result = {}
        for kw in keywords:
            cached = self.get(kw)
            if cached is not None:
                result[kw] = cached
        return result

    # ── 저장 ──────────────────────────────────────────────
    def put(self, stat: KeywordStat, score: int) -> None:
        self.conn.execute(
            """
            INSERT OR REPLACE INTO keyword_stats
            (keyword, pc_volume, mobile_volume, total_volume, comp_idx, score, fetched_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                stat.keyword,
                stat.pc_volume,
                stat.mobile_volume,
                stat.total_volume,
                stat.comp_idx,
                score,
                datetime.now().isoformat(),
            ),
        )
        self.conn.commit()

    def put_many(self, stats: list[KeywordStat]) -> None:
        """여러 KeywordStat 일괄 저장."""
        now = datetime.now().isoformat()
        rows = []
        for s in stats:
            sc = opportunity_score(s)
            rows.append((s.keyword, s.pc_volume, s.mobile_volume, s.total_volume, s.comp_idx, sc, now))
        self.conn.executemany(
            """
            INSERT OR REPLACE INTO keyword_stats
            (keyword, pc_volume, mobile_volume, total_volume, comp_idx, score, fetched_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        self.conn.commit()

    # ── 트렌드 캐시 ─────────────────────────────────────
    def get_trend(self, keyword: str) -> Optional[list[dict]]:
        """트렌드 캐시에서 키워드 조회. TTL 만료면 None 반환."""
        cur = self.conn.execute(
            "SELECT trend_data, fetched_at FROM keyword_trends WHERE keyword = ?",
            (keyword,),
        )
        row = cur.fetchone()
        if row is None:
            return None

        fetched_at = datetime.fromisoformat(row[1])
        if datetime.now() - fetched_at > timedelta(days=TREND_CACHE_TTL_DAYS):
            return None

        try:
            return json.loads(row[0])
        except (json.JSONDecodeError, TypeError):
            return None

    def get_trends_many(self, keywords: list[str]) -> dict[str, list[dict]]:
        """여러 키워드 트렌드 일괄 조회. {keyword: trend_data} 반환."""
        result = {}
        for kw in keywords:
            cached = self.get_trend(kw)
            if cached is not None:
                result[kw] = cached
        return result

    def put_trend(self, keyword: str, trend_data: list[dict]) -> None:
        """트렌드 데이터 저장."""
        self.conn.execute(
            """
            INSERT OR REPLACE INTO keyword_trends (keyword, trend_data, fetched_at)
            VALUES (?, ?, ?)
            """,
            (keyword, json.dumps(trend_data, ensure_ascii=False), datetime.now().isoformat()),
        )
        self.conn.commit()

    def put_trends_many(self, trends: dict[str, list[dict]]) -> None:
        """여러 키워드 트렌드 일괄 저장."""
        now = datetime.now().isoformat()
        rows = [(kw, json.dumps(data, ensure_ascii=False), now) for kw, data in trends.items()]
        self.conn.executemany(
            """
            INSERT OR REPLACE INTO keyword_trends (keyword, trend_data, fetched_at)
            VALUES (?, ?, ?)
            """,
            rows,
        )
        self.conn.commit()

    # ── 쇼핑 키워드 추이 캐시 ─────────────────────────
    def get_shopping_trend(self, category_id: str, keyword: str) -> Optional[list[dict]]:
        """쇼핑 키워드 추이 캐시에서 조회. TTL 만료면 None."""
        cur = self.conn.execute(
            "SELECT trend_data, fetched_at FROM shopping_keyword_trends "
            "WHERE category_id = ? AND keyword = ?",
            (category_id, keyword),
        )
        row = cur.fetchone()
        if row is None:
            return None

        fetched_at = datetime.fromisoformat(row[1])
        if datetime.now() - fetched_at > timedelta(days=SHOP_TREND_CACHE_TTL_DAYS):
            return None

        try:
            return json.loads(row[0])
        except (json.JSONDecodeError, TypeError):
            return None

    def get_shopping_trends_many(
        self, category_id: str, keywords: list[str],
    ) -> dict[str, list[dict]]:
        """여러 키워드 쇼핑 추이 일괄 조회. {keyword: trend_data}."""
        result = {}
        for kw in keywords:
            cached = self.get_shopping_trend(category_id, kw)
            if cached is not None:
                result[kw] = cached
        return result

    def put_shopping_trends_many(
        self, category_id: str, trends: dict[str, list[dict]],
    ) -> None:
        """여러 키워드 쇼핑 추이 일괄 저장."""
        now = datetime.now().isoformat()
        rows = [
            (category_id, kw, json.dumps(data, ensure_ascii=False), now)
            for kw, data in trends.items()
        ]
        self.conn.executemany(
            """
            INSERT OR REPLACE INTO shopping_keyword_trends
            (category_id, keyword, trend_data, fetched_at)
            VALUES (?, ?, ?, ?)
            """,
            rows,
        )
        self.conn.commit()

    # ── 일일 쿼터 ─────────────────────────────────────
    def get_quota(self, api_name: str) -> int:
        """오늘 api_name의 호출 횟수 반환."""
        today = datetime.now().strftime("%Y-%m-%d")
        cur = self.conn.execute(
            "SELECT call_count FROM daily_quota WHERE api_name = ? AND call_date = ?",
            (api_name, today),
        )
        row = cur.fetchone()
        return row[0] if row else 0

    def increment_quota(self, api_name: str, count: int = 1) -> None:
        """오늘 api_name 호출 횟수 증가."""
        today = datetime.now().strftime("%Y-%m-%d")
        self.conn.execute(
            """
            INSERT INTO daily_quota (api_name, call_date, call_count)
            VALUES (?, ?, ?)
            ON CONFLICT(api_name, call_date)
            DO UPDATE SET call_count = call_count + ?
            """,
            (api_name, today, count, count),
        )
        self.conn.commit()

    # ── 연관키워드 캐시 ────────────────────────────────
    def get_related(self, seed_keyword: str) -> Optional[list[dict]]:
        """연관키워드 캐시에서 조회. TTL 만료면 None."""
        cur = self.conn.execute(
            "SELECT related_json, fetched_at FROM related_keywords WHERE seed_keyword = ?",
            (seed_keyword,),
        )
        row = cur.fetchone()
        if row is None:
            return None
        fetched_at = datetime.fromisoformat(row[1])
        if datetime.now() - fetched_at > timedelta(days=CACHE_TTL_DAYS):
            return None
        try:
            return json.loads(row[0])
        except (json.JSONDecodeError, TypeError):
            return None

    def put_related(self, seed_keyword: str, related: list[dict]) -> None:
        """연관키워드 캐시 저장."""
        self.conn.execute(
            """
            INSERT OR REPLACE INTO related_keywords (seed_keyword, related_json, fetched_at)
            VALUES (?, ?, ?)
            """,
            (seed_keyword, json.dumps(related, ensure_ascii=False), datetime.now().isoformat()),
        )
        self.conn.commit()

    def close(self) -> None:
        self.conn.close()


# ================================================================
#  KeywordEnricher -DataFrame 키워드 보강
# ================================================================
class KeywordEnricher:
    """
    ST2 엑셀 DataFrame의 search_keywords를 검색광고 API로 보강.
    결과를 df["keyword_enriched"] 컬럼에 간결 텍스트로 저장.
    """

    def __init__(self, cache: Optional[KeywordCache] = None):
        self.cache = cache or KeywordCache()
        self._category_map: dict[str, str] = self._load_category_map()

    def enrich_dataframe(
        self,
        df: pd.DataFrame,
        progress_cb: Optional[Callable[[str], None]] = None,
    ) -> pd.DataFrame:
        """
        메인 진입점. DataFrame에 keyword_enriched 컬럼을 추가/갱신.

        Args:
            df: ST2_JSON 컬럼이 있는 DataFrame
            progress_cb: 진행 상황 콜백 (로그 메시지 문자열)

        Returns:
            keyword_enriched 컬럼이 추가된 DataFrame
        """
        def _log(msg: str):
            if progress_cb:
                progress_cb(msg)

        # API 키 확인
        key_status = check_api_keys()
        if not key_status.get("ad_api"):
            _log("[enricher] 검색광고 API 키 미설정 -enrichment 스킵")
            return df

        kc = key_status.get("datalab_key_count", 1)
        if kc > 1:
            _log(f"[enricher] 오픈API 키 {kc}개 감지 → 데이터랩 쿼터 {DATALAB_DAILY_QUOTA}/일")

        # 1) 행별 키워드 추출
        row_keywords = self._extract_unique_keywords(df)
        all_keywords = set()
        for kws in row_keywords.values():
            all_keywords.update(kws)
        all_keywords = sorted(all_keywords)
        _log(f"[enricher] 유니크 키워드 {len(all_keywords)}개 추출 (총 {len(row_keywords)}행)")

        if not all_keywords:
            _log("[enricher] 키워드 없음 -enrichment 스킵")
            return df

        # 원본 키워드 개수 기록 (연관키워드 구분용)
        original_kw_counts: dict[int, int] = {idx: len(kws) for idx, kws in row_keywords.items()}

        # 상품 컨텍스트 추출 (연관키워드 적합성 필터용)
        product_context = self._extract_product_context(df)

        # 2) Phase 1: 캐시 확인 & API 호출 (원본 키워드 통계)
        stats = self._fetch_and_cache_stats(all_keywords, _log)
        _log(f"[enricher] 통계 확보: {len(stats)}개 키워드")

        # 2.5) Phase 1.5: 연관키워드 확장 (적합성 필터 적용)
        row_keywords, new_keywords = self._expand_with_related(
            row_keywords, stats, product_context, _log,
        )
        if new_keywords:
            # 새 키워드 통계도 수집 (캐시에 없는 것만)
            new_kw_list = sorted(new_keywords - set(stats.keys()))
            if new_kw_list:
                new_stats = self._fetch_and_cache_stats(new_kw_list, _log)
                stats.update(new_stats)
            # all_keywords 갱신
            all_keywords = sorted(set(all_keywords) | new_keywords)
            _log(f"[enricher] 확장 후 유니크 키워드: {len(all_keywords)}개")

        # 3) Phase 2: 트렌드 수집 + 판별 -> stats에 trend 추가
        key_status = check_api_keys()
        if key_status.get("datalab"):
            keywords_with_volume = [
                kw for kw in all_keywords
                if stats.get(kw, {}).get("total_volume", 0) > 0
            ]
            if keywords_with_volume:
                trends = self._fetch_and_cache_trends(keywords_with_volume, _log)
                classified = 0
                for kw, trend_data in trends.items():
                    if kw in stats:
                        stats[kw]["trend"] = self._classify_trend(trend_data)
                        classified += 1
                _log(f"[trend] 추이 판별 완료: {classified}개 키워드")
            else:
                _log("[enricher] 검색량 > 0 키워드 없음 - 트렌드 스킵")
        else:
            _log("[enricher] DataLab API 키 미설정 - 트렌드 스킵")

        # 3.5) Phase 3: 쇼핑 키워드 추이 수집
        row_categories: dict[int, str] = {}
        if key_status.get("datalab") and self._category_map:
            row_categories = self._extract_row_categories(df)
            # 카테고리별로 키워드 수집
            cat_keywords: dict[str, set[str]] = {}  # category_id -> keywords
            cat_row_map: dict[int, str] = {}  # row_idx -> category_id
            for idx, cat_path in row_categories.items():
                cat_id = self._resolve_category_id(cat_path)
                if cat_id is None:
                    continue
                cat_row_map[idx] = cat_id
                if cat_id not in cat_keywords:
                    cat_keywords[cat_id] = set()
                # 이 행의 검색량 > 0 키워드만 수집
                for kw in row_keywords.get(idx, []):
                    if stats.get(kw, {}).get("total_volume", 0) > 0:
                        cat_keywords[cat_id].add(kw)

            if cat_keywords:
                _log(f"[shop] 매핑된 카테고리 {len(cat_keywords)}개, 쇼핑 추이 수집 시작")
                # 카테고리별 쇼핑 키워드 추이 수집
                shop_trends_all: dict[str, dict[str, list[dict]]] = {}  # cat_id -> {kw: data}
                for cat_id, kws in cat_keywords.items():
                    kw_list = sorted(kws)
                    if not kw_list:
                        continue
                    shop_trends = self._fetch_and_cache_shopping_trends(
                        cat_id, kw_list, _log,
                    )
                    if shop_trends:
                        shop_trends_all[cat_id] = shop_trends

                # stats에 shop_trend 추가 (행별 카테고리에 따라)
                shop_classified = 0
                for idx, cat_id in cat_row_map.items():
                    cat_trends = shop_trends_all.get(cat_id, {})
                    for kw in row_keywords.get(idx, []):
                        if kw in cat_trends and kw in stats:
                            shop_label = self._classify_trend(cat_trends[kw])
                            if shop_label:
                                stats[kw]["shop_trend"] = shop_label
                                shop_classified += 1

                if shop_classified:
                    _log(f"[shop] 쇼핑 추이 판별 완료: {shop_classified}개")
            else:
                _log("[shop] 매핑된 카테고리 없음 -쇼핑 추이 스킵")
        elif not self._category_map:
            _log("[shop] category_id_map.json 비어있음 - 쇼핑 추이 스킵")

        # 4) 행별 간결 요약 생성 (트렌드 포함)
        enriched_col = [""] * len(df)
        for idx, kws in row_keywords.items():
            if kws:
                orig_count = original_kw_counts.get(idx, len(kws))
                enriched_col[idx] = self._build_compact_summary(kws, stats, orig_count)

        df["keyword_enriched"] = enriched_col
        enriched_count = sum(1 for v in enriched_col if v)
        _log(f"[enricher] keyword_enriched 컬럼 생성 완료 ({enriched_count}행)")

        return df

    # ── 카테고리 매핑 ──────────────────────────────────────
    @staticmethod
    def _load_category_map() -> dict[str, str]:
        """category_id_map.json 로드. 없으면 빈 dict."""
        if not os.path.isfile(_CATEGORY_MAP_PATH):
            return {}
        try:
            with open(_CATEGORY_MAP_PATH, "r", encoding="utf-8") as f:
                data = json.load(f)
            return data if isinstance(data, dict) else {}
        except (json.JSONDecodeError, OSError):
            return {}

    def _resolve_category_id(self, category_path: str) -> Optional[str]:
        """카테고리 경로 → 네이버 쇼핑 카테고리 ID. prefix 매칭 (긴 것 우선)."""
        if not category_path or not self._category_map:
            return None
        # 정확히 일치하는 키 우선
        if category_path in self._category_map:
            return self._category_map[category_path]
        # prefix 매칭 (긴 키 우선)
        best_key = ""
        for key in self._category_map:
            if category_path.startswith(key) and len(key) > len(best_key):
                best_key = key
        return self._category_map[best_key] if best_key else None

    def _extract_row_categories(
        self, df: pd.DataFrame,
    ) -> dict[int, str]:
        """각 행의 ST2_JSON에서 meta.카테고리_경로 추출. {row_index: category_path}"""
        result: dict[int, str] = {}
        for idx, row in df.iterrows():
            raw = str(row.get("ST2_JSON", "")).strip()
            if not raw or raw == "nan":
                continue
            try:
                data = json.loads(raw)
            except (json.JSONDecodeError, TypeError):
                continue
            cat = data.get("meta", {}).get("카테고리_경로", "")
            if cat:
                result[idx] = cat
        return result

    # ── Phase 3: 쇼핑 키워드 추이 수집 ───────────────────
    def _fetch_and_cache_shopping_trends(
        self,
        category_id: str,
        keywords: list[str],
        log_fn: Callable[[str], None],
    ) -> dict[str, list[dict]]:
        """
        쇼핑인사이트 카테고리 키워드 추이 수집.
        5키워드/콜. 쿼터는 search_trend + shopping_keyword 합산.

        Returns: {keyword: [{period, ratio}, ...]}
        """
        # 캐시 확인
        cached = self.cache.get_shopping_trends_many(category_id, keywords)
        uncached = [kw for kw in keywords if kw not in cached]

        if not uncached:
            return cached

        # 쿼터 확인 (Phase 2+3 합산)
        trend_used = self.cache.get_quota("search_trend")
        shop_used = self.cache.get_quota("shopping_keyword")
        remaining = DATALAB_DAILY_QUOTA - trend_used - shop_used

        if remaining <= 0:
            log_fn(f"[shop] 일일 쿼터 소진 (trend:{trend_used}+shop:{shop_used}/{DATALAB_DAILY_QUOTA})")
            return cached

        needed_calls = (len(uncached) + BATCH_SIZE - 1) // BATCH_SIZE
        if needed_calls > remaining:
            max_kw = remaining * BATCH_SIZE
            uncached = uncached[:max_kw]
            needed_calls = remaining
            log_fn(f"[shop] 쿼터 부족 -{len(uncached)}개만 수집 (남은: {remaining})")

        # 날짜 계산: 최근 12개월
        now = datetime.now()
        end_date = now.strftime("%Y-%m-%d")
        start_date = (now - timedelta(days=365)).strftime("%Y-%m-%d")

        # API 호출 (5개씩 배치, 멀티키 병렬)
        new_trends: dict[str, list[dict]] = {}
        api_calls = 0
        batches = [uncached[i : i + BATCH_SIZE] for i in range(0, len(uncached), BATCH_SIZE)]
        workers = min(max(naver_key_pool.key_count, 1), 3)

        def _shop_trend_worker(chunk):
            try:
                resp = get_shopping_keyword_trend(
                    category_id, chunk, start_date, end_date, time_unit="month",
                )
                partial = {}
                for r in resp.get("results", []):
                    kw = r.get("title", "")
                    data_points = [
                        {"period": dp.get("period", ""), "ratio": dp.get("ratio", 0)}
                        for dp in r.get("data", [])
                    ]
                    if kw and data_points:
                        partial[kw] = data_points
                return partial
            except Exception as e:
                return {"__error__": str(e)[:100]}

        if workers > 1 and len(batches) > 1:
            with ThreadPoolExecutor(max_workers=workers) as executor:
                for result in executor.map(_shop_trend_worker, batches):
                    if "__error__" in result:
                        log_fn(f"[shop] API 에러 (cat:{category_id}): {result['__error__']}")
                    else:
                        new_trends.update(result)
                        api_calls += 1
        else:
            for chunk in batches:
                result = _shop_trend_worker(chunk)
                if "__error__" in result:
                    log_fn(f"[shop] API 에러 (cat:{category_id}): {result['__error__']}")
                else:
                    new_trends.update(result)
                    api_calls += 1
                if len(batches) > 1:
                    time.sleep(BATCH_DELAY)

        # 쿼터 기록
        if api_calls > 0:
            self.cache.increment_quota("shopping_keyword", api_calls)
            log_fn(f"[shop] cat:{category_id} -{api_calls}콜, {len(new_trends)}개 결과")

        # 캐시 저장
        if new_trends:
            self.cache.put_shopping_trends_many(category_id, new_trends)

        # 합치기
        all_trends = dict(cached)
        all_trends.update(new_trends)
        return all_trends

    # ── 키워드 추출 ───────────────────────────────────────
    def _extract_unique_keywords(self, df: pd.DataFrame) -> dict[int, list[str]]:
        """
        각 행의 ST2_JSON에서 search_keywords 추출.
        Returns: {row_index: [keyword, ...]}
        """
        result: dict[int, list[str]] = {}
        for idx, row in df.iterrows():
            raw = str(row.get("ST2_JSON", "")).strip()
            if not raw or raw == "nan":
                continue
            try:
                data = json.loads(raw)
            except (json.JSONDecodeError, TypeError):
                continue

            kws = data.get("search_keywords", [])
            if isinstance(kws, str):
                kws = [k.strip() for k in kws.split(",") if k.strip()]
            elif isinstance(kws, list):
                kws = [str(k).strip() for k in kws if str(k).strip()]
            else:
                continue

            if kws:
                result[idx] = kws
        return result

    @staticmethod
    def _extract_product_context(df: pd.DataFrame) -> dict[int, set[str]]:
        """
        각 행의 ST2_JSON에서 상품 컨텍스트 토큰 추출.
        연관키워드 적합성 판별에 사용.

        추출 소스:
          - search_keywords (원본 키워드)
          - naming_seeds.상품핵심명사
          - naming_seeds.기능/효과표현
          - naming_seeds.상황/장소/계절
          - core_attributes.상품타입, 상위카테고리
          - meta.카테고리_경로

        Returns: {row_index: set of context tokens (2글자 이상)}
        """
        result: dict[int, set[str]] = {}
        for idx, row in df.iterrows():
            raw = str(row.get("ST2_JSON", "")).strip()
            if not raw or raw == "nan":
                continue
            try:
                data = json.loads(raw)
            except (json.JSONDecodeError, TypeError):
                continue

            tokens: set[str] = set()

            # search_keywords
            kws = data.get("search_keywords", [])
            if isinstance(kws, str):
                kws = [k.strip() for k in kws.split(",")]
            elif not isinstance(kws, list):
                kws = []
            for kw in kws:
                t = str(kw).strip()
                if len(t) >= 2:
                    tokens.add(t)

            # naming_seeds 배열 필드
            seeds = data.get("naming_seeds", {})
            for key in ("상품핵심명사", "기능/효과표현", "상황/장소/계절"):
                val = seeds.get(key, [])
                if isinstance(val, list):
                    for item in val:
                        t = str(item).strip()
                        if len(t) >= 2:
                            tokens.add(t)

            # core_attributes
            core = data.get("core_attributes", {})
            for key in ("상품타입", "상위카테고리"):
                val = core.get(key, "")
                if isinstance(val, str):
                    for part in val.replace(",", " ").replace("/", " ").split():
                        t = part.strip()
                        if len(t) >= 2:
                            tokens.add(t)

            # meta.카테고리_경로
            cat = data.get("meta", {}).get("카테고리_경로", "")
            for part in cat.replace(">", " ").replace("/", " ").split():
                t = part.strip()
                if len(t) >= 2:
                    tokens.add(t)

            if tokens:
                result[idx] = tokens
        return result

    @staticmethod
    def _is_relevant(keyword: str, context: set[str]) -> bool:
        """
        연관키워드가 상품 컨텍스트와 관련 있는지 판별.
        컨텍스트 토큰 중 하나라도 키워드에 포함되거나,
        키워드가 컨텍스트 토큰에 포함되면 관련 있음.
        """
        kw = keyword.strip()
        for ctx in context:
            if ctx in kw or kw in ctx:
                return True
        return False

    # ── Phase 1.5: 연관키워드 확장 ──────────────────────
    def _expand_with_related(
        self,
        row_keywords: dict[int, list[str]],
        stats: dict[str, dict],
        product_context: dict[int, set[str]],
        log_fn: Callable[[str], None],
    ) -> tuple[dict[int, list[str]], set[str]]:
        """
        행별 상위 시드 키워드의 연관키워드를 API로 조회하여 확장.

        - 행별 opportunity_score 상위 RELATED_SEED_COUNT개를 시드로 선택
        - 시드당 최대 RELATED_MAX_PER_SEED개 연관키워드 추가
        - 이미 행에 있는 키워드는 제외

        Returns:
            (확장된 row_keywords, 새로 추가된 키워드 전체 set)
        """
        # 전체 시드 수집 (중복 제거)
        seed_set: set[str] = set()
        row_seeds: dict[int, list[str]] = {}  # row_idx -> [seed keywords]

        for idx, kws in row_keywords.items():
            # opportunity score 기준 상위 N개
            scored = []
            for kw in kws:
                s = stats.get(kw)
                if s and s.get("total_volume", 0) > 0:
                    scored.append((kw, s.get("score", 0)))
            scored.sort(key=lambda x: x[1], reverse=True)
            seeds = [kw for kw, _ in scored[:RELATED_SEED_COUNT]]
            if seeds:
                row_seeds[idx] = seeds
                seed_set.update(seeds)

        if not seed_set:
            log_fn("[related] 시드 키워드 없음 - 연관키워드 확장 스킵")
            return row_keywords, set()

        log_fn(f"[related] 시드 {len(seed_set)}개에서 연관키워드 조회 시작")

        # 캐시 확인 + API 호출
        seed_related: dict[str, list[dict]] = {}  # seed -> [{keyword, total_volume, comp_idx, score}]
        uncached_seeds = []

        for seed in sorted(seed_set):
            cached = self.cache.get_related(seed)
            if cached is not None:
                seed_related[seed] = cached
            else:
                uncached_seeds.append(seed)

        if uncached_seeds:
            log_fn(f"[related] 캐시 히트: {len(seed_related)}개, API 호출: {len(uncached_seeds)}개")
        else:
            log_fn(f"[related] 전체 캐시 히트 ({len(seed_related)}개)")

        # API 호출 (시드별 1콜) — 키 여러 개면 병렬
        workers = max(ad_key_pool.key_count, 1)

        def _fetch_related(seed: str):
            """단일 시드에 대한 연관키워드 API 호출 (retry 포함)."""
            for attempt in range(2):
                try:
                    stats = get_related_keywords(seed, min_volume=RELATED_MIN_VOLUME)
                    related_list = [
                        {"keyword": rs.keyword, "total_volume": rs.total_volume,
                         "comp_idx": rs.comp_idx, "score": opportunity_score(rs)}
                        for rs in stats[:RELATED_MAX_PER_SEED]
                    ]
                    return seed, related_list, stats[:RELATED_MAX_PER_SEED]
                except Exception as e:
                    if "429" in str(e) and attempt == 0:
                        time.sleep(3)
                    else:
                        if attempt == 0:
                            log_fn(f"[related] '{seed}' 에러: {str(e)[:80]}")
                        return seed, [], []
            return seed, [], []

        if workers > 1 and len(uncached_seeds) > 1:
            log_fn(f"[related] 병렬 호출: {len(uncached_seeds)}시드, 워커 {workers}개")
            with ThreadPoolExecutor(max_workers=workers) as executor:
                results = list(executor.map(_fetch_related, uncached_seeds))
            # 캐시 쓰기는 순차 (SQLite 스레드 안전)
            for seed, related_list, raw_stats in results:
                seed_related[seed] = related_list
                self.cache.put_related(seed, related_list)
                if raw_stats:
                    self.cache.put_many(raw_stats)
                if (uncached_seeds.index(seed) + 1) % 10 == 0:
                    log_fn(f"[related] {uncached_seeds.index(seed) + 1}/{len(uncached_seeds)} 시드 완료")
        else:
            for i, seed in enumerate(uncached_seeds):
                _, related_list, raw_stats = _fetch_related(seed)
                seed_related[seed] = related_list
                self.cache.put_related(seed, related_list)
                if raw_stats:
                    self.cache.put_many(raw_stats)
                if i < len(uncached_seeds) - 1:
                    time.sleep(0.3)
                if (i + 1) % 10 == 0:
                    log_fn(f"[related] {i + 1}/{len(uncached_seeds)} 시드 완료")

        # 행별 키워드 확장 (상품 적합성 필터 적용)
        all_new_keywords: set[str] = set()
        filtered_count = 0
        for idx, seeds in row_seeds.items():
            existing = set(row_keywords[idx])
            context = product_context.get(idx, set())
            new_kws = []
            for seed in seeds:
                for rel in seed_related.get(seed, []):
                    kw = rel["keyword"]
                    if kw in existing:
                        continue
                    # 상품 적합성 체크: 컨텍스트 토큰과 겹치는지
                    if context and not self._is_relevant(kw, context):
                        filtered_count += 1
                        continue
                    new_kws.append(kw)
                    existing.add(kw)
                    all_new_keywords.add(kw)
            if new_kws:
                row_keywords[idx] = row_keywords[idx] + new_kws

        log_fn(f"[related] 연관키워드 {len(all_new_keywords)}개 추가, {filtered_count}개 무관 제거")
        return row_keywords, all_new_keywords

    # ── API 호출 & 캐싱 ──────────────────────────────────
    def _fetch_and_cache_stats(
        self,
        keywords: list[str],
        log_fn: Callable[[str], None],
    ) -> dict[str, dict]:
        """
        캐시 히트인 키워드는 캐시에서, 미스인 키워드만 API 호출.
        Returns: {keyword: stats_dict}
        """
        # 캐시 조회
        cached = self.cache.get_many(keywords)
        uncached = [kw for kw in keywords if kw not in cached]

        log_fn(f"[enricher] 캐시 히트: {len(cached)}개, 미스: {len(uncached)}개")

        if not uncached:
            return cached

        # API 호출 (5개씩 배치, exponential backoff)
        all_stats = self._call_api_with_retry(uncached, log_fn)

        # API 반환 키워드 → dict
        api_result_map: dict[str, KeywordStat] = {}
        for s in all_stats:
            api_result_map[s.keyword] = s

        # 캐시 저장
        if all_stats:
            self.cache.put_many(all_stats)
            log_fn(f"[enricher] {len(all_stats)}개 키워드 캐시 저장")

        # 요청했지만 API가 반환하지 않은 키워드 → 검색량 0으로 캐싱
        zero_stats = []
        for kw in uncached:
            if kw not in api_result_map:
                zero_stat = KeywordStat(keyword=kw)
                zero_stats.append(zero_stat)
                api_result_map[kw] = zero_stat
        if zero_stats:
            self.cache.put_many(zero_stats)

        # 합치기
        result = dict(cached)
        for kw, stat in api_result_map.items():
            result[kw] = {
                "keyword": stat.keyword,
                "pc_volume": stat.pc_volume,
                "mobile_volume": stat.mobile_volume,
                "total_volume": stat.total_volume,
                "comp_idx": stat.comp_idx,
                "score": opportunity_score(stat),
            }
        return result

    def _call_api_with_retry(
        self,
        keywords: list[str],
        log_fn: Callable[[str], None],
    ) -> list[KeywordStat]:
        """배치 API 호출 with exponential backoff on 429.
        검색광고 API 키가 여러 개면 배치를 워커별로 분배해 병렬 실행."""
        batches = [
            keywords[i : i + BATCH_SIZE]
            for i in range(0, len(keywords), BATCH_SIZE)
        ]
        total_batches = len(batches)
        workers = max(ad_key_pool.key_count, 1)

        def _run_batch(chunk: list[str]) -> list[KeywordStat]:
            for attempt in range(MAX_RETRIES):
                try:
                    return get_keyword_stats(chunk, batch_size=len(chunk))
                except Exception as e:
                    err_str = str(e)
                    if "429" in err_str and attempt < MAX_RETRIES - 1:
                        wait = 2 ** (attempt + 1)
                        log_fn(f"[enricher] 429 에러, {wait}초 대기 후 재시도 ({attempt + 1}/{MAX_RETRIES})")
                        time.sleep(wait)
                    else:
                        log_fn(f"[enricher] API 에러: {err_str[:100]}")
                        return []
            return []

        all_stats: list[KeywordStat] = []
        if workers > 1 and len(batches) > 1:
            log_fn(f"[enricher] 병렬 호출: {total_batches}배치, 워커 {workers}개")
            with ThreadPoolExecutor(max_workers=workers) as executor:
                for result in executor.map(_run_batch, batches):
                    all_stats.extend(result)
        else:
            for i, chunk in enumerate(batches):
                all_stats.extend(_run_batch(chunk))
                if i < len(batches) - 1:
                    time.sleep(BATCH_DELAY)

        log_fn(f"[enricher] API 호출 완료: {total_batches}배치, {len(all_stats)}개 결과")
        return all_stats

    # ── Phase 2-A: 트렌드 데이터 수집 ─────────────────────
    def _fetch_and_cache_trends(
        self,
        keywords: list[str],
        log_fn: Callable[[str], None],
    ) -> dict[str, list[dict]]:
        """
        검색어트렌드 API로 12개월 월간 시계열 수집.
        5키워드/콜, 0.5초 간격. 쿼터 초과 시 스킵.

        Returns: {keyword: [{period, ratio}, ...]}
        """
        # 캐시 확인
        cached_trends = self.cache.get_trends_many(keywords)
        uncached = [kw for kw in keywords if kw not in cached_trends]

        log_fn(f"[trend] 캐시 히트: {len(cached_trends)}개, 미스: {len(uncached)}개")

        if not uncached:
            log_fn("[trend] 전체 캐시 히트 -API 호출 불필요")
            return cached_trends

        # 쿼터 확인 (Phase 2+3 합산)
        trend_used = self.cache.get_quota("search_trend")
        shop_used = self.cache.get_quota("shopping_keyword")
        current_quota = trend_used + shop_used
        needed_calls = (len(uncached) + BATCH_SIZE - 1) // BATCH_SIZE
        remaining_quota = DATALAB_DAILY_QUOTA - current_quota

        if remaining_quota <= 0:
            log_fn(f"[trend] 일일 쿼터 소진 ({current_quota}/{DATALAB_DAILY_QUOTA}) -트렌드 수집 스킵")
            return cached_trends

        if needed_calls > remaining_quota:
            # 가능한 만큼만 호출
            max_keywords = remaining_quota * BATCH_SIZE
            uncached = uncached[:max_keywords]
            needed_calls = remaining_quota
            log_fn(f"[trend] 쿼터 부족 -{len(uncached)}개 키워드만 수집 (남은 쿼터: {remaining_quota})")

        log_fn(f"[trend] {len(uncached)}개 키워드 → {needed_calls}콜 예정 (현재 쿼터: {current_quota}/{DATALAB_DAILY_QUOTA})")

        # 날짜 계산: 최근 12개월
        now = datetime.now()
        end_date = now.strftime("%Y-%m-%d")
        start_date = (now - timedelta(days=365)).strftime("%Y-%m-%d")

        # API 호출 (5개씩 배치, 멀티키 병렬)
        new_trends: dict[str, list[dict]] = {}
        total_batches = (len(uncached) + BATCH_SIZE - 1) // BATCH_SIZE
        api_calls = 0
        # 데이터랩 API는 동시 접속에 민감 → 워커 최대 3개로 제한
        workers = min(max(naver_key_pool.key_count, 1), 3)

        # 배치 청크 생성
        batches = [uncached[i : i + BATCH_SIZE] for i in range(0, len(uncached), BATCH_SIZE)]

        def _trend_worker(chunk):
            """단일 배치 트렌드 수집 (retry 포함)."""
            for attempt in range(2):
                try:
                    resp = get_search_trend(chunk, start_date, end_date, time_unit="month")
                    partial = {}
                    for r in resp.get("results", []):
                        kw = r.get("title", "")
                        data_points = [
                            {"period": dp.get("period", ""), "ratio": dp.get("ratio", 0)}
                            for dp in r.get("data", [])
                        ]
                        if kw and data_points:
                            partial[kw] = data_points
                    return partial
                except Exception as e:
                    if attempt == 0:
                        time.sleep(2)
                    else:
                        return {"__error__": str(e)[:100]}
            return {"__error__": "max retries"}

        if workers > 1:
            log_fn(f"[trend] 병렬 수집 (워커 {workers}개, {total_batches}배치)")
            with ThreadPoolExecutor(max_workers=workers) as executor:
                for i, result in enumerate(executor.map(_trend_worker, batches)):
                    if "__error__" in result:
                        log_fn(f"[trend] API 에러: {result['__error__']}")
                    else:
                        new_trends.update(result)
                        api_calls += 1
                    if (i + 1) % 20 == 0 or i + 1 == total_batches:
                        log_fn(f"[trend] {i + 1}/{total_batches} 완료")
        else:
            for batch_idx, chunk in enumerate(batches):
                result = _trend_worker(chunk)
                if "__error__" in result:
                    log_fn(f"[trend] API 에러 (배치 {batch_idx + 1}/{total_batches}): {result['__error__']}")
                else:
                    new_trends.update(result)
                    api_calls += 1
                if (batch_idx + 1) % 10 == 0 or batch_idx + 1 == total_batches:
                    log_fn(f"[trend] 배치 {batch_idx + 1}/{total_batches} 완료")
                if batch_idx + 1 < total_batches:
                    time.sleep(BATCH_DELAY)

        # 쿼터 기록
        if api_calls > 0:
            self.cache.increment_quota("search_trend", api_calls)
            log_fn(f"[trend] API {api_calls}콜 완료, 쿼터 갱신 ({current_quota + api_calls}/{DATALAB_DAILY_QUOTA})")

        # 캐시 저장
        if new_trends:
            self.cache.put_trends_many(new_trends)
            log_fn(f"[trend] {len(new_trends)}개 키워드 트렌드 캐시 저장")

        # 합치기
        all_trends = dict(cached_trends)
        all_trends.update(new_trends)
        return all_trends

    # ── Phase 2-B: 추이 판별 ──────────────────────────────
    @staticmethod
    def _classify_trend(trend_data: list[dict]) -> str:
        """
        12개월 ratio 시계열로 추이 판별.

        판별 기준 (30행 475키워드 분포 기반 확정):
          데이터부족: < 4개월 데이터 -> ""
          시즌집중형: CV > 0.6 AND max >= mean*2.0 -> "~"
          상승형: recent_change > 0.2 -> "^"
          하락형: CV < 0.4 AND recent_change < -0.3 -> "v"
            (CV >= 0.4이면 변동 자체가 크므로 하락 미판정)
          유지: 나머지 -> "-"

        Returns: "^" / "v" / "-" / "~" / ""
        """
        ratios = [dp.get("ratio", 0) for dp in trend_data]

        n = len(ratios)
        if n < 4:
            return ""

        mean_val = sum(ratios) / n
        if mean_val <= 0:
            return ""

        variance = sum((x - mean_val) ** 2 for x in ratios) / (n - 1)
        stdev_val = variance ** 0.5
        cv = stdev_val / mean_val
        max_val = max(ratios)

        # 최근 3개월 vs 이전
        recent_3 = sum(ratios[-3:]) / 3
        earlier_n = n - 3
        earlier = sum(ratios[:-3]) / earlier_n if earlier_n > 0 else mean_val
        recent_change = (recent_3 - earlier) / earlier if earlier > 0 else 0

        # 1) 시즌집중형: 극심한 변동 + 뚜렷한 피크
        if cv > 0.6 and max_val >= mean_val * 2.0:
            return "~"

        # 2) 상승형
        if recent_change > 0.2:
            return "^"

        # 3) 하락형: 안정적 키워드(CV < 0.4)에서만 판정
        #    CV >= 0.4면 변동이 커서 단순 하락 판정 위험
        if cv < 0.4 and recent_change < -0.3:
            return "v"

        # 4) 유지
        return "-"

    # ── 간결 요약 생성 ────────────────────────────────────
    def _build_compact_summary(
        self,
        row_keywords: list[str],
        stats: dict[str, dict],
        orig_count: int = 0,
    ) -> str:
        """
        행의 키워드 목록과 전체 통계로 간결 텍스트 생성.

        형식 (트렌드 포함):
          TOP: 단열테이프(16260/H-) 보온테이프(5200/M^)
          OK: 털귀마개(890v) 귀도리(450~)
          X: 이어머프캡 모자귀마개
          REL: 보온필름(3200/M^) 창문테이프(1200/L-)

        orig_count: 원본 키워드 수 (이후는 연관키워드)
        """
        # 원본 / 연관 분리
        if orig_count and orig_count < len(row_keywords):
            orig_kws = row_keywords[:orig_count]
            rel_kws = row_keywords[orig_count:]
        else:
            orig_kws = row_keywords
            rel_kws = []

        top_items = []   # (keyword, total_volume, comp_short, score, trend)
        zero_items = []  # keyword

        for kw in orig_kws:
            s = stats.get(kw)
            if s is None:
                zero_items.append(kw)
                continue

            vol = s.get("total_volume", 0)
            comp = s.get("comp_idx", "")
            score = s.get("score", 0)
            trend = s.get("trend", "")

            if vol <= 0:
                zero_items.append(kw)
            else:
                comp_s = COMP_SHORT.get(comp, "")
                top_items.append((kw, vol, comp_s, score, trend))

        # score 내림차순 정렬
        top_items.sort(key=lambda x: x[3], reverse=True)

        # 상위 TOP_COUNT개는 TOP, 나머지는 OK
        top_list = top_items[:TOP_COUNT]
        ok_list = top_items[TOP_COUNT:]

        lines = []

        if top_list:
            parts = []
            for kw, vol, comp_s, _, trend in top_list:
                if comp_s:
                    parts.append(f"{kw}({vol}/{comp_s}{trend})")
                else:
                    parts.append(f"{kw}({vol}{trend})")
            lines.append("TOP: " + " ".join(parts))

        if ok_list:
            parts = [f"{kw}({vol}{trend})" for kw, vol, _, _, trend in ok_list]
            lines.append("OK: " + " ".join(parts))

        if zero_items:
            lines.append("X: " + " ".join(zero_items))

        # REL 라인: 연관키워드 (검색량 > 0만, score 내림차순)
        if rel_kws:
            rel_items = []
            for kw in rel_kws:
                s = stats.get(kw)
                if s and s.get("total_volume", 0) > 0:
                    vol = s["total_volume"]
                    comp_s = COMP_SHORT.get(s.get("comp_idx", ""), "")
                    score = s.get("score", 0)
                    trend = s.get("trend", "")
                    rel_items.append((kw, vol, comp_s, score, trend))
            rel_items.sort(key=lambda x: x[3], reverse=True)
            if rel_items:
                parts = []
                for kw, vol, comp_s, _, trend in rel_items[:10]:  # 상위 10개만
                    if comp_s:
                        parts.append(f"{kw}({vol}/{comp_s}{trend})")
                    else:
                        parts.append(f"{kw}({vol}{trend})")
                lines.append("REL: " + " ".join(parts))

        # SHOP 라인: 쇼핑 내 추이가 있는 키워드만 (상승 우선 정렬)
        shop_items = []  # (keyword, shop_trend)
        for kw in row_keywords:
            s = stats.get(kw)
            if s and s.get("shop_trend"):
                shop_items.append((kw, s["shop_trend"]))
        if shop_items:
            # 정렬: ^ > - > ~ > v
            _shop_order = {"^": 0, "-": 1, "~": 2, "v": 3}
            shop_items.sort(key=lambda x: _shop_order.get(x[1], 9))
            parts = [f"{kw}{trend}" for kw, trend in shop_items]
            lines.append("SHOP: " + " ".join(parts))

        return "\n".join(lines)
