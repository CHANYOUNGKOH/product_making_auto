from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Set


def get_category_large_medium(full_category: str) -> str:
    parts = [part.strip() for part in full_category.split(">")]
    if len(parts) >= 2:
        return f"{parts[0]} > {parts[1]}"
    return full_category


def normalize_store_categories(categories: List[str]) -> List[str]:
    normalized: List[str] = []
    seen: Set[str] = set()
    for category in categories or []:
        large_medium = get_category_large_medium(category)
        if large_medium and large_medium not in seen:
            seen.add(large_medium)
            normalized.append(large_medium)
    return normalized


def build_sheet_used_combinations_cache(conn, sheet_name: str) -> Dict[str, set]:
    cache: Dict[str, set] = {}
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT DISTINCT combination_index, product_code
        FROM combination_assignments
        WHERE sheet_name = ?
        """,
        (sheet_name,),
    )
    for combo_idx, product_code in cursor.fetchall():
        if product_code and combo_idx is not None:
            if product_code not in cache:
                cache[product_code] = set()
            cache[product_code].add(combo_idx)
    return cache


def build_store_used_product_codes_cache(conn, sheet_name: str, business_number: str) -> set:
    cache = set()
    if not business_number:
        return cache
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT DISTINCT product_code
        FROM combination_assignments
        WHERE sheet_name = ? AND business_number = ?
        """,
        (sheet_name, business_number),
    )
    for row in cursor.fetchall():
        if row[0]:
            cache.add(row[0])
    return cache


def update_store_season_stats(store_season_stats: Dict[str, Any], season_info: Dict[str, Any]) -> None:
    """Aggregate per-category season stats into store-level stats."""
    if "error" in season_info:
        return

    store_season_stats["total_products_before"] += season_info.get("original_count", 0)
    store_season_stats["total_products_after"] += season_info.get("filtered_count", 0)
    store_season_stats["season_excluded_count"] += season_info.get("excluded_count", 0)

    included = season_info.get("included_seasons", {})
    for _, info in included.items():
        season_name = info.get("name")
        if not season_name:
            continue
        if season_name not in store_season_stats["included_seasons"]:
            store_season_stats["included_seasons"][season_name] = 0
        store_season_stats["included_seasons"][season_name] += info.get("count", 0)

    excluded = season_info.get("excluded_seasons", {})
    for _, info in excluded.items():
        season_name = info.get("name")
        if not season_name:
            continue
        if season_name not in store_season_stats["excluded_seasons"]:
            store_season_stats["excluded_seasons"][season_name] = 0
        store_season_stats["excluded_seasons"][season_name] += info.get("count", 0)


def log_category_season_result(
    log_fn: Callable[[str], None],
    category: str,
    season_info: Optional[Dict[str, Any]],
    products_count: int,
    season_config_for_log: Optional[Dict[str, Any]] = None,
    check_season_validity: Optional[Callable[[Dict[str, Any], datetime, Dict[str, Any]], str]] = None,
) -> None:
    """Log season filtering result for one category."""
    if not season_info:
        log_fn(f"    ?? 카테고리 '{category}' 시즌 필터링 정보 없음 (상품 조회 실패 또는 시즌 설정 미적용)")
        return

    if "error" in season_info:
        log_fn(f"    ?? 카테고리 '{category}' 시즌 필터링: {season_info.get('error')}")
        return

    stats = season_info.get("season_stats", {})
    included = season_info.get("included_seasons", {})
    excluded = season_info.get("excluded_seasons", {})

    total_before = season_info.get("original_count", products_count + season_info.get("excluded_count", 0))
    total_after = season_info.get("filtered_count", products_count)

    log_fn(f"    ? 카테고리 '{category}' 시즌 필터링 결과:")
    log_fn(f"      - 전체 상품 코드: {total_before}개")
    log_fn(f"      - 일반 상품: {stats.get('non_season', 0)}개")
    log_fn(f"      - 시즌 상품 (포함): {stats.get('season_valid', 0)}개")
    log_fn(f"      - 시즌 지난 상품 (제외): {stats.get('season_invalid', 0)}개")
    log_fn(f"      - 필터링 후 상품 코드: {total_after}개 → 조합 {products_count}개 생성")

    if included:
        if season_config_for_log and check_season_validity:
            active_included = []
            for season_id, info in included.items():
                season = next(
                    (s for s in season_config_for_log.get("seasons", []) if s.get("id") == season_id),
                    None,
                )
                if season:
                    status = check_season_validity(season, datetime.now(), season_config_for_log)
                    if status == "ACTIVE":
                        active_included.append((season_id, info))
            if active_included:
                log_fn("      ? 포함된 시즌 (출력 가능):")
                for season_id, info in active_included:
                    log_fn(f"        - {info.get('name', season_id)}: {info.get('count', 0)}개")
        else:
            log_fn("      ? 포함된 시즌:")
            for season_id, info in included.items():
                log_fn(f"        - {info.get('name', season_id)}: {info.get('count', 0)}개")

    if excluded:
        log_fn("      ? 제외된 시즌:")
        for season_id, info in excluded.items():
            reason = info.get("reason", "시즌 기간 외")
            name = info.get("name", season_id)
            count = info.get("count", 0)
            reason_clean = reason.replace(f"{name}(", "").replace(")", "").strip()
            log_fn(f"        - {name} - {reason_clean} - {count}개")
