from typing import Dict, List, Set


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
