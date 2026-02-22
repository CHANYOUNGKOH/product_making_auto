import sqlite3
from typing import List


def fetch_available_combinations_by_codes(
    cursor,
    product_codes: List[str],
    sheet_name: str,
    fetch_limit: int = 100,
    chunk_size: int = 300,
):
    """Fetch available combinations in batches, excluding already assigned ones."""
    if not product_codes:
        return []

    rows = []
    try:
        for offset in range(0, len(product_codes), chunk_size):
            chunk_codes = product_codes[offset:offset + chunk_size]
            placeholders = ",".join("?" * len(chunk_codes))
            query = f"""
                SELECT * FROM (
                    SELECT
                        pc.*,
                        ROW_NUMBER() OVER (
                            PARTITION BY pc.product_code
                            ORDER BY pc.combination_index ASC
                        ) AS rn
                    FROM product_combinations pc
                    WHERE pc.product_code IN ({placeholders})
                    AND NOT EXISTS (
                        SELECT 1 FROM combination_assignments ca
                        WHERE ca.sheet_name = ?
                        AND ca.product_code = pc.product_code
                        AND ca.combination_index = pc.combination_index
                    )
                )
                WHERE rn <= ?
                ORDER BY product_code ASC, combination_index ASC
            """
            params = chunk_codes + [sheet_name, fetch_limit]
            cursor.execute(query, params)
            rows.extend(cursor.fetchall())
    except sqlite3.OperationalError:
        # Fallback for environments where window functions are unavailable.
        rows = []
        for product_code in product_codes:
            cursor.execute(
                """
                SELECT * FROM product_combinations
                WHERE product_code = ?
                AND NOT EXISTS (
                    SELECT 1 FROM combination_assignments ca
                    WHERE ca.sheet_name = ?
                    AND ca.product_code = product_combinations.product_code
                    AND ca.combination_index = product_combinations.combination_index
                )
                ORDER BY combination_index ASC
                LIMIT ?
                """,
                (product_code, sheet_name, fetch_limit),
            )
            rows.extend(cursor.fetchall())

    return rows
