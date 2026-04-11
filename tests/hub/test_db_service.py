"""db_service 단위 테스트. conftest의 test_db_path 픽스처 사용."""
import os
import pytest


@pytest.fixture(autouse=True)
def set_db_env(test_db_path):
    os.environ["HUB_DB_PATH"] = test_db_path
    yield


def test_get_dashboard_stats_returns_required_keys():
    from hub.services.db_service import get_dashboard_stats
    stats = get_dashboard_stats()
    for key in ("total_active", "shippable", "processed", "last_sync_at"):
        assert key in stats, f"Missing key: {key}"


def test_total_active_counts_only_active(test_db_path):
    from hub.services.db_service import get_dashboard_stats
    stats = get_dashboard_stats()
    assert stats["total_active"] == 3  # W001, W002, W003 (W004 is INACTIVE)


def test_shippable_requires_oc_price():
    from hub.services.db_service import get_dashboard_stats
    stats = get_dashboard_stats()
    assert stats["shippable"] == 2  # W001(50000), W002(30000); W003 has no oc_price


def test_processed_requires_both_statuses():
    from hub.services.db_service import get_dashboard_stats
    stats = get_dashboard_stats()
    assert stats["processed"] == 1  # W001 only (text_status=done AND image_status=done)


def test_get_products_returns_active_only():
    from hub.services.db_service import get_products
    result = get_products()
    codes = [p["상품코드"] for p in result["items"]]
    assert "W004" not in codes


def test_get_products_text_search():
    from hub.services.db_service import get_products
    result = get_products(q="상품A")
    assert len(result["items"]) == 1
    assert result["items"][0]["상품코드"] == "W001"


def test_get_products_category_filter():
    from hub.services.db_service import get_products
    result = get_products(category="가전/디지털>TV")
    codes = [p["상품코드"] for p in result["items"]]
    assert "W001" in codes
    assert "W002" not in codes


def test_get_products_quick_filter_has_oc_price():
    from hub.services.db_service import get_products
    result = get_products(quick_filter="has_oc_price")
    codes = [p["상품코드"] for p in result["items"]]
    assert "W003" not in codes  # no oc_price


def test_get_products_pagination():
    from hub.services.db_service import get_products
    result = get_products(page=1, per_page=2)
    assert len(result["items"]) == 2
    assert result["total"] == 3


def test_get_categories_returns_list():
    from hub.services.db_service import get_categories
    cats = get_categories()
    assert isinstance(cats, list)
    assert "가전/디지털>TV" in cats


def test_get_stores_returns_seeded():
    from hub.services.db_service import get_stores
    stores = get_stores()
    aliases = [s["alias"] for s in stores]
    assert "고도몰A1-1" in aliases


def test_run_migrations_is_idempotent(test_db_path):
    from hub.services.db_service import run_migrations
    run_migrations(test_db_path)  # already ran in conftest; should not raise
    run_migrations(test_db_path)  # second call also fine
