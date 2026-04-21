"""image slot resolver 테스트."""
from godomall_register.pipeline_run import resolve_export_image_slots


def test_resolve_export_image_slots_prefers_generated_then_nukki_then_original():
    slots = {
        "generated_1x1": [
            {"url": "https://cdn/generated-main.jpg"},
            {"url": "https://cdn/generated-alt.jpg"},
        ],
        "nukki": {"url": "https://cdn/nukki.jpg"},
        "original": [
            {"url": "https://cdn/original-1.jpg"},
            {"url": "https://cdn/original-2.jpg"},
        ],
        "rolled_index": 0,
    }

    resolved = resolve_export_image_slots(slots, index=0)

    assert resolved["main"] == "https://cdn/generated-main.jpg"
    assert resolved["secondary"] == "https://cdn/nukki.jpg"
    assert resolved["tertiary"] == "https://cdn/original-1.jpg"


def test_resolve_export_image_slots_falls_back_when_generated_missing():
    slots = {
        "nukki": {"url": "https://cdn/nukki.jpg"},
        "original": [
            {"url": "https://cdn/original-1.jpg"},
            {"url": "https://cdn/original-2.jpg"},
        ],
        "rolled_index": 0,
    }

    resolved = resolve_export_image_slots(slots, index=3)

    assert resolved["main"] == "https://cdn/nukki.jpg"
    assert resolved["secondary"] == "https://cdn/original-1.jpg"
    assert resolved["tertiary"] == "https://cdn/original-2.jpg"
