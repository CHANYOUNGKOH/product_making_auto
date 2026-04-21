from IMG_pipeline.genai_gui import format_lane_status


def test_format_lane_status_prefers_runtime_lane_assignment():
    product = {"image_slots": {}, "genai_status": ""}
    runtime_state = {"lane": "web", "status": "queued"}

    assert format_lane_status(product, runtime_state) == "웹 · ⏳ 대기열"


def test_format_lane_status_shows_running_without_slots():
    product = {"image_slots": {}, "genai_status": "running"}

    assert format_lane_status(product) == "🔄 실행 중"


def test_format_lane_status_summarizes_completed_lanes():
    product = {
        "image_slots": {
            "web": {"url": "https://example.com/web.jpg"},
            "codex": {"url": "https://example.com/codex.jpg"},
        },
        "genai_status": "done",
    }

    assert format_lane_status(product) == "Codex,웹 · ✅ 완료"
