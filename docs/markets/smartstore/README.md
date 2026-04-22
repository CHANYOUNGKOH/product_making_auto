# 스마트스토어 — 마켓 허브

네이버 스마트스토어 운영 단일 진입점.

## 문서 지도

| 파일 | 상태 |
|---|---|
| [pricing.md](pricing.md) | ✅ (loss cap -200원 일괄 반영 완료) |
| [columns.md](columns.md) | 🔴 TBD — SS 업로드 필드 정책 미정리 |
| [stores.md](stores.md) | 🟡 초안 (20+ 스토어 전수 credential 위치 미확정) |
| [api.md](api.md) | 🟡 초안 (네이버 커머스 API + 쇼핑파트너 세션) |
| bulk-ops.md | 🔴 미작성 — 벌크 업데이트 필요 시 godomall 플레이북 차용 |

## 관련 코드

- [DB_save/sim_smartstore.py](../../../DB_save/sim_smartstore.py) — 시뮬
- `hub/adapters/smartstore.py` — 어댑터 (worktree `heuristic-curran-17d5c1` 에 진행 중)
- `hub/services/smartstore_service.py` — 서비스
- [scripts/bootstrap_smartstore_login.py](../../../scripts/bootstrap_smartstore_login.py) — 로그인 부트스트랩

## 관련 스킬/메모리

- reference_naver_commerce_vs_shopping_partner_api — 커머스 API는 SS 전용, 쇼핑파트너는 Playwright
- project_shopping_partner_session_reuse — OrderHelper 세션 재사용
- project_smartstore_smile_cash_actually_coupon — 15% 표기는 실제로 다운로드 쿠폰

## 상태 요약
현재 SS 관련 작업은 worktree 단위로 분산 진행 중. 운영 벌크 작업(고도몰 Phase 2C 같은) 진행 필요하면 별도 세션.
