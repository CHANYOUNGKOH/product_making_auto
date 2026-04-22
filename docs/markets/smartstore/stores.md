# 스마트스토어 스토어 · 런타임 기본값

## 활성 스토어

약 **20+ 스토어** 운영 (명의 A 5개 + B 7개 사업자 분할).

credential 위치는 워크트리 단위 분산 진행 중 — 통합 레지스트리 미작성.
현재 확인된 참조:
- `hub/adapters/smartstore.py` (worktree `heuristic-curran-17d5c1`)
- `scripts/bootstrap_smartstore_login.py`

## 확인된 정보 (fixture 스냅샷 기준)

`tests/fixtures/sessions_snapshot/smartstore_a01~a11_*.json` 존재 → **최소 11개 A 명의 스토어 + 2 세션씩** 확인. B 명의는 별도 카운트 필요.

총 운영: 20+ 스토어 (A 11 + B 9 이상 추정)

### 세션 관리
- 각 스토어당 2개 세션 스냅샷 (1/2) 보존 → 순환 사용
- `scripts/bootstrap_smartstore_login.py` 로 초기 로그인 → 세션 JSON 저장
- `hub/adapters/smartstore.py` `SmartStoreCollector(owner_letter, headless)` 에서 로드

## 작성필요
- 각 스토어별 사업자 매핑 (A1-1 ~ B2-7)
- 네이버 커머스 API client_id/secret 저장 위치
- 쇼핑파트너 세션 공유 여부 (OrderHelper 세션 재사용 예정 — `project_shopping_partner_session_reuse` 참조)

## 런타임 기본값 (추정)
- 전략 기본: `normal_sale` (초기 확보 단계만 `lowest_price`)
- 할인율: 플랫폼 50% 고정
- 쇼핑파트너 자동 매칭 Y
- 배송비 정책: 스토어별 매핑

## 카테고리 중복 금지 규칙
같은 사업자(group_id) 내 네이버 카테고리 중복 금지 ([project_naver_category_duplication_rule](../../../../..)).
