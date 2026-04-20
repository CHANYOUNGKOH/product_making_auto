# 고도몰 — 마켓 허브

고도몰(NHN커머스) 운영 관련 **단일 진입점**. 하위 문서들은 주제별로 분리되어 있고, 각 문서는 해당 주제의 정식(SSOT) 사양.

## 문서 지도

| 파일 | 범위 | 주요 독자 |
|---|---|---|
| [pricing.md](pricing.md) | 3-전략(최저가/일반판매/광고) · J/C/H 파라미터 · 공식 · 시뮬 | 가격엔진 개발자, 정책 운영 |
| [columns.md](columns.md) | 155컬럼 필드별 값 결정 규칙 (상품명/검색/이미지/상세/배송/메타) | 변환기 개발자, 출고 운영 |
| [stores.md](stores.md) | 8 활성 스토어 · brand_code · 배송비 · 런타임 기본값 | 스토어 관리, 출고 운영 |
| [api.md](api.md) | OpenHub API 스펙 · partial update 제약 · 어드민 URL | API 클라이언트 개발자 |
| [bulk-ops.md](bulk-ops.md) | 벌크 업데이트 3단계 플레이북 · 배치 최적화 · rollback | 운영자 |

## 관련 코드

- [OC_ES_converter/scripts/convert_godomall.py](../../../OC_ES_converter/scripts/convert_godomall.py) — 오너클랜→고도몰 변환
- [godomall_register/](../../../godomall_register/) — OpenHub API 클라이언트 (메인 레포, worktree sparse 제외)
- [DB_save/pricing_strategies.json](../../../DB_save/pricing_strategies.json) — `market_strategies.고도몰` 설정
- [DB_save/sim_godomall.py](../../../DB_save/sim_godomall.py) — 가격 시뮬

## 연관 스킬/메모리

- 스킬 `godomall-bulk-update` — 벌크 API 업데이트 재사용 플레이북
- 스킬 `godomall-ownerclan-converter` (OMC) — OC→고도몰 변환 흐름

## 갱신 규칙

**SSOT**: 이 디렉터리의 각 *.md 가 해당 주제의 단일 출처.
메모리(`~/.claude/projects/.../memory/`)는 일회성 발견·날짜 표시·사용자 결정 로그만 남기고, **영속 규칙은 여기로 이관**한다. 중복 발견 시 hook 또는 에이전트가 이 디렉터리를 기준으로 정리.
