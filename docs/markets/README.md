# 마켓별 운영 허브

각 마켓의 **정책 · 컬럼 · 스토어 · API · 운영 플레이북** 단일 진입점.

| 마켓 | 허브 | 상태 |
|---|---|---|
| 고도몰 (자사몰) | [godomall/](godomall/) | ✅ 완성 (pricing/columns/stores/api/bulk-ops) |
| 스마트스토어 | [smartstore/](smartstore/) | 🟡 pricing 있음, 나머지 scaffold |
| ESM (옥션/지마켓) | [esm/](esm/) | 🟡 pricing 있음, 나머지 scaffold |
| 11번가 | [11st/](11st/) | 🟡 pricing 있음, 나머지 scaffold |
| 이셀러스 (ESM+ 업로드) | [esellers/](esellers/) | 🔴 pricing 없음 (ESM 상속), 변환 코드 존재 |
| 쿠팡 | — | 예정 |
| 멸치쇼핑 | — | 예정 |

## 구조 규약

각 마켓 디렉터리는 아래 6 파일로 통일:

| 파일 | 내용 |
|---|---|
| README.md | 허브 진입점, 하위 문서 링크, 코드/스킬 레퍼런스 |
| pricing.md | 3-전략 파라미터, 공식, 시뮬 근거 |
| columns.md | 필드별 값 결정 규칙 (업로드 엑셀/API payload 기준) |
| stores.md | 활성 스토어 목록, credential 위치, 런타임 기본값 |
| api.md | 마켓 API/Playwright 경로, rate limit, 제약사항 |
| bulk-ops.md | 벌크 업데이트 플레이북 (해당 시) |

## SSOT 원칙

- 이 트리가 정책/운영의 단일 출처.
- `docs/pricing/*.md` (옛 경로) 는 여기로 리다이렉트.
- 메모리(`~/.claude/projects/.../memory/`)에는 일회성 결정·날짜·사용자 feedback만 남김.
- 영속 규칙은 반드시 여기에 기록. Stop hook이 싱크 누락 감지 (godomall 한정, 확장 예정).
