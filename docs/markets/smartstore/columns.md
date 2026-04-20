# 스마트스토어 컬럼/필드 정책

## 🔴 TBD — 정리 필요

SS는 업로드 방식이 두 가지:
1. **일괄등록 엑셀** (판매자센터) — 수십~백여 개 컬럼
2. **커머스 API** (JSON payload) — 스펙 다름

현재까지 확인:
- 상품명: ST4_최종결과 사용
- 키워드: ST2 + ST3 enriched
- alt 주입: OC 본문에 ST2_JSON 기반 자동 ([project_alt_text_injection](../../../../..))
- 카테고리: 네이버 카테고리 자동매핑 (OC API 제공값)
- 속성정보: SS 노출극대화 솔루션 참조 (별도 시스템)
  - 경로: `C:\Users\kohaz\Desktop\Python\스마트스토어전용_노출극대화솔루션`

## 해야 할 정리
- 엑셀 컬럼 전수 정책 (고도몰 columns.md 수준으로)
- 커머스 API 페이로드 스키마
- 두 경로 상호 매핑
- 노출극대화 솔루션의 속성정보 통합

## 관련 코드
- `hub/services/smartstore_service.py` (worktree 분산)
- `hub/services/smartstore_tag_service.py` (태그 전처리)
