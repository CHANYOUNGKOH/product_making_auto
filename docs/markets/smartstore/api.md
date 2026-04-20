# 스마트스토어 API & 쇼핑파트너

## 두 경로

### 1. 네이버 커머스 API (공식)
- 대상: 상품 등록·수정·조회·주문·CS
- 인증: client_id + client_secret → JWT
- **SS 전용** (쇼핑파트너센터용 아님)
- 구현: `hub/adapters/smartstore.py` (worktree 분산)

### 2. 쇼핑파트너센터 (Playwright)
- 대상: 검색 노출 리포트, 광고 리포트, 계약 관리 등
- **공식 API 없음** → Playwright 자동화 필수
- 세션: OrderHelper 기존 로그인 세션 재사용 ([project_shopping_partner_session_reuse](../../../../..))
- 3 페이지 이상 순회 가능 확인됨

## 제약

- 커머스 API 요청 한도: 상세 스펙 미정리 (TBD)
- 쇼핑파트너 Playwright는 UI 변경에 취약
- 상품명 검색품질 체크 API 발견되었으나 파이프라인 적용 위치 미정 ([project_product_name_quality_check](../../../../..))

## 🔴 정리 필요
- 커머스 API 엔드포인트/페이로드 스펙
- 커머스 API rate limit
- 등록 시 필수 vs 선택 필드 매트릭스 (columns.md와 연결)
- 이미지 업로드 플로우
- 옵션 등록 포맷
