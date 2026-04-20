# 11번가 API

## 확인된 경로

### 1. Seller Open API (공식)
- 입구: [openapi.11st.co.kr](https://openapi.11st.co.kr/openapi/OpenApiFrontMain.tmall)
- 발급 방법: 11번가 셀러 계정으로 API 센터 접속 → Seller API 서비스 등록 + 접근 IP 등록(개발/상용)
- 프로토콜: HTTP + **XML**
- 주요 기능: 상품 등록/수정/조회, 주문 조회, 배송 처리, 카테고리 조회
- 상품등록 메서드명: `ProductRegist` (추정) — 정확한 필드 스펙은 openapi.11st.co.kr 로그인 후 "메뉴 > 개발자가이드" 에서 확인

### 2. 판매자센터 엑셀 (수기 일괄등록)
- 사용량 작음, 주로 API 경로로 자동화 권장

## 사전조건 (API 사용 시)

1. 11번가 판매자 계정
2. API 센터 서비스 등록
3. 접근 IP 등록 (개발/상용 분리)
4. **템플릿 사전 등록**:
   - 배송정보 템플릿 (출고지 주소 / 반품·교환지 주소)
   - 발송예정일 템플릿
   - 이 템플릿 번호가 상품등록 페이로드 필수 필드

## rate limit / 제약

- 공식 문서 직접 확인 필요 (현재 세션 외부 fetch 제한으로 미확인)
- 3rd party 연동 사례(이지어드민, 넥스트엔진, 셀러픽 등) 존재 → 대량 등록 자동화 가능 확인됨

## 🔴 작성필요
- ProductRegist XML 요청 스펙 전수
- 필드 한국어/영문명 매핑 (`prdNm`, `sellingPrice`, `dispCtgrNo` 등 추정명)
- 옵션 포맷 (단독/조합)
- 이미지 업로드 프로세스 (멀티파트 vs URL)
- 카테고리 조회 API (`dispCtgrInfo` 등)
- 응답코드 표
- 오류 재시도 정책

## 업로드 경로 분기

이셀러스 변환 경로가 11번가도 커버:
- `Upload_Mapper/solutions/esellers.py` 에 `detected_market == "11번가"` 분기 확인
- 폴더명: `날짜_마켓코드_할N_배N` (다른 마켓과 동일)
- 가격: 판매가 50% 인하 후 price_diff를 옵션에 시프트 (11번가 전용 2-pass)
- 옵션 추가금: 2배 + max_delta 제한

즉 **이셀러스 엑셀 업로드 vs 11번가 Seller API 직접 호출** 두 경로 공존. 현재 주 사용은 이셀러스 엑셀.

Sources:
- [11번가 Open API 소개 (SK)](https://skopenapi.readme.io/reference/11%EB%B2%88%EA%B0%80-%EC%86%8C%EA%B0%9C)
- [11번가 API 키 발급 방법](http://www.sellergo.net/sub/03/board/board_content.asp?t_name=BOARD2&idx=59)
- [API 센터 공식 로그인 (openapi.11st.co.kr)](https://openapi.11st.co.kr/openapi/OpenApiFrontMain.tmall)
