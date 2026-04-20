# 11번가 컬럼/필드 정책

11번가는 **두 경로**로 업로드:
1. **이셀러스 엑셀** (주 사용) — ESM+ 와 다른 11번가 전용 템플릿
2. **Seller Open API** (`ProductRegist`) — [api.md](api.md) 참조

## 1. 이셀러스 엑셀 필드 (현 운영)

78컬럼 정책은 [../esellers/columns.md](../esellers/columns.md) 공통. 11번가 전용 차이만 여기 기록:

### 11번가 전용 규칙 (esellers.py 분석)

- **판매가* 계산 2-pass**:
  1. 이셀러스 등록솔루션 판매가 기준 옵션 가격 2배 보정 (max_delta 제한)
  2. `마켓판매가격` 의 50% 인하 (10원 단위 올림) 후 판매가에 기록
  3. `price_diff = 마켓판매가격 - new_price` 만큼 모든 옵션 추가금에 더하고 rounding_unit 내림
  4. `■대량단가` 라인은 0원 유지 (시프트 제외)
- **폴더명**: `날짜_마켓코드_할N_배N` (다른 마켓과 동일)
- **카테고리 번호***: `"431905000"` 강제 (기본카테고리 파일 제외)

> 이유: 11번가 수수료 17.5% pre_discount 로 C=2.5 특수 구조 + 옵션 가격 시프트로 플랫폼 할인 대응.

## 2. Seller API 필드 (공식 문서 확인 필요)

### 추정 주요 필드 (3rd party 연동 사례 기반)

| 필드명 | 설명 | 매핑 |
|---|---|---|
| `prdNm` | 상품명 | ST4_최종결과 |
| `sellingPrice` | 판매가 | 마켓판매가격 (50% 인하 후) |
| `dispCtgrNo` | 전시 카테고리 번호 | OC API 마켓별 제공값 |
| `stockQty` | 재고 | 9999 고정 |
| `delivCostInfo` | 배송비 정책 | 스토어별 템플릿 번호 |
| `asDetail` | A/S 정보 | 고정 텍스트 |
| `returnExchange` | 반품/교환 정책 | 고정 템플릿 |
| `productImage` | 대표 이미지 | 사용URL 또는 OC 원본 |
| `additionalImages` | 추가 이미지 | OC 원본 |
| `descTxt` | 상세설명 HTML | alt 주입된 OC 본문 + 고정 하단 notice |
| `optionInfo` | 옵션 리스트 | 조합형 포맷 |

### 🔴 공식 필드명/타입 전수 확정 필요

정확한 XML 스키마는 [openapi.11st.co.kr](https://openapi.11st.co.kr/openapi/OpenApiFrontMain.tmall) 로그인 후 "개발자가이드 > ProductRegist" 에서 확인.

## 3. 고도몰/이셀러스 공통 결정 규칙

- **상품명**: `ST4_최종결과`
- **키워드/태그**: ST2 + ST3 enriched
- **이미지 alt 주입**: `seo_alt_injector` 공통 적용
- **원산지**: `convert_origin()` 변환
- **재고**: 9999 고정
- **옵션 추가금 보정**: `OptionPriceCorrector.calculate_max_delta()` 기반

## 작성필요

- 11번가 ProductRegist API XML 전수 스펙
- 이셀러스 엑셀 템플릿 11번가용 컬럼 검증 (실 운영 파일 샘플 1건 분석)
- 배송비 템플릿 번호 스토어별 매핑
