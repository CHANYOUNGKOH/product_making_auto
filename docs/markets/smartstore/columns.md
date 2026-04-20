# 스마트스토어 컬럼/필드 정책

> SS 상품등록은 **일괄등록 엑셀**과 **네이버 커머스 API (POST /v2/products)** 두 경로가 가능.
> 현재 운영 흐름은 주로 엑셀(판매자센터) 기반. API는 Hub 이식 시점에 병렬 지원 예정.

## 1. 커머스 API 페이로드 스키마 (POST /v2/products)

공식 문서: [apicenter.commerce.naver.com](https://apicenter.commerce.naver.com) · GitHub discussions: [commerce-api-naver/commerce-api](https://github.com/commerce-api-naver/commerce-api)

### 1-1. originProduct (상품 기본)

| 필드 | 타입 | 필수 | 이셀러스·고도몰 매핑 |
|---|---|:---:|---|
| `statusType` | enum (SALE/OUTOFSTOCK/...) | ✅ | 고정 SALE |
| `saleType` | enum (NEW/USED) | ✅ | 고정 NEW |
| `leafCategoryId` | number | ✅ | OC API `navercategoryId` 제공값 |
| `name` | string | ✅ | `ST4_최종결과` |
| `detailContent` | string (HTML) | ✅ | alt 주입된 OC 본문 + notice (고도몰/이셀러스와 동일 파이프) |
| `images.representativeImage.url` | string | ✅ | `사용URL` 또는 OC 원본 이미지1 |
| `images.optionalImages[].url` | string[] | | 나머지 이미지 슬롯 |
| `salePrice` | number | ✅ | `마켓판매가격` (price_engine) |
| `stockQuantity` | number | ✅ | 9999 고정 |
| `deliveryInfo` | object | ✅ | 스토어별 배송 템플릿 (별도 표 필요) |

### 1-2. originProduct.detailAttribute (상세 속성)

| 필드 | 설명 |
|---|---|
| `productInfoProvidedNotice` | 상품정보제공고시 (카테고리별 필수 속성) |
| `attributes` | 카테고리별 속성값 매트릭스 (SS 전용 노출극대화 솔루션 연동 영역) |
| `afterServiceInfo` | A/S 정보 |
| `productCertificationInfos` | KC인증 등 (OC API KC 제공여부 조사 backlog) |
| `seoInfo` | SEO 태그·설명 (고도몰 `seoTag*` 대응) |

### 1-3. smartstoreChannelProduct (채널 전용)

| 필드 | 설명 |
|---|---|
| `channelProductName` | 채널별 상품명 (보통 originProduct.name 과 동일) |
| `naverShoppingRegistration` | 네이버쇼핑 등록 여부 (필수 true) |
| `channelProductDisplayStatusType` | 전시상태 ON/WAIT/SUSPENSION — 기본 ON |
| `bbsSeq` | 스토어찜 콘텐츠 연결 (선택) |
| `storeKeepExclusiveProduct` | 알림받기 회원 전용 여부 — 기본 false |

## 2. 일괄등록 엑셀 (판매자센터)

### 🔴 작성필요
- 판매자센터 엑셀 양식 전수 수집 (컬럼 n개)
- 필수/선택 분류
- 엑셀 ↔ 커머스 API 필드 매핑표
- 엑셀 전용 필드 (원산지유형 코드, 알림받기 여부 등)

## 3. 고도몰/이셀러스 공통 결정 규칙 (확정)

- **상품명**: `ST4_최종결과` 사용 (`originProduct.name` 또는 엑셀 상품명)
- **키워드/태그**: ST2 + ST3 enriched (중복 제거) — 커머스 API는 `seoInfo` 쪽에 매핑 예정
- **이미지**: 가공엑셀 `사용URL` → representativeImage, 나머지 원본 → optionalImages
- **alt 주입**: `seo_alt_injector` 로 detailContent HTML 내 img 태그에 자동 주입
- **카테고리**: OC API 마켓별 제공값 자동매핑. 같은 사업자(group_id) 내 카테고리 **중복 금지**
- **원산지**: `convert_origin()` 변환 (국산/수입 구분)
- **재고**: 9999 고정

## 4. 속성정보 (129번 고도몰 컬럼 대응)

- 카테고리별 속성값 매핑은 **별도 솔루션**에 존재:
  - `C:\Users\kohaz\Desktop\Python\스마트스토어전용_노출극대화솔루션`
- Hub 통합 시점에 해당 솔루션의 카테고리별 속성 매트릭스를 import 해야 함 → **backlog**

## 5. 가격 엔진 적용

- `market_strategies.스마트스토어` pricing 에서 계산된 `salePrice` 사용
- 최저가전략: 손해 -200원 일괄 캡 ([pricing.md](pricing.md))
- 할인 필드 `benefitView` 설정 시 플랫폼 할인율 J=50% 반영 (커머스 API `originProduct.benefitView`)

## 6. 작성필요 (공식 문서 직접 참조 필요)

현재 WebFetch가 apicenter 직접 접근 제한 — 다음 방법 중 선택:
1. 사용자가 공식 가이드 PDF/MD 레포에 넣어주면 (`docs/external/`) 반영
2. 또는 `hub/adapters/smartstore.py` 에서 실 호출 예제 포착해서 역산
3. OFF_Solution 같은 3rd party 문서 참고

필드 완전 스펙은 공식 문서 우선 확인.

## 관련 코드
- `hub/adapters/smartstore.py` (worktree `heuristic-curran-17d5c1`) — commerce API 호출
- `hub/services/smartstore_service.py` — 상품 인덱스 서비스
- `hub/services/smartstore_tag_service.py` — 태그 전처리

Sources:
- [스마트스토어 API 적용 - OFF_Solution](https://www.off.co.kr/%EC%8A%A4%EB%A7%88%ED%8A%B8%EC%8A%A4%ED%86%A0%EC%96%B4-api-%EC%A0%81%EC%9A%A9%EC%9D%84-%ED%86%B5%ED%95%9C-%EA%B0%84%ED%8E%B8%ED%95%9C-%EC%83%81%ED%92%88-%EC%A0%84%EC%86%A1/)
- [커머스API 릴리즈 노트 v2.45.0](https://github.com/commerce-api-naver/commerce-api/discussions/2162)
- [상품 등록 API Data 입력 관련 질문](https://github.com/commerce-api-naver/commerce-api/discussions/246)
