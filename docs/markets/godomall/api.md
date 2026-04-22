# 고도몰 OpenHub API — 스펙 · 제약 · 참조

## 기본 정보

- **Base URL**: `https://openhub.godo.co.kr/godomall5`
- **인증**: 모든 요청 body에 `partner_key`, `key` 포함 (`application/x-www-form-urlencoded`)
- **응답**: XML → dict 변환 (`xml.etree.ElementTree`)
- **credential**: [godomall_register/config.py](../../../godomall_register/config.py) `STORES`

## 엔드포인트

| 목적 | 경로 | 파라미터 |
|---|---|---|
| 상품 조회 | `goods/Goods_Search.php` | page, size, filters |
| 상품 등록 | `goods/Goods_Insert.php` | data_url |
| 상품 수정 | `goods/Goods_Update.php` | data_url |
| 상품 삭제 (휴지통) | `goods/Goods_Delete.php` | goodsNo |
| 상품 완전삭제 | `goods/Goods_Totally_Delete.php` | goodsNo |

## Rate Limit

- **기본 throttle**: 15ms 간격 = 66 req/sec (클라이언트측)
- 응답 헤더 `ratelimit-available-level: EXHAUSTED` → 자동 **2초 대기**
- HTTP 429 → **3/6/9초 지수 백오프** × 최대 3회
- **스토어별(partner_key별) 독립 한도** → 스토어 병렬은 자유

## 응답 구조

```xml
<data>
  <header>
    <code>000</code>  <!-- 000=성공, 898=필수값 누락, 999=기타 -->
    <msg>성공</msg>
    <total>9900</total>
    <max_page>99</max_page>
    <now_page>1</now_page>
  </header>
  <return>
    <goods_data>...</goods_data>
    <!-- 배치 응답 시 idx 속성으로 여러 goods_data 반복 -->
  </return>
</data>
```

## data_url 방식 (등록/수정)

- `data_url` = 외부 공개 XML URL (직접 XML body POST 불가)
- 구현: Cloudflare R2 업로드 → 공개 URL 전달 ([godomall_register/xml_host.py](../../../godomall_register/xml_host.py))
- boto3 `max_pool_connections=64` 권장 (기본 10은 병렬 병목)

## 🔒 중요 제약 — Partial Update 미지원

`Goods_Update.php` 는 **부분 업데이트 불가**. goodsNo + 변경 필드만 보내면 `code=898 msg="goodsNmFl 값은 필수 값입니다"` 등 필수값 에러 연쇄 발생.

### 해결 — 전 필드 복사 + 변경 필드 덮어쓰기
```python
from godomall_register.xml_builder import SIMPLE_FIELDS, build_product_xml

# 1. 기존 상품 fetch (search_products)
existing = client.search_products(page=1, size=10)["products"][0]

# 2. 전 SIMPLE_FIELDS 복사
payload = {"goodsNo": existing["goodsNo"]}
for f in SIMPLE_FIELDS:
    v = existing.get(f)
    if v not in (None, ""):
        payload[f] = v

# 3. 변경 필드만 덮어쓰기
payload["goodsPrice"] = str(new_price)
payload["brandCd"] = "001"

# 4. XML 빌드 + R2 업로드 + update_product
xml = build_product_xml(payload, mode="update")
```

옵션/이미지 변경은 optionData/\*ImageData 포함 전체 세트 필요.

## 배치 XML (검증 완료 2026-04-20)

- `build_product_xml(products=[p1, ..., p50], mode="update")` → 1 XML에 50 goods_data
- 1 API 호출로 50건 동시 업데이트, 응답 list 반환 (idx 속성 매핑)
- 예상 성능: 9,900건 전체가 **5~8분** 완료 (기존 60분 대비 10배 개선)
- 구현 위치: [scripts/phase2d_batch_test.py](../../../scripts/phase2d_batch_test.py) 참조

## 어드민 상품 직링크

```
{admin_url}/goods/goods_list.php?key=goodsNo&keyword={goodsNo}&searchFl=y
```

⚠️ `goods_view.php?goodsNo=N` 은 **동작 안 함** (사용자 확인). goods_list 검색 파라미터 방식만 유효.

## 주의

- B 스토어 일부는 admin_url config 오기(psunset8952 vs 실제 psunset8982) 가능 — credential 수정 시 확인
- partner_key 본사/공급사 분리 (공급사키 상품은 본사 goods_list.php 비노출)
- `excelFl=R` 이 어드민 노출 키. API 등록 시 미지정하면 빈값=숨김
