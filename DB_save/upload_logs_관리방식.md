# upload_logs 테이블 관리 방식

## 1. 테이블 구조

```sql
CREATE TABLE upload_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    business_number TEXT,              -- 사업자번호
    market_id INTEGER,                  -- 마켓 ID
    market_name TEXT,                   -- 시트명 (마켓 타입: "쿠팡", "스마트스토어" 등)
    product_id INTEGER,                 -- 상품 ID
    product_code TEXT,                  -- 상품코드
    used_product_name TEXT,             -- 사용한 상품명 (해당 줄의 상품명)
    used_nukki_url TEXT,               -- 사용한 누끼 URL
    used_mix_url TEXT,                  -- 사용한 믹스 URL
    product_name_index INTEGER,         -- 사용한 상품명의 줄 번호 (0부터 시작)
    image_nukki_index INTEGER,          -- 누끼 이미지 인덱스
    image_mix_index INTEGER,            -- 믹스 이미지 인덱스
    upload_strategy TEXT,               -- 업로드 전략 (JSON)
    upload_status TEXT,                 -- 업로드 상태 ('SUCCESS' 또는 'FAILED')
    uploaded_at TEXT,                   -- 업로드 시간
    notes TEXT                          -- 추가 정보 (카테고리, 마켓명, 스토어별칭 등)
)
```

## 2. 중복 방지 메커니즘

### 조합 키 (Combination Key)
중복 체크는 **3가지 요소의 조합**으로 이루어집니다:
- `(상품코드, URL, 상품명)`

예시:
- `("PROD001", "https://mix-url.com/img1.jpg", "상품명1")` ← 조합 1
- `("PROD001", "https://mix-url.com/img1.jpg", "상품명2")` ← 조합 2 (다른 조합)
- `("PROD001", "https://nukki-url.com/img1.jpg", "상품명1")` ← 조합 3 (다른 조합)

### 시트별 독립 관리
- **같은 시트(market_name) 내에서만** 중복 체크
- 예: "쿠팡" 시트에서 사용한 조합은 "스마트스토어" 시트에서 재사용 가능

### 조합 소진 순서
1. **첫 번째 줄**: 믹스url + 상품명(1줄) → 누끼url + 상품명(1줄)
2. **두 번째 줄**: 믹스url + 상품명(2줄) → 누끼url + 상품명(2줄)
3. **세 번째 줄**: 믹스url + 상품명(3줄) → 누끼url + 상품명(3줄)
4. ... (마지막 줄까지)

## 3. 동작 흐름

### Step 1: 사용 가능한 조합 조회
```python
# get_products_for_upload() 호출 시
1. upload_logs에서 시트명 기준으로 이미 사용한 조합 조회
2. used_combinations_mix = {(상품코드, 믹스url, 상품명), ...}
3. used_combinations_nukki = {(상품코드, 누끼url, 상품명), ...}
```

### Step 2: 사용 가능한 조합 필터링
```python
# 각 상품의 각 줄별로
1. 믹스url + 상품명 조합이 used_combinations_mix에 있는지 확인
2. 없으면 → 사용 가능한 조합으로 추가
3. 누끼url + 상품명 조합이 used_combinations_nukki에 있는지 확인
4. 없으면 → 사용 가능한 조합으로 추가
```

### Step 3: 조합 사용 및 기록
```python
# 데이터 출고 시
1. 사용 가능한 조합 중 우선순위에 따라 선택
2. 각 스토어당 상품코드 1개만 사용 (중복 방지)
3. 사용한 조합을 upload_logs에 기록
   - used_mix_url 또는 used_nukki_url 중 하나만 기록
   - used_product_name: 사용한 상품명
   - product_name_index: 사용한 줄 번호
```

## 4. 예시 시나리오

### 상품 정보
- 상품코드: "PROD001"
- 믹스url: "https://mix.com/img1.jpg"
- 누끼url: "https://nukki.com/img1.jpg"
- 상품명: 
  - 1줄: "상품명1"
  - 2줄: "상품명2"
  - 3줄: "상품명3"

### 사용 가능한 조합 (총 6개)
1. (PROD001, 믹스url, 상품명1)
2. (PROD001, 누끼url, 상품명1)
3. (PROD001, 믹스url, 상품명2)
4. (PROD001, 누끼url, 상품명2)
5. (PROD001, 믹스url, 상품명3)
6. (PROD001, 누끼url, 상품명3)

### 사용 과정
1. **스토어1 (쿠팡A1-0)**: 조합 1 사용 → upload_logs 기록
2. **스토어2 (쿠팡A1-1)**: 조합 2 사용 → upload_logs 기록
3. **스토어3 (쿠팡A1-2)**: 조합 3 사용 → upload_logs 기록
4. ...
5. **스토어7 (쿠팡A1-6)**: 조합 없음 (이미 6개 모두 소진)

### upload_logs 기록 예시
```
| product_code | market_name | used_mix_url              | used_nukki_url | used_product_name | product_name_index |
|--------------|-------------|---------------------------|----------------|-------------------|-------------------|
| PROD001      | 쿠팡        | https://mix.com/img1.jpg  |                | 상품명1           | 0                 |
| PROD001      | 쿠팡        |                           | https://nukki...| 상품명1           | 0                 |
| PROD001      | 쿠팡        | https://mix.com/img1.jpg  |                | 상품명2           | 1                 |
```

## 5. 중복 방지 확인 방법

### 다음 출고 시
1. `get_products_for_upload()` 호출
2. upload_logs에서 이미 사용한 조합 조회
3. 사용 가능한 조합만 반환 (이미 사용한 조합 제외)
4. → **자동으로 중복 방지됨**

### 조회 방법
```python
# 상품코드별 조회
logs = db_handler.get_upload_logs_by_product_code("PROD001", "쿠팡")

# 마켓별 조회
logs = db_handler.get_upload_logs_by_market("쿠팡")
```

## 6. 주요 특징

✅ **시트별 독립 관리**: 같은 시트 내에서만 중복 체크
✅ **조합 단위 관리**: (상품코드, URL, 상품명) 조합으로 관리
✅ **자동 중복 방지**: 이미 사용한 조합은 자동으로 제외
✅ **상세 기록**: 어떤 마켓, 어떤 스토어, 어떤 조합을 사용했는지 모두 기록
✅ **추적 가능**: 언제, 어디서, 어떤 조합을 사용했는지 추적 가능

