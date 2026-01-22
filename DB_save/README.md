# 상품 업로드 관리 시스템

SQLite 기반 상품 데이터 관리 및 마켓 업로드 시스템

## 📁 파일 구조

```
DB_save/
├── data_entry.py             # 데이터 입고 도구 (엑셀 → SQLite)
├── data_export.py            # 데이터 출고 도구 (SQLite → 마켓 업로드)
├── config.py                 # 설정 및 마켓 계정 로드
├── database/
│   ├── __init__.py
│   └── db_handler.py         # DB 담당 클래스
├── ui/
│   ├── __init__.py
│   └── main_window.py        # GUI 창
└── markets/
    ├── __init__.py
    └── manager.py            # 업로드 총괄 관리
```

## 🚀 사용 방법

### 전체 작업 흐름

```
엑셀 파일 → [data_entry.py] → SQLite DB → [data_export.py] → 마켓 업로드
```

### 1. 데이터 입고 (Data Entry) - data_entry.py

**역할**: 엑셀 파일을 SQLite 데이터베이스로 변환 (DB 추가/업데이트)

```bash
python data_entry.py
```

**작업 내용**:
1. 엑셀 파일 선택 (상품명/이미지 스테이지 완료된 신상 데이터)
2. DB 파일 자동 생성/업데이트 (기본: `products.db` - 누적 저장)
3. "DB 변환 시작" 버튼 클릭
4. 지정된 컬럼만 정확히 기록됨 (공란 허용)

**기록되는 컬럼** (지정된 컬럼만 정확히 기록):
- 상품코드, 카테고리명, 원본상품명
- ST1_정제상품명, ST1_판매형태, ST2_JSON, ST3_결과상품명
- IMG_S1_휴먼라벨, IMG_S1_휴먼노트, IMG_S1_AI라벨
- view_point, subject_position, subject_size, lighting_condition
- color_tone, shadow_presence, background_simplicity, is_flat_lay
- bg_layout_hint_en, bg_positive_en, bg_negative_en
- video_motion_prompt_en, video_full_prompt_en
- 누끼url, 믹스url
- **ST3_결과상품명**은 줄바꿈으로 구분된 상품명들을 `product_names_json`에 JSON 배열로 저장 (맨 상단이 가장 품질 좋음)

**결과**: 
- `products.db` 파일 자동 생성/업데이트 (프로그램 실행 폴더에 생성)
- `products` 테이블: 지정된 컬럼만 정확히 저장 (공란 허용)
- `metadata` 테이블: 엑셀 파일 정보 저장

**중요**: 
- 이 도구는 **DB에 데이터를 추가/업데이트**만 합니다. 업로드 기능은 없습니다.
- DB 파일은 **자동으로 누적 저장**됩니다 (기존 데이터 유지).
- 엑셀에 더 많은 컬럼이 있어도 **지정된 컬럼만 기록**됩니다.

### 2. 데이터 출고 (Data Export) - data_export.py

**역할**: SQLite DB에서 상품을 읽어 마켓에 업로드할 DB를 내보냅니다. DB 출고 이력을 기록

```bash
python data_export.py
```

**기능**:
1. **마켓 업로드용 데이터 출고** (완료된 DB)
   - 마켓에 업로드할 때 DB를 빼낼 때 사용
   - 입고된 DB 중 중복된 DB를 재가공하지 않기 위해 중복을 확인하는 용도
   - **출력 방식**: 
     - `상품코드`, `누끼url`, `믹스url` 중 **`믹스url` 우선 출력**
     - `ST3_결과상품명`은 **상단부터 1줄 출력**
     - 이미 사용한 조합은 제외 (중복 방지)
   
2. **미완료 DB 출고** (재가공용)
   - 미완료된 DB 전체를 출력하여 재가공용으로 사용
   - **미완료 기준**: `ST3_결과상품명`, `누끼url`, `믹스url` 모두 공란인 경우
   - `ST3_결과상품명`은 공란이 아니면 사용 가능 (기존 썸네일 재활용)

**작업 내용**:
1. 카테고리 검색 (예: "원피스")
2. 출력 모드 선택 (마켓 업로드용 / 미완료 DB)
3. 마켓 업로드용인 경우: 업로드할 마켓/스토어 선택 (트리뷰에서 체크박스로 선택)
4. "데이터 출고" 버튼 클릭
5. 엑셀 파일로 저장 (마켓 업로드용: 선택된 컬럼만, 미완료 DB: 전체 컬럼)

**결과**:
- **마켓 업로드용**: 엑셀 파일로 저장 (상품코드, 믹스url, ST3_결과상품명 첫 줄)
- **미완료 DB**: 엑셀 파일로 저장 (전체 컬럼)
- 중복 체크: `upload_logs` 테이블을 기준으로 이미 사용한 조합은 제외

**중복 판단 기준**:
- **사업자번호** (`business_number`) + **상품코드** (`product_code`) 조합
- `upload_logs` 테이블에서 `upload_status = 'SUCCESS'`인 레코드 확인
- 같은 사업자번호와 상품코드로 이미 성공적으로 업로드된 이력이 있으면 중복으로 판단하여 스킵

**중요**: 
- 이 도구는 **데이터 입고 및 출고**를 담당합니다.
- 마켓에 업로드할 때 DB를 빼낼 때 사용합니다.
- 입고된 DB 중 중복된 DB를 재가공하지 않기 위해 중복을 확인하는 용도입니다.

```bash
python main.py
```

**작업 내용**:
1. 마켓 계정 엑셀 파일 선택 (`Market_id_pw.xlsx`)
2. DB 파일 선택 (`products.db`)
3. 카테고리 검색 (예: "원피스")
4. 마켓/스토어 선택 (체크박스)
5. "시작" 버튼 클릭
6. **중복 체크** → 업로드 전략 배정 → 마켓 API 호출

**중복 판단 기준**:
- **사업자번호** (`business_number`) + **상품코드** (`product_code`) 조합
- `upload_logs` 테이블에서 `upload_status = 'SUCCESS'`인 레코드 확인
- 같은 사업자번호와 상품코드로 이미 성공적으로 업로드된 이력이 있으면 중복으로 판단하여 스킵

**결과**: 선택한 마켓에 상품이 업로드됨

### 3. 마켓 계정 설정

`Market_id_pw.xlsx` 파일 구조:
- **마켓별로 시트로 구분** (예: 쿠팡, 네이버, 11번가 등)
- **공통 컬럼** (모든 시트에 동일):
  - **사용여부** (Y: 사용, N: 정지/사용불가)
  - **별칭** (마켓명 + 명의자 + 사업자번호 + 마켓번호, 예: "쿠팡A1-0")
  - **아이디** (마켓 접속 아이디)
  - **비밀번호** (마켓 접속 비밀번호)

별칭 형식 예시:
- `쿠팡A1-0`: 쿠팡, 명의자A, 사업자번호1, 마켓번호0
- `네이버B2-1`: 네이버, 명의자B, 사업자번호2, 마켓번호1

**중요**: 엑셀 파일을 수정하면(비밀번호 변경, 새 행 추가) 프로그램에서 "🔄 다시 로드" 버튼을 클릭하면 자동으로 반영됩니다.

## 📋 사용 시나리오

### 시나리오 1: 처음부터 시작

1. **데이터 입고** (`data_entry.py` 실행)
   - 엑셀 파일 선택
   - 마켓 정보 입력
   - DB 변환 → `products.db` 생성

2. **데이터 출고** (`data_export.py` 실행)
   - 마켓 계정 엑셀 파일 선택
   - `products.db` 선택
   - 카테고리 검색 및 마켓 선택
   - 업로드 시작

### 시나리오 2: 기존 DB에 추가

1. **데이터 입고** (`data_entry.py` 실행)
   - 기존 `products.db` 파일 선택
   - 새 엑셀 파일 변환 → 기존 DB에 추가

2. **데이터 출고** (`data_export.py` 실행)
   - 업데이트된 `products.db` 사용

### 시나리오 3: 마켓 계정 변경

1. **엑셀 파일 수정**: `Market_id_pw.xlsx`에서 비밀번호 변경 또는 새 행 추가
2. **데이터 출고** (`data_export.py` 실행)
   - "🔄 다시 로드" 버튼 클릭
   - 변경된 계정 정보 반영

## 🔧 주요 기능

### database/db_handler.py
- DB 연결 및 테이블 생성
- 마켓 정보 삽입/조회
- 상품 데이터 삽입/조회
- 중복 체크 (`check_business_duplicate`)
- 업로드 로그 기록 (`log_upload`)

### config.py
- 엑셀 파일에서 마켓 계정 정보 로드 (시트별로 읽기)
- `AccountLoader` 클래스로 계정 관리
- 엑셀 파일 경로를 `config_settings.json`에 저장하여 재사용
- 공통 컬럼만 사용: "사용여부", "별칭", "아이디", "비밀번호"
- 사용여부 Y인 계정만 로드

### ui/main_window.py
- 카테고리 검색 GUI
- 마켓 선택 체크박스
- 스레딩으로 GUI 멈춤 방지
- 실시간 로그 출력

### markets/manager.py
- 중복 체크 (`check_business_duplicate`)
- 업로드 전략 배정 (`get_upload_strategy`)
- 실제 마켓 API 호출 (시뮬레이션)
- 업로드 로그 기록

## 📊 데이터베이스 스키마

### markets 테이블
- 마켓 정보 저장

### products 테이블
- 상품 데이터 저장
- `product_names_json`: 상품명 리스트 (JSON 배열)
- `product_status`: ACTIVE/STOP 상태 관리

### upload_logs 테이블 (업로드 이력 기록)

**목적**: 어떤 마켓에 어떤 상품명과 이미지를 사용하여 업로드했는지 기록

**주요 컬럼**:
- `market_name`: 마켓명 (별칭, 예: "쿠팡A1-0")
- `product_code`: 상품코드
- `used_product_name`: 사용한 상품명
- `used_nukki_url`: 사용한 누끼 이미지 URL
- `used_mix_url`: 사용한 연출 이미지 URL
- `product_name_index`: 사용한 상품명 인덱스 (0, 1, 2...)
- `image_nukki_index`: 사용한 누끼 이미지 인덱스
- `image_mix_index`: 사용한 연출 이미지 인덱스
- `upload_strategy`: 업로드 전략 (JSON)
- `upload_status`: 업로드 상태 (SUCCESS/FAILED)
- `uploaded_at`: 업로드 일시

**사용 예시**:
```sql
-- 특정 마켓에 업로드한 상품 목록 조회
SELECT market_name, product_code, used_product_name, uploaded_at 
FROM upload_logs 
WHERE market_name = '쿠팡A1-0' AND upload_status = 'SUCCESS';

-- 특정 상품이 어떤 마켓에 업로드되었는지 조회
SELECT market_name, used_product_name, used_nukki_url, uploaded_at 
FROM upload_logs 
WHERE product_code = 'PROD001' AND upload_status = 'SUCCESS';
```

## 🔄 확장 방법

### 마켓별 API 연동

`markets/manager.py`의 `_upload_to_market` 메서드를 확장하여 실제 API를 호출하세요:

```python
# markets/coupang.py
def upload_product(product, strategy, account):
    # 쿠팡 API 호출
    pass

# markets/naver.py
def upload_product(product, strategy, account):
    # 네이버 API 호출
    pass
```

## ⚠️ 주의사항

- `Market_id_pw.xlsx` 파일이 없으면 마켓 계정을 로드할 수 없습니다.
- 엑셀 파일은 마켓별로 시트로 구분되어 있어야 합니다.
- 공통 컬럼("사용여부", "별칭", "아이디", "비밀번호")이 모든 시트에 있어야 합니다.
- 사용여부가 "Y"인 계정만 로드됩니다.
- 엑셀 파일을 수정한 후 "🔄 다시 로드" 버튼을 클릭해야 변경사항이 반영됩니다.
- DB 파일 경로를 올바르게 설정해야 합니다.
- 스레딩으로 GUI가 멈추지 않지만, 실제 API 호출 시 rate limit을 고려하세요.
- 별칭에서 사업자번호 추출은 간단한 로직을 사용하므로, 필요시 별도 매핑 테이블을 추가할 수 있습니다.

