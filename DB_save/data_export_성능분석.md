# data_export.py 스크립트 성능 분석 보고서

## 📋 개요

`data_export.py`는 SQLite DB에서 상품 데이터를 읽어 마켓 업로드용 엑셀 파일을 생성하는 스크립트입니다. 
현재 코드 구조를 분석하여 성능 병목 지점을 파악하고 개선 방안을 제시합니다.

## 🔍 현재 구조 분석

### 실행 흐름

1. **MainWindow 초기화** (`data_export.py` → `ui/main_window.py`)
2. **데이터 출고 실행** (`_run_export_for_upload` 메서드)
   - 시트별로 마켓 그룹화
   - 각 스토어별로 처리
   - 각 카테고리별로 상품 조회
   - 조합 할당 및 DB 기록
   - 엑셀 파일 생성

---

## ⚠️ 주요 성능 병목 지점

### 1. **반복적인 DB 쿼리 (중요도: 매우 높음)**

#### 문제점
- **위치**: `ui/main_window.py:5170-5179`
- **현상**: 각 스토어의 각 카테고리마다 `get_products_for_upload()` 호출
  ```python
  for category in store_categories:
      products = db_handler.get_products_for_upload(
          category, sheet_name, business_number, 
          exclude_assigned=exclude_assigned,
          season_filter_enabled=season_filter_enabled
      )
  ```

#### 내부 쿼리 (각 호출마다 실행)
1. **combination_assignments 조회** (시트 전체)
   ```sql
   SELECT DISTINCT combination_index, product_code
   FROM combination_assignments 
   WHERE sheet_name = ?
   ```

2. **스토어별 사용 상품코드 조회**
   ```sql
   SELECT DISTINCT product_code
   FROM combination_assignments 
   WHERE sheet_name = ? AND business_number = ?
   ```

3. **카테고리별 상품 조회 (LIKE 쿼리)**
   ```sql
   SELECT DISTINCT p.상품코드, ...
   FROM products p
   WHERE p.카테고리명 LIKE ? 
   AND p.product_status = ?
   ```

4. **각 상품코드별 조합 조회** (루프 내부)
   ```sql
   SELECT * FROM product_combinations 
   WHERE product_code = ? 
   AND combination_index NOT IN (...)
   ```

#### 영향도
- **10개 카테고리 × 5개 스토어 = 50번의 get_products_for_upload 호출**
- 각 호출마다 3~4개의 쿼리 실행 = **150~200개의 쿼리**
- LIKE 쿼리는 인덱스 활용 불가 → 전체 테이블 스캔 가능
- **예상 소요 시간**: 카테고리당 0.5~2초 → 총 25~100초 (10카테고리 기준)

---

### 2. **개별 INSERT + COMMIT (중요도: 높음)**

#### 문제점 A: combination_assignments INSERT
- **위치**: `ui/main_window.py:5446-5459`
- **현상**: 각 조합마다 개별 INSERT + COMMIT
  ```python
  cursor.execute("""
      INSERT OR IGNORE INTO combination_assignments 
      (sheet_name, business_number, product_code, combination_index)
      VALUES (?, ?, ?, ?)
  """, (sheet_name, business_number, product_code, combination_index))
  db_handler.conn.commit()  # 각 INSERT마다 COMMIT!
  ```

#### 문제점 B: upload_logs INSERT
- **위치**: `database/db_handler.py:868`
- **현상**: 각 상품마다 개별 INSERT + COMMIT
  ```python
  def log_upload(...):
      cursor.execute("INSERT INTO upload_logs (...) VALUES (...)")
      self.conn.commit()  # 각 INSERT마다 COMMIT!
  ```

#### 영향도
- **1000개 상품 출고 시**: 1000번의 COMMIT
- 각 COMMIT마다 디스크 I/O 발생
- **예상 소요 시간**: 상품당 5~20ms → 총 5~20초 (1000개 기준)
- WAL 모드 사용 시에도 각 COMMIT마다 WAL 파일 동기화 발생

---

### 3. **중복 쿼리 (중요도: 중간)**

#### 문제점 A: market_id 조회
- **위치**: `ui/main_window.py:5535`
- **현상**: 각 상품마다 동일한 market_id 조회
  ```python
  for product_code in product_codes_list:
      # ... (루프 내부)
      cursor.execute("SELECT id FROM markets WHERE market_name = ?", (sheet_name,))
      row = cursor.fetchone()
  ```

#### 문제점 B: 전체 조합 조회
- **위치**: `ui/main_window.py:5078-5091`
- **현상**: 시작 시 전체 combination_assignments를 메모리로 로드
  ```python
  cursor.execute("""
      SELECT ca.product_code, ca.combination_index, ...
      FROM combination_assignments ca
      JOIN product_combinations pc 
      ON ca.product_code = pc.product_code 
      AND ca.combination_index = pc.combination_index
  """)
  for row in cursor.fetchall():  # 모든 레코드를 메모리로 로드
      global_used_combinations_db.add(...)
  ```
- **문제**: 조합이 많을 경우 메모리 사용량 증가 및 초기 로딩 시간 증가

---

### 4. **시즌 필터링 처리 (중요도: 낮음)**

#### 문제점
- **위치**: `database/db_handler.py:595-669`
- **현상**: 각 카테고리 조회마다 시즌 설정 파일을 로드하고 필터링
- **영향도**: 파일 I/O 및 JSON 파싱 오버헤드 (상대적으로 작음)

---

## 📊 성능 영향 요약

| 병목 지점 | 현재 동작 | 예상 소요 시간 | 우선순위 |
|---------|---------|-------------|---------|
| 반복 쿼리 | 50번 호출 × 4쿼리 = 200개 쿼리 | 25~100초 | 🔴 매우 높음 |
| 개별 COMMIT | 1000개 × 2개 INSERT = 2000번 COMMIT | 5~20초 | 🔴 높음 |
| 중복 쿼리 | 1000번 market_id 조회 | 1~5초 | 🟡 중간 |
| 전체 조합 로드 | 모든 조합 메모리 로드 | 1~3초 | 🟡 중간 |
| 시즌 필터링 | 파일 I/O + 파싱 | 0.5~2초 | 🟢 낮음 |

**총 예상 소요 시간 (1000개 상품, 10카테고리 기준)**: **32~130초**

---

## 💡 개선 방안

### 🎯 우선순위 1: 배치 INSERT + 트랜잭션 통합

#### 개선 내용
1. **combination_assignments 배치 INSERT**
   - 모든 조합을 리스트로 수집
   - `executemany()` 또는 한 번의 INSERT로 배치 처리
   - 마지막에 한 번만 COMMIT

2. **upload_logs 배치 INSERT**
   - 모든 로그를 리스트로 수집
   - 배치 INSERT 후 한 번만 COMMIT

#### 예상 효과
- **COMMIT 횟수**: 2000번 → 2번 (1000배 감소)
- **예상 소요 시간**: 5~20초 → 0.1~0.5초
- **개선율**: **95~97%**

---

### 🎯 우선순위 2: 쿼리 캐싱 및 배치 조회

#### 개선 내용
1. **카테고리별 상품 배치 조회**
   - 모든 카테고리를 한 번에 조회 (IN 절 사용)
   - 또는 시트별로 한 번만 조회하여 메모리에서 필터링

2. **combination_assignments 캐싱**
   - 시트별로 한 번만 조회하여 메모리에 캐싱
   - 각 카테고리 조회 시 캐시 재사용

3. **market_id 캐싱**
   - 시트명 → market_id 매핑을 딕셔너리로 캐싱
   - 루프 시작 전에 미리 조회

#### 예상 효과
- **쿼리 횟수**: 200개 → 10~20개 (10~20배 감소)
- **예상 소요 시간**: 25~100초 → 2.5~10초
- **개선율**: **90%**

---

### 🎯 우선순위 3: 전체 조합 조회 최적화

#### 개선 내용
- 필요한 시트/조합만 조회 (WHERE 절 추가)
- 또는 필요할 때만 조회 (Lazy Loading)

#### 예상 효과
- **메모리 사용량**: 50~90% 감소
- **초기 로딩 시간**: 1~3초 → 0.1~0.5초

---

### 🎯 우선순위 4: 인덱스 최적화 (이미 적용됨)

#### 현재 상태
- ✅ `idx_products_category_status` (카테고리명, product_status)
- ✅ `idx_upload_logs_market_status` (market_name, upload_status)
- ✅ `idx_combination_assignments_sheet_store` (sheet_name, business_number, product_code)

#### 추가 고려사항
- LIKE 쿼리는 인덱스 활용 불가 (앞뒤 % 패턴)
- 정확한 카테고리명 매칭 시 인덱스 활용 가능

---

## 📈 개선 후 예상 성능

| 항목 | 현재 | 개선 후 | 개선율 |
|-----|------|---------|--------|
| 총 소요 시간 (1000개) | 32~130초 | **3~12초** | **90~95%** |
| DB 쿼리 수 | 200개 | 10~20개 | **90%** |
| COMMIT 횟수 | 2000번 | 2번 | **99.9%** |
| 메모리 사용량 | 높음 | 중간 | **50%** |

---

## 🛠️ 구현 우선순위

1. **1단계 (즉시 적용 가능, 효과 큼)**
   - ✅ 배치 INSERT + 트랜잭션 통합
   - ✅ market_id 캐싱

2. **2단계 (중기 개선)**
   - ✅ combination_assignments 캐싱
   - ✅ 카테고리 배치 조회

3. **3단계 (장기 개선)**
   - ✅ 전체 조합 조회 최적화
   - ✅ 시즌 필터링 캐싱

---

## 📝 참고사항

- **WAL 모드**: 이미 활성화되어 있음 (동시 읽기 성능 향상)
- **PRAGMA 설정**: cache_size, synchronous 등 이미 최적화됨
- **인덱스**: 주요 인덱스는 이미 생성됨
- **데이터 규모**: 현재 1.7GB DB, 향후 5만~20만 개 상품 예상

---

## 결론

현재 코드는 기능적으로는 완성되었으나, 성능 최적화 여지가 큽니다.
특히 **배치 INSERT + 트랜잭션 통합**과 **쿼리 캐싱**을 적용하면 
**90% 이상의 성능 개선**을 기대할 수 있습니다.

