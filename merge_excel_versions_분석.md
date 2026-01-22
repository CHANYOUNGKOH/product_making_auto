# 엑셀 버전 병합 로직 분석

## 병합 프로세스 단계별 설명

### 1단계: 안전장치 확인 (`_merge_pair` 시작)
```python
file1_root = get_root_filename(file1_path)
file2_root = get_root_filename(file2_path)
```
- **목적**: 두 파일이 같은 공통 분모(root_name)를 가지는지 확인
- **검증**: `file1_root == file2_root` 여부 확인
- **실패 시**: 병합 중단, `None` 반환

### 2단계: 파일 로드
```python
df = pd.read_excel(file_path, dtype=str, engine='openpyxl')
```
- **모든 데이터를 문자열(`dtype=str`)로 읽음**
- **상품코드 컬럼 존재 여부 확인**
- **버전 정보 추출** (`extract_version_info`)

### 3단계: 버전 정렬 및 기준 파일 선택
```python
dataframes.sort(key=lambda x: (x[2] or 0, x[3] or 0), reverse=True)
base_df = dataframes[0]  # 최신 버전
```
- **T 버전 우선, 같으면 I 버전 우선으로 정렬**
- **최신 버전 파일을 `base_df`로 설정** (기준 파일)
- **두 번째 파일을 `new_df`로 설정** (병합할 파일)

### 4단계: DataFrame 병합 (`_merge_dataframes`)

#### 4-1. 인덱스 설정
```python
base_df = base_df.set_index(product_code_col)
new_df = new_df.set_index(product_code_col)
```
- **상품코드를 인덱스로 변환**
- ⚠️ **위험**: 상품코드가 중복되면 마지막 값만 남음 (데이터 손실 가능)

#### 4-2. 새 컬럼 추가
```python
for col in new_df.columns:
    if col not in base_df.columns:
        base_df[col] = None  # 새 컬럼을 None으로 초기화
```
- **`new_df`에만 있는 컬럼을 `base_df`에 추가** (값은 `None`)

#### 4-3. 행별 병합
```python
for product_code in new_df.index:
    if product_code in base_df.index:
        # 기존 행: 새 컬럼만 채우기
        for col in new_df.columns:
            if col not in base_df.columns:
                if not pd.isna(...) and str(...).strip() != "":
                    base_df.at[product_code, col] = new_df.at[product_code, col]
    else:
        # 새 행: 전체 행 추가
        new_row = new_df.loc[[product_code]].copy()
        # base_df에 없는 컬럼을 None으로 채우기
        base_df = pd.concat([base_df, new_row])
```

**병합 규칙**:
- ✅ **기존 행이 있는 경우**: `base_df`의 기존 컬럼은 **절대 업데이트하지 않음**, 새 컬럼만 채움
- ✅ **새 행인 경우**: `new_df`의 행을 추가하고, `base_df`에만 있는 컬럼은 `None`으로 채움

#### 4-4. 인덱스 복원
```python
base_df = base_df.reset_index()
```
- **인덱스를 컬럼으로 복원** (상품코드 컬럼이 다시 나타남)

### 5단계: 파일명 생성 및 저장
```python
base_output_filename = f"{root_base}_T{max_t}_I{max_i}{ext}"
```
- **최신 버전(T, I 최대값)으로 파일명 생성**
- **기존 파일과 겹치지 않도록 번호 추가** (`(1)`, `(2)` 등)

---

## 🚨 잠재적 문제점 및 위험성

### 1. **상품코드 중복 문제** (심각)
```python
base_df = base_df.set_index(product_code_col)
```
- **문제**: 상품코드가 중복되면 `set_index()` 시 마지막 값만 남고 이전 행이 삭제됨
- **영향**: 데이터 손실 가능
- **예시**: 
  ```
  상품코드 | 이름
  A001    | 상품1
  A001    | 상품2  ← 이 행이 삭제됨!
  ```

### 2. **데이터 타입 변환 문제** (중간)
```python
df = pd.read_excel(file_path, dtype=str, engine='openpyxl')
```
- **문제**: 모든 데이터를 문자열로 읽음
- **영향**: 
  - 숫자 데이터가 문자열로 변환 (예: `123` → `"123"`)
  - 날짜 데이터가 문자열로 변환 (예: `2024-01-01` → `"2024-01-01"`)
  - 이후 계산이나 정렬 시 문제 발생 가능

### 3. **빈 값 처리 문제** (중간)
```python
if not pd.isna(new_df.at[product_code, col]) and str(new_df.at[product_code, col]).strip() != "":
```
- **문제**: `"nan"`, `"None"`, `"null"` 같은 문자열도 유효한 값으로 처리됨
- **영향**: 의도하지 않은 빈 값이 채워질 수 있음

### 4. **상품코드가 NaN/빈 값인 경우** (중간)
- **문제**: `set_index()` 시 NaN이나 빈 문자열이 인덱스로 설정되면 문제 발생
- **영향**: 인덱스 중복 또는 오류 발생 가능

### 5. **인덱스 복원 시 컬럼 중복 가능성** (낮음)
```python
base_df = base_df.reset_index()
```
- **문제**: 만약 `base_df`에 이미 `product_code_col` 이름의 컬럼이 있다면?
- **현재 코드**: 인덱스로 설정했으므로 중복 가능성 낮음 (하지만 완전히 안전하지 않음)

### 6. **새 행 추가 시 컬럼 순서 문제** (낮음)
```python
new_row = new_df.loc[[product_code]].copy()
for col in base_df.columns:
    if col not in new_row.columns:
        new_row[col] = None
base_df = pd.concat([base_df, new_row])
```
- **문제**: `new_row`의 컬럼 순서가 `base_df`와 다를 수 있음
- **영향**: 컬럼 순서가 뒤섞일 수 있음 (데이터 손실은 없음)

### 7. **대소문자 구분 문제** (낮음)
- **문제**: 상품코드가 대소문자를 구분함 (`"A001"` ≠ `"a001"`)
- **영향**: 같은 상품인데 다른 행으로 처리될 수 있음

### 8. **공백 처리 문제** (낮음)
- **문제**: 상품코드 앞뒤 공백이 있으면 다른 값으로 인식 (`"A001"` ≠ `" A001"`)
- **영향**: 같은 상품인데 다른 행으로 처리될 수 있음

---

## 💡 개선 제안

### 1. 상품코드 중복 검사 추가
```python
# 파일 로드 후 중복 검사
if base_df[product_code_col].duplicated().any():
    self._log(f"  ⚠️ 기준 파일에 상품코드 중복이 있습니다. 첫 번째 값만 사용됩니다.")
    # 또는 중복 제거 또는 오류 처리
```

### 2. 데이터 타입 보존 옵션 추가
```python
# dtype=str 대신 기본 타입 유지
df = pd.read_excel(file_path, engine='openpyxl')
# 또는 특정 컬럼만 문자열로 변환
```

### 3. 빈 값 처리 개선
```python
# 더 엄격한 빈 값 체크
value = new_df.at[product_code, col]
if pd.isna(value) or str(value).strip() in ["", "nan", "None", "null", "NaN"]:
    continue
```

### 4. 상품코드 정규화
```python
# 상품코드 앞뒤 공백 제거 및 대소문자 통일
base_df[product_code_col] = base_df[product_code_col].astype(str).str.strip().str.upper()
new_df[product_code_col] = new_df[product_code_col].astype(str).str.strip().str.upper()
```

### 5. 인덱스 설정 전 중복 제거 또는 경고
```python
# 중복 확인
if base_df[product_code_col].duplicated().any():
    self._log(f"  ⚠️ 기준 파일에 상품코드 중복이 있습니다.")
    # 선택: 중복 제거 또는 오류 처리
    base_df = base_df.drop_duplicates(subset=[product_code_col], keep='first')
```

---

## 요약

**현재 병합 로직의 핵심 원칙**:
1. ✅ 기존 컬럼은 절대 업데이트하지 않음 (보존)
2. ✅ 새 컬럼만 추가
3. ✅ 새 행은 추가 (base_df에 없는 상품코드)

**주요 위험 요소**:
1. 🚨 **상품코드 중복 시 데이터 손실** (가장 심각)
2. ⚠️ **모든 데이터를 문자열로 읽음** (타입 정보 손실)
3. ⚠️ **빈 값 처리 로직이 완벽하지 않음**

**권장 사항**:
- 상품코드 중복 검사 및 처리 로직 추가
- 데이터 타입 보존 옵션 고려
- 빈 값 처리 로직 개선
- 상품코드 정규화 (공백 제거, 대소문자 통일)


