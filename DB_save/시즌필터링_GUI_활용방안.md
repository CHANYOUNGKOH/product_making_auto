# 시즌 필터링 GUI 활용 방안

## 📋 현재 구현된 기능

### 1. Excel 파일 관리
- ✅ Excel 파일 로드 및 표시
- ✅ 시트별 탭으로 구분
- ✅ 행 추가/삭제
- ✅ 셀 편집 (더블클릭)
- ✅ Excel 저장
- ✅ Excel 구조 확인

### 2. JSON 컴파일
- ✅ Excel → JSON 변환
- ✅ JSON 미리보기
- ✅ 자동 캐시 갱신 (mtime 기반)

---

## 🎯 활용 방안

### 방안 1: 독립 실행형 관리 도구 (현재)

**사용 시나리오:**
1. 시즌 필터 관리자가 Excel 파일을 열어서 시즌/키워드 관리
2. "JSON 컴파일" 버튼으로 `season_filters.json` 생성
3. `data_export.py`에서 자동으로 JSON을 읽어서 필터링 적용

**장점:**
- ✅ 관리와 실행이 분리되어 명확함
- ✅ Excel 파일을 직접 수정할 수 있음
- ✅ JSON 컴파일을 명시적으로 제어 가능

**단점:**
- ⚠️ "JSON 컴파일" 버튼을 누르는 것을 잊을 수 있음

---

### 방안 2: data_export.py에 통합 (추천)

**사용 시나리오:**
1. `data_export.py` 실행 시 자동으로 Excel → JSON 컴파일 (mtime 기반)
2. GUI는 별도로 실행하여 Excel 편집
3. 편집 후 `data_export.py`를 다시 실행하면 자동으로 최신 설정 반영

**구현 방법:**
```python
# data_export.py에 추가
from season_filter_manager_gui import load_season_config

# 프로그램 시작 시
season_config = load_season_config(
    excel_path="Season_Filter_Seasons_Keywords.xlsx",
    json_path="season_filters.json"
)
```

**장점:**
- ✅ "설정 저장" 버튼 불필요 (자동 갱신)
- ✅ Excel만 수정하면 자동 반영
- ✅ 운영 난이도 최소

---

### 방안 3: GUI 내장형 (통합)

**사용 시나리오:**
1. `data_export.py`에 "시즌 필터 설정" 버튼 추가
2. 버튼 클릭 시 시즌 필터 관리 GUI 창 열기
3. GUI에서 Excel 편집 후 저장
4. `data_export.py`에서 자동으로 최신 설정 사용

**구현 방법:**
```python
# main_window.py에 추가
def _open_season_filter_manager(self):
    """시즌 필터 관리 창 열기"""
    from season_filter_manager_gui import SeasonFilterManagerGUI
    manager = SeasonFilterManagerGUI()
    manager.mainloop()
```

**장점:**
- ✅ 하나의 프로그램에서 모든 기능 사용
- ✅ 통합된 사용자 경험

**단점:**
- ⚠️ GUI가 복잡해질 수 있음

---

### 방안 4: 하이브리드 방식 (최종 추천)

**사용 시나리오:**
1. **관리용**: `season_filter_manager_gui.py` 독립 실행
   - 시즌/키워드 대량 편집
   - Excel 구조 확인
   - JSON 컴파일 및 미리보기

2. **실행용**: `data_export.py`에 자동 통합
   - Excel 변경 감지 시 자동 JSON 컴파일
   - 시즌 필터링 자동 적용
   - "시즌 필터 설정" 버튼으로 빠른 편집 가능

**구현 구조:**
```
season_filter_manager_gui.py (독립 실행)
├── Excel 편집 기능
├── JSON 컴파일 기능
└── Excel 구조 확인

data_export.py (통합)
├── 자동 캐시 로딩 (load_season_config)
├── 시즌 필터링 적용
└── "시즌 필터 설정" 버튼 (선택적)
```

---

## 🔧 구체적인 통합 방법

### Step 1: season_filter_manager_gui.py에 유틸리티 함수 추가

```python
def load_season_config(excel_path: str, json_path: str) -> Dict:
    """
    시즌 설정 로딩 (자동 캐시 갱신)
    - Excel과 JSON의 mtime 비교
    - Excel이 더 최신이면 자동 재생성
    """
    # ... (구현 생략 - 검토 문서 참고)
    pass

def compile_excel_to_json(excel_path: str, json_path: str) -> Dict:
    """
    Excel을 JSON으로 컴파일
    """
    # ... (구현 생략)
    pass
```

### Step 2: data_export.py에 통합

```python
# main_window.py의 __init__에 추가
self.season_config = None  # 시즌 필터 설정

# _start_export 메서드에 추가
def _start_export(self):
    # ... 기존 코드 ...
    
    # 시즌 필터 설정 로드
    try:
        from season_filter_manager_gui import load_season_config
        excel_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)),
            "DB_save", "Season_Filter_Seasons_Keywords.xlsx"
        )
        json_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)),
            "DB_save", "season_filters.json"
        )
        self.season_config = load_season_config(excel_path, json_path)
        if self.season_config:
            self._log("✅ 시즌 필터 설정 로드 완료")
    except Exception as e:
        self._log(f"⚠️ 시즌 필터 설정 로드 실패: {e}")

# UI에 버튼 추가
btn_season_filter = ttk.Button(
    btn_frame, 
    text="🗓️ 시즌 필터 설정",
    command=self._open_season_filter_manager
)
```

### Step 3: db_handler.py에 필터링 통합

```python
# get_products_for_upload 메서드 수정
def get_products_for_upload(self, category: str, sheet_name: str, 
                            business_number: str, status: str = 'ACTIVE',
                            season_config: Optional[Dict] = None) -> List[Dict]:
    # 1. 기본 상품 조회
    products = self._get_products_base(category, status)
    
    # 2. 시즌 필터링 적용 (마켓별 출력제한 기록 확인 전에 수행)
    if season_config:
        from season_filter_manager_gui import filter_products_by_season
        products, excluded_count, excluded_seasons = filter_products_by_season(
            products, season_config
        )
    
    # 3. 마켓별 출력제한 기록 확인 (기존 로직)
    # ...
    
    return products
```

---

## 💡 추가 기능 제안

### 1. 시즌 필터 미리보기
- 선택한 카테고리에서 시즌 필터가 어떻게 적용되는지 미리보기
- 제외될 상품 수 예상

### 2. 시즌 필터 통계
- 각 시즌별로 매칭되는 상품 수
- 제외될 상품 수
- 시즌별 점수 분포

### 3. 키워드 검색 및 자동 완성
- 키워드 입력 시 자동 완성
- 중복 키워드 검색
- 시즌별 키워드 통계

### 4. 시즌 일정 캘린더
- 시즌 시작/종료일을 캘린더로 표시
- PREP/ACTIVE/GRACE 기간 시각화

### 5. Excel 템플릿 생성
- 새 시즌 추가 시 템플릿 제공
- 필수 필드 자동 채우기

---

## 🎯 최종 추천 방안

**하이브리드 방식 (방안 4)**을 추천합니다:

1. **독립 실행**: `season_filter_manager_gui.py`로 Excel 대량 편집
2. **자동 통합**: `data_export.py`에서 자동으로 Excel 변경 감지 및 JSON 컴파일
3. **빠른 편집**: `data_export.py`에 "시즌 필터 설정" 버튼 추가 (선택적)

이 방식의 장점:
- ✅ 운영 난이도 최소 (Excel만 수정하면 자동 반영)
- ✅ 유연성 (독립 실행 또는 통합 사용 가능)
- ✅ 안정성 (자동 캐시로 Excel 문제 시에도 이전 설정 사용)

