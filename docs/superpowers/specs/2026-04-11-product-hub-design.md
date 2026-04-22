# Product Hub — 통합 관리 플랫폼 설계 스펙

날짜: 2026-04-11  
상태: 승인됨

---

## 개요

기존 Tkinter 기반 분산 UI(main_launcher_v9.py, DB_save/ui/main_window.py, converter_gui.py 등)를 대체하는 DB 중심 통합 관리 웹 플랫폼. 오너클랜 도매처 → 상품 가공 파이프라인 → 마켓 출고까지의 전 과정을 하나의 인터페이스로 관리하며, 장기적으로 상품 순환 자동화를 지원하는 기반이 된다.

기존 Tkinter UI는 병행 유지하며 새 웹 앱은 같은 products.db를 공유한다.

---

## 기술 스택

| 레이어 | 선택 | 이유 |
|---|---|---|
| 백엔드 | FastAPI (Python) | 기존 Python 모듈 직접 import, async 지원 |
| 프론트엔드 | Vanilla JS + fetch() | 빌드 도구 없음, 복잡한 테이블/실시간 업데이트 직접 제어 |
| 실시간 업데이트 | SSE (Server-Sent Events) | 파이프라인 진행률 스트리밍 |
| 데이터베이스 | SQLite (products.db) WAL 모드 | 기존 DB 공유, 동시 접속 허용 |
| 실행 | `python -m uvicorn hub.app:app --reload` → localhost:8000 | |

---

## 레이아웃

- **사이드바 고정 + 메인 콘텐츠** 구조 (사이드바 너비 230px)
- **다크 테마**: 배경 `#16181f`, 카드 `#1d2030`, 텍스트 `#f0f1f8`
- 사이드바 하단: OC 동기화 상태 표시 (마지막 동기화 시각 + 건수)

---

## 사이드바 메뉴 구성

```
Overview
  📊 대시보드

Data
  📦 상품 DB
  🏪 공급사

Processing
  ⚙️ 파이프라인

Export
  🛒 마켓 출고
  🏢 스토어 관리

System
  ⚙ 설정
```

---

## 페이지별 설계

### 1. 대시보드

**통계 카드 (상단 4개)**
- 전체 상품 (DB ACTIVE 상품코드 수)
- 출고 가능 (oc_price IS NOT NULL AND > 0)
- 가공 완료 (텍스트 + 이미지 완료 기준)
- 마켓 등록 (실제 등록 확인된 총 건수)

**패널**
- 마켓 출고 현황: 마켓별 등록 수 / 목표 슬롯 수 (바 차트)
- 파이프라인 상태: 각 단계 진행률 + 상태 pill
- 최근 활동 로그: OC 동기화, 출고, 파이프라인 실행 이력

**액션 버튼**
- 🔄 OC 동기화 (sync_existing() 실행)
- ▶ 출고 실행 (마켓 출고 페이지로 이동)

---

### 2. 상품 DB

**필터바**
- 텍스트 검색 (상품코드 / 상품명 / 스토어 별칭)
- 카테고리 드롭다운
- 마켓 드롭다운 (옥션/지마켓 각각 분리)
- 빠른 필터 칩: 전체 / OC 가격 있음 / 마켓 미배정 / 일부 미배정

**테이블 컬럼**
| 컬럼 | 설명 |
|---|---|
| 상품코드 | 모노스페이스, 파란색 |
| 상품명 | 말줄임 처리 |
| 카테고리 | 대 › 소 |
| OC 원가 | oc_price, 노란색 |
| 텍스트 | 완료/진행중/미완료 pill |
| 이미지 | 완료/진행중/미완료 pill |
| 배정 스토어 | 마켓별 컬러 별칭 태그, 4개 초과 시 +N 호버 팝업 |
| 동기화 | OC 마지막 동기화 시각 |

**배정 스토어 2-레이어 추적**
- `출고됨`: 우리 시스템에서 해당 스토어로 엑셀 출고한 이력
- `등록확인`: API(스마트스토어/고도몰/쿠팡) 또는 사용자 엑셀 import(옥션/지마켓/11번가)로 실제 등록 검증
- `불일치 ⚠️`: 출고 기록과 실제 등록 상태가 다를 때 경고 표시

**마켓별 컬러 코딩**
- 고도몰: 파랑 `#7ab0ff`
- 스마트스토어: 초록 `#34d474`
- 옥션: 주황 `#fb923c`
- 지마켓: 노랑 `#facc15`
- 11번가: 보라 `#c084fc`
- 쿠팡: 빨강 `#f87171`

**페이지네이션**: 100개씩, 서버사이드

---

### 3. 공급사

오너클랜 공급사(vendor) 목록.

| 컬럼 | 설명 |
|---|---|
| 공급사 코드 | OC vendor ID |
| 공급사명 | |
| DB 상품 수 | 해당 vendor의 DB 내 상품 수 |
| 마지막 동기화 | import_new_from_vendor() 마지막 실행 시각 |
| 액션 | 신규 입고 버튼 → import_new_from_vendor() 실행 + SSE 진행률 |

---

### 4. 파이프라인

기존 main_launcher_v9.py의 역할을 웹으로 대체.

**OC 동기화 섹션**
- 현재 DB 내 oc_price 있는 상품 수 표시
- sync_existing() 실행 버튼 + SSE 실시간 진행률

**텍스트 파이프라인 (S1~S4)**
- 각 단계별 상태 표시 (대기 / 실행중 / 완료 / 오류)
- 단계별 실행 버튼 (기존 스크립트를 subprocess로 호출)
- SSE로 진행률 스트리밍

**이미지 파이프라인 (S1~S5)**
- 동일 구조

---

### 5. 마켓 출고

기존 DB_save/ui/main_window.py + godomall_register/pipeline_run.py 통합.

**출고 설정 패널**
- 카테고리 선택 (트리 체크박스)
- 스토어 선택 (마켓별 그룹, 스토어 별칭 표시)
- 전략 선택 (스토어별: 최저가 / 일반판매 / 광고)
- 출고 형식 (고도몰 엑셀 / 이셀러스 엑셀)

**실행**
- 미리보기 (dry-run): 상품 목록 + 계산된 가격 테이블 표시
- 출고 실행: 파일 생성 후 다운로드 링크
- 출고 이력 기록 → DB 반영

**실제 등록 갱신 (reconcile)**
- 스마트스토어/고도몰/쿠팡: API 호출로 실제 등록 상품 목록 가져와 DB 갱신
- 옥션/지마켓/11번가: 사용자가 마켓에서 받은 상품 목록 엑셀 import → DB 갱신
- 두 상태 불일치 시 해당 상품코드에 ⚠️ 표시

---

### 6. 스토어 관리

Market_id_pw.xlsx를 DB로 import하여 관리.

**스토어 목록 테이블**
| 컬럼 | 설명 |
|---|---|
| 별칭 | 스마트스토어A1-1, 옥션A2-3 등 |
| 마켓 | 색상 태그 |
| 명의자 그룹 | A1/A2/B1 등 |
| 활성화 여부 | Y/N 토글 |
| 전략 배정 | 최저가 / 일반판매 / 광고 (스토어별 고정) |
| 마지막 출고 | 마지막 출고 날짜 |
| 등록 상품 수 | 실제 등록 확인된 수 |

**액션**
- Market_id_pw.xlsx import (스토어 목록 갱신)
- 스토어별 전략 배정 편집
- 비활성화 스토어 숨김

---

## DB 추가 필요 컬럼

기존 products.db에 추가:

```sql
ALTER TABLE products ADD COLUMN text_status TEXT;      -- 텍스트 가공 상태
ALTER TABLE products ADD COLUMN image_status TEXT;     -- 이미지 가공 상태
ALTER TABLE products ADD COLUMN export_log TEXT;       -- JSON: [{store, date, file}]
ALTER TABLE products ADD COLUMN registered_stores TEXT; -- JSON: [{store, confirmed_at, source}]
-- source: 'api' | 'excel_import'
```

stores 테이블 신규 생성:
```sql
CREATE TABLE stores (
  id INTEGER PRIMARY KEY,
  alias TEXT UNIQUE,       -- 스마트스토어A1-1
  market TEXT,             -- 스마트스토어/옥션/지마켓/...
  group_id TEXT,           -- A1/A2/B1
  login_id TEXT,
  active INTEGER DEFAULT 1,
  strategy TEXT,           -- lowest_price/normal_sale/cpc_ad
  slot_count INTEGER,      -- 목표 슬롯 수
  created_at TEXT,
  updated_at TEXT
);
```

---

## 파일 구조

```
hub/                         ← 신규 패키지
  app.py                     ← FastAPI 앱 진입점
  routers/
    products.py              ← /api/products
    pipeline.py              ← /api/pipeline + SSE /stream/progress
    export.py                ← /api/export
    stores.py                ← /api/stores
    suppliers.py             ← /api/suppliers
    dashboard.py             ← /api/dashboard
  services/
    db_service.py            ← SQLite 쿼리 레이어
    oc_service.py            ← sync_existing, import_new_from_vendor 래퍼
    export_service.py        ← pipeline_run.run() 래퍼
    reconcile_service.py     ← 실제 등록 갱신 (API + 엑셀 import)
  static/
    index.html               ← 단일 HTML 앱 (JS 포함)
    style.css
    app.js
    pages/
      dashboard.js
      products.js
      pipeline.js
      export.js
      stores.js
      suppliers.js
```

---

## 기존 모듈 재사용 맵

| 기능 | 기존 모듈 | 호출 방식 |
|---|---|---|
| OC 동기화 | godomall_register/oc_import.py | Python import |
| 가격 계산 | DB_save/price_engine.py | Python import |
| 전략 설정 | DB_save/pricing_strategies.py | Python import |
| 고도몰 변환 | OC_ES_converter/scripts/convert_godomall.py | Python import |
| 출고 파이프라인 | godomall_register/pipeline_run.py | Python import |
| 기존 DB 출고 로직 | DB_save/database/db_handler.py | Python import |
| 텍스트/이미지 파이프라인 | stage*_*_gui.py | subprocess 호출 |

---

## 구현 범위 (1차)

1. FastAPI 서버 + 정적 파일 서빙
2. 대시보드 (DB 통계 API)
3. 상품 DB 페이지 (조회/검색/필터/페이지네이션)
4. 파이프라인 페이지 (OC 동기화 + SSE 진행률)
5. 마켓 출고 페이지 (dry-run + 실행 + 이력 기록)
6. 스토어 관리 (Excel import + 목록)

실제 등록 갱신(reconcile), 텍스트/이미지 파이프라인 실행, 공급사 신규 입고는 2차 구현.
