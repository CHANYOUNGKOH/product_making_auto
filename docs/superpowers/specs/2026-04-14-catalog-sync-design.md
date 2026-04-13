# OC 카탈로그 증분 동기화 + Hermes Agent 설계

## 배경 및 목적

oc_catalog.db에 9.4M개 OC 상품이 수집 완료됨 (CSV 전체 스캔, 22시간 소요).
이후 **정보 갱신**, **신규 상품 감지**, **신규 공급사 발견**을 효율적으로 처리해야 함.

### 현재 동기화 수단

| 기존 API | 용도 | 소요시간 |
|----------|------|---------|
| `sync-existing` | products.db 내 65k 상품 가격/상태 갱신 | 3~5분 |
| `scan-csv` | CSV 기반 전체 재구축 (9.4M) | ~22시간 |
| `scan/start` | allItems 전체 스캔 (레거시) | 수시간 |

### 부족한 것

1. **등록 공급사의 신규 상품** — oc_catalog.db에 없는 새 OC 상품 감지 불가
2. **미등록 공급사 발견** — CSV 재스캔 없이는 새 공급사를 알 수 없음
3. **자동 스케줄링** — 모든 동기화가 수동 트리거

---

## 결정 사항

| 항목 | 결정 |
|------|------|
| 스캔 대상 범위 | vendors 테이블에 등록된 공급사만 (status != 'inactive') |
| 신규 상품 처리 | oc_catalog.db + products.db 자동 backfill |
| 신규 공급사 발견 | 월간 경량 전체 스캔 (key+vendorKey만) |
| 스캔 방식 | allItems(vendor=code) 순차 스캔 (방식 1) |
| 스케줄링 | Hub UI 수동 + Hermes Agent (Schedule/Remote Triggers) |
| 스케줄러 | Claude Code Schedule (Remote Triggers) — OpenClaw 대신 |

---

## 아키텍처

### 3-tier 동기화 체계

```
일간 03:00 — sync-existing (기존 API)
  products.db 내 65k 상품 가격/상태 갱신
  소요: 3~5분

주간 일 04:00 — vendor-scan (신규)
  등록 공급사별 allItems(vendor=code) 스캔
  → oc_catalog.db upsert + products.db backfill
  소요: ~10분 (등록 공급사 ~50개 기준)

월간 1일 02:00 — discovery-scan (신규)
  allItems(status=available) 경량 스캔 (key + vendorKey만)
  → 신규 vendor_code 감지 → vendors 테이블에 status='discovered' 삽입
  소요: ~3-4시간
```

### vendor-scan 플로우

```
1. vendors 테이블에서 status != 'inactive' 공급사 목록 조회
2. 각 vendor_code마다:
   a. allItems(vendor=code, status=available) 커서 페이지네이션
      - CATALOG_FIELDS 사용 (key, status, price, shipping 등)
      - first=100, 커서로 전체 순회
   b. oc_catalog.db UPSERT (INSERT OR REPLACE)
      - vendor_code, price_krw, shipping_fee, status 등
      - last_scanned_at = now
   c. 신규 키 감지: oc_catalog.db에 없던 key = 신규 상품
   d. products.db backfill (신규 키만)
      - vendor_code, oc_price, oc_status COALESCE 패턴
3. vendors 테이블 갱신
   - product_count 업데이트
   - last_imported_at = now
4. 결과 반환:
   {
     scanned_vendors: int,
     total_items: int,
     new_items: int,
     updated_items: int,
     backfilled: int,
     errors: list,
     duration_sec: float
   }
```

### discovery-scan 플로우

```
1. allItems(status=available) 경량 스캔
   - fields: key, metadata { vendorKey } 만 요청
   - first=1000, 커서 페이지네이션
   - 체크포인트: oc_catalog_meta에 'discovery_scan_cursor' 저장
2. 수집한 vendor_code SET 구성
3. 기존 oc_catalog.db vendor_code SET과 비교
   - 신규 = 수집 - 기존
4. 신규 vendor_code → vendors 테이블 INSERT
   - status='discovered', source='oc_discovery'
   - product_count = 해당 vendor의 상품 수
5. oc_catalog_meta 갱신
   - last_discovery_scan_at, total_discovery_vendors
6. 결과 반환:
   {
     total_scanned: int,
     known_vendors: int,
     new_vendors: int,
     new_vendor_codes: list[str],  # 상위 20개만
     duration_sec: float
   }
```

---

## API 엔드포인트

### 신규

| Method | Path | 용도 |
|--------|------|------|
| POST | `/api/catalog/vendor-scan/start` | 등록 공급사별 증분 스캔 시작 |
| POST | `/api/catalog/discovery-scan/start` | 경량 전체 스캔 (신규 공급사 발견) 시작 |

### 기존 (변경 없음)

| Method | Path | 용도 |
|--------|------|------|
| POST | `/api/catalog/sync-existing/start` | products.db 가격/상태 갱신 |
| GET | `/api/catalog/scan/status` | 현재 실행 중인 스캔 상태 조회 |

모든 스캔은 기존 `_scan_status` 싱글턴을 공유:
- `running: bool`, `job_type: str`, `message: str`, `current/total: int`
- 동시에 2개 스캔 불가 (이미 running이면 409 반환)

---

## 파일 구조

| 파일 | 변경 | 역할 |
|------|------|------|
| `hub/services/catalog_service.py` | MOD | vendor_scan(), discovery_scan(), start_*_bg() 추가 |
| `hub/routers/catalog.py` | MOD | 2개 엔드포인트 추가 |
| `tests/hub/test_catalog_sync.py` | NEW | vendor_scan, discovery_scan 테스트 |

---

## Hermes Agent (Schedule/Remote Triggers) 설계

### 스케줄 정의

```
daily-sync:
  cron: "0 3 * * *"          # 매일 03:00
  action: POST /api/catalog/sync-existing/start
  wait_for_completion: true    # GET /api/catalog/scan/status 폴링
  report: telegram             # 완료 후 텔레그램 리포트

weekly-vendor-scan:
  cron: "0 4 * * 0"          # 매주 일요일 04:00
  action: POST /api/catalog/vendor-scan/start
  wait_for_completion: true
  report: telegram

monthly-discovery:
  cron: "0 2 1 * *"          # 매월 1일 02:00
  action: POST /api/catalog/discovery-scan/start
  wait_for_completion: true
  report: telegram
```

### Agent 프롬프트 (Schedule에 등록)

각 Schedule trigger는 Claude Code agent로 실행됨. Agent는:

1. Hub 서버에 스캔 API 호출
2. `/api/catalog/scan/status` 폴링으로 완료 대기
3. 결과 파싱 → 텔레그램으로 요약 리포트 전송
4. 오류 발생 시 텔레그램으로 경고

예시 agent prompt:
```
Hub 서버(localhost:8080)에 POST /api/catalog/vendor-scan/start 호출.
GET /api/catalog/scan/status를 30초 간격으로 폴링, running=false 될 때까지 대기.
완료 후 result 객체에서 scanned_vendors, new_items, errors 추출.
텔레그램 chat_id={CHAT_ID}로 결과 리포트 전송.
오류 시 "⚠ vendor-scan 실패: {error}" 전송.
```

### 셋업 순서

1. 백엔드 API 구현 (vendor-scan, discovery-scan)
2. 테스트 통과 확인
3. `/schedule` 스킬로 3개 trigger 등록
4. 텔레그램 리포트 연동 확인

---

## 대시보드 UI 연동

기존 대시보드의 `GET /api/catalog/scan/status`로 동기화 상태 표시:
- vendor-scan / discovery-scan도 같은 status 구조 사용
- `job_type` 필드로 구분: `"vendor_scan"`, `"discovery_scan"`, `"sync_existing"`, `"csv_scan"`

추가 UI 변경 없음 — 기존 SSE 스트림 + status 폴링으로 충분.

---

## 구현 후 확인 체크리스트

- [ ] `POST /api/catalog/vendor-scan/start` → 등록 공급사 스캔 시작
- [ ] `POST /api/catalog/discovery-scan/start` → 경량 전체 스캔 시작
- [ ] vendor-scan: oc_catalog.db upsert + products.db backfill 동작
- [ ] discovery-scan: 신규 vendor_code → vendors 테이블 status='discovered'
- [ ] 기존 scan/status API로 진행 상황 조회 가능
- [ ] 동시 실행 방지 (running=true일 때 409)
- [ ] Schedule trigger 3개 등록 완료
- [ ] 텔레그램 리포트 수신 확인
