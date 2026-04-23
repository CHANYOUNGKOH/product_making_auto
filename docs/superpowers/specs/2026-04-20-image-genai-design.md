# Image GenAI Pipeline — Design Spec

작성일: 2026-04-20
브랜치: `feat/image-genai`
대상 워크트리: `.claude/worktrees/image-genai/`

---

## 1. 목표

ST2_JSON(상품 분석)과 누끼 이미지를 입력으로 **1:1 비율 썸네일 이미지**를 생성하여 `products.image_slots`에 저장하고, Upload_Mapper/godomall_register 의 이미지 슬롯 채우기 단계에서 우선순위 규칙으로 재주입(reinject)한다.

생성형 모델은 **ChatGPT Pro 구독요금**(코덱스 경로)을 활용한다.

---

## 2. 범위

### IN
- 누끼 보유 상품 자동 감지 + DB 태깅
- ST2_JSON + 누끼를 입력으로 1:1 썸네일 N장 생성
- `products.image_slots` JSON 컬럼에 결과 저장 (생성/누끼/원본 분리)
- Upload_Mapper/godomall_register 슬롯 채우기 단계 우선순위 규칙 적용
- 캐시 (상품코드+ST2_JSON 해시 → 결과 재사용)
- 실패/스킵/재시도 상태 추적

### OUT (이번 사이클 제외)
- 상세페이지 본문 이미지 생성 (썸네일만)
- 마켓별 차별화 (전 스토어 동일 썸네일 사용)
- 모델 학습/파인튜닝
- 누끼가 없는 상품의 새 누끼 자동 생성 (Remove_imgBG는 별 트랙)

---

## 3. DB 스키마 변경

### `products` 테이블 추가 컬럼 (멱등 ALTER)

| 컬럼 | 타입 | 의미 |
|---|---|---|
| `has_nukki` | INTEGER (0/1) | 누끼 보유 여부 (Phase 1 감지기 결과) |
| `image_slots` | TEXT (JSON) | 슬롯 메타 (아래 스키마) |
| `genai_status` | TEXT | `pending` / `running` / `done` / `skipped` / `failed` |
| `genai_error` | TEXT | 마지막 실패 사유 |
| `genai_updated_at` | TEXT | 마지막 갱신 시각 |

기존 `누끼url`, `연출url` 컬럼은 그대로 유지 (호환성). `image_slots`가 새 정규 위치.

### `image_slots` JSON 스키마

```json
{
  "nukki": {
    "url": "https://.../abc_nukki.png",
    "local_path": "C:/.../Remove_imgBG/output/abc.png",
    "detected_at": "2026-04-20T10:00:00",
    "alpha_ratio": 0.34
  },
  "generated_1x1": [
    {
      "url": "https://r2.../abc_gen_001.jpg",
      "prompt_hash": "sha256:...",
      "model": "gpt-image-1",
      "scene": "lifestyle",
      "generated_at": "2026-04-20T10:05:00"
    }
  ],
  "original": [
    {"url": "...", "source": "ownerclan", "slot_hint": "main"}
  ],
  "rolled_index": 0
}
```

### 마이그레이션 위치
`hub/services/db_service.py::run_migrations()` 의 `_HUB_COLUMNS` 에 5개 컬럼 추가.

---

## 4. 누끼 감지 (Phase 1)

### 모듈
`IMG_pipeline/nukki_detector.py` (신설 디렉토리)

### 입력
- `products` 테이블 전체
- `Remove_imgBG/output/` 디렉토리 (기존 누끼 산출물)
- 기존 `누끼url` 컬럼 (이미 마킹된 경우)

### 판정 규칙 (우선순위 순)
1. `누끼url` 컬럼이 비어있지 않으면 `has_nukki=1`
2. `Remove_imgBG/output/{상품코드}.png` 파일 존재 시 `has_nukki=1` + 알파채널 검사
3. 알파 픽셀 비율 < 5% (배경이 안 빠진 케이스) → `has_nukki=0`

### 출력
- `products.has_nukki` 업데이트
- `products.image_slots.nukki` 채움 (URL/로컬경로/알파비율)

### 실행
- CLI: `python -m IMG_pipeline.nukki_detector --batch 1000`
- Hub UI 트리거: `POST /api/images/detect-nukki` (옵션, 후속)

---

## 5. 1:1 썸네일 생성 (Phase 2)

### 모듈
`IMG_pipeline/genai_thumbnail.py`

### 입력
- 상품 1건: `상품코드`, `ST2_JSON`, `image_slots.nukki` (있으면)

### 프롬프트 빌드 (ST2_JSON에서 추출)
```
{product_kind} 1:1 썸네일, {usage} 사용 장면, {scene_type} 배경,
{material} 재질감, {color} 톤, 깔끔하고 SEO 친화적, 텍스트 없음.
```
ST2_JSON 키 매핑 (실제 키는 `stage2_prompt_builder.py` 참조 후 확정):
- `product_kind` → 상품 종류
- `usage` → 핵심 사용성
- `scene_type` → 사용/연출 장면
- `material`, `color` → 보조 속성

### 호출 경로 (★ 결정 필요)

**Option A: ChatGPT Pro 구독 + 코덱스 CLI**
- `codex` CLI에 이미지 생성 프롬프트 전달
- 산출 이미지 다운로드 후 R2/Cloudflare 업로드
- ⚠ 코덱스 CLI가 이미지 생성 출력을 반환하는지 미검증

**Option B: CLIProxyAPI 경유 (memory: reference_codex_subscription_via_cliproxy)**
- ChatGPT Pro 토큰을 OpenAI API 형태로 프록시
- `gpt-image-1` 엔드포인트 호출
- ⚠ ChatGPT Pro 구독 토큰으로 image API 호출 가능 여부 미검증

**Option C: 무료 티어 폴백 (오너클랜 원본 활용)**
- 누끼만 1:1 캔버스에 올리고 단순 배경 합성 (PIL/IMG_stage4 ComfyUI 재활용)
- API 비용 0, 품질 낮음

→ **Phase 2 시작 전 사용자 결정 + 1건 PoC 필수**.

### 결과 저장
- 로컬: `IMG_pipeline/output/{상품코드}/gen_{idx}.jpg`
- 원격: Cloudflare R2 (기존 `IMG_stage5/cloudflare_upload_gui.py` 흐름 재사용)
- DB: `image_slots.generated_1x1[]` 에 URL 추가

### 캐시 키
```
sha256(상품코드 + ST2_JSON + 누끼_local_path_mtime)
```
→ 동일 키면 재생성 스킵.

### 동시성
- codex 인스턴스 N개 병렬 (사용자 지정, 기본 3)
- DB 쓰기는 별도 큐 → 단일 워커가 commit (SQLite 락 회피)

---

## 6. Reinject 우선순위 (Phase 3)

### 적용 대상
- `Upload_Mapper/` (오픈마켓 업로드 매퍼)
- `godomall_register/` (고도몰 등록 API)

### 슬롯 채우기 규칙 (2+1 구조 유지)
| 슬롯 | 1순위 | 2순위 | 3순위 |
|---|---|---|---|
| 메인(slot1) | `generated_1x1[rolled_index]` | `nukki` | `original.main` |
| 보조(slot2) | `nukki` | `original.main` | `original[1]` |
| 보조(slot3) | `original[0]` | `original[1]` | (빈 슬롯) |

규칙:
- 원본 1번은 메인에 절대 배치 안 함 (memory: project_godomall_image_slot_policy)
- `generated_1x1` 가 있으면 메인 자동 승격
- `rolled_index` 는 출고 카운터 기반 순환 (memory: project_godomall_export_name_image_sequence 참조)

### 코드 위치
- `Upload_Mapper/image_slot_resolver.py` (신설 또는 기존 파일에 함수 추가)
- 호출 지점: 엑셀/API 업로드 직전 한 번

---

## 7. 실행/운영 흐름

```
[사용자] Hub UI > 이미지 파이프라인 패널 > "전체 실행"
  ↓
[1] nukki_detector.run_all()       — 전체 has_nukki 갱신 (수 분)
  ↓
[2] genai_thumbnail.queue(상품들)   — has_nukki=1 + ST2_JSON 있음 + genai_status=pending
  ↓
[3] codex 워커 N개 병렬 처리       — 결과 R2 업로드 + image_slots 갱신
  ↓
[4] Upload_Mapper 다음 출고 시      — image_slots 기반 슬롯 채움
```

---

## 8. 테스트 (TDD 우선)

- `tests/img_pipeline/test_nukki_detector.py`
  - 알파채널 5% 미만 → has_nukki=0
  - 누끼url 컬럼 있으면 무조건 has_nukki=1
- `tests/img_pipeline/test_genai_thumbnail.py`
  - 캐시 키 동일하면 API 호출 안 됨 (mock)
  - 프롬프트 빌더가 ST2_JSON 누락 키에 안전
- `tests/img_pipeline/test_image_slot_resolver.py`
  - generated 있으면 메인=generated
  - generated 없고 nukki 있으면 메인=nukki, 보조=original
  - 원본 1번은 메인 절대 안 옴

---

## 9. 마일스톤 / 작업 분할 (codex 3 인스턴스)

| Phase | 담당 | 산출물 |
|---|---|---|
| 0 | Opus | 본 spec, 사용자 승인 |
| 1a | Codex C1 | DB 마이그레이션 + image_slots 헬퍼 |
| 1b | Codex C2 | nukki_detector + 테스트 |
| 1c | Codex C3 | 호출 경로 PoC (Option A/B/C 1건씩) |
| 2 | Opus 결정 후 Codex | genai_thumbnail 본구현 + 캐시 |
| 3 | Codex C1 | image_slot_resolver + Upload_Mapper/godomall_register 연결 |
| 4 | Opus | Hub UI 패널 + 통합 검증 |

---

## 10. 리스크 / 미해결

1. **★ ChatGPT Pro 구독으로 image API 호출 가능 여부 미검증** — Phase 1c PoC로 확정
2. R2 업로드 토큰/버킷 설정 — `IMG_stage5/cloudflare_upload_gui.py` 에서 추출 필요
3. ST2_JSON 실제 키 명세 — `stage2_prompt_builder.py` 확인 후 prompt builder 확정
4. 동시성 시 SQLite 락 — WAL 모드 + 단일 writer 큐 패턴 적용
5. 출고 시점 `rolled_index` 갱신 주체 — 기존 출고 카운터 코드와 충돌 점검 필요

---

## 11. 사용자 결정 요청

- [ ] 호출 경로 Option A/B/C 중 PoC 우선순위
- [ ] codex 워커 수 (기본 3)
- [ ] 첫 배치 대상: 전체 vs 특정 벤더/카테고리
- [ ] R2 업로드 vs 로컬만 (초기)
