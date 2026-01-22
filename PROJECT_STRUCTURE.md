# 상품가공프로그램 프로젝트 구조

> v9 통합런처 기준 핵심 파일 구조 문서

## 빠른 시작

```bash
# 1. 저장소 클론
git clone <repository-url>
cd 상품가공프로그램

# 2. API 키 설정 (각 stage 폴더에 생성)
# - .openai_api_key_stage1_batch
# - .openai_api_key_stage2_batch
# - .openai_api_key_stage3_batch
# - .openai_api_key_stage4_batch
# - .openai_api_key_bg_prompt (IMG_stage3)
# - .openai_api_key_img_analysis (IMG_stage3)

# 3. 실행
python main_launcher_v9.py
```

---

## 핵심 파일 트리

```
상품가공프로그램/
│
├── main_launcher_v9.py          # 메인 통합 런처 (프로덕션)
├── main_launcher_v10_gemini.py  # Gemini 테스트용 런처
├── main_launcher_v8_Casche.py   # 참고용 (이전 버전)
├── job_history.json             # 작업 이력
├── merge_excel_versions.py      # 엑셀 병합 도구
│
├── stage1_product_name/         # [Stage 1] 상품명 정제
├── stage2_product_name/         # [Stage 2] 상세설명 & 이미지 분석
├── stage3_product_name/         # [Stage 3] 최종 상품명 생성
├── stage4_product_name/         # [Stage 4] 검수 및 필터링
│
├── Remove_imgBG/                # [I1-I2] 배경 제거 & 라벨링
├── IMG_stage3/                  # [I3] 이미지 분석 & 프롬프트
├── IMG_stage4/                  # [I4] 배경 생성 & 합성
├── IMG_stage5/                  # [I5] 품질 검증 & 업로드
│
├── DB_save/                     # 데이터베이스 관리
└── Upload_Mapper/               # 마켓 업로드 매핑
```

---

## Stage별 상세 구조

### Stage 1: 상품명 정제 (`stage1_product_name/`)

| 파일 | 설명 |
|------|------|
| `stage1_mapping_tool.py` | 매핑 도구 (필수 시작점) |
| `Gui_stage1_batch_Casche.py` | 배치 처리 GUI |
| `batch_stage1_core_Casche.py` | 배치 처리 코어 로직 |
| `prompts_stage1.py` | LLM 프롬프트 정의 |
| `*_gemini.py` | Gemini 버전 (v10 테스트용) |

### Stage 2: 상세설명 & 이미지 분석 (`stage2_product_name/`)

| 파일 | 설명 |
|------|------|
| `Product_detaildescription.py` | HTML에서 이미지 URL 추출 |
| `stage2_LLM_gui.py` | LLM 분석 GUI |
| `stage2_batch_api_Cachever_resize.py` | 배치 API (리사이즈 포함) |
| `stage2_core_Cache.py` | 코어 로직 |
| `stage2_prompt_builder.py` | 프롬프트 빌더 |
| `stage2_pipeline_GUI.py` | 파이프라인 GUI |
| `.blocked_image_urls.json` | 차단된 이미지 URL 목록 |
| `.blocked_keywords.json` | 차단 키워드 목록 |

### Stage 3: 최종 상품명 생성 (`stage3_product_name/`)

| 파일 | 설명 |
|------|------|
| `stage3_LLM_gui.py` | LLM GUI |
| `stage3_batch_api_Casche.py` | 배치 API |
| `stage3_core_Casche.py` | 코어 로직 |
| `*_gemini.py` | Gemini 버전 (v10 테스트용) |

### Stage 4: 검수 및 필터링 (`stage4_product_name/`)

| 파일 | 설명 |
|------|------|
| `stage4_1_filter_gui.py` | 필터링 GUI |
| `stage4_2_gui.py` | 검수 GUI |
| `stage4_2_batch_api_Casche.py` | 배치 API |
| `stage4_2_core_Casche.py` | 코어 로직 |

---

## 이미지 처리 파이프라인

### I1-I2: 배경 제거 (`Remove_imgBG/`)

| 파일 | 설명 |
|------|------|
| `Remove_BG_gui_from_excel_I1.py` | 배경 제거 GUI |
| `bg_label_gui_I2.py` | 이미지 라벨링 GUI |

### I3: 이미지 분석 & 프롬프트 (`IMG_stage3/`)

| 파일 | 설명 |
|------|------|
| `IMG_Batch_analysis_gui_Casche.py` | 이미지 분석 배치 GUI |
| `IMG_analysis_core_Casche.py` | 이미지 분석 코어 |
| `bg_Batch_prompt_gui_Casche.py` | 배경 프롬프트 생성 GUI |
| `bg_prompt_core_Casche.py` | 배경 프롬프트 코어 |
| `*_gemini.py` | Gemini 버전 (토큰 효율 테스트용) |

### I4: 배경 생성 & 합성 (`IMG_stage4/`)

| 파일 | 설명 |
|------|------|
| `Bg_Generation_V2.py` | 배경 생성 (Flux API) |
| `IMG_mixing.py` | 이미지 합성 |
| `convert_workflow_to_api.py` | ComfyUI 워크플로우 변환 |
| `bg_mixing_config.json` | 합성 설정 |

### I5: 품질 검증 & 업로드 (`IMG_stage5/`)

| 파일 | 설명 |
|------|------|
| `Stage5_Review.py` | 품질 검증 GUI |
| `cloudflare_upload_gui.py` | Cloudflare 이미지 업로드 |

---

## 데이터 관리

### DB_save/

| 파일/폴더 | 설명 |
|------|------|
| `data_entry.py` | 데이터 입력 |
| `data_export.py` | 데이터 출력 |
| `season_filter_manager_gui.py` | 시즌 필터 관리 GUI |
| `database/db_handler.py` | SQLite DB 핸들러 |
| `ui/main_window.py` | 메인 윈도우 UI |
| `config_settings.json` | 설정 파일 |
| `season_filters.json` | 시즌 필터 설정 |

### Upload_Mapper/

| 파일/폴더 | 설명 |
|------|------|
| `main.py` | 메인 진입점 |
| `config_manager.py` | 설정 관리 |
| `solutions/` | 마켓별 솔루션 (다팔자, 이셀러스 등) |
| `rules/` | 매핑 규칙 (옵션 가격 보정 등) |

---

## 필수 설정 파일

### API 키 파일 (각 폴더에 생성 필요)

```
stage1_product_name/.openai_api_key_stage1_batch
stage2_product_name/.openai_api_key_stage2_batch
stage3_product_name/.openai_api_key_stage3_batch
stage4_product_name/.openai_api_key_stage4_2
IMG_stage3/.openai_api_key_bg_prompt
IMG_stage3/.openai_api_key_img_analysis
```

> API 키 파일은 .gitignore에 포함되어 있어 커밋되지 않습니다.
> 새 PC에서 설정 시 각 파일을 수동으로 생성해야 합니다.

### 설정 파일 템플릿

- `DB_save/config_settings.json` - DB 설정
- `DB_save/season_filters.json` - 시즌 필터 설정
- `IMG_stage4/bg_mixing_config.json` - 이미지 합성 설정
- `Upload_Mapper/upload_mapper_config.json` - 업로드 매퍼 설정

---

## 파일 버전 규칙

| 접미사 | 설명 | 관리 |
|--------|------|------|
| `*_Casche.py` | 현재 프로덕션 버전 (캐시 최적화) | Git 추적 |
| `*_gemini.py` | Gemini API 테스트 버전 | Git 추적 |
| `*_기존*.py` | 이전 버전 백업 | Git 추적 |
| `*_원본*.py` | 원본 백업 | Git 추적 |

---

## 작업 흐름

```
[소싱 데이터]
     │
     ▼
┌─────────────────────────────────────────┐
│  Stage 1: 상품명 정제                    │
│  - stage1_mapping_tool.py (매핑)        │
│  - batch_stage1_core_Casche.py (LLM)    │
└─────────────────────────────────────────┘
     │
     ▼
┌─────────────────────────────────────────┐
│  Stage 2: 상세설명 분석                  │
│  - Product_detaildescription.py         │
│  - stage2_batch_api_Cachever_resize.py  │
└─────────────────────────────────────────┘
     │
     ▼
┌─────────────────────────────────────────┐
│  Stage 3: 최종 상품명 생성               │
│  - stage3_batch_api_Casche.py           │
└─────────────────────────────────────────┘
     │
     ▼
┌─────────────────────────────────────────┐
│  Stage 4: 검수 및 필터링                 │
│  - stage4_1_filter_gui.py               │
│  - stage4_2_batch_api_Casche.py         │
└─────────────────────────────────────────┘
     │
     ▼
[DB_save: 데이터 저장]
     │
     ├──────────────────────────┐
     ▼                          ▼
[Upload_Mapper]          [이미지 처리 I1-I5]
  마켓 업로드               배경 제거/합성
```

---

## 새 PC 설정 가이드

1. **Python 환경**: Python 3.10+ 권장
2. **필수 패키지**: `pip install -r requirements.txt` (각 폴더)
3. **API 키 설정**: 위 "API 키 파일" 섹션 참조
4. **DB 초기화**: `DB_save/products.db`는 자동 생성됨
5. **실행**: `python main_launcher_v9.py`

---

## 문의 및 참고

- 메인 런처: `main_launcher_v9.py`
- Gemini 테스트: `main_launcher_v10_gemini.py`
- 이전 버전 참고: `main_launcher_v8_Casche.py`
