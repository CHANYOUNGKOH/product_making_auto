# 🎨 ComfyUI 배포 가이드

## 📋 목차
1. [ComfyUI 배포 시 필요한 것들](#1-comfyui-배포-시-필요한-것들)
2. [워크플로우 JSON 파일 포함 확인](#2-워크플로우-json-파일-포함-확인)
3. [다른 PC에서 ComfyUI 설정](#3-다른-pc에서-comfyui-설정)
4. [문제 해결](#4-문제-해결)

---

## 1. ComfyUI 배포 시 필요한 것들

### ✅ 자동으로 포함되는 것들

`build_exe.py`가 자동으로 다음 파일들을 포함합니다:

1. **워크플로우 JSON 파일들**
   - `배경생성_251209ver.json` (배경 생성용)
   - `배경합성_251209ver.json` (이미지 합성용)
   - 기타 워크플로우 JSON 파일들

2. **설정 파일들**
   - `bg_generation_config.json`
   - `bg_mixing_config.json`

3. **Python 스크립트들**
   - `Bg_Generation.py`
   - `IMG_mixing.py`

### ⚠️ 별도로 필요한 것들

**ComfyUI 자체는 별도 설치가 필요합니다!**

1. **ComfyUI 서버**
   - ComfyUI를 별도로 설치해야 함
   - 다른 PC에도 ComfyUI가 설치되어 있어야 함

2. **ComfyUI 모델 및 노드**
   - 사용하는 모델 파일들 (예: SDXL, ControlNet 등)
   - 커스텀 노드들 (있는 경우)

---

## 2. 워크플로우 JSON 파일 포함 확인

### 빌드 후 확인

빌드 완료 후 다음 경로에 JSON 파일들이 포함되어 있는지 확인:

```
dist/상품가공프로그램/
└── IMG_stage4/
    ├── 배경생성_251209ver.json  ← 필수
    ├── 배경합성_251209ver.json  ← 필수
    ├── bg_generation_config.json
    ├── bg_mixing_config.json
    └── ... (기타 JSON 파일들)
```

### 확인 방법

1. 빌드 완료 후 `dist/상품가공프로그램/IMG_stage4/` 폴더 확인
2. JSON 파일들이 모두 포함되어 있는지 확인
3. 누락된 파일이 있으면 수동으로 복사

---

## 3. 다른 PC에서 ComfyUI 설정

### 3.1 ComfyUI 설치 (필수)

**다른 PC에서도 ComfyUI를 설치해야 합니다!**

1. **ComfyUI 다운로드 및 설치**
   - GitHub에서 ComfyUI 다운로드
   - 설치 및 실행 확인

2. **필요한 모델 설치**
   - 워크플로우에서 사용하는 모델 파일들
   - 예: SDXL, ControlNet, IPAdapter 등

3. **커스텀 노드 설치** (있는 경우)
   - 워크플로우에서 사용하는 커스텀 노드들

### 3.2 ComfyUI 서버 실행

**방법 1: 수동 실행**
```bash
# ComfyUI 폴더에서
python main.py --port 8188
```

**방법 2: bat 파일 사용** (권장)
- ComfyUI 실행용 `.bat` 파일 생성
- 프로그램에서 "ComfyUI bat 파일" 경로 설정

### 3.3 프로그램 설정

1. **ComfyUI 서버 주소 설정**
   - 기본값: `127.0.0.1:8188`
   - 다른 PC에서도 동일하게 설정

2. **워크플로우 JSON 파일 경로**
   - 프로그램이 자동으로 찾음
   - `IMG_stage4` 폴더에 JSON 파일이 있으면 자동 감지

3. **ComfyUI bat 파일 경로** (선택사항)
   - 자동 서버 시작 기능 사용 시 필요
   - ComfyUI 실행용 `.bat` 파일 경로 설정

---

## 4. 문제 해결

### 문제 1: 워크플로우 JSON 파일을 찾을 수 없음

**증상**: 프로그램 실행 시 워크플로우 파일을 찾지 못함

**해결 방법**:
1. `dist/상품가공프로그램/IMG_stage4/` 폴더에 JSON 파일 확인
2. 누락된 파일이 있으면 원본에서 복사
3. 재빌드

### 문제 2: ComfyUI 서버에 연결할 수 없음

**증상**: "ComfyUI 서버에 연결할 수 없습니다" 오류

**해결 방법**:
1. ComfyUI 서버가 실행 중인지 확인
   ```bash
   # 브라우저에서 확인
   http://127.0.0.1:8188
   ```

2. 포트 번호 확인
   - 기본값: `8188`
   - 다른 포트 사용 시 프로그램에서도 변경

3. 방화벽 확인
   - Windows 방화벽에서 포트 허용 확인

### 문제 3: 워크플로우 실행 시 모델을 찾을 수 없음

**증상**: ComfyUI에서 "모델을 찾을 수 없습니다" 오류

**원인**: ComfyUI에 필요한 모델이 설치되지 않음

**해결 방법**:
1. 워크플로우 JSON 파일을 열어서 사용하는 모델 확인
2. 해당 모델을 ComfyUI의 `models/` 폴더에 설치
3. 필요한 커스텀 노드도 설치

### 문제 4: 다른 PC에서 워크플로우가 다르게 작동함

**원인**: 
- 모델 버전이 다름
- 커스텀 노드가 없음
- ComfyUI 버전이 다름

**해결 방법**:
1. 동일한 ComfyUI 버전 사용
2. 동일한 모델 파일 사용
3. 동일한 커스텀 노드 설치

---

## 📝 배포 체크리스트

### 빌드 전
- [ ] 워크플로우 JSON 파일들이 `IMG_stage4` 폴더에 있는지 확인
- [ ] 설정 파일들(`bg_generation_config.json`, `bg_mixing_config.json`) 확인

### 빌드 후
- [ ] `dist/상품가공프로그램/IMG_stage4/` 폴더에 JSON 파일들 확인
- [ ] 모든 워크플로우 JSON 파일이 포함되어 있는지 확인

### 다른 PC 배포 시
- [ ] `dist/상품가공프로그램/` 폴더 전체 복사
- [ ] ComfyUI 설치 안내 문서 포함
- [ ] 필요한 모델 목록 문서 포함

### 사용자 안내
- [ ] ComfyUI 설치 방법 안내
- [ ] 필요한 모델 설치 안내
- [ ] 서버 실행 방법 안내

---

## 💡 권장 배포 구성

```
배포패키지/
├── 상품가공프로그램/
│   ├── 상품가공프로그램.exe
│   ├── IMG_stage4/
│   │   ├── 배경생성_251209ver.json
│   │   ├── 배경합성_251209ver.json
│   │   └── ... (기타 파일들)
│   └── ... (기타 폴더들)
├── ComfyUI_설치가이드.txt
└── README.txt
```

**ComfyUI_설치가이드.txt 내용 예시:**
```
ComfyUI 설치 및 설정 가이드

1. ComfyUI 설치
   - GitHub에서 다운로드: https://github.com/comfyanonymous/ComfyUI
   - 설치 및 실행 확인

2. 필요한 모델 설치
   - SDXL 모델
   - ControlNet 모델
   - IPAdapter 모델
   (워크플로우에 따라 다를 수 있음)

3. ComfyUI 서버 실행
   - 포트: 8188 (기본값)
   - 프로그램에서 자동 시작 옵션 사용 가능

4. 프로그램 설정
   - ComfyUI 서버 주소: 127.0.0.1:8188
   - 워크플로우 JSON 파일은 자동으로 감지됨
```

---

## 🎯 요약

### ✅ 자동 포함되는 것 (빌드 시 자동 포함)
- ✅ 워크플로우 JSON 파일들 (`배경생성_251209ver.json`, `배경합성_251209ver.json` 등)
- ✅ 설정 파일들 (`bg_generation_config.json`, `bg_mixing_config.json`)
- ✅ Python 스크립트들 (`Bg_Generation.py`, `IMG_mixing.py`)
- ✅ `IMG_stage4` 폴더 전체

**확인 방법**: `build_exe.py`가 `main_launcher_v7.py`의 SCRIPTS를 읽어 `IMG_stage4` 폴더를 자동 감지하고 포함합니다.

### ⚠️ 별도 설치 필요 (다른 PC에서)
- ⚠️ **ComfyUI 서버** (필수) - GitHub에서 다운로드 및 설치
- ⚠️ **모델 파일들** (필수) - SDXL, ControlNet, IPAdapter 등
- ⚠️ **커스텀 노드들** (있는 경우) - 워크플로우에서 사용하는 노드들

### 📦 배포 시 포함할 것
- `dist/상품가공프로그램/` 폴더 전체
- ComfyUI 설치 가이드 문서 (이 문서 또는 별도 안내)

### ✅ 결론
**워크플로우 JSON 파일과 설정 파일은 자동으로 포함됩니다!** 

하지만 **ComfyUI 서버 자체는 다른 PC에서도 별도로 설치해야 합니다.** 
ComfyUI는 별도의 애플리케이션이며, 이 프로그램은 ComfyUI 서버에 API로 연결하여 사용하는 구조입니다.

**비유**: 
- 이 프로그램 = 클라이언트 (자동 포함됨 ✅)
- ComfyUI = 서버 (별도 설치 필요 ⚠️)

🎨

