# 📦 ComfyUI 모델 복사 가이드

## 🎯 목차
1. [필수 모델 확인](#1-필수-모델-확인)
2. [모델 복사 방법](#2-모델-복사-방법)
3. [권장 방법: 필요한 모델만 복사](#3-권장-방법-필요한-모델만-복사)
4. [전체 복사 방법](#4-전체-복사-방법)

---

## 1. 필수 모델 확인

### 현재 워크플로우에서 사용하는 모델

#### 배경 생성 워크플로우 (`배경생성_251209ver.json`)
- ✅ **Checkpoint**: `juggernautXL_ragnarokBy.safetensors`
- ✅ **IPAdapter**: 사용 (모델 파일 확인 필요)
- ✅ **VAE**: 체크포인트에 포함되어 있을 수 있음

#### 배경 합성 워크플로우 (`배경합성_251209ver.json`)
- ✅ **Checkpoint**: `majicmixRealistic_v7.safetensors`
- ✅ **IPAdapter**: 사용 가능성 있음
- ✅ **VAE**: 체크포인트에 포함되어 있을 수 있음

### 모델 파일 위치

ComfyUI의 모델 파일들은 다음 폴더 구조로 되어 있습니다:

```
ComfyUI/
├── models/
│   ├── checkpoints/          ← 메인 모델 (SDXL 등)
│   │   ├── juggernautXL_ragnarokBy.safetensors
│   │   └── majicmixRealistic_v7.safetensors
│   ├── ipadapter/            ← IPAdapter 모델
│   ├── controlnet/           ← ControlNet 모델 (사용 시)
│   ├── vae/                  ← VAE 모델 (별도 설치 시)
│   ├── loras/                ← LoRA 모델 (사용 시)
│   └── clip/                 ← CLIP 모델 (보통 자동 다운로드)
```

---

## 2. 모델 복사 방법

### 방법 1: 필요한 모델만 복사 (권장) ⭐

**장점:**
- 용량 절약 (수 GB ~ 수십 GB 절약 가능)
- 필요한 모델만 관리
- 빠른 복사

**단점:**
- 워크플로우 변경 시 추가 모델 필요할 수 있음

#### 단계별 가이드

1. **원본 PC에서 모델 확인**
   ```
   C:\ComfyUI_windows_portable_nvidia\ComfyUI_windows_portable\ComfyUI\models\
   ```

2. **필수 모델 복사**
   - `checkpoints/juggernautXL_ragnarokBy.safetensors`
   - `checkpoints/majicmixRealistic_v7.safetensors`
   - `ipadapter/` 폴더 전체 (IPAdapter 사용 시)
   - 기타 워크플로우에서 사용하는 모델들

3. **대상 PC에 복사**
   ```
   대상PC:\ComfyUI\models\
   ```

### 방법 2: 전체 models 폴더 복사

**장점:**
- 모든 모델 포함 (워크플로우 변경에도 대응)
- 빠른 설정

**단점:**
- 용량이 매우 큼 (수십 GB ~ 수백 GB)
- 복사 시간 오래 걸림

#### 단계별 가이드

1. **원본 PC에서 models 폴더 전체 복사**
   ```
   C:\ComfyUI_windows_portable_nvidia\ComfyUI_windows_portable\ComfyUI\models\
   ```

2. **대상 PC에 붙여넣기**
   ```
   대상PC:\ComfyUI\models\
   ```

---

## 3. 권장 방법: 필요한 모델만 복사

### 📋 필수 모델 체크리스트

#### 1단계: Checkpoint 모델 (필수)

```
models/checkpoints/
├── juggernautXL_ragnarokBy.safetensors  ← 배경 생성용
└── majicmixRealistic_v7.safetensors      ← 배경 합성용
```

**용량**: 각각 약 6-7GB (총 12-14GB)

#### 2단계: IPAdapter 모델 (필수)

워크플로우에서 IPAdapter를 사용하는 경우:

```
models/ipadapter/
├── (IPAdapter 모델 파일들)
└── (확인 필요: 워크플로우에서 사용하는 IPAdapter 모델)
```

**확인 방법:**
1. 워크플로우 JSON 파일에서 `IPAdapter` 노드 찾기
2. 사용하는 모델 파일명 확인
3. 해당 파일만 복사

#### 3단계: 기타 모델 (선택)

- **VAE**: 체크포인트에 포함되어 있으면 별도 불필요
- **ControlNet**: 워크플로우에서 사용하는 경우만
- **LoRA**: 워크플로우에서 사용하는 경우만

### 복사 명령어 예시 (PowerShell)

```powershell
# 원본 경로
$source = "C:\ComfyUI_windows_portable_nvidia\ComfyUI_windows_portable\ComfyUI\models"

# 대상 경로 (USB 드라이브 예시)
$target = "E:\ComfyUI_models_backup"

# Checkpoint 모델만 복사
Copy-Item "$source\checkpoints\juggernautXL_ragnarokBy.safetensors" -Destination "$target\checkpoints\" -Force
Copy-Item "$source\checkpoints\majicmixRealistic_v7.safetensors" -Destination "$target\checkpoints\" -Force

# IPAdapter 폴더 전체 복사 (있는 경우)
if (Test-Path "$source\ipadapter") {
    Copy-Item "$source\ipadapter\*" -Destination "$target\ipadapter\" -Recurse -Force
}
```

---

## 4. 전체 복사 방법

### Windows 탐색기 사용

1. **원본 폴더 열기**
   ```
   C:\ComfyUI_windows_portable_nvidia\ComfyUI_windows_portable\ComfyUI\models
   ```

2. **전체 선택** (Ctrl + A)

3. **복사** (Ctrl + C)

4. **대상 PC의 ComfyUI models 폴더에 붙여넣기** (Ctrl + V)

### Robocopy 사용 (대용량 파일에 유리)

```cmd
robocopy "C:\ComfyUI_windows_portable_nvidia\ComfyUI_windows_portable\ComfyUI\models" "대상경로\ComfyUI\models" /E /COPYALL /R:3 /W:5
```

**옵션 설명:**
- `/E`: 하위 폴더 포함
- `/COPYALL`: 모든 속성 복사
- `/R:3`: 실패 시 3회 재시도
- `/W:5`: 재시도 대기 5초

---

## 🔍 워크플로우에서 사용하는 모델 확인 방법

### JSON 파일 직접 확인

1. 워크플로우 JSON 파일 열기
2. `CheckpointLoaderSimple` 노드 찾기
   - `ckpt_name` 필드 확인
3. `IPAdapter` 노드 찾기 (있는 경우)
   - 사용하는 모델 파일명 확인
4. `ControlNet` 노드 찾기 (있는 경우)
   - 사용하는 모델 파일명 확인

### ComfyUI에서 확인

1. ComfyUI 실행
2. 워크플로우 JSON 로드
3. 각 노드 클릭하여 사용하는 모델 확인

---

## ✅ 복사 후 확인 사항

### 1. 파일 존재 확인

대상 PC에서 다음 파일들이 있는지 확인:

```
ComfyUI/models/checkpoints/
├── juggernautXL_ragnarokBy.safetensors  ✅
└── majicmixRealistic_v7.safetensors      ✅

ComfyUI/models/ipadapter/
└── (필요한 IPAdapter 모델들)              ✅
```

### 2. ComfyUI 실행 테스트

1. ComfyUI 서버 실행
2. 워크플로우 JSON 로드
3. 모델이 정상적으로 로드되는지 확인
4. 간단한 테스트 이미지 생성

### 3. 프로그램 연결 테스트

1. `상품가공프로그램.exe` 실행
2. ComfyUI 서버 주소 설정 (`127.0.0.1:8188`)
3. 연결 확인 버튼 클릭
4. ✅ 연결 성공 확인

---

## 💡 팁

### 용량 절약 팁

1. **압축 사용**
   - 모델 파일들을 압축하여 전송
   - 7-Zip 또는 WinRAR 사용
   - 압축률: 약 10-20% 절약

2. **네트워크 전송**
   - 같은 네트워크에 있으면 네트워크 공유 사용
   - 더 빠를 수 있음

3. **USB 3.0 사용**
   - 대용량 파일 복사 시 USB 3.0 이상 사용 권장

### 문제 해결

**문제**: 모델을 찾을 수 없다는 오류

**해결**:
1. 파일 경로 확인 (대소문자 구분)
2. 파일명 정확히 일치하는지 확인
3. ComfyUI 재시작

**문제**: 모델 로드 실패

**해결**:
1. 파일이 손상되지 않았는지 확인
2. ComfyUI 버전 호환성 확인
3. 모델 파일 재다운로드

---

## 📝 요약

### ✅ 권장 방법
1. **필수 모델만 복사** (약 12-20GB)
   - Checkpoint: 2개
   - IPAdapter: 필요한 것만
   - 빠르고 효율적

### ⚠️ 전체 복사 방법
2. **전체 models 폴더 복사** (수십 GB ~ 수백 GB)
   - 모든 모델 포함
   - 워크플로우 변경에도 대응
   - 용량과 시간 소요 큼

### 🎯 최종 권장사항

**처음 배포 시**: 필요한 모델만 복사 (용량 절약)
**워크플로우 변경 예정**: 전체 복사 (편의성)

---

## 📞 추가 도움

모델 파일 위치나 이름이 다른 경우:
1. 워크플로우 JSON 파일 확인
2. ComfyUI에서 실제 사용하는 모델 확인
3. 해당 모델만 복사

**중요**: 모델 파일은 용량이 크므로, 네트워크나 USB로 전송 시 시간이 오래 걸릴 수 있습니다. 인내심을 가지세요! 🚀

