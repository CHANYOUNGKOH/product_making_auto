# Upload_Mapper EXE 빌드 가이드

## 준비사항

1. Python 3.7 이상 설치
2. 필요한 패키지 설치:
   ```bash
   pip install -r requirements.txt
   ```

## 빌드 방법

### 방법 1: 배치 파일 사용 (가장 간단, Windows 권장)

`build_exe.bat` 파일을 더블클릭하거나 실행:

```bash
build_exe.bat
```

이 방법은 자동으로 필요한 패키지를 확인하고 설치한 후 빌드를 진행합니다.

### 방법 2: Python 스크립트 사용

```bash
python build_exe.py
```

### 방법 3: PyInstaller 직접 사용

```bash
pyinstaller --onefile --windowed --name=Upload_Mapper --clean --add-data="templates;templates" --add-data="upload_mapper_config.json;." main.py
```

## 빌드 결과

빌드가 완료되면 `dist` 폴더에 다음 파일들이 생성됩니다:
- `Upload_Mapper.exe` - 실행 파일
- `templates/` - 템플릿 폴더 (자동 복사됨)
- `upload_mapper_config.json` - 설정 파일 (자동 복사됨)

## 배포 방법

다른 PC에서 사용하려면:

1. **권장 방법**: `dist` 폴더 전체를 USB 또는 네트워크를 통해 다른 PC로 복사
2. 복사한 후 `Upload_Mapper.exe` 파일을 실행하면 됩니다.

### 폴더 구조
```
dist/
├── Upload_Mapper.exe
├── upload_mapper_config.json
└── templates/
    ├── detail_top_templates.json
    └── detail_bottom_templates.json
```

## 실행 방법

1. `Upload_Mapper.exe` 파일을 더블클릭
2. 첫 실행 시 Windows SmartScreen 경고가 나타날 수 있습니다.
   - "추가 정보" 클릭
   - "실행" 클릭
3. 프로그램이 정상적으로 실행됩니다.

## 주의사항

### 보안 경고
- Windows Defender 또는 백신 프로그램에서 경고가 나타날 수 있습니다.
  이는 PyInstaller로 생성된 EXE 파일의 일반적인 현상입니다.
- 신뢰할 수 있는 소스에서 받은 파일이라면 안전하게 실행할 수 있습니다.

### 설정 파일 수정
- `upload_mapper_config.json` 파일을 직접 수정하여 설정을 변경할 수 있습니다.
- 프로그램을 종료한 후 수정하고 다시 실행하세요.

### 템플릿 파일 수정
- `templates` 폴더의 JSON 파일을 직접 수정할 수 있습니다.
- 프로그램을 종료한 후 수정하고 다시 실행하세요.

## 문제 해결

### 빌드 실패 시
1. Python 버전 확인: `python --version` (3.7 이상 필요)
2. 패키지 설치 확인: `pip list | findstr pyinstaller`
3. 오류 메시지를 확인하여 문제 파악

### 실행 오류 시
1. EXE 파일과 같은 폴더에 `templates` 폴더와 `upload_mapper_config.json` 파일이 있는지 확인
2. Windows 이벤트 뷰어에서 오류 로그 확인
3. 백신 프로그램의 실시간 보호를 일시적으로 비활성화 후 시도

## 기술 정보

- **빌드 도구**: PyInstaller
- **Python 버전**: 3.7 이상
- **주요 의존성**: pandas, openpyxl, tkinter

