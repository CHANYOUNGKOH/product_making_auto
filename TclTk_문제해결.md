# 🔧 Tcl/Tk 버전 충돌 문제 해결 가이드

## 문제 증상

```
_tkinter.TclError: version conflict for package "Tcl": have 8.6.12, need exactly 8.6.13
```

## 원인

PyInstaller가 Tcl/Tk 데이터를 포함할 때 버전 불일치가 발생합니다.
- 빌드 PC의 Python에 Tcl 8.6.13이 설치되어 있음
- 실행 PC의 Python에 Tcl 8.6.12가 설치되어 있거나
- PyInstaller가 잘못된 버전의 Tcl/Tk 데이터를 포함함

## 해결 방법

### 방법 1: PyInstaller 업그레이드 (권장)

```bash
pip install --upgrade pyinstaller
```

최신 PyInstaller는 Tcl/Tk 데이터를 더 잘 처리합니다.

### 방법 2: 빌드 폴더 완전 삭제 후 재빌드

```bash
# 기존 빌드 결과 삭제
rmdir /s /q dist
rmdir /s /q build
rmdir /s /q __pycache__

# 재빌드
python build_exe.py
```

### 방법 3: Python 버전 통일

**개발 PC와 배포 PC의 Python 버전을 동일하게 맞추세요.**

- Python 3.11 이상 권장
- 같은 마이너 버전 사용 (예: 둘 다 3.11.x)

### 방법 4: Tcl/Tk 데이터 수동 복사 (최후의 수단)

만약 위 방법들이 작동하지 않으면:

1. **개발 PC에서 Tcl/Tk 데이터 찾기**
   ```bash
   # Python 설치 경로 확인
   python -c "import sys; print(sys.executable)"
   
   # Tcl 데이터 경로 (일반적으로)
   C:\Program Files\Python311\tcl\tcl8.6
   C:\Program Files\Python311\tcl\tk8.6
   ```

2. **빌드 결과에 수동으로 복사**
   ```
   dist/상품가공프로그램/_internal/
   ├── _tcl_data/  ← Tcl 데이터 복사
   └── _tk_data/   ← Tk 데이터 복사
   ```

### 방법 5: 환경 변수 설정 (임시 해결책)

실행 PC에서 환경 변수를 설정하여 Tcl/Tk 경로를 지정:

```cmd
set TCL_LIBRARY=C:\상품가공프로그램\_internal\_tcl_data
set TK_LIBRARY=C:\상품가공프로그램\_internal\_tk_data
```

또는 배치 파일로 실행:

```batch
@echo off
set TCL_LIBRARY=%~dp0_internal\_tcl_data
set TK_LIBRARY=%~dp0_internal\_tk_data
상품가공프로그램.exe
```

## 자동 해결 스크립트

`build_exe.py`가 자동으로:
1. Python 설치 경로에서 Tcl/Tk 데이터를 찾습니다
2. 올바른 버전의 데이터를 포함합니다
3. 빌드 후 검증을 수행합니다

## 체크리스트

빌드 전:
- [ ] PyInstaller 최신 버전 확인: `pip show pyinstaller`
- [ ] Python 버전 확인: `python --version`
- [ ] 기존 빌드 폴더 삭제: `rmdir /s /q dist build`

빌드 후:
- [ ] `dist/상품가공프로그램/_internal/_tcl_data/` 폴더 존재 확인
- [ ] `dist/상품가공프로그램/_internal/_tk_data/` 폴더 존재 확인
- [ ] 로컬에서 실행 테스트

배포 후:
- [ ] 다른 PC에서 실행 테스트
- [ ] Tcl/Tk 오류가 발생하지 않는지 확인

## 추가 팁

### PyInstaller 버전 확인

```bash
pip show pyinstaller
```

최소 버전: 5.13.0 이상 권장

### Python 버전 확인

```bash
python --version
```

Python 3.11 이상 권장 (Tcl/Tk 8.6.13 포함)

### 빌드 로그 확인

빌드 시 다음 메시지가 나오는지 확인:
```
✅ Tcl 데이터 발견: C:\Program Files\Python311\tcl\tcl8.6
✅ Tk 데이터 발견: C:\Program Files\Python311\tcl\tk8.6
✅ Tcl/Tk 데이터가 올바르게 포함되었습니다.
```

## 문제가 계속되면

1. **개발 PC와 배포 PC의 Python 버전을 동일하게 맞추기**
2. **PyInstaller를 최신 버전으로 업그레이드**
3. **빌드 폴더를 완전히 삭제하고 재빌드**

이 세 가지를 시도하면 대부분의 문제가 해결됩니다! 🎯

