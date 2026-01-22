@echo off
chcp 65001 >nul
echo ========================================
echo 상품가공프로그램 패키지 자동 설치
echo ========================================
echo.

echo [1/4] Python 경로 찾기...
set PYTHON_CMD=

REM 방법 1: python 명령어 시도
python --version >nul 2>&1
if not errorlevel 1 (
    set PYTHON_CMD=python
    echo ✅ Python 발견: python
    goto :found_python
)

REM 방법 2: py 명령어 시도 (Python Launcher)
py --version >nul 2>&1
if not errorlevel 1 (
    set PYTHON_CMD=py
    echo ✅ Python 발견: py
    goto :found_python
)

REM 방법 3: python3 명령어 시도
python3 --version >nul 2>&1
if not errorlevel 1 (
    set PYTHON_CMD=python3
    echo ✅ Python 발견: python3
    goto :found_python
)

REM 방법 4: 일반적인 설치 경로 확인
if exist "C:\Program Files\Python312\python.exe" (
    set PYTHON_CMD="C:\Program Files\Python312\python.exe"
    echo ✅ Python 발견: C:\Program Files\Python312\python.exe
    goto :found_python
)
if exist "C:\Program Files\Python311\python.exe" (
    set PYTHON_CMD="C:\Program Files\Python311\python.exe"
    echo ✅ Python 발견: C:\Program Files\Python311\python.exe
    goto :found_python
)
if exist "C:\Python312\python.exe" (
    set PYTHON_CMD="C:\Python312\python.exe"
    echo ✅ Python 발견: C:\Python312\python.exe
    goto :found_python
)
if exist "C:\Python311\python.exe" (
    set PYTHON_CMD="C:\Python311\python.exe"
    echo ✅ Python 발견: C:\Python311\python.exe
    goto :found_python
)

REM Python을 찾지 못한 경우
echo ❌ 오류: Python을 찾을 수 없습니다!
echo.
echo 해결 방법:
echo 1. Python이 설치되어 있는지 확인하세요
echo 2. 명령 프롬프트에서 다음 명령어를 시도해보세요:
echo    - python --version
echo    - py --version
echo    - python3 --version
echo 3. Python이 PATH에 추가되어 있는지 확인하세요
echo.
echo 또는 수동 설치:
echo    python -m pip install -r requirements.txt
echo    또는
echo    py -m pip install -r requirements.txt
echo.
pause
exit /b 1

:found_python
%PYTHON_CMD% --version

echo.
echo [2/4] pip 업그레이드...
%PYTHON_CMD% -m pip install --upgrade pip

echo.
echo [3/4] requirements.txt 파일 확인...
if not exist "requirements.txt" (
    echo ❌ 오류: requirements.txt 파일을 찾을 수 없습니다!
    echo 현재 폴더에 requirements.txt가 있는지 확인하세요.
    pause
    exit /b 1
)
echo ✅ requirements.txt 파일 발견

echo.
echo [4/4] 필수 패키지 설치 중...
echo (시간이 다소 걸릴 수 있습니다...)
echo.
echo 중요: numpy를 먼저 설치한 후 pandas를 설치합니다.
echo.

REM numpy 먼저 설치 (pandas 호환성 보장)
echo [4-1/4] numpy 설치 중...
%PYTHON_CMD% -m pip install "numpy>=1.24.0,<2.0.0"
if errorlevel 1 (
    echo ⚠️ numpy 설치 실패, 계속 진행...
)

echo.
echo [4-2/4] 나머지 패키지 설치 중...
%PYTHON_CMD% -m pip install -r requirements.txt

if errorlevel 1 (
    echo.
    echo ========================================
    echo 설치 중 오류가 발생했습니다!
    echo ========================================
    echo.
    echo 해결 방법:
    echo 1. 인터넷 연결 확인
    echo 2. Python 버전 확인 (Python 3.8 이상 권장)
    echo 3. 관리자 권한으로 실행
    echo.
    pause
    exit /b 1
)

echo.
echo ========================================
echo 설치 완료!
echo ========================================
echo.
echo 다음 단계:
echo 1. 상품가공프로그램.exe 실행
echo 2. 또는 %PYTHON_CMD% main_launcher_v7.py 실행
echo.
echo 사용된 Python: %PYTHON_CMD%
echo.
pause

