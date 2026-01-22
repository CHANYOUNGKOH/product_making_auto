@echo off
chcp 65001 >nul
echo ========================================
echo 상품가공프로그램 패키지 수동 설치
echo ========================================
echo.
echo 이 스크립트는 여러 방법으로 Python을 찾아서 설치를 시도합니다.
echo.
echo.

REM 현재 폴더 확인
cd /d "%~dp0"
echo 현재 폴더: %CD%
echo.

REM requirements.txt 확인
if not exist "requirements.txt" (
    echo ❌ 오류: requirements.txt 파일을 찾을 수 없습니다!
    echo 현재 폴더에 requirements.txt가 있는지 확인하세요.
    pause
    exit /b 1
)

echo ========================================
echo 방법 1: python 명령어 시도
echo ========================================
python --version >nul 2>&1
if not errorlevel 1 (
    echo ✅ python 명령어 사용 가능
    python --version
    echo.
    echo pip 업그레이드 중...
    python -m pip install --upgrade pip
    echo.
    echo 패키지 설치 중...
    python -m pip install -r requirements.txt
    if not errorlevel 1 (
        echo.
        echo ✅ 설치 완료! (python 사용)
        pause
        exit /b 0
    )
) else (
    echo ❌ python 명령어를 찾을 수 없습니다.
)
echo.

echo ========================================
echo 방법 2: py 명령어 시도 (Python Launcher)
echo ========================================
py --version >nul 2>&1
if not errorlevel 1 (
    echo ✅ py 명령어 사용 가능
    py --version
    echo.
    echo pip 업그레이드 중...
    py -m pip install --upgrade pip
    echo.
    echo 패키지 설치 중...
    py -m pip install -r requirements.txt
    if not errorlevel 1 (
        echo.
        echo ✅ 설치 완료! (py 사용)
        pause
        exit /b 0
    )
) else (
    echo ❌ py 명령어를 찾을 수 없습니다.
)
echo.

echo ========================================
echo 방법 3: python3 명령어 시도
echo ========================================
python3 --version >nul 2>&1
if not errorlevel 1 (
    echo ✅ python3 명령어 사용 가능
    python3 --version
    echo.
    echo pip 업그레이드 중...
    python3 -m pip install --upgrade pip
    echo.
    echo 패키지 설치 중...
    python3 -m pip install -r requirements.txt
    if not errorlevel 1 (
        echo.
        echo ✅ 설치 완료! (python3 사용)
        pause
        exit /b 0
    )
) else (
    echo ❌ python3 명령어를 찾을 수 없습니다.
)
echo.

echo ========================================
echo ❌ 모든 방법 실패
echo ========================================
echo.
echo Python을 찾을 수 없습니다.
echo.
echo 해결 방법:
echo 1. Python이 설치되어 있는지 확인
echo 2. 명령 프롬프트를 열고 다음 명령어를 직접 실행:
echo.
echo    python -m pip install -r requirements.txt
echo    또는
echo    py -m pip install -r requirements.txt
echo.
echo 3. VSCode에서 터미널을 열고 실행:
echo    pip install -r requirements.txt
echo.
pause

