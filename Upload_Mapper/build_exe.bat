@echo off
chcp 65001 >nul
echo ========================================
echo Upload_Mapper EXE 빌드
echo ========================================
echo.

REM Python 설치 확인
python --version >nul 2>&1
if errorlevel 1 (
    echo [오류] Python이 설치되어 있지 않습니다.
    echo Python을 설치한 후 다시 시도하세요.
    pause
    exit /b 1
)

REM 필요한 패키지 설치 확인
echo 필요한 패키지 설치 확인 중...
pip show pyinstaller >nul 2>&1
if errorlevel 1 (
    echo PyInstaller가 설치되지 않았습니다. 설치 중...
    pip install -r requirements.txt
    if errorlevel 1 (
        echo [오류] 패키지 설치 실패
        pause
        exit /b 1
    )
)

REM 빌드 실행
echo.
echo 빌드 시작...
python build_exe.py

if errorlevel 1 (
    echo.
    echo [오류] 빌드 실패
    pause
    exit /b 1
)

echo.
echo 빌드가 완료되었습니다!
echo dist 폴더에 Upload_Mapper.exe 파일이 생성되었습니다.
echo.
pause

