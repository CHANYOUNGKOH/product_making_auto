@echo off
chcp 65001 >nul
echo ========================================
echo 상품가공프로그램 자동 빌드 스크립트
echo ========================================
echo.

echo [1/4] PyInstaller 버전 확인...
pip show pyinstaller >nul 2>&1
if errorlevel 1 (
    echo PyInstaller가 설치되어 있지 않습니다. 설치 중...
    pip install pyinstaller
) else (
    echo PyInstaller 업그레이드 확인 중...
    pip install --upgrade pyinstaller
)

echo.
echo [2/4] Python 버전 확인...
python --version

echo.
echo [3/4] 기존 빌드 폴더 삭제...
if exist dist (
    echo dist 폴더 삭제 중...
    rmdir /s /q dist
)
if exist build (
    echo build 폴더 삭제 중...
    rmdir /s /q build
)
if exist __pycache__ (
    echo __pycache__ 폴더 삭제 중...
    rmdir /s /q __pycache__
)

echo.
echo [4/4] 빌드 시작...
python build_exe.py

echo.
echo ========================================
echo 빌드 완료!
echo ========================================
echo.
echo 배포 폴더: dist\상품가공프로그램
echo.
pause

