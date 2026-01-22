@echo off
chcp 65001 >nul
echo ============================================================
echo 순환식 조합 변경 전 백업 스크립트
echo ============================================================
echo.

cd /d "%~dp0"
python backup_before_circular_change.py

echo.
echo 실행 완료. 아무 키나 누르면 종료합니다.
pause >nul
