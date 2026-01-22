@echo off
chcp 65001 >nul
echo ============================================================
echo 순환식 조합 구조 마이그레이션
echo ============================================================
echo.

cd /d "%~dp0"
python migrate_to_circular_combinations.py

echo.
echo 실행 완료. 아무 키나 누르면 종료합니다.
pause >nul
