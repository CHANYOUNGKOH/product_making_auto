@echo off
chcp 65001 >nul
echo ========================================
echo numpy/pandas 호환성 문제 해결
echo ========================================
echo.
echo 이 스크립트는 numpy와 pandas 호환성 문제를 해결합니다.
echo.

REM Python 경로 찾기
set PYTHON_CMD=

python --version >nul 2>&1
if not errorlevel 1 (
    set PYTHON_CMD=python
    goto :found_python
)

py --version >nul 2>&1
if not errorlevel 1 (
    set PYTHON_CMD=py
    goto :found_python
)

python3 --version >nul 2>&1
if not errorlevel 1 (
    set PYTHON_CMD=python3
    goto :found_python
)

echo ❌ Python을 찾을 수 없습니다!
pause
exit /b 1

:found_python
echo ✅ Python 발견: %PYTHON_CMD%
%PYTHON_CMD% --version
echo.

echo ========================================
echo [1/3] 기존 numpy, pandas 제거
echo ========================================
echo.
%PYTHON_CMD% -m pip uninstall -y numpy pandas
echo.

echo ========================================
echo [2/3] numpy 재설치 (먼저 설치)
echo ========================================
echo.
%PYTHON_CMD% -m pip install "numpy>=1.24.0,<2.0.0"
if errorlevel 1 (
    echo ❌ numpy 설치 실패!
    pause
    exit /b 1
)
echo.

echo ========================================
echo [3/3] pandas 재설치 (numpy 이후)
echo ========================================
echo.
%PYTHON_CMD% -m pip install "pandas>=2.0.0,<3.0.0"
if errorlevel 1 (
    echo ❌ pandas 설치 실패!
    pause
    exit /b 1
)
echo.

echo ========================================
echo ✅ 설치 완료!
echo ========================================
echo.
echo numpy와 pandas가 호환되는 버전으로 재설치되었습니다.
echo.
echo 테스트:
%PYTHON_CMD% -c "import numpy; import pandas; print('✅ numpy:', numpy.__version__); print('✅ pandas:', pandas.__version__)"
echo.
pause

