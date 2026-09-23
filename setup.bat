@echo off
setlocal
chcp 65001 >nul
cd /d "%~dp0" || exit /b 2
where uv >nul 2>nul
if not errorlevel 1 (
    uv sync --no-dev
    if errorlevel 1 exit /b 1
    echo Setup complete. Run runtwitchbot.bat.
    exit /b 0
)
if not exist ".venv\Scripts\python.exe" (
    where py >nul 2>nul
    if not errorlevel 1 (
        py -3 -m venv .venv
    ) else (
        python -m venv .venv
    )
    if errorlevel 1 exit /b 1
)
".venv\Scripts\python.exe" -c "import sys; assert (3,12) <= sys.version_info[:2] < (3,15), 'Use Python 3.12, 3.13 or 3.14'"
if errorlevel 1 exit /b 2
".venv\Scripts\python.exe" -m pip install -r requirements.txt
if errorlevel 1 exit /b 1
echo Setup complete. Run runtwitchbot.bat.
