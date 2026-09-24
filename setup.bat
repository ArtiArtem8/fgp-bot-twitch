@echo off
setlocal
chcp 65001 >nul
cd /d "%~dp0" || exit /b 2
where uv >nul 2>nul
if errorlevel 1 (
    echo ERROR: Install uv and add it to PATH before running setup.bat.
    exit /b 2
)
uv sync --locked --no-dev
if errorlevel 1 exit /b 1
echo Setup complete. Run runtwitchbot.bat.
