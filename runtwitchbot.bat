@echo off
setlocal
chcp 65001 >nul
cd /d "%~dp0" || exit /b 2
if not exist ".venv\Scripts\python.exe" (
    echo ERROR: Run setup.bat first. Virtual environment is missing.
    exit /b 2
)
".venv\Scripts\python.exe" -c "import aiohttp, dotenv, msgspec; import fgpbot.cli" >nul 2>nul
if errorlevel 1 (
    echo ERROR: Python dependencies or source files are incomplete. Run setup.bat and selftest.bat.
    exit /b 2
)
:RUN
".venv\Scripts\python.exe" -u main.py run
set "BOT_EXIT=%ERRORLEVEL%"
if "%BOT_EXIT%"=="0" exit /b 0
if "%BOT_EXIT%"=="2" (
    echo Configuration error. Fix .env and restart the launcher.
    exit /b 2
)
if "%BOT_EXIT%"=="3" exit /b 3
echo FGPbot exited with code %BOT_EXIT%. Restarting in 20 seconds...
powershell.exe -NoLogo -NoProfile -NonInteractive -Command "Start-Sleep -Seconds 20"
goto RUN
