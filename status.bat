@echo off
setlocal
chcp 65001 >nul
cd /d "%~dp0" || exit /b 2
".venv\Scripts\python.exe" main.py status
set "RESULT=%ERRORLEVEL%"
pause
exit /b %RESULT%
