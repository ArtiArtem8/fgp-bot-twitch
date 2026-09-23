@echo off
setlocal
chcp 65001 >nul
cd /d "%~dp0" || exit /b 2
echo This check will send ONE diagnostic message to the configured Twitch chat.
".venv\Scripts\python.exe" main.py check-chat --send
set "RESULT=%ERRORLEVEL%"
pause
exit /b %RESULT%
