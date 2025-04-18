@echo off
setlocal EnableDelayedExpansion

:: Configuration
set BOT_NAME=FGPbot
set BOT_DIR="%~dp0"
set VENV_ACTIVATE=".venv\Scripts\activate"

:: Change to bot directory
cd /d %BOT_DIR%

:: Verify virtual environment exists
if not exist %VENV_ACTIVATE% (
    echo ERROR: Virtual environment not found in %BOT_DIR%
    echo Please create a virtual environment in the .venv folder
    pause
    exit /b 1
)

:MAIN_LOOP
cls
echo ==============================================
echo      Starting %BOT_NAME% Twitch Bot
echo ==============================================
echo.

:: Activate virtual environment and run bot
call %VENV_ACTIVATE%
echo [%time%] Starting %BOT_NAME%...
python main.py
set "EXIT_CODE=%ERRORLEVEL%"

:: Handle exit codes
if %EXIT_CODE% EQU 0 (
    echo [%time%] Bot stopped normally
) else (
    echo [%time%] Bot crashed with error code %EXIT_CODE%
)

:: Restart logic
echo.
choice /M "Restart bot? (Y will restart, N will exit)" /C YN /T 10 /D N
if errorlevel 2 (
    echo Closing %BOT_NAME%...
    timeout /t 1 /nobreak > nul
    exit /b 0
)
if errorlevel 1 (
    echo Restarting %BOT_NAME% in 3 seconds...
    timeout /t 3 /nobreak > nul
    goto MAIN_LOOP
)

endlocal
exit /b 0