@echo off
setlocal EnableDelayedExpansion

REM RS Dashboard - Windows Startup Script

echo.
echo ==========================================
echo   RS Dashboard - Starting...
echo ==========================================
echo.

REM Check if uv is installed
where uv >nul 2>nul
if %ERRORLEVEL% NEQ 0 (
    echo [1/3] uv not found. Installing...
    echo.

    REM Download and run uv installer via PowerShell
    powershell -ExecutionPolicy ByPass -Command "irm https://astral.sh/uv/install.ps1 | iex"

    REM Add common uv install locations to PATH for this session
    set "PATH=%USERPROFILE%\.local\bin;%LOCALAPPDATA%\uv;%PATH%"

    REM Verify uv is now available
    where uv >nul 2>nul
    if !ERRORLEVEL! NEQ 0 (
        echo.
        echo   ERROR: uv installation may have failed or requires a restart.
        echo.
        echo   Please try one of these options:
        echo   1. Close this window and run run.bat again
        echo   2. Manually install uv from: https://github.com/astral-sh/uv
        echo   3. Open a new terminal and run: powershell -c "irm https://astral.sh/uv/install.ps1 | iex"
        echo.
        pause
        exit /b 1
    )
    echo   √ uv installed successfully
) else (
    echo [1/3] uv already installed √
)

REM Sync dependencies
echo [2/3] Syncing dependencies...
uv sync
if %ERRORLEVEL% NEQ 0 (
    echo.
    echo   ERROR: Failed to sync dependencies.
    echo   Make sure you're in the rs-metrics-app directory.
    echo.
    pause
    exit /b 1
)
echo   √ Dependencies ready

REM Run the app
echo [3/3] Starting RS Dashboard...
echo.
echo   App running at: http://localhost:5001
echo   Press Ctrl+C to stop
echo.

REM Open browser after 3 seconds (in background)
start /b cmd /c "timeout /t 3 /nobreak >nul && start http://localhost:5001"

REM Run the FastAPI app
uv run python -m uvicorn api.main:app --host 0.0.0.0 --port 5001 --reload

endlocal
