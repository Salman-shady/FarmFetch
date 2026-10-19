@echo off
title FarmFetch - Hyperlocal Farm Produce Platform
cd /d "%~dp0"

echo =========================================================
echo       FarmFetch - Farm to Table Platform
echo =========================================================
echo.

:: 1. Check Python
where python >nul 2>nul
if errorlevel 1 goto NO_PYTHON

:: 2. Check or Setup Virtual Environment
if exist ".venv\Scripts\python.exe" goto VENV_READY

echo [1/2] Creating virtual environment...
python -m venv .venv
if errorlevel 1 goto ERROR_VENV

echo [2/2] Installing requirements into virtual environment...
".venv\Scripts\python.exe" -m pip install --upgrade pip
".venv\Scripts\python.exe" -m pip install -r requirements.txt
if errorlevel 1 goto ERROR_DEPS

:VENV_READY
:: 3. Launch browser in background after 2 seconds
start "" cmd /c "timeout /t 2 /nobreak >nul & start http://localhost:5000"

echo.
echo =========================================================
echo  Application is starting on: http://localhost:5000
echo  Press Ctrl+C in this window to stop the server.
echo =========================================================
echo.

".venv\Scripts\python.exe" app.py

if errorlevel 1 goto ERROR_SERVER
exit /b 0

:ERROR_SERVER
echo.
echo Server exited with an error.
pause
exit /b 1

:NO_PYTHON
echo [ERROR] Python is not installed or not in system PATH.
echo Please install Python from https://www.python.org/
pause
exit /b 1

:ERROR_VENV
echo [ERROR] Failed to create virtual environment.
pause
exit /b 1

:ERROR_DEPS
echo [ERROR] Failed to install dependencies.
pause
exit /b 1
