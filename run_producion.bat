@echo off
setlocal

REM Move to project directory (directory of this .bat file)
cd /d "%~dp0"

REM Runtime mode (real/demo) is read from config.py (set manually there)
set "PYTHONUNBUFFERED=1"

if exist ".venv\Scripts\python.exe" (
    echo Starting trading service using .venv...
    ".venv\Scripts\python.exe" service.py
    goto :eof
)

where py >nul 2>nul
if %ERRORLEVEL%==0 (
    echo Starting trading service using py launcher...
    py -3 service.py
    goto :eof
)

echo Starting trading service using system python...
python service.py
