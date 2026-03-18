@echo off
REM Aktiverer Python-miljø og starter Streamlit UI

REM Find venv
set VENV_DIR=venv
if not exist "%VENV_DIR%\Scripts\activate.bat" (
    echo Virtuelt miljø ikke fundet. Kør install.bat først.
    pause
    exit /b 1
)

REM Aktiver venv
call "%VENV_DIR%\Scripts\activate.bat"

REM Start ollama (hvis installeret)
where ollama >nul 2>nul
if %errorlevel%==0 (
    ollama serve >nul 2>nul
)

REM Start Streamlit UI
streamlit run juridisk_rag_streamlit_new.py

pause
