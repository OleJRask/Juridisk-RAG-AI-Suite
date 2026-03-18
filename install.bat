@echo off
setlocal

REM --- Step 1: Check for Python ---
echo [1/7] Tjekker for Python...
python --version >nul 2>&1
if errorlevel 1 (
    echo Python ikke fundet. Installer Python 3.10+ og genstart install.bat.
    pause
    exit /b
)

REM --- Step 2: Opret venv ---
echo [2/7] Opretter Python environment...
python -m venv venv
if errorlevel 1 (
    echo Kunne ikke oprette venv.
    pause
    exit /b
)

REM --- Step 3: Installer requirements ---
echo [3/7] Opdaterer pip...
venv\Scripts\python.exe -m pip install --upgrade pip
if errorlevel 1 (
    echo Kunne ikke opdatere pip.
    pause
    exit /b
)
echo [3/7] Installerer dependencies...
REM Fjern sqlite3 fra requirements.txt hvis den findes
findstr /v /i "sqlite3" requirements.txt > requirements_clean.txt
move /y requirements_clean.txt requirements.txt
venv\Scripts\python.exe -m pip install -r requirements.txt
if errorlevel 1 (
    echo Kunne ikke installere dependencies.
    echo Tjek at requirements.txt er korrekt, at du har internetforbindelse, og at Python-versionen er kompatibel.
    pause
    exit /b
)


REM --- Step 4: Filtrerer love ---
echo [4/8] Filtrerer love (kun Social- og Boligministeriet, år >= 2000)...
venv\Scripts\python.exe filter_laws.py
if errorlevel 1 (
    echo Fejl ved filtrering af love.
    pause
    exit /b
)

REM --- Step 5: Henter love ---
echo [5/8] Henter love (kan tage 10-30 min)...
venv\Scripts\python.exe law_fetcher.py --input FilteredLaws.csv --output laws_rag --allow-html-only
if errorlevel 1 (
    echo Fejl ved hentning af love.
    pause
    exit /b
)


REM --- Step 6: Genererer summaries ---
echo [6/8] Genererer summaries (kan tage 10-30 min)...
venv\Scripts\python.exe generate_summaries.py laws_rag/laws
if errorlevel 1 (
    echo Fejl ved summary-generering.
    pause
    exit /b
)


REM --- Step 7: Bygger database ---
echo [7/8] Bygger database...
venv\Scripts\python.exe -c "from juridisk_rag import LawDB; db = LawDB(); db.conn = __import__('sqlite3').connect('laws.db'); db.import_from_laws_rag()"
if errorlevel 1 (
    echo Fejl ved database-bygning.
    pause
    exit /b
)


REM --- Step 8: Bygger index og vectorizer ---
echo [8/8] Bygger index og vectorizer...
venv\Scripts\python.exe -c "from hybrid_rag import HybridRAG; rag = HybridRAG(); rag.update_index()"
if errorlevel 1 (
    echo Fejl ved index-bygning.
    pause
    exit /b
)

REM --- Step 7: Starter Streamlit UI ---
echo [7/7] Starter Streamlit UI...
venv\Scripts\python.exe -m streamlit run juridisk_rag_streamlit_new.py

echo.
echo Installation og start er færdig. Du kan lukke dette vindue, når du ønsker.
pause

endlocal
