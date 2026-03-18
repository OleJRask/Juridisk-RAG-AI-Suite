@echo off
setlocal

REM --- Start Python environment ---
echo Starter Python environment...
call venv\Scripts\activate.bat

REM --- Start LLM backend (Ollama) ---
echo Starter Ollama LLM-server...
start ollama serve

REM --- Start database og index (valgfrit, hvis du vil genopbygge)
REM echo Bygger database og index...
REM python -c "from juridisk_rag import LawDB; db = LawDB(); db.conn = __import__('sqlite3').connect('laws.db'); db.import_from_laws_rag()"
REM python -c "from hybrid_rag import HybridRAG; rag = HybridRAG(); rag.update_index()"

REM --- Start Streamlit UI ---
echo Starter Streamlit UI...
python -m streamlit run juridisk_rag_streamlit_new.py

echo.
echo Programmet er nu startet. Luk dette vindue for at stoppe alt.
pause

endlocal
