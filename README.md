# Juridisk RAG AI Suite

Et fuldautomatisk system til analyse, filtrering og besvarelse af juridiske spørgsmål baseret på dansk lovgivning. Programmet henter, strukturerer og sammenfatter store mængder lovdata, genererer præcise resuméer, bygger søgbare databaser og leverer brugertilpassede svar via et intuitivt webinterface. Løsningen kombinerer avanceret tekstforståelse, robust databehandling og effektiv parallelisering for at sikre høj præcision, skalerbarhed og brugervenlighed i alle led – fra datakilde til slutbruger.
Denne version er beregnet som illustration og er derfor begrænset til kun at behandle love efter år 2000 fra Social- og Boligministeriet.

## Installation og brug

1. Download hele release-mappen.
2. Kør `install.bat` (dobbeltklik).
3. Følg instruktioner og vent på progress.
4. Når installationen er færdig, åbnes Streamlit UI automatisk.

### Krav
- Windows
- Python 3.10+ (installeres hvis mangler)

### Progress og tid
- Progress vises for hvert trin.
- Estimeret tid for lov-hentning: 10-30 min.
- Resten tager typisk 1-5 min.

### Filer i release:
- install.bat
- run.bat
- requirements.txt
- juridisk_rag.py
- hybrid_rag.py
- law_fetcher.py
- audit_paragraphs.py
- test_rag.py
- juridisk_rag_streamlit_new.py
- CurrentLaws.csv
- README.md

### Fejl
- Hvis du får fejl om Python, installer Python 3.10+ og genstart.
- Hvis du får andre fejl, kontakt projektets GitHub.

## Kontakt
- Se GitHub for issues og support.

## Ny funktionalitet

- Dynamisk summary-længde: Antallet af sætninger i det samlede summary tilpasses nu automatisk efter lovens længde (flere sætninger for lange love, færre for korte).
- Større segmenter: Hvert segment er nu op til 3500 tegn, så selv meget lange love deles i færre bidder.
- Parallel summary-generering: generate_summaries.py bruger nu op til 14 CPU-kerner for hurtigere behandling.
- Statusbeskeder: Du ser nu beskeder som "Summary er genereret for: X/Y" under kørsel, så du kan følge fremdriften.
