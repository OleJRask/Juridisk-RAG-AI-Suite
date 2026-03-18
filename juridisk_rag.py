import csv
import sqlite3
from pathlib import Path
from typing import List, Dict, Any
import requests
import subprocess
import time
import os

LAWS_CSV = Path("CurrentLaws.csv")
DB_PATH = Path("laws.db")

class LawDB:
    def import_from_laws_rag(self, base_folder=None):
        import os, json
        # Auto-detect the correct laws folder if not given
        candidate_dirs = [
            "laws",
            os.path.join("laws_rag", "laws"),
            os.path.join("..", "laws_rag", "laws"),
            os.path.join("..", "..", "laws_rag", "laws"),
            os.path.join("..", "laws"),
        ]
        if base_folder is None:
            for candidate in candidate_dirs:
                if os.path.isdir(candidate):
                    base_folder = candidate
                    break
            if base_folder is None:
                raise RuntimeError("Kunne ikke finde en gyldig lov-mappe. Angiv stien som argument eller placer 'laws' eller 'laws_rag/laws' i projektet.")
        count = 0
        for ressort in os.listdir(base_folder):
            ressort_path = os.path.join(base_folder, ressort)
            if not os.path.isdir(ressort_path):
                continue
            for year in os.listdir(ressort_path):
                year_path = os.path.join(ressort_path, year)
                if not os.path.isdir(year_path):
                    continue
                for lawtype in os.listdir(year_path):
                    lawtype_path = os.path.join(year_path, lawtype)
                    if not os.path.isdir(lawtype_path):
                        continue
                    for lawid in os.listdir(lawtype_path):
                        lawid_path = os.path.join(lawtype_path, lawid)
                        if not os.path.isdir(lawid_path):
                            continue
                        txt_path = os.path.join(lawid_path, "full_text_da.txt")
                        meta_path = os.path.join(lawid_path, "metadata.json")
                        if not os.path.exists(txt_path) or not os.path.exists(meta_path):
                            continue
                        with open(txt_path, encoding="utf-8") as f:
                            lawtext = f.read().strip()
                        with open(meta_path, encoding="utf-8") as f:
                            metadata = json.load(f)
                        law_id = lawid
                        # Robust extraction for law_name and eli_url
                        law_name = "UNKNOWN"
                        eli_url = "UNKNOWN"
                        # Try csv_row first
                        csv_row = metadata.get("csv_row", {})
                        if csv_row:
                            law_name = csv_row.get("Titel") or law_name
                            eli_url = csv_row.get("EliUrl") or eli_url
                            popular_title = csv_row.get("PopulærTitel") or ""
                            year_val = csv_row.get("År") or ""
                            ressort_val = csv_row.get("Ressort") or ressort or ""
                            authority = csv_row.get("AdministrerendeMyndighed") or ""
                            document_id = csv_row.get("DokumentId") or ""
                            accn = csv_row.get("ACCN") or ""
                            published_date = csv_row.get("PubliceretTidspunkt") or ""
                            notes = csv_row.get("RedaktionelNote") or ""
                        else:
                            # Fallback to top-level keys
                            law_name = metadata.get("Titel") or law_name
                            eli_url = metadata.get("EliUrl") or eli_url
                            popular_title = metadata.get("PopulærTitel") or ""
                            year_val = metadata.get("År") or year or ""
                            ressort_val = metadata.get("Ressort") or ressort or ""
                            authority = metadata.get("AdministrerendeMyndighed") or ""
                            document_id = metadata.get("DokumentId") or ""
                            accn = metadata.get("ACCN") or ""
                            published_date = metadata.get("PubliceretTidspunkt") or ""
                            notes = metadata.get("RedaktionelNote") or ""
                        meta_str = json.dumps(metadata, ensure_ascii=False)
                        self.conn.execute(
                            "INSERT OR IGNORE INTO laws (law_id, law_name, popular_title, year, ressort, authority, document_id, accn, published_date, notes, metadata, full_text) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                            (law_id, law_name, popular_title, year_val, ressort_val, authority, document_id, accn, published_date, notes, meta_str, lawtext)
                        )
                        # Extract only ordinary numbered law-paragraphs (not references)
                        import re
                        # Define paragraf_blocks from lawtext
                        paragraf_blocks = re.split(r'(§\s*\d+[a-zA-Z]*)', lawtext)
                        for i in range(1, len(paragraf_blocks), 2):
                            paragraf = paragraf_blocks[i].strip()
                            block = paragraf_blocks[i+1].strip() if i+1 < len(paragraf_blocks) else ''
                            stykker = re.split(r'(stk\.\s*\d+)', block)
                            if len(stykker) > 1:
                                for j in range(1, len(stykker), 2):
                                    stykke = stykker[j].strip()
                                    stykke_text = stykker[j+1].strip() if j+1 < len(stykker) else ''
                                    self.conn.execute(
                                        "INSERT INTO paragraphs (law_id, paragraph, section, text, law_name, eli_url) VALUES (?, ?, ?, ?, ?, ?)",
                                        (law_id, paragraf, stykke, stykke_text, law_name, eli_url)
                                    )
                            else:
                                self.conn.execute(
                                    "INSERT INTO paragraphs (law_id, paragraph, section, text, law_name, eli_url) VALUES (?, ?, ?, ?, ?, ?)",
                                    (law_id, paragraf, "1", block, law_name, eli_url)
                                )
                        count += 1
        self.conn.commit()
        print(f"Imported {count} laws from {base_folder} into laws.db")
    def search_paragraphs(self, query: str, limit: int = 10) -> list:
        # Synonym expansion for Danish legal queries
        synonym_map = {
            "rødt lys": ["rødt lys", "signal", "færdselssignal", "trafiklys", "stoplys", "trafik", "færdsel"],
            "trafik": ["trafik", "færdsel", "vej", "kørsel"],
            "identitetstegnebog": ["identitetstegnebog", "digital identitet", "eID", "identitet"],
            # Tilføj flere relevante synonymer
        }
        expanded = set([query.lower()])
        for key, syns in synonym_map.items():
            if key in query.lower():
                expanded.update([s.lower() for s in syns])
        # Byg LIKE-forespørgsler for alle synonymer
        like_clauses = " OR ".join([f"LOWER(paragraphs.text) LIKE '%{syn}%'" for syn in expanded])
        sql = (
            f"SELECT paragraphs.*, laws.law_name, laws.popular_title, laws.metadata FROM paragraphs "
            f"JOIN laws ON paragraphs.law_id = laws.law_id "
            f"WHERE ({like_clauses}) "
            f"LIMIT ?"
        )
        cur = self.conn.cursor()
        cur.execute(sql, (limit,))
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        results = []
        for row in rows:
            result = dict(zip(columns, row))
            # Extract URL from metadata
            meta = result.get("metadata")
            url = None
            if meta:
                try:
                    meta_dict = meta if isinstance(meta, dict) else json.loads(meta)
                    url = meta_dict.get("EliUrl")
                except Exception:
                    url = None
            result["url"] = url
            results.append(result)
        return results
    # All communication in Danish
    def generate_answer(self, question: str) -> str:
        paragraffer = self.search_paragraphs(question, limit=5)
        if not paragraffer:
            return "Beklager, jeg fandt ingen relevante lovparagraffer til dit spørgsmål."
        svar = "Svar baseret på lovtekst:\n"
        for p in paragraffer:
            navn = p.get('law_name') or ''
            url = p.get('url') or ''
            paragraf = p.get('paragraph') or ''
            sektion = p.get('section') or ''
            tekst = p.get('text') or ''
            svar += f"{navn} ({url}): {paragraf} {sektion}\n{tekst}\n\n"
        return svar

    def get_law_by_id(self, law_id: str) -> dict:
        cur = self.conn.cursor()
        cur.execute("SELECT * FROM laws WHERE law_id = ?", (law_id,))
        row = cur.fetchone()
        if row:
            columns = [desc[0] for desc in cur.description]
            return dict(zip(columns, row))
        return {}

    def _create_table(self):
        try:
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS laws (
                    law_id TEXT PRIMARY KEY,
                    law_name TEXT,
                    popular_title TEXT,
                    year TEXT,
                    ressort TEXT,
                    authority TEXT,
                    document_id TEXT,
                    accn TEXT,
                    published_date TEXT,
                    notes TEXT,
                    metadata TEXT,
                    full_text TEXT
                )
                """
            )
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS paragraphs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    law_id TEXT,
                    paragraph TEXT,
                    section TEXT,
                    text TEXT,
                    law_name TEXT,
                    eli_url TEXT,
                    FOREIGN KEY(law_id) REFERENCES laws(law_id)
                )
                """
            )
        except sqlite3.OperationalError as e:
            if 'database is locked' in str(e):
                print("[DB LOCKED] Waiting and retrying...")
                self.conn.close()
                time.sleep(2)
                if os.path.exists(DB_PATH):
                    os.remove(DB_PATH)
                self.conn = sqlite3.connect(DB_PATH)
                self._create_table()
            else:
                raise

    def __init__(self, db_path=DB_PATH):
        self.conn = sqlite3.connect(db_path)
        self._create_table()

    def reset_database(self):
        self.conn.close()
        if os.path.exists(DB_PATH):
            os.remove(DB_PATH)
        self.conn = sqlite3.connect(DB_PATH)
        self._create_table()

    def robust_import_from_csv(self, csv_path=LAWS_CSV, max_retries=3, log_path="import_failures.log", max_laws=None):
        import re, json, time
        failed_laws = []
        try:
            with open(csv_path, encoding="utf-8-sig") as f:
                reader = csv.DictReader(f, delimiter=';', quotechar='"')
                rows = list(reader)
        except UnicodeDecodeError:
            with open(csv_path, encoding="utf-16-le") as f:
                reader = csv.DictReader(f, delimiter=';', quotechar='"')
                rows = list(reader)
        last_livstegn = time.time()
        total = len(rows) if max_laws is None else min(len(rows), max_laws)
        for idx, row in enumerate(rows):
            # Print livstegn every 30 seconds
            if time.time() - last_livstegn > 30:
                print(f"Livstegn: {idx}/{total} love behandlet ({round(idx/total*100,1)}%)")
                last_livstegn = time.time()
            if max_laws is not None and idx >= max_laws:
                print(f"[TEST] Stoppede efter {max_laws} love.")
                break
            row = {k.strip('"'): v for k, v in row.items()}
            law_id = row.get("DokumentId") or row.get("ACCN") or str(idx)
            url = row.get("EliUrl")
            law_name = row.get("Titel")
            popular_title = row.get("PopulærTitel")
            year = row.get("År")
            ressort = row.get("Ressort")
            authority = row.get("AdministrerendeMyndighed")
            document_id = row.get("DokumentId")
            accn = row.get("ACCN")
            published_date = row.get("PubliceretTidspunkt")
            notes = row.get("RedaktionelNote")
            metadata = {k: v for k, v in row.items() if k not in ["EliUrl","Titel","PopulærTitel","År","Ressort","AdministrerendeMyndighed","DokumentId","ACCN","PubliceretTidspunkt","RedaktionelNote"]}
            lawtext = None
            for attempt in range(1, max_retries+1):
                try:
                    # Directly fetch lawtext from ELI URL (HTML)
                    html_resp = requests.get(url, timeout=7)
                    if html_resp.status_code == 200:
                        from bs4 import BeautifulSoup
                        soup = BeautifulSoup(html_resp.text, 'html.parser')
                        for tag in soup(['script', 'style', 'nav', 'footer']):
                            tag.decompose()
                        # Try to find the main lawtext container
                        doc_content = soup.find('div', class_='doc-content')
                        if doc_content:
                            text = doc_content.get_text(separator='\n', strip=True)
                        else:
                            main = soup.find('main')
                            if main:
                                text = main.get_text(separator='\n', strip=True)
                            else:
                                text = soup.get_text(separator='\n', strip=True)
                        import re
                        text = re.sub(r'\n\s*\n', '\n\n', text)
                        text = re.sub(r'\n{3,}', '\n\n', text)
                        text = re.sub(r'[ \t]+', ' ', text)
                        lawtext = '\n'.join(line.strip() for line in text.splitlines() if line.strip())
                        lawtext = lawtext.strip()
                    if lawtext:
                        break
                except Exception as e:
                    print(f"[RETRY {attempt}/{max_retries}] {law_name} ({url}): {e}")
                    time.sleep(2)
            if not lawtext:
                failed_laws.append({"idx": idx, "law_name": law_name, "url": url, "error": f"Failed after {max_retries} attempts"})
                continue
            # Indsæt én række pr. lov i 'laws' tabellen
            self.conn.execute(
                "INSERT OR IGNORE INTO laws (law_id, law_name, popular_title, year, ressort, authority, document_id, accn, published_date, notes, metadata, full_text) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (law_id, law_name, popular_title, year, ressort, authority, document_id, accn, published_date, notes, json.dumps(metadata), lawtext)
            )
            # Split og indsæt paragraffer/stykker i 'paragraphs' tabellen
            import re
            # Try to split by § (paragraf) and stk. (section)
            paragraf_blocks = re.split(r'(§\s*\d+[a-zA-Z]*)', lawtext)
            for i in range(1, len(paragraf_blocks), 2):
                paragraf = paragraf_blocks[i].strip()
                block = paragraf_blocks[i+1].strip() if i+1 < len(paragraf_blocks) else ''
                # Try to split by stk. (section), but fallback to full block if not found
                stykker = re.split(r'(stk\.\s*\d+)', block)
                if len(stykker) > 1:
                    for j in range(1, len(stykker), 2):
                        stykke = stykker[j].strip()
                        stykke_text = stykker[j+1].strip() if j+1 < len(stykker) else ''
                        self.conn.execute(
                            "INSERT INTO paragraphs (law_id, paragraph, section, text, law_name, eli_url) VALUES (?, ?, ?, ?, ?, ?)",
                            (law_id, paragraf, stykke, stykke_text, law_name, url)
                        )
                else:
                    self.conn.execute(
                        "INSERT INTO paragraphs (law_id, paragraph, section, text, law_name, eli_url) VALUES (?, ?, ?, ?, ?, ?)",
                        (law_id, paragraf, "1", block, law_name, url)
                    )
        self.conn.commit()
        # Log failures
        if failed_laws:
            with open(log_path, "w", encoding="utf-8") as logf:
                for fail in failed_laws:
                    logf.write(json.dumps(fail, ensure_ascii=False) + "\n")
            print(f"Import failures logged to {log_path}. Total failures: {len(failed_laws)}")

    def search_relevant_laws(self, question: str, limit: int = 10) -> list:
        cur = self.conn.cursor()
        q = question.lower()
        sql = (
            "SELECT * FROM laws "
            "WHERE ("
            "LOWER(law_name) LIKE ? OR "
            "LOWER(popular_title) LIKE ? OR "
            "LOWER(paragraph) LIKE ? OR "
            "LOWER(section) LIKE ? OR "
            "LOWER(text) LIKE ?"
            ") "
            "LIMIT ?"
        )
        like = f"%{q}%"
        cur.execute(sql, (like, like, like, like, like, limit))
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        return [dict(zip(columns, row)) for row in rows]

    def count_rows(self) -> int:
        cur = self.conn.cursor()
        cur.execute("SELECT COUNT(*) FROM laws")
        return cur.fetchone()[0]

    def count_empty_lawtext_rows(self) -> int:
        cur = self.conn.cursor()
        cur.execute("SELECT COUNT(*) FROM laws WHERE text IS NULL OR text = ''")
        return cur.fetchone()[0]

    def count_duplicate_laws(self) -> int:
        cur = self.conn.cursor()
        cur.execute("SELECT law_name, COUNT(*) FROM laws GROUP BY law_name HAVING COUNT(*) > 1")
        duplicates = cur.fetchall()
        return len(duplicates)

    def print_import_progress(self):
        cur = self.conn.cursor()
        cur.execute("SELECT COUNT(*) FROM laws")
        total_laws = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM laws WHERE full_text IS NULL OR full_text = ''")
        empty_laws = cur.fetchone()[0]
        print(f"Laws imported: {total_laws}")
        print(f"Laws with empty text: {empty_laws}")
        cur.execute("SELECT COUNT(*) FROM paragraphs")
        total_paragraphs = cur.fetchone()[0]
        print(f"Paragraphs imported: {total_paragraphs}")

if __name__ == "__main__":
    start = time.time()
    db = LawDB()
    db.reset_database()
    db.robust_import_from_csv()
    db.print_import_progress()
    elapsed = time.time() - start
    print(f"Full import complete. Elapsed time: {int(elapsed)} seconds.")
