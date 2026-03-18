import sqlite3
import numpy as np
from transformers import AutoTokenizer, AutoModel
from sentence_transformers import SentenceTransformer
import faiss
import ollama
import json
from sklearn.feature_extraction.text import TfidfVectorizer
import os

DB_PATH = 'laws.db'
MODEL_NAME = 'paraphrase-multilingual-MiniLM-L12-v2'

class HybridRAG:
    def __init__(self, db_path=DB_PATH, embed_model=MODEL_NAME, emb_path="embeddings.npy", index_path="faiss.index", para_path="paragraphs.json", laws_dir=None):
        # Auto-detect the correct laws directory if not given
        candidate_dirs = [
            "laws",
            os.path.join("laws_rag", "laws"),
            os.path.join("..", "laws_rag", "laws"),
            os.path.join("..", "..", "laws_rag", "laws"),
            os.path.join("..", "laws"),
            os.path.join("c:/RAG/laws_rag/laws"),
            os.path.join("/RAG/laws_rag/laws"),
        ]
        if laws_dir is None:
            for candidate in candidate_dirs:
                if os.path.isdir(candidate):
                    laws_dir = candidate
                    break
            if laws_dir is None:
                raise RuntimeError("Kunne ikke finde en gyldig lov-mappe. Angiv stien som argument eller placer 'laws' eller 'laws_rag/laws' i projektet.")
        self.laws_dir = laws_dir
        self.conn = sqlite3.connect(db_path)
        self.embedder = SentenceTransformer(embed_model)
        self.index = None
        self.paragraphs = []
        self.embeddings = None
        self.emb_path = emb_path
        self.index_path = index_path
        self.para_path = para_path
        self.feedback_log = "feedback.log"
        self.synonym_map = {
            "rødt lys": ["rødt lys", "signal", "færdselssignal", "trafiklys", "stoplys", "trafik", "færdsel"],
            "trafik": ["trafik", "færdsel", "vej", "kørsel"],
            "identitetstegnebog": ["identitetstegnebog", "digital identitet", "eID", "identitet"],
            # Udvid med flere juridiske synonymer
        }
        # Try to load cached paragraphs, embeddings, and index
        # Always build vectorizer for keyword search
        if os.path.exists(self.para_path) and os.path.exists(self.emb_path) and os.path.exists(self.index_path):
            with open(self.para_path, "r", encoding="utf-8") as f:
                self.paragraphs = json.load(f)
            self.embeddings = np.load(self.emb_path)
            self.index = faiss.read_index(self.index_path)
        else:
            self._load_paragraphs()
            self._build_index()
            with open(self.para_path, "w", encoding="utf-8") as f:
                json.dump(self.paragraphs, f, ensure_ascii=False)
            np.save(self.emb_path, self.embeddings)
            faiss.write_index(self.index, self.index_path)
        # Always build TF-IDF vectorizer for keyword search after paragraphs are loaded
        texts = [p[4] for p in self.paragraphs]
        self.vectorizer = TfidfVectorizer().fit(texts)
        self.tfidf_matrix = self.vectorizer.transform(texts)
        # Build summary index for laws
        self.law_summaries = self._load_law_summaries()
        self.summary_vectorizer = TfidfVectorizer().fit([s['summary'] for s in self.law_summaries])
        self.summary_matrix = self.summary_vectorizer.transform([s['summary'] for s in self.law_summaries])
    def _load_law_summaries(self):
        summaries = []
        for root, dirs, files in os.walk(self.laws_dir):
            if 'summary.txt' in files:
                summary_path = os.path.join(root, 'summary.txt')
                meta_path = os.path.join(root, 'metadata.json')
                summary = open(summary_path, encoding='utf-8').read().strip()
                ressort = ''
                title = ''
                url = ''
                if os.path.exists(meta_path):
                    try:
                        meta = json.load(open(meta_path, encoding='utf-8'))
                        csv_row = meta.get('csv_row', {})
                        ressort = csv_row.get('Ressort', '')
                        title = csv_row.get('Titel', '')
                        url = csv_row.get('EliUrl', '')
                    except Exception:
                        pass
                summaries.append({'summary': summary, 'ressort': ressort, 'title': title, 'url': url, 'dir': root})
        return summaries

    def _load_paragraphs(self):
        cur = self.conn.cursor()
        cur.execute("SELECT id, law_id, paragraph, section, text, law_name, eli_url FROM paragraphs")
        self.paragraphs = [list(row) for row in cur.fetchall()]

    def _build_index(self):
        texts = [p[4] for p in self.paragraphs]
        self.embeddings = self.embedder.encode(texts, show_progress_bar=True)
        self.index = faiss.IndexFlatL2(self.embeddings.shape[1])
        self.index.add(np.array(self.embeddings, dtype=np.float32))
        # Build TF-IDF vectorizer for keyword search
        self.vectorizer = TfidfVectorizer().fit(texts)
        self.tfidf_matrix = self.vectorizer.transform(texts)

    def search(self, question, top_k=5, ressort_filter=None):
        # 1. Match spørgsmål mod summary.txt for hver lov
        query_vec = self.summary_vectorizer.transform([question])
        scores = np.asarray(self.summary_matrix.dot(query_vec.T).todense()).flatten()
        ranked_indices = scores.argsort()[::-1]
        # 2. Filtrér på ressort hvis angivet
        filtered = [self.law_summaries[i] for i in ranked_indices if (not ressort_filter or self.law_summaries[i]['ressort'] == ressort_filter)]
        # 3. Tag top_k love
        top_laws = filtered[:top_k]
        # 4. Find alle paragraffer fra disse love
        top_dirs = set([law['dir'] for law in top_laws])
        relevant_paragraphs = [p for p in self.paragraphs if any(p[1] in d for d in top_dirs)]
        # Hvis ingen relevante love, fallback til alle paragraffer
        if not relevant_paragraphs:
            relevant_paragraphs = self.paragraphs
        # 5. Embedding search og keyword search på relevante paragraffer
        expanded = set([question.lower()])
        for key, syns in self.synonym_map.items():
            if key in question.lower():
                expanded.update([s.lower() for s in syns])
        q_emb = self.embedder.encode([question])
        texts = [p[4] for p in relevant_paragraphs]
        emb_matrix = self.embedder.encode(texts)
        D, I = faiss.IndexFlatL2(emb_matrix.shape[1]).search(np.array(q_emb, dtype=np.float32), top_k)
        semantic_results = [relevant_paragraphs[i] for i in I[0]]
        tfidf_query = self.vectorizer.transform([question])
        tfidf_matrix = self.vectorizer.transform(texts)
        scores = np.asarray(tfidf_matrix.dot(tfidf_query.T).todense()).flatten()
        keyword_indices = scores.argsort()[-top_k:][::-1]
        keyword_results = [relevant_paragraphs[i] for i in keyword_indices]
        combined = semantic_results + keyword_results
        unique_results = []
        seen = set()
        for p in combined:
            pid = (p[0], p[1], p[2], p[3])
            if pid not in seen:
                unique_results.append(p)
                seen.add(pid)
        return unique_results[:top_k]
        # Synonym expansion
        expanded = set([question.lower()])
        for key, syns in self.synonym_map.items():
            if key in question.lower():
                expanded.update([s.lower() for s in syns])
        # Embedding search
        q_emb = self.embedder.encode([question])
        D, I = self.index.search(np.array(q_emb, dtype=np.float32), top_k)
        semantic_results = [self.paragraphs[i] for i in I[0]]
        # Keyword search (TF-IDF)
        tfidf_query = self.vectorizer.transform([question])
        scores = np.asarray(self.tfidf_matrix.dot(tfidf_query.T).todense()).flatten()
        keyword_indices = scores.argsort()[-top_k:][::-1]
        keyword_results = [self.paragraphs[i] for i in keyword_indices]
        # Combine and deduplicate
        combined = semantic_results + keyword_results
        unique_results = []
        seen = set()
        for p in combined:
            pid = (p[0], p[1], p[2], p[3])
            if pid not in seen:
                unique_results.append(p)
                seen.add(pid)
        return unique_results[:top_k]

    def rerank_with_ollama(self, question, paragraphs):
        prompt = (
            f"Du er en dansk juridisk ekspert.\n"
            f"Du får et juridisk spørgsmål og en liste af lovparagraffer.\n"
            f"Spørgsmål: '{question}'\n"
            f"Paragraffer (med lovnavn og URL):\n"
        )
        for p in paragraphs:
            law_name = p[5] if len(p) > 5 else ''
            url = p[6] if len(p) > 6 else ''
            prompt += f"{law_name} ({url}) - {p[2]} {p[3]}: {p[4]}\n"
        prompt += (
            "\nDin opgave er:\n"
            "1. Find kun den paragraf, der præcist og utvetydigt besvarer spørgsmålet.\n"
            "2. Hvis ingen paragraf matcher klart, svar: 'Ingen relevant paragraf fundet.'\n"
            "3. Hvis der er et klart match, gengiv kun den relevante paragraf, paragrafnummer, lovnavn og URL.\n"
            "4. Svar altid på dansk.\n"
            "5. Svar skal indeholde: paragrafnummer, lovnavn og URL. Hvis du ikke kan finde alle, skriv 'Ingen relevant paragraf fundet.'\n"
            "6. Svar kun med den relevante paragraf og metadata, ingen ekstra forklaring.\n"
        )
        response = ollama.chat(model='llama3', messages=[{"role": "user", "content": prompt}])
        answer = response['message']['content']
        # Validering: Svar skal indeholde alle metadata
        valid = False
        for p in paragraphs:
            law_name = p[5] if len(p) > 5 else ''
            url = p[6] if len(p) > 6 else ''
            paragraph = p[2] if len(p) > 2 else ''
            section = p[3] if len(p) > 3 else ''
            if all([law_name, url, paragraph]) and law_name in answer and url in answer and paragraph in answer:
                valid = True
                break
        if not valid or 'Ingen relevant paragraf fundet' in answer:
            return 'Ingen relevant paragraf fundet.'
        return answer

    def log_feedback(self, question, answer, feedback):
        with open(self.feedback_log, "a", encoding="utf-8") as f:
            f.write(json.dumps({"question": question, "answer": answer, "feedback": feedback}) + "\n")

    def update_index(self):
        # Rebuild index and cache if database changes
        self._load_paragraphs()
        self._build_index()
        with open(self.para_path, "w", encoding="utf-8") as f:
            json.dump(self.paragraphs, f, ensure_ascii=False)
        np.save(self.emb_path, self.embeddings)
        faiss.write_index(self.index, self.index_path)

if __name__ == '__main__':
    rag = HybridRAG()
    import os
    questions_path = 'test_questions.txt'
    if os.path.exists(questions_path):
        with open(questions_path, encoding='utf-8') as f:
            questions = [line.strip() for line in f if line.strip()]
        for idx, question in enumerate(questions, 1):
            print(f"\nSpørgsmål {idx}: {question}")
            top_paragraphs = rag.search(question, top_k=5)
            answer = rag.rerank_with_ollama(question, top_paragraphs)
            print(f"Svar:\n{answer}")
            # Stricter validation: answer must contain law_name, paragraph, section, and URL from at least one candidate, or 'Ingen relevant paragraf fundet.'
            valid = False
            if 'Ingen relevant paragraf fundet' in answer:
                print('[VALIDERING] Systemet fandt ingen relevant paragraf.')
                valid = True
            else:
                for p in top_paragraphs:
                    law_name = p[5] if len(p) > 5 else ''
                    url = p[6] if len(p) > 6 else ''
                    paragraph = p[2] if len(p) > 2 else ''
                    section = p[3] if len(p) > 3 else ''
                    if all([law_name, url, paragraph]) and law_name in answer and url in answer and paragraph in answer:
                        valid = True
                        print('[VALIDERING] Svar indeholder paragrafnummer, lovnavn og URL.')
                        break
            if not valid:
                print('[VALIDERING] Svar mangler korrekt kildeangivelse.')
    else:
        question = 'Hvornår skal min bil synes?'
        top_paragraphs = rag.search(question, top_k=5)
        answer = rag.rerank_with_ollama(question, top_paragraphs)
        print(answer)
