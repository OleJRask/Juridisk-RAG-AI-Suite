import os
import subprocess
from pathlib import Path

SEGMENT_SIZE = 2000  # Balanceret segmentstørrelse for hastighed og kvalitet
LLM_MODEL = "mistral"  # Bedre dansk-understøttelse


def run_ollama_prompt(prompt: str, timeout: int = 300):
    # Send prompt via stdin for at undgå meget lange CLI-argumenter på Windows.
    return subprocess.run(
        ["ollama", "run", LLM_MODEL],
        input=prompt,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=timeout,
    )

# Find alle love hvor summary.txt mangler
def find_laws_without_summary(laws_root):
    for root, dirs, files in os.walk(laws_root):
        if "full_text_da.txt" in files and not "summary.txt" in files:
            yield Path(root)

def safe_filename(value: str, fallback: str = "untitled", max_len: int = 60) -> str:
    import re
    value = re.sub(r'[<>:"/|?*\\\x00-\x1f]', "_", (value or "").strip())
    value = re.sub(r"\s+", " ", value).strip(" .")
    return (value or fallback)[:max_len]

def summarize_law(law_dir):
    # Forkort alle dele af stien for at undgå WinError 206
    short_parts = [safe_filename(part, max_len=30) for part in law_dir.parts]
    short_law_dir = Path(*short_parts)
    text_path = short_law_dir / "full_text_da.txt"
    summary_path = short_law_dir / "summary.txt"
    meta_path = short_law_dir / "metadata.json"
    if not text_path.exists():
        return
    full_text = text_path.read_text(encoding="utf-8", errors="replace")
    # Hent metadata
    ressort = ""
    title = ""
    if meta_path.exists():
        import json
        meta = json.load(meta_path.open(encoding="utf-8"))
        csv_row = meta.get("csv_row", {})
        ressort = csv_row.get("Ressort", "")
        title = csv_row.get("Titel", "")
    # Del loven i segmenter
    segments = [full_text[i:i+SEGMENT_SIZE] for i in range(0, len(full_text), SEGMENT_SIZE)]
    segment_summaries = []
    for idx, segment in enumerate(segments):
        seg_prompt = (
            f"Opsummer denne del af loven kort og præcist for en jurist. Brug kun 1-3 sætninger. Undgå gentagelser og unødvendige detaljer.\nSvar udelukkende på dansk. Skriv intet på engelsk.\n\nRessort: {ressort}\nTitel: {title}\nDel {idx+1}/{len(segments)}:\n{segment}"
        )
        print(f"[DEBUG] Kører ollama for segment {idx+1}/{len(segments)} (prompt-længde: {len(seg_prompt)})...")
        try:
            result = run_ollama_prompt(seg_prompt, timeout=300)
            print(f"[DEBUG] ollama returneret for segment {idx+1}/{len(segments)} (rc={result.returncode})")
            if result.stderr:
                print(f"[DEBUG][stderr]: {result.stderr}")
            if result.returncode != 0:
                err = (result.stderr or "").strip() or "ukendt fejl"
                raise RuntimeError(f"Ollama fejlede med rc={result.returncode}: {err}")
            seg_summary = result.stdout
            if isinstance(seg_summary, bytes):
                seg_summary = seg_summary.decode("utf-8", errors="replace")
            seg_summary = seg_summary.strip()
            segment_summaries.append(seg_summary)
        except Exception as e:
            print(f"[DEBUG] Exception i ollama-kald: {e}")
            segment_summaries.append(f"Resumé-fejl: {e}")

    # Dynamisk antal sætninger i samlet summary afhængig af lovens længde
    total_chars = len(full_text)
    # 3-7 sætninger for korte-mellem love, op til 15 for meget lange
    if total_chars < 8000:
        min_sent, max_sent = 3, 7
    elif total_chars < 20000:
        min_sent, max_sent = 6, 10
    else:
        min_sent, max_sent = 10, 15
    summary_prompt = (
        f"Opsummer kort og præcist følgende del-resuméer til ét samlet resumé for loven. Brug kun {min_sent}-{max_sent} sætninger. Undgå gentagelser.\n"
        f"Svar kun på dansk. Brug aldrig engelsk. Hvis du svarer på engelsk, er det en fejl.\n"
        f"Du må ikke oversætte teksten eller bruge engelske ord eller sætninger.\n"
        f"Hvis du ikke kan svare på dansk, skriv: 'Kunne ikke generere resumé på dansk.'\n"
        f"\nRessort: {ressort}\nTitel: {title}\n\nDel-resuméer:\n" + '\n'.join(segment_summaries)
    )
    print(f"[DEBUG] Kører ollama for samlet summary (prompt-længde: {len(summary_prompt)})...")
    try:
        result = run_ollama_prompt(summary_prompt, timeout=300)
        print(f"[DEBUG] ollama returneret for samlet summary (rc={result.returncode})")
        if result.stderr:
            print(f"[DEBUG][stderr]: {result.stderr}")
        if result.returncode != 0:
            err = (result.stderr or "").strip() or "ukendt fejl"
            raise RuntimeError(f"Ollama fejlede med rc={result.returncode}: {err}")
        summary = result.stdout
        if isinstance(summary, bytes):
            summary = summary.decode("utf-8", errors="replace")
        summary = summary.strip()
        print(f"[DEBUG] Samlet summary-output:\n{summary}\n---")
        if summary:
            summary_path.write_text(summary, encoding="utf-8", errors="replace")
            print(f"[DEBUG] Skrev summary til: {summary_path}")
        else:
            print(f"[DEBUG] Tomt summary, skriver fejlbesked til: {summary_path}")
            summary_path.write_text("Resumé kunne ikke genereres.", encoding="utf-8", errors="replace")
    except Exception as e:
        print(f"[DEBUG] Exception i ollama-kald (samlet summary): {e}")
        summary_path.write_text(f"Resumé-fejl: {e}", encoding="utf-8", errors="replace")

import multiprocessing
def worker(law_dir):
    print(f"Genererer summary for: {law_dir}")
    summarize_law(law_dir)

if __name__ == "__main__":
    import sys
    import os
    # Try to auto-detect the correct laws directory if not given as argument
    candidate_dirs = [
        "laws",
        os.path.join("laws_rag", "laws"),
        os.path.join("..", "laws_rag", "laws"),
        os.path.join("..", "..", "laws_rag", "laws"),
        os.path.join("..", "laws"),
    ]
    if len(sys.argv) > 1:
        laws_root = sys.argv[1]
    else:
        laws_root = None
        for candidate in candidate_dirs:
            if os.path.isdir(candidate):
                laws_root = candidate
                break
        if not laws_root:
            raise RuntimeError("Kunne ikke finde en gyldig lov-mappe. Angiv stien som argument eller placer 'laws' eller 'laws_rag/laws' i projektet.")
    print(f"Starter batch summary-generering for love i: {laws_root}")
    law_dirs = list(find_laws_without_summary(laws_root))
    # Brug op til 8 CPU-kerner, men aldrig flere end der er fysisk tilgængelige
    max_procs = 4
    num_processes = min(max_procs, os.cpu_count() or 1)
    print(f"[INFO] Bruger {num_processes} parallelle processer og segment-størrelse {SEGMENT_SIZE} tegn.")
    with multiprocessing.Pool(processes=num_processes) as pool:
        pool.map(worker, law_dirs)
