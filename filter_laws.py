import csv

INPUT_CSV = "CurrentLaws.csv"
OUTPUT_CSV = "FilteredLaws.csv"
RESSORT_FILTER = "social- og boligministeriet"
YEAR_MIN = 2000


def open_csv_with_fallback(input_csv):
    encodings = ["utf-8-sig", "utf-16", "latin1"]
    last_error = None
    for enc in encodings:
        try:
            f = open(input_csv, encoding=enc)
            # Try reading a line to trigger decode errors early
            f.readline()
            f.seek(0)
            print(f"[INFO] Læser {input_csv} med encoding: {enc}")
            return f
        except UnicodeDecodeError as e:
            last_error = e
            continue
    raise UnicodeDecodeError(f"Kunne ikke læse {input_csv} med kendte encodings. Sidste fejl: {last_error}")

try:
    with open_csv_with_fallback(INPUT_CSV) as infile, open(OUTPUT_CSV, "w", encoding="utf-8", newline="") as outfile:
        reader = csv.DictReader(infile, delimiter=";", quotechar='"')
        writer = csv.DictWriter(outfile, fieldnames=reader.fieldnames, delimiter=";", quotechar='"')
        writer.writeheader()
        count = 0
        for row in reader:
            ressort = (row.get("Ressort") or "").strip().lower()
            try:
                year = int((row.get("År") or "0").strip())
            except Exception:
                year = 0
            if ressort == RESSORT_FILTER and year >= YEAR_MIN:
                writer.writerow(row)
                count += 1
        print(f"[INFO] Skrev {count} love til {OUTPUT_CSV}")
except UnicodeDecodeError as e:
    print(f"[ERROR] Kunne ikke læse {INPUT_CSV}: {e}")
    exit(1)
