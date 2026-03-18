import sqlite3

def audit_paragraphs(db_path='laws.db'):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute('SELECT COUNT(*) FROM paragraphs')
    total = cur.fetchone()[0]
    cur.execute('''
        SELECT id, law_id, law_name, paragraph, section, text, eli_url
        FROM paragraphs
        WHERE law_name IS NULL OR law_name = '' OR law_name = 'UNKNOWN'
           OR paragraph IS NULL OR paragraph = '' OR paragraph = 'UNKNOWN'
           OR eli_url IS NULL OR eli_url = '' OR eli_url = 'UNKNOWN'
    ''')
    missing = cur.fetchall()
    conn.close()
    if not missing:
        print(f'Alle paragraffer har lovnavn, paragrafnummer og URL. Total: {total}')
        return
    print(f"Total paragraphs: {total}")
    print(f"Paragraphs with missing metadata: {len(missing)}")
    for row in missing[:20]:
        print(f"ID: {row[0]}, law_id: {row[1]}, law_name: '{row[2]}', paragraph: '{row[3]}', section: '{row[4]}', eli_url: '{row[6]}'")
    if len(missing) > 20:
        print(f"...and {len(missing)-20} more.")

if __name__ == '__main__':
    audit_paragraphs()
