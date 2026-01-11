import argparse
import requests
import time
import os
import sqlite3
import hashlib
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

API_URL = "https://ru.wikipedia.org/w/api.php"
HEADERS = {"User-Agent": "VDA educational"}

MIN_WORDS = 500
REQUEST_DELAY = 0.5  
MAX_RETRIES = 3
WORKERS = 8
DB_PATH = "fetch_state.db"
METADATA_CSV = "metadata.csv"
CORPUS_DIR = "corpus"


def open_db(path=DB_PATH):
    conn = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA temp_store=MEMORY;")
    return conn

def init_db(conn):
    cur = conn.cursor()
    cur.executescript("""
    CREATE TABLE IF NOT EXISTS pages (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        title TEXT UNIQUE,
        processed INTEGER DEFAULT 0, -- 0 new, 1 done, 2 in-progress
        sha1 TEXT,
        word_count INTEGER,
        source_url TEXT,
        doc_id INTEGER
    );
    CREATE INDEX IF NOT EXISTS idx_pages_processed ON pages(processed);
    CREATE TABLE IF NOT EXISTS categories_queue (
        cat TEXT PRIMARY KEY,
        enqueued INTEGER DEFAULT 0
    );
    CREATE TABLE IF NOT EXISTS state (
        k TEXT PRIMARY KEY,
        v TEXT
    );
    """)
    cur.execute("INSERT OR IGNORE INTO state(k,v) VALUES('next_doc_id','1')")
    conn.commit()

def api_get(session, params, retry=MAX_RETRIES):
    params = dict(params)
    params["format"] = "json"
    for attempt in range(retry):
        try:
            r = session.get(API_URL, params=params, headers=HEADERS, timeout=30)
            r.raise_for_status()
            time.sleep(REQUEST_DELAY)
            return r.json()
        except Exception as e:
            wait = 0.5 * (2 ** attempt)
            print(f"[!] API request failed (attempt {attempt+1}/{retry}): {e}. retry in {wait:.1f}s", file=sys.stderr)
            time.sleep(wait)
    raise RuntimeError("API requests repeatedly failed")

def bfs_enqueue_categories(root_category, conn, max_cats=100000):

    q = [root_category]
    cur = conn.cursor()

    cur.execute("INSERT OR IGNORE INTO categories_queue(cat,enqueued) VALUES(?,0)", (root_category,))
    conn.commit()

    while True:
        cur.execute("SELECT cat FROM categories_queue WHERE enqueued=0 LIMIT 1")
        row = cur.fetchone()
        if not row:
            break
        cat = row[0]

        cur.execute("UPDATE categories_queue SET enqueued=1 WHERE cat=?", (cat,))
        conn.commit()

        session = requests.Session()
        cont = {}
        while True:
            params = {
                "action": "query",
                "list": "categorymembers",
                "cmtitle": f"Category:{cat}",
                "cmlimit": "500"
            }
            params.update(cont)
            data = api_get(session, params)
            for item in data.get("query", {}).get("categorymembers", []):
                ns = item.get("ns", 0)
                title = item.get("title")
                if ns == 0:

                    try:
                        cur.execute("INSERT OR IGNORE INTO pages(title) VALUES(?)", (title,))
                    except Exception:
                        pass
                elif ns == 14:

                    subcat = title.replace("Category:", "")
                    try:
                        cur.execute("INSERT OR IGNORE INTO categories_queue(cat,enqueued) VALUES(?,0)", (subcat,))
                    except Exception:
                        pass
            conn.commit()
            if "continue" not in data:
                break
            cont = data["continue"]
        session.close()
    cur.close()

def fetch_article_and_save(session, conn, title, min_words=MIN_WORDS, outdir=CORPUS_DIR):

    params = {"action": "query", "prop": "extracts|info", "inprop": "url", "explaintext": True, "titles": title}
    data = api_get(session, params)
    pages = data.get("query", {}).get("pages", {})
    page = next(iter(pages.values()))
    extract = page.get("extract")
    fullurl = page.get("fullurl", "")
    if not extract:
        return False

    lines = []
    for line in extract.splitlines():
        s = line.strip()
        if not s:
            continue
        if s.startswith("==") or s.startswith("{{") or s.startswith("[[File:") or s.startswith("[[Category:"):
            continue
        lines.append(s)
    text = "\n".join(lines)
    words = text.split()
    if len(words) < min_words:
        return False

    sha1 = hashlib.sha1(text.encode("utf-8")).hexdigest()

    cur = conn.cursor()
    cur.execute("SELECT doc_id FROM pages WHERE sha1=?", (sha1,))
    if cur.fetchone():
        return False 

    cur.execute("BEGIN IMMEDIATE")
    cur.execute("SELECT v FROM state WHERE k='next_doc_id'")
    row = cur.fetchone()
    if not row:
        next_doc = 1
        cur.execute("INSERT OR REPLACE INTO state(k,v) VALUES('next_doc_id',?)", (str(next_doc+1),))
    else:
        next_doc = int(row[0])
        cur.execute("UPDATE state SET v=? WHERE k='next_doc_id'", (str(next_doc+1),))

    cur.execute("UPDATE pages SET processed=1, sha1=?, word_count=?, source_url=?, doc_id=? WHERE title=?",
                (sha1, len(words), fullurl, next_doc, title))
    conn.commit()
    cur.close()

    os.makedirs(outdir, exist_ok=True)
    filename = os.path.join(outdir, f"doc{next_doc:05d}.txt")
    with open(filename, "w", encoding="utf-8") as f:
        f.write(title + "\n\n")
        f.write(text)

    with metadata_lock:
        write_header = not os.path.exists(METADATA_CSV)
        with open(METADATA_CSV, "a", encoding="utf-8", newline="") as mf:
            if write_header:
                mf.write("doc_id,title,source_url,word_count\n")

            safe_title = title.replace('"', '""')
            mf.write(f'{next_doc},"{safe_title}",{fullurl},{len(words)}\n')

    return True

metadata_lock = threading.Lock()

def worker_main(db_path, worker_id, min_words, request_delay, max_docs):
    session = requests.Session()
    conn = open_db(db_path)
    while True:
        cur = conn.cursor()
        cur.execute("BEGIN IMMEDIATE")
        cur.execute("SELECT id, title FROM pages WHERE processed=0 LIMIT 1")
        row = cur.fetchone()
        if not row:
            conn.commit()
            cur.close()
            break
        rowid, title = row
        cur.execute("UPDATE pages SET processed=2 WHERE id=? AND processed=0", (rowid,))
        if cur.rowcount != 1:
            conn.commit()
            cur.close()
            continue 
        conn.commit()
        cur.close()

        try:
            success = fetch_article_and_save(session, conn, title, min_words=min_words, outdir=CORPUS_DIR)
            if success:
                print(f"[W{worker_id}] Saved: {title}")
            else:

                cur2 = conn.cursor()
                cur2.execute("UPDATE pages SET processed=1 WHERE title=?", (title,))
                conn.commit()
                cur2.close()
                print(f"[W{worker_id}] Skipped: {title}")
        except Exception as e:

            cur3 = conn.cursor()
            cur3.execute("UPDATE pages SET processed=0 WHERE title=?", (title,))
            conn.commit()
            cur3.close()
            print(f"[W{worker_id}] Error processing {title}: {e}", file=sys.stderr)

        cur4 = conn.cursor()
        cur4.execute("SELECT v FROM state WHERE k='next_doc_id'")
        next_doc = int(cur4.fetchone()[0])
        cur4.close()
        if max_docs and (next_doc - 1) >= max_docs:
            break

    conn.close()
    session.close()

def count_processed_docs(conn):
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM pages WHERE doc_id IS NOT NULL")
    c = cur.fetchone()[0]
    cur.close()
    return c

def main():
    global DB_PATH, CORPUS_DIR, MIN_WORDS, WORKERS, REQUEST_DELAY
    
    parser = argparse.ArgumentParser(description="Optimized Wikipedia corpus fetcher (resume-capable)")
    parser.add_argument("--category", required=True, help="Root Wikipedia category (without 'Category:')")
    parser.add_argument("--max-docs", type=int, default=30000)
    parser.add_argument("--outdir", default=CORPUS_DIR)
    parser.add_argument("--min-words", type=int, default=MIN_WORDS)
    parser.add_argument("--workers", type=int, default=WORKERS)
    parser.add_argument("--db", default=DB_PATH)
    parser.add_argument("--request-delay", type=float, default=REQUEST_DELAY)
    args = parser.parse_args()

    DB_PATH = args.db
    CORPUS_DIR = args.outdir
    MIN_WORDS = args.min_words
    WORKERS = args.workers
    REQUEST_DELAY = args.request_delay

    conn = open_db(DB_PATH)
    init_db(conn)

    print("[*] Enqueueing categories and discovering page titles (this may take some minutes)...")
    bfs_enqueue_categories(args.category, conn)

    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM pages")
    total_candidates = cur.fetchone()[0]
    print(f"[*] Discovered {total_candidates} candidate pages (titles in DB).")

    print(f"[*] Starting {WORKERS} worker(s) to fetch articles...")
    futures = []
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        for wid in range(WORKERS):
            futures.append(ex.submit(worker_main, DB_PATH, wid+1, MIN_WORDS, REQUEST_DELAY, args.max_docs))

        try:
            for f in as_completed(futures):
                _ = f.result()
        except KeyboardInterrupt:
            print("[*] Interrupted by user, shutting down...", file=sys.stderr)

    processed = count_processed_docs(conn)
    conn.close()
    print(f"[*] Done. Documents saved: {processed}")

if __name__ == "__main__":
    main()
