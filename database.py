import sqlite3
import threading
import json
from datetime import datetime
from typing import Optional, Dict, Any, List
import logging

logger = logging.getLogger(__name__)


class CrawlerDatabase:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.connection = self._create_connection()
        self.lock = threading.RLock()
        self._setup_tables()
    
    def _create_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(
            self.db_path,
            check_same_thread=False,
            isolation_level=None
        )
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA foreign_keys=ON;")
        return conn
    
    def _setup_tables(self):
        with self.lock:
            cursor = self.connection.cursor()
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS categories (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT UNIQUE NOT NULL,
                    processed BOOLEAN DEFAULT FALSE,
                    added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS pages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    title TEXT UNIQUE NOT NULL,
                    status INTEGER DEFAULT 0,  -- 0=ожидание, 1=успех, 2=ошибка, 3=в процессе
                    attempts INTEGER DEFAULT 0,
                    last_error TEXT,
                    discovered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    processed_at TIMESTAMP
                )
            """)
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS documents (
                    doc_id INTEGER PRIMARY KEY,
                    page_id INTEGER REFERENCES pages(id),
                    url TEXT NOT NULL,
                    html_content TEXT NOT NULL,
                    text_content TEXT NOT NULL,
                    title TEXT NOT NULL,
                    source_name TEXT DEFAULT 'Wikipedia',
                    content_hash TEXT NOT NULL,
                    word_count INTEGER,
                    fetched_at INTEGER,
                    last_modified TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS crawler_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    level TEXT,
                    message TEXT,
                    details TEXT
                )
            """)
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS crawler_settings (
                    key TEXT PRIMARY KEY,
                    value TEXT,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            cursor.execute("""
                INSERT OR IGNORE INTO crawler_settings (key, value) 
                VALUES ('next_doc_id', '1')
            """)
            
            self.connection.commit()
    
    def add_category(self, category_name: str) -> bool:
        try:
            with self.lock:
                cursor = self.connection.cursor()
                cursor.execute(
                    "INSERT OR IGNORE INTO categories (name) VALUES (?)",
                    (category_name,)
                )
                self.connection.commit()
                return cursor.rowcount > 0
        except Exception as e:
            logger.error(f"Ошибка добавления категории {category_name}: {e}")
            return False
    
    def add_page(self, title: str) -> bool:
        try:
            with self.lock:
                cursor = self.connection.cursor()
                cursor.execute(
                    "INSERT OR IGNORE INTO pages (title) VALUES (?)",
                    (title,)
                )
                self.connection.commit()
                return cursor.rowcount > 0
        except Exception as e:
            logger.debug(f"Страница уже существует: {title}")
            return False
    
    def get_next_category(self) -> Optional[str]:
        with self.lock:
            cursor = self.connection.cursor()
            cursor.execute(
                "SELECT name FROM categories WHERE processed = FALSE ORDER BY id LIMIT 1"
            )
            row = cursor.fetchone()
            if row:
                category = row[0]
                cursor.execute(
                    "UPDATE categories SET processed = TRUE WHERE name = ?",
                    (category,)
                )
                self.connection.commit()
                return category
            return None
    
    def get_next_page(self) -> Optional[tuple]:
        with self.lock:
            cursor = self.connection.cursor()
            cursor.execute("BEGIN IMMEDIATE TRANSACTION")

            try:
                cursor.execute("""
                    SELECT id, title FROM pages 
                    WHERE status IN (0, 2) 
                    ORDER BY id ASC 
                    LIMIT 1
                """)
                row = cursor.fetchone()

                if not row:
                    cursor.execute("COMMIT") 
                    return None

                page_id, title = row

                cursor.execute("""
                    UPDATE pages 
                    SET status = 3, attempts = attempts + 1 
                    WHERE id = ? AND status IN (0, 2)
                """, (page_id,))

                if cursor.rowcount == 1:
                    self.connection.commit()
                    return page_id, title
                else:
                    cursor.execute("ROLLBACK")
                    return None

            except Exception as e:
                cursor.execute("ROLLBACK") 
                logger.error(f"Ошибка при атомарном получении страницы: {e}")
                return None
    
    def save_document(self, page_id: int, title: str, url: str, 
                     html_content: str, text_content: str, 
                     content_hash: str, word_count: int) -> Optional[int]:
        try:
            with self.lock:
                cursor = self.connection.cursor()
                
                cursor.execute(
                    "SELECT value FROM crawler_settings WHERE key = 'next_doc_id'"
                )
                row = cursor.fetchone()
                if not row:
                    return None
                
                doc_id = int(row[0])
                
                cursor.execute("""
                    INSERT INTO documents 
                    (doc_id, page_id, url, html_content, text_content, title, 
                     content_hash, word_count, fetched_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (doc_id, page_id, url, html_content, text_content, 
                      title, content_hash, word_count, int(datetime.now().timestamp())))
                
                cursor.execute(
                    "UPDATE crawler_settings SET value = ? WHERE key = 'next_doc_id'",
                    (str(doc_id + 1),)
                )
                
                cursor.execute(
                    "UPDATE pages SET status = 1, processed_at = CURRENT_TIMESTAMP WHERE id = ?",
                    (page_id,)
                )
                
                self.connection.commit()
                return doc_id
                
        except Exception as e:
            logger.error(f"Ошибка сохранения документа {title}: {e}")
            return None
    
    def mark_page_failed(self, page_id: int, error_message: str):
        with self.lock:
            cursor = self.connection.cursor()
            cursor.execute(
                "UPDATE pages SET status = 2, last_error = ? WHERE id = ?",
                (error_message[:500], page_id)
            )
            self.connection.commit()
    
    def is_content_duplicate(self, content_hash: str) -> bool:
        with self.lock:
            cursor = self.connection.cursor()
            cursor.execute(
                "SELECT 1 FROM documents WHERE content_hash = ? LIMIT 1",
                (content_hash,)
            )
            return cursor.fetchone() is not None
    
    def get_statistics(self) -> Dict[str, Any]:
        with self.lock:
            cursor = self.connection.cursor()
            
            stats = {}
            
            cursor.execute("SELECT COUNT(*) FROM pages")
            stats['total_pages'] = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM pages WHERE status = 1")
            stats['successful'] = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM pages WHERE status = 2")
            stats['failed'] = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM documents")
            stats['documents'] = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM categories")
            stats['categories'] = cursor.fetchone()[0]
            
            return stats
    
    def close(self):
        self.connection.close()