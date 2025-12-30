import logging
import time
from typing import Dict, Optional
import os
import hashlib

logger = logging.getLogger(__name__)


class PageWorker:
    def __init__(self, worker_id: int, database, api_client, config: Dict):
        self.worker_id = worker_id
        self.db = database
        self.api = api_client
        self.config = config
        self.min_words = config['crawler'].get('min_words', 500)
        
        self.html_dir = config['output'].get('html_directory', 'html_corpus')
        self.text_dir = config['output'].get('text_directory', 'text_corpus')
        os.makedirs(self.html_dir, exist_ok=True)
        os.makedirs(self.text_dir, exist_ok=True)
        
        self.metadata_file = config['output'].get('metadata_file', 'metadata.csv')
        self._init_metadata_file()
    
    def _init_metadata_file(self):
        if not os.path.exists(self.metadata_file):
            with open(self.metadata_file, 'w', encoding='utf-8') as f:
                f.write('doc_id,title,url,word_count,content_hash,fetched_at\n')
    
    def _append_metadata(self, doc_id: int, title: str, url: str, 
                        word_count: int, content_hash: str):
        import csv
        with open(self.metadata_file, 'a', encoding='utf-8', newline='') as f:
            writer = csv.writer(f)
            writer.writerow([
                doc_id,
                title.replace('"', '""'),
                url,
                word_count,
                content_hash,
                int(time.time())
            ])
    
    def process_next_page(self) -> bool:
        page_data = self.db.get_next_page()
        if not page_data:
            return False
        
        page_id, title = page_data
        
        try:
            logger.debug(f"[Worker {self.worker_id}] Обрабатываем: {title}")
            page_content = self.api.fetch_page_content(title)
            if not page_content:
                self.db.mark_page_failed(page_id, "Не удалось получить содержимое")
                return False

            raw_text = page_content.get('text_content', '')
            cleaned_text = self.api.clean_wiki_text(raw_text)

            words = cleaned_text.split()
            if len(words) < self.min_words:
                self.db.mark_page_failed(page_id, f"Слишком мало слов: {len(words)}")
                return False
            content_hash = self.api.calculate_content_hash(cleaned_text)

            if self.db.is_content_duplicate(content_hash):
                logger.debug(f"[Worker {self.worker_id}] Дубликат: {title}")
                self.db.mark_page_failed(page_id, "Дубликат содержимого")
                return False

            doc_id = self.db.save_document(
                page_id=page_id,
                title=title,
                url=page_content.get('url', ''),
                html_content=page_content.get('html_content', ''),
                text_content=cleaned_text,
                content_hash=content_hash,
                word_count=len(words)
            )
            
            if not doc_id:
                self.db.mark_page_failed(page_id, "Ошибка сохранения в БД")
                return False

            self._save_to_files(doc_id, title, page_content['html_content'], cleaned_text)

            self._append_metadata(doc_id, title, page_content['url'], len(words), content_hash)
            
            logger.info(f"[Worker {self.worker_id}] Сохранено: {title} (ID: {doc_id})")
            return True
            
        except Exception as e:
            error_msg = f"{type(e).__name__}: {str(e)}"
            self.db.mark_page_failed(page_id, error_msg)
            logger.error(f"[Worker {self.worker_id}] Ошибка обработки {title}: {error_msg}")
            return False
    
    def _save_to_files(self, doc_id: int, title: str, html: str, text: str):
        html_file = os.path.join(self.html_dir, f"doc{doc_id:05d}.html")
        with open(html_file, 'w', encoding='utf-8') as f:
            f.write(f"<!-- {title} -->\n")
            f.write(html)
        text_file = os.path.join(self.text_dir, f"doc{doc_id:05d}.txt")
        with open(text_file, 'w', encoding='utf-8') as f:
            f.write(f"{title}\n\n")
            f.write(text)
    
    def run(self, max_documents: Optional[int] = None):
        logger.info(f"[Worker {self.worker_id}] Запущен")
        
        processed = 0
        while True:
            if max_documents:
                stats = self.db.get_statistics()
                if stats.get('documents', 0) >= max_documents:
                    logger.info(f"[Worker {self.worker_id}] Достигнут лимит документов")
                    break
            success = self.process_next_page()
            if not success:
                time.sleep(1)
                continue
            
            processed += 1
        
        logger.info(f"[Worker {self.worker_id}] Завершен. Обработано: {processed}")