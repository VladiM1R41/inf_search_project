import requests
import time
import logging
from typing import Optional, Dict, Any, List
import hashlib

logger = logging.getLogger(__name__)


class WikipediaAPI:
    def __init__(self, base_url: str, user_agent: str, delay: float = 0.5, max_retries: int = 3):
        self.base_url = base_url
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': user_agent,
            'Accept': 'application/json',
            'Accept-Encoding': 'gzip, deflate'
        })
        self.delay = delay
        self.max_retries = max_retries
        self.last_request_time = 0
    
    def _wait_if_needed(self):
        elapsed = time.time() - self.last_request_time
        if elapsed < self.delay:
            time.sleep(self.delay - elapsed)
        self.last_request_time = time.time()
    
    def _make_request(self, params: Dict) -> Optional[Dict]:
        for attempt in range(self.max_retries):
            try:
                self._wait_if_needed()
                
                response = self.session.get(
                    self.base_url,
                    params=params,
                    timeout=30
                )

                if response.status_code == 429:
                    wait_time = 2 ** (attempt + 1)
                    logger.warning(f"Слишком много запросов. Ждем {wait_time} сек.")
                    time.sleep(wait_time)
                    continue
                
                response.raise_for_status()
                return response.json()
                
            except requests.exceptions.Timeout:
                logger.warning(f"Таймаут запроса (попытка {attempt + 1}/{self.max_retries})")
                if attempt < self.max_retries - 1:
                    time.sleep(2)
                    
            except requests.exceptions.RequestException as e:
                logger.warning(f"Ошибка запроса (попытка {attempt + 1}/{self.max_retries}): {e}")
                if attempt < self.max_retries - 1:
                    time.sleep(1)
        
        return None
    
    def fetch_category_members(self, category: str) -> List[Dict]:
        all_members = []
        continue_params = {}
        
        while True:
            params = {
                'action': 'query',
                'list': 'categorymembers',
                'cmtitle': f'Category:{category}',
                'cmlimit': 'max',
                'format': 'json'
            }
            params.update(continue_params)
            
            data = self._make_request(params)
            if not data:
                break
            
            members = data.get('query', {}).get('categorymembers', [])
            all_members.extend(members)

            if 'continue' in data:
                continue_params = {
                    'cmcontinue': data['continue']['cmcontinue']
                }
            else:
                break
        
        logger.info(f"Найдено {len(all_members)} статей в категории {category}")
        return all_members
    
    def fetch_page_content(self, title: str) -> Optional[Dict]:
        html_params = {
            'action': 'parse',
            'page': title,
            'prop': 'text',
            'format': 'json',
            'utf8': 1
        }
        
        html_data = self._make_request(html_params)
        if not html_data:
            return None
        text_params = {
            'action': 'query',
            'prop': 'extracts|info',
            'inprop': 'url',
            'explaintext': True,
            'titles': title,
            'format': 'json'
        }
        
        text_data = self._make_request(text_params)
        if not text_data:
            return None
        parse_result = html_data.get('parse', {})
        query_result = text_data.get('query', {}).get('pages', {})
        
        if not parse_result or not query_result:
            return None
        
        page_data = next(iter(query_result.values()))
        
        result = {
            'title': title,
            'html_content': parse_result.get('text', {}).get('*', ''),
            'text_content': page_data.get('extract', ''),
            'url': page_data.get('fullurl', f"https://ru.wikipedia.org/wiki/{title.replace(' ', '_')}"),
            'pageid': page_data.get('pageid', 0)
        }
        
        return result
    
    @staticmethod
    def calculate_content_hash(content: str) -> str:
        return hashlib.sha256(content.encode('utf-8')).hexdigest()
    
    @staticmethod
    def clean_wiki_text(text: str) -> str:
        lines = []
        for line in text.splitlines():
            line = line.strip()
            if not line:
                continue
            if (line.startswith('==') or line.startswith('{{') or 
                line.startswith('[[File:') or line.startswith('[[Категория:')):
                continue
            lines.append(line)
        return '\n'.join(lines)


class CategoryExplorer:
    def __init__(self, database, api_client):
        self.db = database
        self.api = api_client
    
    def explore_from_root(self, root_category: str, max_depth: int = 2):
        logger.info(f"Начинаем обход от категории: {root_category}")
        self.db.add_category(root_category)
        
        processed_categories = set()
        
        while True:
            category = self.db.get_next_category()
            if not category:
                break
            
            if category in processed_categories:
                continue
            
            logger.info(f"Обрабатываем категорию: {category}")
            processed_categories.add(category)
            members = self.api.fetch_category_members(category)
            
            for member in members:
                title = member.get('title', '')
                ns = member.get('ns', 0)
                
                if ns == 0:
                    self.db.add_page(title)
                elif ns == 14:
                    subcat = title.replace('Category:', '').replace('Категория:', '')
                    if subcat not in processed_categories:
                        self.db.add_category(subcat)