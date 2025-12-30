import argparse
import yaml
import logging
import sys
import signal
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Any

from database import CrawlerDatabase
from wiki_api import WikipediaAPI, CategoryExplorer
from workers import PageWorker

def setup_logging(log_file: str, verbose: bool = False):
    log_level = logging.DEBUG if verbose else logging.INFO
    
    log_format = '%(asctime)s [%(levelname)s] %(message)s'
    date_format = '%Y-%m-%d %H:%M:%S'
    
    handlers = [
        logging.FileHandler(log_file, encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
    
    logging.basicConfig(
        level=log_level,
        format=log_format,
        datefmt=date_format,
        handlers=handlers
    )
    
    return logging.getLogger(__name__)


def load_config(config_path: str) -> Dict[str, Any]:
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        
        default_config = {
            'database': {'path': 'wiki_crawler.db'},
            'crawler': {
                'root_category': 'Персоналии по алфавиту',
                'max_documents': 35000,
                'min_words': 500,
                'request_delay': 0.5,
                'max_retries': 3,
                'parallel_workers': 4
            },
            'output': {
                'html_directory': 'html_corpus',
                'text_directory': 'text_corpus',
                'metadata_file': 'metadata.csv',
                'log_file': 'crawler.log'
            },
            'api': {
                'base_url': 'https://ru.wikipedia.org/w/api.php',
                'user_agent': 'EducationalBot/1.0 (University Project)'
            }
        }
        
        for key, value in default_config.items():
            if key not in config:
                config[key] = value
            elif isinstance(value, dict):
                config[key] = {**value, **config[key]}
        
        return config
        
    except Exception as e:
        print(f"Ошибка загрузки конфига {config_path}: {e}")
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(
        description='Поисковый робот Wikipedia для ЛР2'
    )
    parser.add_argument(
        'config',
        help='Путь к YAML конфигурационному файлу'
    )
    parser.add_argument(
        '--resume',
        action='store_true',
        help='Продолжить с места остановки (пропустить обход категорий)'
    )
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Подробный вывод логов'
    )
    
    args = parser.parse_args()

    config = load_config(args.config)

    logger = setup_logging(config['output']['log_file'], args.verbose)
    
    logger.info("ЗАПУСК ПОИСКОВОГО РОБОТА WIKIPEDIA")
    logger.info(f"Конфигурационный файл: {args.config}")
    logger.info(f"Корневая категория: {config['crawler']['root_category']}")
    logger.info(f"Максимум документов: {config['crawler']['max_documents']}")
    logger.info(f"Минимум слов: {config['crawler']['min_words']}")
    logger.info(f"Параллельных воркеров: {config['crawler']['parallel_workers']}")
    logger.info(f"Задержка запросов: {config['crawler']['request_delay']} сек.")
    logger.info(f"Режим продолжения: {'ДА' if args.resume else 'НЕТ'}")
    logger.info("=" * 70)

    db = CrawlerDatabase(config['database']['path'])
    
    api_client = WikipediaAPI(
        base_url=config['api']['base_url'],
        user_agent=config['api']['user_agent'],
        delay=config['crawler']['request_delay'],
        max_retries=config['crawler']['max_retries']
    )

    if not args.resume:
        logger.info("\n[ФАЗА 1] Обход категорий Wikipedia...")
        
        explorer = CategoryExplorer(db, api_client)
        
        try:
            explorer.explore_from_root(config['crawler']['root_category'])
        except KeyboardInterrupt:
            logger.warning("\nОбход категорий прерван пользователем")
            logger.info("Для продолжения запустите с флагом --resume")
            db.close()
            sys.exit(0)
    else:
        logger.info("\n[ФАЗА 1] Пропущена (режим --resume)")
    
    logger.info("\n[ФАЗА 2] Обработка страниц Wikipedia...")
    
    workers_count = config['crawler']['parallel_workers']
    max_docs = config['crawler']['max_documents']
    
    logger.info(f"Запуск {workers_count} параллельных воркеров...")
    
    def signal_handler(sig, frame):
        logger.warning("\nПолучен сигнал прерывания. Завершение работы...")
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    
    try:
        with ThreadPoolExecutor(max_workers=workers_count) as executor:
            futures = []
            for i in range(workers_count):
                worker = PageWorker(i + 1, db, api_client, config)
                future = executor.submit(worker.run, max_docs)
                futures.append(future)
            
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"Ошибка в воркере: {e}")
                    
    except KeyboardInterrupt:
        logger.warning("\nОбработка прервана пользователем")
        logger.info("Состояние сохранено. Для продолжения запустите с --resume")
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}")
    
    logger.info("ФИНАЛЬНАЯ СТАТИСТИКА")
    
    stats = db.get_statistics()
    logger.info(f"Всего страниц обнаружено: {stats.get('total_pages', 0)}")
    logger.info(f"Успешно обработано: {stats.get('successful', 0)}")
    logger.info(f"Неудачных попыток: {stats.get('failed', 0)}")
    logger.info(f"Сохранено документов: {stats.get('documents', 0)}")
    logger.info(f"Обработано категорий: {stats.get('categories', 0)}")
    
    logger.info(f"\nБаза данных: {config['database']['path']}")
    logger.info(f"Файл логов: {config['output']['log_file']}")
    logger.info(f"HTML документы: {config['output']['html_directory']}/")
    logger.info(f"Текстовые документы: {config['output']['text_directory']}/")
    logger.info(f"Метаданные: {config['output']['metadata_file']}")
    
    db.close()


if __name__ == "__main__":
    main()