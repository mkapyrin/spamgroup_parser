#!/usr/bin/env python3
"""
Telegram Group Parser - Основной скрипт
Парсинг информации о группах Telegram из CSV файла
"""

import os
import sys
import argparse
import asyncio
from pathlib import Path

# Добавляем src в путь
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from src.config import Config
from src.telegram_parser import TelegramGroupParser
from src.logger_config import setup_logging

async def main():
    """Основная функция"""
    
    parser = argparse.ArgumentParser(
        description="Парсер информации о Telegram группах",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры использования:
  python main.py                              # Обработать input/groups.csv
  python main.py --input custom.csv           # Обработать custom.csv
  python main.py --input data.csv --output result.csv  # Задать выходной файл
  python main.py --verbose                    # Подробный вывод
        """
    )
    
    parser.add_argument(
        '--input', '-i',
        default=None,
        help='Путь к входному CSV файлу (по умолчанию из .env)'
    )
    
    parser.add_argument(
        '--output', '-o',
        default=None,
        help='Путь к выходному CSV файлу (генерируется автоматически если не указан)'
    )
    
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Подробный вывод (DEBUG уровень)'
    )
    
    parser.add_argument(
        '--check-config',
        action='store_true',
        help='Проверить конфигурацию и выйти'
    )
    
    parser.add_argument(
        '--all-files', '-a',
        action='store_true',
        help='Обработать все CSV файлы из директории input/'
    )
    
    args = parser.parse_args()
    
    # Настройка логирования
    log_level = 'DEBUG' if args.verbose else 'INFO'
    logger = setup_logging()
    
    logger.info("🚀 Telegram Group Parser запущен")
    logger.info("=" * 50)
    
    try:
        # Проверяем конфигурацию
        config_errors = Config.validate()
        if config_errors:
            logger.error("❌ Ошибки конфигурации:")
            for error in config_errors:
                logger.error(f"  - {error}")
            logger.error("\n💡 Создайте файл .env на основе .env.example и заполните его")
            return 1
        
        if args.check_config:
            logger.info("✅ Конфигурация корректна")
            return 0
        
        # Определяем входной файл
        if args.input:
            input_file = args.input
            if not os.path.isabs(input_file):
                input_file = os.path.join(Config.BASE_DIR, input_file)
        else:
            input_file = Config.get_input_file_path()
        
        # Проверяем существование входного файла
        if not os.path.exists(input_file):
            logger.error(f"❌ Входной файл не найден: {input_file}")
            return 1
        
        # Определяем выходной файл
        output_file = args.output
        if output_file and not os.path.isabs(output_file):
            output_file = os.path.join(Config.OUTPUT_DIR, output_file)
        
        logger.info(f"📂 Входной файл: {input_file}")
        logger.info(f"📂 Выходной файл: {output_file or 'автоматически'}")
        logger.info(f"⏱️  Задержка между запросами: {Config.DELAY_BETWEEN_REQUESTS}s")
        logger.info(f"🔄 Максимум повторов: {Config.MAX_RETRIES}")
        
        # Создаем директории если нужно
        os.makedirs(Config.OUTPUT_DIR, exist_ok=True)
        os.makedirs(Config.LOGS_DIR, exist_ok=True)
        
        # Запускаем парсер
        async with TelegramGroupParser() as parser:
            if args.all_files:
                # Обрабатываем все файлы из input/
                result = await parser.process_all_input_files()
                
                logger.info("=" * 50)
                logger.info("🎉 Обработка всех файлов завершена!")
                logger.info(f"📊 Статистика объединения:")
                logger.info(f"  - Объединено файлов: {result.get('merged_files', 0)}")
                logger.info(f"  - Добавлено новых групп: {result.get('added_groups', 0)}")
                logger.info(f"  - Пропущено дубликатов: {result.get('skipped_groups', 0)}")
                logger.info(f"  - Всего групп в groups.csv: {result.get('total_groups_in_groups_csv', 0)}")
                logger.info(f"")
                logger.info(f"📡 Статистика обработки через API:")
                logger.info(f"  - Всего записей: {result.get('api_total', 0)}")
                logger.info(f"  - Успешно: {result.get('api_successful', 0)}")
                logger.info(f"  - Пропущено: {result.get('api_skipped', 0)}")
                logger.info(f"  - Доступ запрещен: {result.get('api_access_denied', 0)}")
                logger.info(f"  - Ошибки: {result.get('api_errors', 0)}")
                logger.info(f"💾 Результат: {result.get('output_file', 'N/A')}")
            else:
                # Обрабатываем один файл
                result = await parser.process_csv_file(input_file, output_file)
                
                logger.info("=" * 50)
                logger.info("🎉 Обработка завершена!")
                logger.info(f"📊 Статистика:")
                logger.info(f"  - Всего записей: {result['total']}")
                logger.info(f"  - Успешно: {result['successful']}")
                logger.info(f"  - Доступ запрещен: {result['access_denied']}")
                logger.info(f"  - Ошибки: {result['errors']}")
                logger.info(f"💾 Результат: {result['output_file']}")
            
            return 0
    
    except KeyboardInterrupt:
        logger.warning("⚠️  Прервано пользователем")
        return 1
    
    except Exception as e:
        logger.error(f"💥 Критическая ошибка: {e}")
        if args.verbose:
            import traceback
            logger.error(traceback.format_exc())
        return 1

def check_dependencies_on_startup():
    """Проверяет критические зависимости при запуске"""
    critical_modules = ['telethon', 'pandas', 'dotenv', 'aiohttp']
    missing = []
    
    for module in critical_modules:
        try:
            if module == 'dotenv':
                __import__('dotenv')
            else:
                __import__(module)
        except ImportError:
            missing.append(module)
    
    if missing:
        print("❌ Отсутствуют критические зависимости:")
        for module in missing:
            print(f"   - {module}")
        print("\n💡 Установите зависимости:")
        print("   pip install -r requirements.txt")
        return False
    
    return True

if __name__ == "__main__":
    # Проверяем версию Python
    if sys.version_info < (3, 7):
        print("❌ Требуется Python 3.7 или новее")
        sys.exit(1)
    
    # Проверяем критические зависимости
    if not check_dependencies_on_startup():
        sys.exit(1)
    
    # Запускаем основную функцию
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
