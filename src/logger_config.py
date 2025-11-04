import os
import logging
from datetime import datetime
from colorama import init, Fore, Style

# Инициализация colorama для цветного вывода
init(autoreset=True)

def setup_logging(log_level=logging.INFO):
    """Настройка логирования в файл и консоль"""
    
    # Создаем директорию для логов если её нет
    log_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'logs')
    os.makedirs(log_dir, exist_ok=True)
    
    # Имя файла лога с текущей датой
    log_filename = f"telegram_parser_{datetime.now().strftime('%Y%m%d')}.log"
    log_filepath = os.path.join(log_dir, log_filename)
    
    # Настройка форматирования
    formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Настройка корневого логгера
    logger = logging.getLogger()
    logger.setLevel(log_level)
    
    # Очищаем существующие обработчики
    logger.handlers.clear()
    
    # Обработчик для файла
    file_handler = logging.FileHandler(log_filepath, encoding='utf-8')
    file_handler.setFormatter(formatter)
    file_handler.setLevel(log_level)
    logger.addHandler(file_handler)
    
    # Обработчик для консоли с цветами
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(ColoredFormatter())
    console_handler.setLevel(log_level)
    logger.addHandler(console_handler)
    
    return logger

class ColoredFormatter(logging.Formatter):
    """Цветной форматтер для консольного вывода"""
    
    COLORS = {
        'DEBUG': Fore.CYAN,
        'INFO': Fore.GREEN,
        'WARNING': Fore.YELLOW,
        'ERROR': Fore.RED,
        'CRITICAL': Fore.MAGENTA + Style.BRIGHT,
    }
    
    def format(self, record):
        # Применяем цвет к уровню логирования
        levelname = record.levelname
        if levelname in self.COLORS:
            record.levelname = f"{self.COLORS[levelname]}{levelname}{Style.RESET_ALL}"
        
        # Форматируем сообщение
        formatted = f"{Fore.BLUE}{record.asctime}{Style.RESET_ALL} - {record.levelname} - {record.getMessage()}"
        return formatted

def log_separator(logger, text=""):
    """Добавляет разделительную линию в лог"""
    separator = "=" * 50
    if text:
        logger.info(f"{separator} {text} {separator}")
    else:
        logger.info(separator)

def log_progress(current, total, item_name="элементов"):
    """Логирует прогресс обработки"""
    percentage = (current / total) * 100 if total > 0 else 0
    return f"[{current}/{total}] ({percentage:.1f}%) {item_name}"
