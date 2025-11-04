import os
from dotenv import load_dotenv

# Загружаем переменные окружения
load_dotenv()

class Config:
    """Конфигурация приложения"""
    
    # Telegram API
    API_ID = os.getenv('API_ID')
    API_HASH = os.getenv('API_HASH')
    PHONE = os.getenv('PHONE')
    BOT_TOKEN = os.getenv('BOT_TOKEN')  # Optional: for better member count accuracy
    
    # Настройки сессии
    SESSION_NAME = os.getenv('SESSION_NAME', 'group_parser_session')
    
    # Настройки скорости запросов
    DELAY_BETWEEN_REQUESTS = float(os.getenv('DELAY_BETWEEN_REQUESTS', 2))
    MAX_RETRIES = int(os.getenv('MAX_RETRIES', 3))
    
    # Настройки файлов
    INPUT_FILE = os.getenv('INPUT_FILE', 'input/groups.csv')
    OUTPUT_SUFFIX = os.getenv('OUTPUT_SUFFIX', '_enhanced')
    UNIFIED_OUTPUT_FILE = os.getenv('UNIFIED_OUTPUT_FILE', 'groups_enhanced.csv')
    
    # Директории проекта
    BASE_DIR = os.path.dirname(os.path.dirname(__file__))
    INPUT_DIR = os.path.join(BASE_DIR, 'input')
    OUTPUT_DIR = os.path.join(BASE_DIR, 'output')
    LOGS_DIR = os.path.join(BASE_DIR, 'logs')
    PROCESSED_FILES_LOG = os.path.join(BASE_DIR, 'logs', 'processed_files.log')
    
    @classmethod
    def validate(cls):
        """Проверяет наличие обязательных настроек"""
        errors = []
        
        if not cls.API_ID:
            errors.append("API_ID не установлен")
        if not cls.API_HASH:
            errors.append("API_HASH не установлен")
        if not cls.PHONE:
            errors.append("PHONE не установлен")
            
        return errors
    
    @classmethod
    def get_input_file_path(cls):
        """Возвращает полный путь к входному файлу"""
        return os.path.join(cls.BASE_DIR, cls.INPUT_FILE)
    
    @classmethod
    def get_output_file_path(cls, input_filename=None):
        """Возвращает полный путь к выходному файлу"""
        if input_filename:
            base_name = os.path.splitext(input_filename)[0]
            output_filename = f"{base_name}{cls.OUTPUT_SUFFIX}.csv"
            return os.path.join(cls.OUTPUT_DIR, output_filename)
        else:
            # Единый выходной файл для всех входных файлов
            return os.path.join(cls.OUTPUT_DIR, cls.UNIFIED_OUTPUT_FILE)
