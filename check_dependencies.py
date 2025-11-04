#!/usr/bin/env python3
"""
Скрипт для проверки зависимостей и окружения
Проверяет наличие всех необходимых библиотек и работу в venv
"""

import sys
import os

def check_python_version():
    """Проверяет версию Python"""
    if sys.version_info < (3, 7):
        print("❌ Требуется Python 3.7 или новее")
        print(f"   Текущая версия: {sys.version}")
        return False
    print(f"✅ Python версия: {sys.version.split()[0]}")
    return True

def check_venv():
    """Проверяет, работает ли код в виртуальном окружении"""
    in_venv = hasattr(sys, 'real_prefix') or (
        hasattr(sys, 'base_prefix') and sys.base_prefix != sys.prefix
    )
    
    if in_venv:
        print(f"✅ Работает в виртуальном окружении: {sys.prefix}")
        return True
    else:
        print("⚠️  Не обнаружено виртуальное окружение")
        print("   Рекомендуется использовать venv для изоляции зависимостей")
        print("   Создайте venv: python3 -m venv venv")
        print("   Активируйте: source venv/bin/activate (Linux/Mac) или venv\\Scripts\\activate (Windows)")
        return False

def check_dependencies():
    """Проверяет наличие всех необходимых зависимостей"""
    required_packages = {
        'telethon': 'Telethon',
        'pandas': 'pandas',
        'dotenv': 'python-dotenv',
        'asyncio_throttle': 'asyncio-throttle',
        'tqdm': 'tqdm',
        'aiohttp': 'aiohttp',
        'colorama': 'colorama',
    }
    
    missing_packages = []
    installed_packages = []
    
    for module_name, package_name in required_packages.items():
        try:
            # Особый случай для dotenv - модуль называется dotenv
            if module_name == 'dotenv':
                __import__('dotenv')
            else:
                __import__(module_name)
            installed_packages.append(package_name)
            print(f"✅ {package_name} установлен")
        except ImportError:
            missing_packages.append(package_name)
            print(f"❌ {package_name} не установлен")
    
    if missing_packages:
        print("\n📦 Для установки отсутствующих пакетов выполните:")
        print(f"   pip install {' '.join(missing_packages)}")
        print("   или")
        print("   pip install -r requirements.txt")
        return False
    
    return True

def check_config_files():
    """Проверяет наличие необходимых файлов конфигурации"""
    config_files = {
        '.env': 'Файл с переменными окружения (API_ID, API_HASH, PHONE)',
        'requirements.txt': 'Файл с зависимостями',
    }
    
    missing_files = []
    
    for file_name, description in config_files.items():
        if os.path.exists(file_name):
            print(f"✅ {file_name} существует")
        else:
            missing_files.append((file_name, description))
            print(f"⚠️  {file_name} не найден - {description}")
    
    if missing_files:
        print("\n💡 Создайте отсутствующие файлы:")
        for file_name, description in missing_files:
            print(f"   - {file_name}: {description}")
        return False
    
    return True

def check_directories():
    """Проверяет наличие необходимых директорий"""
    required_dirs = ['input', 'output', 'logs']
    
    missing_dirs = []
    
    for dir_name in required_dirs:
        if os.path.exists(dir_name):
            print(f"✅ Директория {dir_name}/ существует")
        else:
            missing_dirs.append(dir_name)
            print(f"⚠️  Директория {dir_name}/ не найдена")
    
    if missing_dirs:
        print("\n📁 Создайте отсутствующие директории:")
        for dir_name in missing_dirs:
            os.makedirs(dir_name, exist_ok=True)
            print(f"   ✅ Создана директория {dir_name}/")
    
    return True

def main():
    """Основная функция проверки"""
    print("🔍 Проверка окружения и зависимостей\n")
    print("=" * 50)
    
    all_checks = []
    
    print("\n1. Проверка версии Python:")
    all_checks.append(check_python_version())
    
    print("\n2. Проверка виртуального окружения:")
    venv_check = check_venv()
    all_checks.append(True)  # Не критично, только предупреждение
    
    print("\n3. Проверка зависимостей:")
    all_checks.append(check_dependencies())
    
    print("\n4. Проверка файлов конфигурации:")
    config_check = check_config_files()
    all_checks.append(True)  # Не критично, только предупреждение
    
    print("\n5. Проверка директорий:")
    all_checks.append(check_directories())
    
    print("\n" + "=" * 50)
    
    if all(all_checks[:2] + all_checks[2:3] + all_checks[4:]):  # Критичные проверки
        print("\n✅ Все критические проверки пройдены!")
        if not venv_check:
            print("⚠️  Рекомендуется использовать venv")
        if not config_check:
            print("⚠️  Создайте файл .env с настройками")
        return 0
    else:
        print("\n❌ Некоторые проверки не пройдены")
        print("   Устраните ошибки перед запуском программы")
        return 1

if __name__ == "__main__":
    sys.exit(main())

