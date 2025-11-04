#!/usr/bin/env python3
"""
Утилита для работы с CSV файлами групп
Позволяет анализировать и подготавливать данные
"""

import pandas as pd
import argparse
import os
import sys

def analyze_csv(file_path):
    """Анализирует структуру CSV файла"""
    try:
        df = pd.read_csv(file_path)
        
        print(f"📊 Анализ файла: {file_path}")
        print("=" * 50)
        print(f"Строк: {len(df)}")
        print(f"Колонок: {len(df.columns)}")
        print(f"Колонки: {', '.join(df.columns)}")
        print()
        
        # Проверяем обязательные колонки
        required_cols = ['id', 'username', 'title']
        missing_cols = [col for col in required_cols if col not in df.columns]
        
        if missing_cols:
            print(f"⚠️  Отсутствуют колонки: {', '.join(missing_cols)}")
        else:
            print("✅ Все обязательные колонки присутствуют")
        
        print()
        
        # Статистика по данным
        print("📈 Статистика:")
        if 'id' in df.columns:
            id_count = df['id'].notna().sum()
            print(f"  - Записей с ID: {id_count}")
        
        if 'username' in df.columns:
            username_count = df['username'].notna().sum()
            print(f"  - Записей с username: {username_count}")
        
        if 'title' in df.columns:
            title_count = df['title'].notna().sum()
            print(f"  - Записей с названием: {title_count}")
        
        # Показываем первые несколько строк
        print()
        print("📋 Первые 5 строк:")
        print(df.head().to_string())
        
        # Проверяем на пустые значения
        print()
        print("🔍 Проблемы с данными:")
        for col in df.columns:
            null_count = df[col].isna().sum()
            if null_count > 0:
                print(f"  - {col}: {null_count} пустых значений")
        
        # Проверяем дубликаты
        if 'id' in df.columns:
            duplicates = df['id'].duplicated().sum()
            if duplicates > 0:
                print(f"  - Дублирующиеся ID: {duplicates}")
        
        return True
        
    except Exception as e:
        print(f"❌ Ошибка анализа файла: {e}")
        return False

def clean_csv(input_file, output_file):
    """Очищает и подготавливает CSV файл"""
    try:
        df = pd.read_csv(input_file)
        
        print(f"🧹 Очистка файла: {input_file}")
        
        original_count = len(df)
        
        # Удаляем полностью пустые строки
        df = df.dropna(how='all')
        print(f"  - Удалено пустых строк: {original_count - len(df)}")
        
        # Очищаем пробелы в строковых колонках
        string_cols = df.select_dtypes(include=['object']).columns
        for col in string_cols:
            df[col] = df[col].astype(str).str.strip()
            df[col] = df[col].replace('nan', '')
        
        # Исправляем username формат
        if 'username' in df.columns:
            df['username'] = df['username'].apply(lambda x: 
                f"@{x.lstrip('@')}" if x and x != '' and x != 'nan' else '')
        
        # Удаляем дубликаты по ID
        if 'id' in df.columns:
            before_dedup = len(df)
            df = df.drop_duplicates(subset=['id'], keep='first')
            print(f"  - Удалено дубликатов по ID: {before_dedup - len(df)}")
        
        # Сохраняем очищенный файл
        df.to_csv(output_file, index=False, encoding='utf-8')
        
        print(f"✅ Очищенный файл сохранен: {output_file}")
        print(f"📊 Итого строк: {len(df)}")
        
        return True
        
    except Exception as e:
        print(f"❌ Ошибка очистки файла: {e}")
        return False

def convert_to_required_format(input_file, output_file):
    """Конвертирует CSV в требуемый формат"""
    try:
        df = pd.read_csv(input_file)
        
        # Создаем новый DataFrame с требуемыми колонками
        new_df = pd.DataFrame()
        
        # Пытаемся найти и сопоставить колонки
        column_mapping = {
            'id': ['id', 'chat_id', 'group_id', 'channel_id'],
            'username': ['username', 'link', 'url', 'handle'],
            'title': ['title', 'name', 'group_name', 'channel_name'],
            'date': ['date', 'created', 'added_date', 'timestamp']
        }
        
        print(f"🔄 Конвертация файла: {input_file}")
        print("Сопоставление колонок:")
        
        for target_col, possible_cols in column_mapping.items():
            found_col = None
            for col in possible_cols:
                if col in df.columns:
                    found_col = col
                    break
            
            if found_col:
                new_df[target_col] = df[found_col]
                print(f"  - {target_col} <- {found_col}")
            else:
                new_df[target_col] = ''
                print(f"  - {target_col} <- [пусто]")
        
        # Сохраняем
        new_df.to_csv(output_file, index=False, encoding='utf-8')
        
        print(f"✅ Конвертированный файл сохранен: {output_file}")
        return True
        
    except Exception as e:
        print(f"❌ Ошибка конвертации: {e}")
        return False

def main():
    parser = argparse.ArgumentParser(description="Утилита для работы с CSV файлами групп")
    parser.add_argument('command', choices=['analyze', 'clean', 'convert'], 
                       help='Команда для выполнения')
    parser.add_argument('--input', '-i', required=True, help='Входной CSV файл')
    parser.add_argument('--output', '-o', help='Выходной CSV файл (для clean/convert)')
    
    args = parser.parse_args()
    
    if not os.path.exists(args.input):
        print(f"❌ Файл не найден: {args.input}")
        return 1
    
    if args.command == 'analyze':
        success = analyze_csv(args.input)
        return 0 if success else 1
        
    elif args.command == 'clean':
        if not args.output:
            base_name = os.path.splitext(args.input)[0]
            args.output = f"{base_name}_cleaned.csv"
        
        success = clean_csv(args.input, args.output)
        return 0 if success else 1
        
    elif args.command == 'convert':
        if not args.output:
            base_name = os.path.splitext(args.input)[0]
            args.output = f"{base_name}_converted.csv"
        
        success = convert_to_required_format(args.input, args.output)
        return 0 if success else 1

if __name__ == "__main__":
    sys.exit(main())
