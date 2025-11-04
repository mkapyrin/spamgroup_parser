#!/bin/bash

# Скрипт для быстрого копирования исходного файла в проект
# Использование: ./copy_csv.sh /path/to/your/file.csv

if [ "$#" -ne 1 ]; then
    echo "Использование: $0 <путь_к_csv_файлу>"
    echo "Пример: $0 ~/Downloads/от_спамботов.csv"
    exit 1
fi

SOURCE_FILE="$1"
DEST_FILE="input/groups.csv"

if [ ! -f "$SOURCE_FILE" ]; then
    echo "❌ Файл не найден: $SOURCE_FILE"
    exit 1
fi

# Создаем резервную копию если файл уже существует
if [ -f "$DEST_FILE" ]; then
    BACKUP="input/groups_backup_$(date +%Y%m%d_%H%M%S).csv"
    cp "$DEST_FILE" "$BACKUP"
    echo "📦 Создана резервная копия: $BACKUP"
fi

# Копируем новый файл
cp "$SOURCE_FILE" "$DEST_FILE"

if [ $? -eq 0 ]; then
    echo "✅ Файл скопирован: $SOURCE_FILE -> $DEST_FILE"
    echo "📊 Анализ файла:"
    
    # Активируем виртуальное окружение если есть
    if [ -d "venv" ]; then
        source venv/bin/activate
        python utils.py analyze --input "$DEST_FILE"
    else
        echo "📋 Строк в файле: $(wc -l < "$DEST_FILE")"
        echo "📋 Первые 3 строки:"
        head -n 3 "$DEST_FILE"
    fi
    
    echo ""
    echo "🚀 Готово! Теперь можете запустить:"
    echo "   ./run.sh"
else
    echo "❌ Ошибка копирования файла"
    exit 1
fi
