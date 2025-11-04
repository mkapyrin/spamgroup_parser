# 📚 Примеры использования Telegram Group Parser

Этот файл содержит практические примеры использования парсера для различных сценариев.

## 🚀 Быстрый старт

### Пример 1: Первый запуск

```bash
# 1. Подготовьте входной файл
echo "id,username,title,date" > input/my_groups.csv
echo ",example_group,Example Group,2025-01-01" >> input/my_groups.csv

# 2. Запустите парсер
python main.py --input my_groups.csv --verbose

# 3. Проверьте результаты
head output/my_groups_enhanced.csv
```

### Пример 2: Обработка списка популярных групп

```bash
# Создайте файл с популярными группами
cat > input/popular_groups.csv << 'CSV'
id,username,title,date
,python,Python,2025-01-01
,javascript,JavaScript,2025-01-01
,webdev,Web Development,2025-01-01
CSV

# Запустите с увеличенной задержкой для безопасности
# Отредактируйте .env: DELAY_BETWEEN_REQUESTS=10
python main.py --input popular_groups.csv
```

## 📊 Анализ результатов

### Пример 3: Анализ данных с помощью Python

```python
import pandas as pd
import matplotlib.pyplot as plt

# Загружаем результаты
df = pd.read_csv('output/groups_enhanced.csv')

# Базовая статистика
print("📊 Статистика групп:")
print(f"Всего групп: {len(df)}")
print(f"Общее количество участников: {df['members_count'].sum():,}")
print(f"Средний размер группы: {df['members_count'].mean():.0f}")
print(f"Медианный размер группы: {df['members_count'].median():.0f}")

# Топ-10 самых больших групп
print("\n🏆 Топ-10 самых больших групп:")
top_groups = df.nlargest(10, 'members_count')[['actual_title', 'members_count', 'actual_username']]
for idx, row in top_groups.iterrows():
    print(f"{row['actual_title']}: {row['members_count']:,} участников ({row['actual_username']})")

# Анализ по типам чатов
print("\n📈 Распределение по типам:")
chat_types = df['chat_type'].value_counts()
for chat_type, count in chat_types.items():
    print(f"{chat_type}: {count} групп")

# Анализ активности
print("\n⚡ Анализ активности:")
active_groups = df[df['online_count'] > 0]
print(f"Групп с онлайн пользователями: {len(active_groups)}")
if len(active_groups) > 0:
    print(f"Среднее количество онлайн: {active_groups['online_count'].mean():.1f}")

# Группы с медленным режимом
slow_mode_groups = df[df['slow_mode_delay'] > 0]
print(f"Групп с медленным режимом: {len(slow_mode_groups)}")
```

### Пример 4: Экспорт в Excel с форматированием

```python
import pandas as pd
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill
from openpyxl.utils.dataframe import dataframe_to_rows

# Загружаем данные
df = pd.read_csv('output/groups_enhanced.csv')

# Создаем Excel файл
wb = Workbook()
ws = wb.active
ws.title = "Telegram Groups"

# Добавляем данные
for r in dataframe_to_rows(df, index=False, header=True):
    ws.append(r)

# Форматируем заголовки
header_font = Font(bold=True, color="FFFFFF")
header_fill = PatternFill(start_color="366092", end_color="366092", fill_type="solid")

for cell in ws[1]:
    cell.font = header_font
    cell.fill = header_fill

# Автоширина колонок
for column in ws.columns:
    max_length = 0
    column_letter = column[0].column_letter
    for cell in column:
        try:
            if len(str(cell.value)) > max_length:
                max_length = len(str(cell.value))
        except:
            pass
    adjusted_width = min(max_length + 2, 50)
    ws.column_dimensions[column_letter].width = adjusted_width

# Сохраняем
wb.save('output/groups_analysis.xlsx')
print("📊 Excel файл сохранен: output/groups_analysis.xlsx")
```

## 🔄 Автоматизация и мониторинг

### Пример 5: Скрипт для регулярного мониторинга

```bash
#!/bin/bash
# monitor_groups.sh - Скрипт для регулярного мониторинга групп

LOG_FILE="logs/monitor_$(date +%Y%m%d).log"
GROUPS_FILE="input/monitored_groups.csv"
RESULTS_DIR="output/monitoring"

echo "🚀 Начинаем мониторинг групп: $(date)" >> $LOG_FILE

# Создаем директорию для результатов
mkdir -p $RESULTS_DIR

# Запускаем парсер
python main.py --input $GROUPS_FILE --output "$RESULTS_DIR/groups_$(date +%Y%m%d_%H%M).csv" >> $LOG_FILE 2>&1

if [ $? -eq 0 ]; then
    echo "✅ Мониторинг завершен успешно: $(date)" >> $LOG_FILE
    
    # Отправляем уведомление (опционально)
    # curl -X POST "https://api.telegram.org/bot$BOT_TOKEN/sendMessage" \
    #      -d "chat_id=$CHAT_ID" \
    #      -d "text=✅ Мониторинг групп завершен успешно"
else
    echo "❌ Ошибка при мониторинге: $(date)" >> $LOG_FILE
fi
```

### Пример 6: Cron задача для автоматического запуска

```bash
# Добавьте в crontab (crontab -e):

# Запуск каждые 6 часов
0 */6 * * * cd /path/to/group-parser && ./monitor_groups.sh

# Запуск каждый день в 2:00 AM
0 2 * * * cd /path/to/group-parser && python main.py --input input/daily_groups.csv

# Запуск каждую неделю в воскресенье в 1:00 AM
0 1 * * 0 cd /path/to/group-parser && python main.py --input input/weekly_groups.csv
```

## 🔧 Продвинутые сценарии

### Пример 7: Обработка больших списков с батчингом

```python
import pandas as pd
import time
import os

def process_large_list(input_file, batch_size=50):
    """Обрабатывает большой список групп по частям"""
    
    # Читаем полный список
    df = pd.read_csv(input_file)
    total_groups = len(df)
    
    print(f"📊 Обработка {total_groups} групп по {batch_size} за раз")
    
    # Разбиваем на батчи
    for i in range(0, total_groups, batch_size):
        batch_num = i // batch_size + 1
        batch_df = df.iloc[i:i+batch_size]
        
        # Сохраняем батч
        batch_file = f"temp/batch_{batch_num}.csv"
        os.makedirs("temp", exist_ok=True)
        batch_df.to_csv(batch_file, index=False)
        
        print(f"🔄 Обработка батча {batch_num}/{(total_groups-1)//batch_size + 1}")
        
        # Запускаем парсер для батча
        os.system(f"python main.py --input {batch_file}")
        
        # Пауза между батчами
        if i + batch_size < total_groups:
            print("⏳ Пауза 60 секунд между батчами...")
            time.sleep(60)
    
    print("✅ Обработка всех батчей завершена!")

# Использование
process_large_list("input/huge_groups_list.csv", batch_size=30)
```

### Пример 8: Сравнение изменений во времени

```python
import pandas as pd
from datetime import datetime, timedelta

def compare_group_changes(old_file, new_file):
    """Сравнивает изменения в группах между двумя сканированиями"""
    
    old_df = pd.read_csv(old_file)
    new_df = pd.read_csv(new_file)
    
    # Объединяем по ID для сравнения
    merged = pd.merge(old_df, new_df, on='id', suffixes=('_old', '_new'))
    
    print("📊 АНАЛИЗ ИЗМЕНЕНИЙ В ГРУППАХ")
    print("=" * 50)
    
    # Изменения в количестве участников
    member_changes = merged[merged['members_count_old'] != merged['members_count_new']]
    
    if len(member_changes) > 0:
        print(f"\n👥 Изменения в количестве участников ({len(member_changes)} групп):")
        for _, row in member_changes.iterrows():
            old_count = row['members_count_old']
            new_count = row['members_count_new']
            change = new_count - old_count
            change_pct = (change / old_count * 100) if old_count > 0 else 0
            
            emoji = "📈" if change > 0 else "📉"
            print(f"{emoji} {row['actual_title_new']}: {old_count:,} → {new_count:,} ({change:+,}, {change_pct:+.1f}%)")
    
    # Изменения в названиях
    title_changes = merged[merged['actual_title_old'] != merged['actual_title_new']]
    if len(title_changes) > 0:
        print(f"\n📝 Изменения в названиях ({len(title_changes)} групп):")
        for _, row in title_changes.iterrows():
            print(f"• {row['actual_username_new']}: '{row['actual_title_old']}' → '{row['actual_title_new']}'")
    
    # Новые группы
    new_groups = new_df[~new_df['id'].isin(old_df['id'])]
    if len(new_groups) > 0:
        print(f"\n✨ Новые группы ({len(new_groups)}):")
        for _, row in new_groups.iterrows():
            print(f"• {row['actual_title']} ({row['actual_username']}) - {row['members_count']:,} участников")
    
    # Исчезнувшие группы
    removed_groups = old_df[~old_df['id'].isin(new_df['id'])]
    if len(removed_groups) > 0:
        print(f"\n🗑️ Исчезнувшие группы ({len(removed_groups)}):")
        for _, row in removed_groups.iterrows():
            print(f"• {row['actual_title']} ({row['actual_username']})")

# Использование
compare_group_changes("output/groups_2025_01_01.csv", "output/groups_2025_01_08.csv")
```

## 🛠 Устранение неполадок

### Пример 9: Диагностика проблем

```python
import pandas as pd
import os
from datetime import datetime

def diagnose_parsing_issues(results_file):
    """Анализирует результаты парсинга на предмет проблем"""
    
    if not os.path.exists(results_file):
        print(f"❌ Файл {results_file} не найден")
        return
    
    df = pd.read_csv(results_file)
    
    print("🔍 ДИАГНОСТИКА РЕЗУЛЬТАТОВ ПАРСИНГА")
    print("=" * 50)
    
    # Общая статистика
    total = len(df)
    successful = len(df[df['access_status'] == 'success'])
    errors = len(df[df['access_status'] == 'error'])
    access_denied = len(df[df['access_status'] == 'access_denied'])
    
    print(f"📊 Общая статистика:")
    print(f"   Всего записей: {total}")
    print(f"   ✅ Успешно: {successful} ({successful/total*100:.1f}%)")
    print(f"   🚫 Доступ запрещен: {access_denied} ({access_denied/total*100:.1f}%)")
    print(f"   ❌ Ошибки: {errors} ({errors/total*100:.1f}%)")
    
    # Анализ ошибок
    if errors > 0:
        print(f"\n❌ Анализ ошибок:")
        error_groups = df[df['access_status'] == 'error']
        error_messages = error_groups['error_message'].value_counts()
        for error, count in error_messages.items():
            print(f"   • {error}: {count} случаев")
    
    # Группы без участников
    no_members = df[(df['access_status'] == 'success') & (df['members_count'] == 0)]
    if len(no_members) > 0:
        print(f"\n⚠️ Группы без участников ({len(no_members)}):")
        for _, row in no_members.head(5).iterrows():
            print(f"   • {row['actual_title']} ({row['actual_username']})")
    
    # Рекомендации
    print(f"\n💡 Рекомендации:")
    if access_denied > total * 0.3:
        print("   • Много приватных групп - рассмотрите использование Bot API")
    if errors > total * 0.1:
        print("   • Высокий процент ошибок - увеличьте задержки между запросами")
    if successful < total * 0.8:
        print("   • Низкий процент успеха - проверьте качество входных данных")

# Использование
diagnose_parsing_issues("output/groups_enhanced.csv")
```

### Пример 10: Восстановление после сбоя

```bash
#!/bin/bash
# recovery.sh - Скрипт восстановления после сбоя

echo "🔄 Запуск процедуры восстановления..."

# Проверяем наличие резервных копий
if [ -f "output/groups_enhanced.csv.backup" ]; then
    echo "📋 Найдена резервная копия, восстанавливаем..."
    cp output/groups_enhanced.csv.backup output/groups_enhanced.csv
fi

# Проверяем логи на наличие ошибок
echo "🔍 Анализ логов..."
LATEST_LOG=$(ls -t logs/*.log | head -1)
if grep -q "FloodWaitError" "$LATEST_LOG"; then
    echo "⚠️ Обнаружены FloodWait ошибки, увеличиваем задержки..."
    sed -i 's/DELAY_BETWEEN_REQUESTS=5/DELAY_BETWEEN_REQUESTS=10/' .env
fi

# Перезапускаем с безопасными настройками
echo "🚀 Перезапуск с безопасными настройками..."
python main.py --verbose

echo "✅ Процедура восстановления завершена"
```

---

## 📞 Получение помощи

Если у вас возникли проблемы с любым из этих примеров:

1. Проверьте логи в папке `logs/`
2. Убедитесь, что все зависимости установлены
3. Проверьте правильность конфигурации в `.env`
4. Создайте Issue с описанием проблемы и приложите логи

**Удачного парсинга! 🚀**
