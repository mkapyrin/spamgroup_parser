.PHONY: help setup test run clean install analyze

# Telegram Group Parser - Makefile
# Удобные команды для управления проектом

help: ## Показать справку
	@echo "🚀 Telegram Group Parser"
	@echo "========================"
	@echo "Доступные команды:"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'

setup: ## Первоначальная настройка проекта
	@echo "🛠  Настройка проекта..."
	chmod +x setup.sh run.sh test.sh
	./setup.sh

test: ## Проверка конфигурации
	@echo "🔍 Проверка настроек..."
	chmod +x test.sh
	./test.sh

run: ## Запуск парсера (input/groups.csv)
	@echo "🚀 Запуск парсера..."
	chmod +x run.sh
	./run.sh

run-sample: ## Запуск с примером файла
	@echo "🧪 Запуск с примером..."
	chmod +x run.sh
	./run.sh --input input/sample.csv --verbose

run-verbose: ## Запуск с подробным выводом
	@echo "📝 Подробный запуск..."
	chmod +x run.sh
	./run.sh --verbose

install: ## Установка зависимостей (после setup)
	@echo "📦 Установка зависимостей..."
	source venv/bin/activate && pip install -r requirements.txt

clean: ## Очистка временных файлов
	@echo "🧹 Очистка..."
	rm -rf venv/__pycache__ src/__pycache__
	rm -rf *.session *.session-journal
	rm -f logs/*.log

analyze: ## Анализ CSV файла (make analyze FILE=path/to/file.csv)
	@echo "📊 Анализ файла: $(FILE)"
	source venv/bin/activate && python utils.py analyze --input $(FILE)

convert: ## Конвертация CSV в нужный формат (make convert FILE=path/to/file.csv)
	@echo "🔄 Конвертация файла: $(FILE)"
	source venv/bin/activate && python utils.py convert --input $(FILE)

status: ## Показать статус проекта
	@echo "📈 Статус проекта:"
	@echo "=================="
	@if [ -d "venv" ]; then echo "✅ Виртуальное окружение: OK"; else echo "❌ Виртуальное окружение: НЕ НАЙДЕНО"; fi
	@if [ -f ".env" ]; then echo "✅ Конфигурация: OK"; else echo "❌ Конфигурация: НЕ НАЙДЕНА"; fi
	@echo "📂 Файлы в input/:"
	@ls -la input/ 2>/dev/null || echo "  [пусто]"
	@echo "📂 Файлы в output/:"
	@ls -la output/ 2>/dev/null || echo "  [пусто]"
	@if [ -d "logs" ]; then echo "📊 Логи: $(shell ls logs/ 2>/dev/null | wc -l) файлов"; fi

# Значения по умолчанию
FILE ?= input/sample.csv

# Цветной вывод
GREEN  := $(shell tput -Txterm setaf 2)
YELLOW := $(shell tput -Txterm setaf 3)
WHITE  := $(shell tput -Txterm setaf 7)
RESET  := $(shell tput -Txterm sgr0)

# Добавляем цвета к help
TARGET_MAX_CHAR_NUM=15
