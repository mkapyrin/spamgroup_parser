import os
import random
import pandas as pd
import asyncio
import aiohttp
import json
import hashlib
import glob
import time
import warnings
import sqlite3
import subprocess
from pathlib import Path
from telethon import TelegramClient
from telethon.errors import (
    ChannelPrivateError, 
    UsernameNotOccupiedError, 
    FloodWaitError,
    ChatAdminRequiredError,
    UserBannedInChannelError,
    PhoneCodeInvalidError,
    SessionPasswordNeededError,
    PhoneNumberInvalidError,
    PhoneCodeExpiredError
)
from datetime import datetime
import logging
from tqdm.asyncio import tqdm

from .config import Config
from .logger_config import setup_logging, log_separator, log_progress

class CriticalFloodWaitError(Exception):
    """Исключение для критического FloodWait, который требует прерывания обработки"""
    def __init__(self, wait_time, chat_identifier):
        self.wait_time = wait_time
        self.chat_identifier = chat_identifier
        self.wait_hours = wait_time / 3600
        self.wait_days = wait_time / 86400
        super().__init__(f"FloodWait слишком долгий: {self.wait_hours:.1f} часов для {chat_identifier}")

class TelegramGroupParser:
    """Класс для парсинга информации о Telegram группах"""
    
    def __init__(self):
        self.logger = setup_logging()
        self.client = None
        # Используем случайную задержку от 3 до 7 секунд вместо фиксированного throttler
        self.min_delay = 3.0
        self.max_delay = 7.0
        self.current_user_id = None  # ID текущего пользователя для проверки прав
        
        # Подавляем предупреждение о существующей сессии глобально при инициализации
        warnings.filterwarnings("ignore", message=".*session already had an authorized user.*", category=UserWarning)
        warnings.filterwarnings("ignore", message=".*the session already had an authorized user.*", category=UserWarning)
        warnings.filterwarnings("ignore", message=".*did not login to the user account.*", category=UserWarning)
        
        # Настраиваем логирование Telethon - только WARNING и выше, чтобы не засорять логи
        # Сетевые ошибки (Connection reset, Can't assign requested address и т.д.) - это нормально
        # Telethon автоматически переподключается, не нужно логировать каждое событие
        telethon_logger = logging.getLogger('telethon')
        telethon_logger.setLevel(logging.WARNING)
        
        # Фильтруем специфичные сообщения Telethon, которые не важны
        class TelethonFilter(logging.Filter):
            """Фильтр для подавления лишних сообщений Telethon"""
            def filter(self, record):
                # Подавляем сообщения о переподключениях и обновлениях
                message = record.getMessage()
                # Игнорируем рутинные сообщения о переподключениях
                if any(phrase in message for phrase in [
                    "Got difference for account updates",
                    "Got difference for channel",
                    "Connection closed while receiving data",
                    "Closing current connection to begin reconnect",
                    "Connection to",
                    "Connection complete",
                    "Disconnecting from",
                    "Disconnection from",
                    "Not disconnecting",
                    "during disconnect",
                    "Server closed the connection",
                    "Server resent the older message",
                    "Server sent a very old message"
                ]):
                    return False
                return True
        
        # Применяем фильтр к логгерам Telethon
        for handler in telethon_logger.handlers:
            handler.addFilter(TelethonFilter())
        
        # Также применяем к корневому логгеру, если Telethon использует его
        root_logger = logging.getLogger()
        for handler in root_logger.handlers:
            if not any(isinstance(f, TelethonFilter) for f in handler.filters):
                handler.addFilter(TelethonFilter())
        
    async def get_member_count_via_bot_api(self, chat_identifier, bot_token=None):
        '''Получает количество участников через Bot API (если доступен bot token)'''
        if not bot_token:
            return None
        
        if not chat_identifier:
            self.logger.warning("Bot API: пустой chat_identifier")
            return None
            
        try:
            # Формируем URL для Bot API
            url = f"https://api.telegram.org/bot{bot_token}/getChatMembersCount"
            params = {"chat_id": chat_identifier}
            
            # Создаем таймаут для запроса (10 секунд)
            timeout = aiohttp.ClientTimeout(total=10)
            
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(url, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        if data.get("ok"):
                            member_count = data.get("result", 0)
                            if member_count and member_count > 0:
                                self.logger.info(f"Bot API: {member_count} участников в {chat_identifier}")
                                return member_count
                            else:
                                self.logger.debug(f"Bot API: некорректное количество участников для {chat_identifier}")
                        else:
                            error_desc = data.get('description', 'Unknown error')
                            self.logger.warning(f"Bot API ошибка для {chat_identifier}: {error_desc}")
                    elif response.status == 401:
                        self.logger.error("Bot API: неверный токен бота")
                    elif response.status == 400:
                        self.logger.warning(f"Bot API: некорректный запрос для {chat_identifier}")
                    elif response.status == 403:
                        self.logger.warning(f"Bot API: доступ запрещен для {chat_identifier}")
                    else:
                        self.logger.warning(f"Bot API HTTP {response.status} для {chat_identifier}")
                        
        except asyncio.TimeoutError:
            self.logger.debug(f"Bot API: таймаут для {chat_identifier}")
        except aiohttp.ClientError as e:
            self.logger.debug(f"Bot API: ошибка клиента для {chat_identifier}: {e}")
        except json.JSONDecodeError as e:
            self.logger.warning(f"Bot API: ошибка парсинга JSON для {chat_identifier}: {e}")
        except Exception as e:
            self.logger.debug(f"Bot API: неожиданная ошибка для {chat_identifier}: {e}")
            
        return None
        
    async def initialize_client(self):
        """Инициализация Telegram клиента"""
        try:
            # Валидация конфигурации
            if not Config.API_ID:
                self.logger.error("API_ID не установлен")
                return False
            if not Config.API_HASH:
                self.logger.error("API_HASH не установлен")
                return False
            if not Config.PHONE:
                self.logger.error("PHONE не установлен")
                return False
            
            session_path = Config.SESSION_NAME
            self.client = TelegramClient(session_path, Config.API_ID, Config.API_HASH)
            
            self.logger.info("Подключение к Telegram...")
            
            # Подключаемся к серверу
            await self.client.connect()
            
            # Подавляем предупреждение о существующей авторизованной сессии
            # Telethon выдает это предупреждение при вызове start() с phone, если сессия уже авторизована
            # Это нормальное поведение - мы просто используем существующую сессию
            with warnings.catch_warnings():
                # Подавляем все варианты сообщения о существующей сессии
                warnings.filterwarnings("ignore", message=".*session already had an authorized user.*", category=UserWarning)
                warnings.filterwarnings("ignore", message=".*the session already had an authorized user.*", category=UserWarning)
                warnings.filterwarnings("ignore", message=".*did not login to the user account.*", category=UserWarning)
                
                # Проверяем, авторизован ли уже клиент
                if await self.client.is_user_authorized():
                    self.logger.info("✅ Используется существующая авторизованная сессия")
                    me = await self.client.get_me()
                else:
                    # Если сессия не авторизована, делаем авторизацию
                    self.logger.info("Авторизация нового пользователя...")
                    await self.client.start(phone=Config.PHONE)
                    me = await self.client.get_me()
            
            if not me:
                self.logger.error("Не удалось получить информацию о пользователе")
                return False
            
            # Сохраняем ID текущего пользователя для проверки прав
            self.current_user_id = me.id
            self.logger.info(f"✅ Установлен current_user_id: {self.current_user_id}")
            
            self.logger.info(f"Авторизован как: {me.first_name} {me.last_name or ''} (@{me.username or 'без username'})")
            
            return True
            
        except PhoneCodeInvalidError:
            self.logger.error("Неверный код подтверждения")
            return False
        except PhoneCodeExpiredError:
            self.logger.error("Код подтверждения истек")
            return False
        except PhoneNumberInvalidError:
            self.logger.error("Неверный номер телефона")
            return False
        except SessionPasswordNeededError:
            self.logger.error("Требуется пароль двухфакторной аутентификации")
            return False
        except ValueError as e:
            self.logger.error(f"Ошибка валидации при инициализации: {e}")
            return False
        except sqlite3.OperationalError as e:
            if "database is locked" in str(e).lower():
                self.logger.error(f"❌ База данных сессии заблокирована другим процессом")
                self.logger.error(f"   Это означает, что парсер уже запущен в другом окне/процессе")
                
                # Пытаемся найти процесс, который блокирует файл
                session_path = Config.SESSION_NAME
                if os.path.exists(session_path):
                    try:
                        # Используем lsof для поиска процессов, использующих файл
                        result = subprocess.run(
                            ['lsof', session_path],
                            capture_output=True,
                            text=True,
                            timeout=5
                        )
                        if result.returncode == 0 and result.stdout:
                            lines = result.stdout.strip().split('\n')
                            if len(lines) > 1:  # Первая строка - заголовок
                                self.logger.error(f"   Найден процесс, использующий сессию:")
                                for line in lines[1:]:  # Пропускаем заголовок
                                    parts = line.split()
                                    if len(parts) >= 2:
                                        pid = parts[1]
                                        cmd = ' '.join(parts[8:]) if len(parts) > 8 else 'unknown'
                                        self.logger.error(f"      PID: {pid}, Команда: {cmd}")
                                self.logger.error(f"   Решение: завершите процесс (kill {parts[1]}) или подождите завершения")
                            else:
                                self.logger.error(f"   Решение: подождите несколько секунд и попробуйте снова")
                        else:
                            self.logger.error(f"   Решение: подождите несколько секунд и попробуйте снова")
                    except Exception:
                        self.logger.error(f"   Решение: подождите несколько секунд и попробуйте снова")
                else:
                    self.logger.error(f"   Решение: подождите несколько секунд и попробуйте снова")
            else:
                self.logger.error(f"Ошибка SQLite: {e}")
            return False
        except Exception as e:
            error_msg = str(e).lower()
            if "database is locked" in error_msg or "sqlite" in error_msg:
                self.logger.error(f"❌ База данных заблокирована: {e}")
                self.logger.error(f"   Вероятно, парсер уже запущен. Завершите другие процессы и попробуйте снова")
            else:
                self.logger.error(f"Ошибка инициализации клиента: {e}")
            return False
    
    async def get_chat_info(self, chat_identifier, retries=0):
        """Получает информацию о чате/канале с повторными попытками"""
        
        # Валидация входных данных
        if not chat_identifier:
            return self._create_error_info(chat_identifier, "Пустой идентификатор чата")
        
        if not self.client or not self.client.is_connected():
            return self._create_error_info(chat_identifier, "Клиент не подключен")
        
        # Случайная задержка от 3 до 7 секунд перед запросом
        delay = random.uniform(self.min_delay, self.max_delay)
        self.logger.info(f"⏳ Задержка {delay:.2f} секунд перед запросом для {chat_identifier}")
        await asyncio.sleep(delay)
        
        try:
            # Пробуем получить сущность с валидацией
            try:
                if isinstance(chat_identifier, str) and chat_identifier.startswith('@'):
                    entity = await asyncio.wait_for(
                        self.client.get_entity(chat_identifier),
                        timeout=30.0
                    )
                else:
                    try:
                        chat_id = int(chat_identifier)
                        if chat_id <= 0:
                            raise ValueError("ID чата должен быть положительным числом")
                        entity = await asyncio.wait_for(
                            self.client.get_entity(chat_id),
                            timeout=30.0
                        )
                    except (ValueError, TypeError) as e:
                        return self._create_error_info(chat_identifier, f"Неверный формат ID: {e}")
            except UsernameNotOccupiedError as e:
                # Username не существует - не повторяем
                error_msg = f"Username не существует: {str(e)}"
                self.logger.warning(f"⚠️  Username не существует для {chat_identifier} - пропускаем без повторов")
                return self._create_error_info(chat_identifier, error_msg, 'access_denied')
            except Exception as e:
                # Проверяем другие ошибки "No user has", "Nobody is using", "username is unacceptable"
                error_str = str(e).lower()
                if any(phrase in error_str for phrase in [
                    'no user has',
                    'username not occupied',
                    'nobody is using this username',
                    'username is unacceptable',
                    'nobody is using'
                ]):
                    error_msg = f"Username не существует или неприемлем: {str(e)}"
                    self.logger.warning(f"⚠️  Username не существует или неприемлем для {chat_identifier} - пропускаем без повторов")
                    return self._create_error_info(chat_identifier, error_msg, 'access_denied')
                # Для других ошибок пробрасываем дальше
                raise
            except asyncio.TimeoutError:
                return self._create_error_info(chat_identifier, "Таймаут при получении сущности")
            
            if not entity:
                return self._create_error_info(chat_identifier, "Не удалось получить сущность")
            
            # Получаем количество участников с подробным логированием
            members_count = 0
            member_count_method = "none"
            
            try:
                # Метод 1: Прямое получение из атрибута participants_count
                if hasattr(entity, 'participants_count') and entity.participants_count:
                    members_count = entity.participants_count
                    member_count_method = "participants_count_attr"
                    self.logger.debug(f"Получено {members_count} участников через participants_count для {chat_identifier}")
                
                # Метод 2: Получение через get_participants (если первый метод не сработал)
                elif members_count == 0:
                    try:
                        participants = await self.client.get_participants(entity, limit=0)
                        members_count = participants.total
                        member_count_method = "get_participants"
                        self.logger.debug(f"Получено {members_count} участников через get_participants для {chat_identifier}")
                    except (ChatAdminRequiredError, UserBannedInChannelError) as e:
                        self.logger.warning(f"Нет прав для получения участников {chat_identifier}: {e}")
                        # Пробуем получить из атрибута как fallback
                        members_count = getattr(entity, 'participants_count', 0)
                        member_count_method = "fallback_attr"
                    except Exception as e:
                        self.logger.warning(f"Ошибка при получении участников {chat_identifier}: {e}")
                        members_count = 0
                        member_count_method = "failed"
                
                # Метод 3: Попытка получить через полную информацию о чате
                if members_count == 0:
                    try:
                        full_chat = await self.client.get_entity(entity)
                        if hasattr(full_chat, 'participants_count'):
                            members_count = full_chat.participants_count
                            member_count_method = "full_chat_attr"
                            self.logger.debug(f"Получено {members_count} участников через full_chat для {chat_identifier}")
                    except Exception as e:
                        self.logger.debug(f"Не удалось получить full_chat для {chat_identifier}: {e}")
                
                # Метод 4: Попытка через Bot API (если доступен bot token)
                if members_count == 0:
                    bot_token = getattr(Config, 'BOT_TOKEN', None)
                    if bot_token:
                        bot_api_count = await self.get_member_count_via_bot_api(chat_identifier, bot_token)
                        if bot_api_count:
                            members_count = bot_api_count
                            member_count_method = "bot_api"
                
                if members_count == 0:
                    self.logger.warning(f"Не удалось получить количество участников для {chat_identifier}")
                else:
                    self.logger.info(f"Участников в {chat_identifier}: {members_count} (метод: {member_count_method})")
                    
            except Exception as e:
                self.logger.error(f"Критическая ошибка при получении участников {chat_identifier}: {e}")
                members_count = 0
                member_count_method = "error" 
            
            # Получаем расширенную информацию о группе
            extended_info = await self._get_extended_group_info(entity)
            
            # Проверяем права пользователя на отправку сообщений
            can_send_messages = await self._check_user_send_permissions(entity)
            
            # Собираем только значимую информацию
            info = {
                'id': entity.id,
                'actual_title': entity.title,
                'actual_username': f"https://t.me/{entity.username}" if entity.username else None,
                'members_count': members_count,
                'chat_type': self._determine_chat_type(entity),
                'created_date': self._format_date(getattr(entity, 'date', None)),
                'check_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'access_status': 'success',
                'error_message': None,
                # Только значимые поля из расширенной информации
                'online_count': extended_info.get('online_count', 0),
                'slow_mode_delay': extended_info.get('slow_mode_delay', 0),
                'pinned_message_id': extended_info.get('pinned_message_id', None),
                'linked_chat_id': extended_info.get('linked_chat_id', None),
                # Права пользователя на отправку сообщений
                'can_send_messages': can_send_messages
            }
            
            self.logger.debug(f"Успешно получена информация о {chat_identifier}")
            return info
            
        except FloodWaitError as e:
            wait_time = e.seconds
            wait_hours = wait_time / 3600
            wait_days = wait_time / 86400
            
            # Если FloodWait слишком долгий (более 2 часов), прерываем обработку
            MAX_WAIT_HOURS = 2
            if wait_time > MAX_WAIT_HOURS * 3600:
                self.logger.error(f"❌ FloodWait для {chat_identifier}: требуется ожидание {wait_hours:.1f} часов ({wait_days:.1f} дней)")
                self.logger.error(f"   Это слишком долго для автоматического ожидания (максимум: {MAX_WAIT_HOURS} часа)")
                self.logger.error(f"   Telegram API ограничил запросы из-за предыдущей активности")
                self.logger.error(f"   Рекомендация: прервите обработку (Ctrl+C) и попробуйте через {wait_hours:.1f} часов")
                self.logger.error(f"   ⚠️  Обработка будет прервана для предотвращения длительного ожидания")
                # Вызываем специальное исключение для прерывания обработки
                raise CriticalFloodWaitError(wait_time, chat_identifier)
            
            self.logger.warning(f"⏳ FloodWait для {chat_identifier}: ждем {wait_time} секунд ({wait_hours:.1f} часов)")
            
            # Добавляем небольшую случайную задержку для избежания синхронизации
            additional_wait = random.randint(1, 5)
            total_wait = wait_time + additional_wait
            
            self.logger.info(f"⏱️  Общее время ожидания: {total_wait} секунд ({total_wait/3600:.1f} часов) (FloodWait: {wait_time}s + случайная: {additional_wait}s)")
            
            try:
                await asyncio.sleep(total_wait)
            except asyncio.CancelledError:
                self.logger.warning(f"Ожидание FloodWait прервано для {chat_identifier}")
                return self._create_error_info(chat_identifier, "FloodWait прерван пользователем")
            
            if retries < Config.MAX_RETRIES:
                self.logger.info(f"🔄 Повторная попытка {retries + 1}/{Config.MAX_RETRIES} для {chat_identifier}")
                return await self.get_chat_info(chat_identifier, retries + 1)
            else:
                return self._create_error_info(chat_identifier, f"FloodWait превышен после {Config.MAX_RETRIES} попыток")
                
        except (ChannelPrivateError, UsernameNotOccupiedError, ChatAdminRequiredError) as e:
            error_msg = self._get_error_message(e)
            self.logger.warning(f"Доступ к {chat_identifier}: {error_msg}")
            return self._create_error_info(chat_identifier, error_msg, 'access_denied')
            
        except asyncio.TimeoutError:
            error_msg = "Таймаут при получении информации о чате"
            self.logger.error(f"Таймаут для {chat_identifier}")
            
            if retries < Config.MAX_RETRIES:
                self.logger.info(f"Повтор попытки {retries + 1}/{Config.MAX_RETRIES} для {chat_identifier}")
                await asyncio.sleep(5)
                return await self.get_chat_info(chat_identifier, retries + 1)
            else:
                return self._create_error_info(chat_identifier, error_msg, 'error')
        
        except ConnectionError as e:
            error_msg = f"Ошибка подключения: {str(e)}"
            self.logger.error(f"Ошибка подключения для {chat_identifier}: {e}")
            
            if retries < Config.MAX_RETRIES:
                self.logger.info(f"Повтор попытки {retries + 1}/{Config.MAX_RETRIES} для {chat_identifier}")
                await asyncio.sleep(10)  # Ждем дольше при проблемах с подключением
                return await self.get_chat_info(chat_identifier, retries + 1)
            else:
                return self._create_error_info(chat_identifier, error_msg, 'error')
        
        except Exception as e:
            error_msg = f"Неожиданная ошибка: {str(e)}"
            error_str = str(e).lower()
            
            # Проверяем, является ли это ошибкой "No user has", "Nobody is using" или "Username не существует"
            # Такие ошибки не нужно повторять - username не появится
            is_no_user_error = (
                'no user has' in error_str or 
                'username not occupied' in error_str or
                'username не существует' in error_str or
                'username does not exist' in error_str or
                'nobody is using this username' in error_str or
                'username is unacceptable' in error_str or
                'nobody is using' in error_str
            )
            
            if is_no_user_error:
                self.logger.warning(f"⚠️  Username не существует для {chat_identifier}: {error_msg} - пропускаем без повторов")
                return self._create_error_info(chat_identifier, f"Username не существует: {str(e)}", 'access_denied')
            
            self.logger.error(f"Ошибка для {chat_identifier}: {error_msg}")
            
            if retries < Config.MAX_RETRIES:
                self.logger.info(f"Повтор попытки {retries + 1}/{Config.MAX_RETRIES} для {chat_identifier}")
                await asyncio.sleep(5)  # Ждем 5 секунд перед повтором
                return await self.get_chat_info(chat_identifier, retries + 1)
            else:
                return self._create_error_info(chat_identifier, error_msg, 'error')
    


    async def _get_pinned_message_content(self, entity):
        """Получает содержимое закрепленного сообщения"""
        try:
            # Получаем полную информацию о чате
            from telethon.tl.functions.channels import GetFullChannelRequest
            
            if hasattr(entity, 'megagroup') or hasattr(entity, 'broadcast'):
                full_info = await self.client(GetFullChannelRequest(entity))
                pinned_msg_id = getattr(full_info.full_chat, 'pinned_msg_id', None)
                
                if pinned_msg_id:
                    # Получаем закрепленное сообщение
                    pinned_message = await self.client.get_messages(entity, ids=pinned_msg_id)
                    if pinned_message and pinned_message.message:
                        return {
                            'pinned_message_text': pinned_message.message[:500],  # Ограничиваем длину
                            'pinned_message_date': pinned_message.date.strftime('%Y-%m-%d %H:%M:%S') if pinned_message.date else None,
                            'pinned_message_author': pinned_message.sender_id,
                            'pinned_message_views': getattr(pinned_message, 'views', 0)
                        }
        except Exception as e:
            self.logger.debug(f"Не удалось получить закрепленное сообщение для {entity.id}: {e}")
        
        return {
            'pinned_message_text': None,
            'pinned_message_date': None,
            'pinned_message_author': None,
            'pinned_message_views': 0
        }
    
    async def _get_recent_messages_stats(self, entity, limit=100):
        """Получает статистику последних сообщений"""
        try:
            messages = await self.client.get_messages(entity, limit=limit)
            
            if not messages:
                return {
                    'recent_messages_count': 0,
                    'avg_message_length': 0,
                    'messages_with_media': 0,
                    'messages_with_links': 0,
                    'most_active_hour': None,
                    'last_message_date': None
                }
            
            # Анализируем сообщения
            total_length = 0
            media_count = 0
            links_count = 0
            hours = {}
            
            for msg in messages:
                if msg.message:
                    total_length += len(msg.message)
                    # Проверяем наличие ссылок
                    if 'http' in msg.message.lower() or 't.me' in msg.message.lower():
                        links_count += 1
                
                if msg.media:
                    media_count += 1
                
                if msg.date:
                    hour = msg.date.hour
                    hours[hour] = hours.get(hour, 0) + 1
            
            # Находим самый активный час
            most_active_hour = max(hours.keys(), key=lambda k: hours[k]) if hours else None
            
            return {
                'recent_messages_count': len(messages),
                'avg_message_length': total_length // len(messages) if messages else 0,
                'messages_with_media': media_count,
                'messages_with_links': links_count,
                'most_active_hour': most_active_hour,
                'last_message_date': messages[0].date.strftime('%Y-%m-%d %H:%M:%S') if messages and messages[0].date else None
            }
            
        except Exception as e:
            self.logger.debug(f"Не удалось получить статистику сообщений для {entity.id}: {e}")
            return {
                'recent_messages_count': 0,
                'avg_message_length': 0,
                'messages_with_media': 0,
                'messages_with_links': 0,
                'most_active_hour': None,
                'last_message_date': None
            }
    
    async def _get_detailed_description(self, entity):
        """Получает детальное описание группы"""
        try:
            from telethon.tl.functions.channels import GetFullChannelRequest
            
            if hasattr(entity, 'megagroup') or hasattr(entity, 'broadcast'):
                full_info = await self.client(GetFullChannelRequest(entity))
                full_chat = full_info.full_chat
                
                description = getattr(full_chat, 'about', '') or ''
                
                return {
                    'full_description': description[:1000] if description else '',  # Полное описание до 1000 символов
                    'description_length': len(description) if description else 0,
                    'has_description': bool(description)
                }
        except Exception as e:
            self.logger.debug(f"Не удалось получить детальное описание для {entity.id}: {e}")
        
        return {
            'full_description': '',
            'description_length': 0,
            'has_description': False
        }
    
    async def _get_group_links(self, entity, limit=50):
        """Извлекает ссылки на другие группы из сообщений (как в оригинальном проекте)"""
        try:
            import re
            links = set()
            
            # Ищем сообщения с ссылками на Telegram
            async for message in self.client.iter_messages(entity, search="t.me/", limit=limit):
                if message.text:
                    # Ищем ссылки вида t.me/username или t.me/joinchat/hash
                    telegram_links = re.findall(r't\.me/([a-zA-Z0-9_]+)', message.text)
                    join_links = re.findall(r't\.me/joinchat/([a-zA-Z0-9_-]+)', message.text)
                    
                    links.update(telegram_links)
                    links.update(join_links)
            
            return {
                'found_group_links': list(links)[:20],  # Ограничиваем до 20 ссылок
                'group_links_count': len(links)
            }
            
        except Exception as e:
            self.logger.debug(f"Не удалось извлечь ссылки для {entity.id}: {e}")
            return {
                'found_group_links': [],
                'group_links_count': 0
            }

    async def _get_extended_group_info(self, entity):
        """Получает расширенную информацию о группе"""
        extended_info = {
            'admin_count': 0,
            'online_count': 0,
            'recent_actions': 0,
            'slow_mode_delay': 0,
            'has_pinned_message': False,
            'pinned_message_id': None,
            'can_view_stats': False,
            'has_location': False,
            'location': None,
            'invite_link': None,
            'linked_chat_id': None,
            'default_banned_rights': None,
            'has_scheduled_messages': False,
            'folder_id': None,
            'call_active': False,
            'call_not_empty': False,
            'video_calls_available': False,
            'groupcall_default_join_as': None,
            # Информация о закрепленном сообщении (значения по умолчанию для ошибок)
            'pinned_message_text': None,
            'pinned_message_date': None,
            'pinned_message_author': None,
            'pinned_message_views': 0,
            # Статистика сообщений (значения по умолчанию для ошибок)
            'recent_messages_count': 0,
            'avg_message_length': 0,
            'messages_with_media': 0,
            'messages_with_links': 0,
            'most_active_hour': None,
            'last_message_date': None,
            # Детальное описание (значения по умолчанию для ошибок)
            'full_description': '',
            'description_length': 0,
            'has_description': False,
            # Ссылки на другие группы (значения по умолчанию для ошибок)
            'found_group_links': [],
            'group_links_count': 0
        }
        
        try:
            # Получаем полную информацию о чате
            if hasattr(entity, 'id'):
                full_chat = await self.client.get_entity(entity.id)
                
                # Пытаемся получить полную информацию через get_full_chat
                try:
                    from telethon.tl.functions.channels import GetFullChannelRequest
                    from telethon.tl.functions.messages import GetFullChatRequest
                    
                    if hasattr(entity, 'megagroup') or hasattr(entity, 'broadcast'):
                        # Для каналов и супергрупп
                        full_info = await self.client(GetFullChannelRequest(entity))
                        full_chat_info = full_info.full_chat
                        
                        # Извлекаем дополнительную информацию
                        extended_info.update({
                            'admin_count': getattr(full_chat_info, 'admins_count', 0),
                            'online_count': getattr(full_chat_info, 'online_count', 0),
                            'slow_mode_delay': getattr(full_chat_info, 'slowmode_seconds', 0),
                            'has_pinned_message': bool(getattr(full_chat_info, 'pinned_msg_id', None)),
                            'pinned_message_id': getattr(full_chat_info, 'pinned_msg_id', None),
                            'can_view_stats': getattr(full_chat_info, 'can_view_stats', False),
                            'has_location': bool(getattr(full_chat_info, 'location', None)),
                            'invite_link': getattr(full_chat_info, 'exported_invite', {}).get('link') if hasattr(getattr(full_chat_info, 'exported_invite', {}), 'get') else None,
                            'linked_chat_id': getattr(full_chat_info, 'linked_chat_id', None),
                            'has_scheduled_messages': getattr(full_chat_info, 'has_scheduled', False),
                            'folder_id': getattr(full_chat_info, 'folder_id', None),
                            'call_active': getattr(full_chat_info, 'call', {}).get('id') is not None if hasattr(getattr(full_chat_info, 'call', {}), 'get') else False,
                            'video_calls_available': getattr(full_chat_info, 'video_calls_available', False)
                        })
                        
                        # Получаем информацию о правах по умолчанию
                        if hasattr(full_chat_info, 'default_banned_rights'):
                            rights = full_chat_info.default_banned_rights
                            extended_info['default_banned_rights'] = {
                                'send_messages': not getattr(rights, 'send_messages', False),
                                'send_media': not getattr(rights, 'send_media', False),
                                'send_stickers': not getattr(rights, 'send_stickers', False),
                                'send_gifs': not getattr(rights, 'send_gifs', False),
                                'send_games': not getattr(rights, 'send_games', False),
                                'send_inline': not getattr(rights, 'send_inline', False),
                                'embed_links': not getattr(rights, 'embed_links', False),
                                'send_polls': not getattr(rights, 'send_polls', False),
                                'change_info': not getattr(rights, 'change_info', False),
                                'invite_users': not getattr(rights, 'invite_users', False),
                                'pin_messages': not getattr(rights, 'pin_messages', False)
                            }
                        
                    else:
                        # Для обычных групп
                        full_info = await self.client(GetFullChatRequest(entity.id))
                        full_chat_info = full_info.full_chat
                        
                        extended_info.update({
                            'admin_count': len(getattr(full_chat_info, 'participants', {}).get('participants', [])) if hasattr(getattr(full_chat_info, 'participants', {}), 'get') else 0
                        })
                        
                except Exception as e:
                    self.logger.debug(f"Не удалось получить полную информацию о чате {entity.id}: {e}")
                
                # Пытаемся получить статистику активности
                try:
                    # Получаем последние сообщения для оценки активности
                    messages = await self.client.get_messages(entity, limit=10)
                    if messages:
                        # Считаем сообщения за последние 24 часа
                        from datetime import datetime, timedelta
                        yesterday = datetime.now() - timedelta(days=1)
                        recent_messages = [msg for msg in messages if msg.date and msg.date > yesterday]
                        extended_info['recent_actions'] = len(recent_messages)
                except Exception as e:
                    self.logger.debug(f"Не удалось получить статистику активности для {entity.id}: {e}")
                    
        except Exception as e:
            self.logger.debug(f"Ошибка при получении расширенной информации для {entity.id}: {e}")
        
        # Получаем дополнительную информацию
        try:
            # Получаем содержимое закрепленного сообщения
            pinned_info = await self._get_pinned_message_content(entity)
            extended_info.update(pinned_info)
            
            # Получаем статистику сообщений
            messages_stats = await self._get_recent_messages_stats(entity)
            extended_info.update(messages_stats)
            
            # Получаем детальное описание
            description_info = await self._get_detailed_description(entity)
            extended_info.update(description_info)
            
            # Получаем ссылки на другие группы
            links_info = await self._get_group_links(entity)
            extended_info.update(links_info)
            
        except Exception as e:
            self.logger.debug(f"Ошибка при получении дополнительной информации для {entity.id}: {e}")
        
        return extended_info

    def _determine_chat_type(self, entity):
        """Определяет тип чата"""
        if hasattr(entity, 'broadcast') and entity.broadcast:
            return 'channel'
        elif hasattr(entity, 'megagroup') and entity.megagroup:
            return 'supergroup'
        else:
            return 'group'
    
    async def _check_user_send_permissions(self, entity):
        """Проверяет права текущего пользователя на отправку сообщений в группу/канал
        
        Returns:
            str: "можно" если можно отправлять сообщения, "нельзя" если нельзя, "не ясно" если не удалось проверить
        """
        if not self.client or not self.current_user_id:
            return "не ясно"
        
        try:
            # Получаем права пользователя в чате
            permissions = await self.client.get_permissions(entity, self.current_user_id)
            
            # Проверяем право на отправку сообщений
            # В Telethon это может быть send_messages или просто bool
            if hasattr(permissions, 'send_messages'):
                can_send = permissions.send_messages
                self.logger.debug(f"Права на отправку сообщений в {entity.id}: {can_send}")
                return "можно" if can_send else "нельзя"
            # Также проверяем через ParticipantPermissions
            elif hasattr(permissions, 'participant') and hasattr(permissions.participant, 'banned_rights'):
                # Если есть banned_rights, проверяем send_messages
                banned_rights = permissions.participant.banned_rights
                if banned_rights and hasattr(banned_rights, 'send_messages'):
                    can_send = not banned_rights.send_messages
                    self.logger.debug(f"Права через banned_rights в {entity.id}: {can_send}")
                    return "можно" if can_send else "нельзя"
            else:
                # Если атрибут send_messages отсутствует, проверяем другие индикаторы
                # Например, если пользователь заблокирован или ограничен
                if hasattr(permissions, 'banned') and permissions.banned:
                    self.logger.debug(f"Пользователь заблокирован в {entity.id}")
                    return "нельзя"
                elif hasattr(permissions, 'restricted') and permissions.restricted:
                    # Если ограничен, проверяем можно ли отправлять сообщения
                    if hasattr(permissions, 'rights') and permissions.rights:
                        rights = permissions.rights
                        if hasattr(rights, 'send_messages'):
                            return "можно" if rights.send_messages else "нельзя"
                    self.logger.debug(f"Пользователь ограничен в {entity.id}")
                    return "нельзя"
                else:
                    # Если нет явных ограничений, считаем что можно отправлять
                    self.logger.debug(f"Права не ограничены в {entity.id}, можно отправлять")
                    return "можно"
                    
        except ChatAdminRequiredError:
            self.logger.debug(f"Нет прав администратора для проверки прав в {entity.id}")
            return "не ясно"
        except UserBannedInChannelError:
            self.logger.debug(f"Пользователь заблокирован в {entity.id}")
            return "нельзя"
        except Exception as e:
            self.logger.debug(f"Ошибка при проверке прав в {entity.id}: {e}")
            return "не ясно"
    
    def _safe_get_description(self, entity):
        """Безопасно получает описание чата"""
        description = getattr(entity, 'about', '') or ''
        # Обрезаем описание до 300 символов
        return description[:300] if description else ''
    
    def _format_date(self, date_obj):
        """Форматирует дату"""
        if date_obj:
            return date_obj.strftime('%Y-%m-%d %H:%M:%S')
        return None
    
    def _get_error_message(self, error):
        """Преобразует ошибку в понятное сообщение"""
        error_messages = {
            ChannelPrivateError: "Канал приватный или недоступен",
            UsernameNotOccupiedError: "Username не существует",
            ChatAdminRequiredError: "Требуются права администратора",
            UserBannedInChannelError: "Пользователь заблокирован в канале"
        }
        return error_messages.get(type(error), str(error))
    
    def _create_error_info(self, chat_identifier, error_message, status='error'):
        """Создает оптимизированную структуру данных для ошибки"""
        return {
            'id': None,
            'actual_title': 'Error/Unavailable',
            'actual_username': None,
            'members_count': 0,
            'chat_type': 'unknown',
            'created_date': None,
            'check_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'access_status': status,
            'error_message': error_message[:200],  # Ограничиваем длину сообщения
            # Только значимые поля
            'online_count': 0,
            'slow_mode_delay': 0,
            'pinned_message_id': None,
            'linked_chat_id': None,
            'can_send_messages': "не ясно"  # Не удалось проверить
        }
    

    def _load_existing_data(self, output_file_path):
        """Загружает существующие данные из выходного файла"""
        try:
            if not output_file_path:
                self.logger.info("📋 Выходной файл не указан, начинаем с нуля")
                return pd.DataFrame(), set(), set()
            
            if not os.path.exists(output_file_path):
                self.logger.info("📋 Выходной файл не существует, начинаем с нуля")
                return pd.DataFrame(), set(), set()
            
            # Проверяем доступность файла
            if not os.access(output_file_path, os.R_OK):
                self.logger.warning(f"⚠️  Нет доступа на чтение файла: {output_file_path}")
                return pd.DataFrame(), set(), set()
            
            # Проверяем размер файла
            file_size = os.path.getsize(output_file_path)
            if file_size == 0:
                self.logger.info("📋 Выходной файл пуст, начинаем с нуля")
                return pd.DataFrame(), set(), set()
            
            try:
                existing_df = pd.read_csv(output_file_path, encoding='utf-8')
            except UnicodeDecodeError:
                # Пробуем другие кодировки
                try:
                    existing_df = pd.read_csv(output_file_path, encoding='latin-1')
                    self.logger.warning("⚠️  Файл загружен с кодировкой latin-1 вместо utf-8")
                except Exception as e:
                    self.logger.error(f"❌ Ошибка чтения файла: {e}")
                    return pd.DataFrame(), set(), set()
            
            # Проверяем, что файл не пустой после загрузки
            if existing_df.empty:
                self.logger.info("📋 Загруженный файл пуст, начинаем с нуля")
                return pd.DataFrame(), set(), set()
            
            # Создаем множество уже обработанных ID и username для быстрого поиска
            processed_ids = set()
            processed_usernames = set()
            
            if 'id' in existing_df.columns:
                processed_ids = set(existing_df['id'].dropna().astype(str))
            
            if 'actual_username' in existing_df.columns:
                # Нормализуем username из существующих данных (убираем https://t.me/ и @)
                processed_usernames = set()
                for username_val in existing_df['actual_username'].dropna():
                    username_str = str(username_val).strip()
                    # Убираем https://t.me/ или @ в начале
                    if username_str.startswith('https://t.me/'):
                        username_str = username_str[13:]  # Убираем 'https://t.me/'
                    elif username_str.startswith('@'):
                        username_str = username_str[1:]  # Убираем '@'
                    if username_str and username_str.lower() not in ('nan', 'none', ''):
                        processed_usernames.add(username_str.lower())
            
            # Также проверяем поле username из входного CSV (если оно есть в выходном файле)
            if 'username' in existing_df.columns:
                for username_val in existing_df['username'].dropna():
                    username_str = str(username_val).strip()
                    # Убираем https://t.me/ или @ в начале
                    if username_str.startswith('https://t.me/'):
                        username_str = username_str[13:]  # Убираем 'https://t.me/'
                    elif username_str.startswith('@'):
                        username_str = username_str[1:]  # Убираем '@'
                    if username_str and username_str.lower() not in ('nan', 'none', ''):
                        processed_usernames.add(username_str.lower())
            
            self.logger.info(f"📋 Загружено {len(existing_df)} существующих записей")
            self.logger.info(f"   - По ID: {len(processed_ids)} записей")
            self.logger.info(f"   - По username: {len(processed_usernames)} записей")
            
            return existing_df, processed_ids, processed_usernames
                
        except pd.errors.EmptyDataError:
            self.logger.warning("⚠️  CSV файл пуст")
            return pd.DataFrame(), set(), set()
        except pd.errors.ParserError as e:
            self.logger.error(f"❌ Ошибка парсинга CSV файла: {e}")
            return pd.DataFrame(), set(), set()
        except PermissionError as e:
            self.logger.error(f"❌ Нет прав доступа к файлу {output_file_path}: {e}")
            return pd.DataFrame(), set(), set()
        except FileNotFoundError:
            self.logger.info("📋 Выходной файл не найден, начинаем с нуля")
            return pd.DataFrame(), set(), set()
        except Exception as e:
            self.logger.warning(f"⚠️  Ошибка при загрузке существующих данных: {e}")
            return pd.DataFrame(), set(), set()
    
    def _is_already_processed(self, chat_identifier, processed_ids, processed_usernames, row=None):
        """Проверяет, была ли группа уже обработана
        
        Args:
            chat_identifier: ID группы (int) или username (str)
            processed_ids: Множество уже обработанных ID
            processed_usernames: Множество уже обработанных username (нормализованных)
            row: Опционально, строка DataFrame для проверки поля username из входного CSV
        """
        # Проверяем по ID
        if isinstance(chat_identifier, int):
            if str(chat_identifier) in processed_ids:
                return True
        
        # Также проверяем ID из входной строки (если есть)
        if row is not None:
            input_id = row.get('id')
            if input_id and pd.notna(input_id):
                try:
                    input_id_str = str(int(float(input_id)))  # Конвертируем в int через float для корректной обработки
                    if input_id_str in processed_ids:
                        return True
                except (ValueError, TypeError):
                    pass
        
        # Проверяем по username из chat_identifier
        if isinstance(chat_identifier, str):
            # Нормализуем username: убираем @, https://t.me/, приводим к lowercase
            username_str = chat_identifier.strip()
            if username_str.startswith('https://t.me/'):
                username_str = username_str[13:]  # Убираем 'https://t.me/'
            elif username_str.startswith('@'):
                username_str = username_str[1:]  # Убираем '@'
            username_str = username_str.lower()
            if username_str in processed_usernames:
                return True
        
        # Также проверяем username из входной строки (если есть)
        if row is not None:
            input_username = row.get('username')
            if input_username and pd.notna(input_username):
                username_str = str(input_username).strip()
                # Убираем https://t.me/ или @ в начале
                if username_str.startswith('https://t.me/'):
                    username_str = username_str[13:]
                elif username_str.startswith('@'):
                    username_str = username_str[1:]
                username_str = username_str.lower()
                if username_str and username_str not in ('nan', 'none', '') and username_str in processed_usernames:
                    return True
        
        return False
    
    def _merge_with_existing_data(self, new_df, existing_df):
        """Объединяет новые данные с существующими"""
        if existing_df.empty:
            return new_df
        
        if new_df.empty:
            return existing_df
        
        try:
            # Объединяем данные, избегая дубликатов
            # Подавляем предупреждение FutureWarning для пустых колонок
            import warnings
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", FutureWarning)
                combined_df = pd.concat([existing_df, new_df], ignore_index=True)
            
            # Удаляем дубликаты по ID (если есть)
            if 'id' in combined_df.columns:
                combined_df = combined_df.drop_duplicates(subset=['id'], keep='last')
            
            # Удаляем дубликаты по username (если нет ID)
            elif 'actual_username' in combined_df.columns:
                combined_df = combined_df.drop_duplicates(subset=['actual_username'], keep='last')
            
            self.logger.info(f"📊 Объединено данных: {len(existing_df)} существующих + {len(new_df)} новых = {len(combined_df)} итого")
            
            return combined_df
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка при объединении данных: {e}")
            return new_df

    async def process_csv_file(self, input_file_path, output_file_path=None, existing_data=None, skip_client_init=False):
        """Обрабатывает CSV файл с группами с проверкой дубликатов
        
        Args:
            input_file_path: Путь к входному CSV файлу
            output_file_path: Путь к выходному CSV файлу (опционально)
            existing_data: Кортеж (existing_df, processed_ids, processed_usernames) для избежания повторной загрузки
            skip_client_init: Если True, не инициализирует клиент (используется при обработке нескольких файлов)
        """
        
        log_separator(self.logger, "НАЧАЛО ОБРАБОТКИ")
        
        try:
            # Валидация входного файла
            if not input_file_path:
                raise ValueError("Не указан входной файл")
            
            if not os.path.exists(input_file_path):
                raise FileNotFoundError(f"Входной файл не найден: {input_file_path}")
            
            if not os.access(input_file_path, os.R_OK):
                raise PermissionError(f"Нет прав на чтение файла: {input_file_path}")
            
            # Читаем CSV файл
            self.logger.info(f"📂 Чтение файла: {input_file_path}")
            try:
                df = pd.read_csv(input_file_path, encoding='utf-8')
            except UnicodeDecodeError:
                # Пробуем другие кодировки
                try:
                    df = pd.read_csv(input_file_path, encoding='latin-1')
                    self.logger.warning("⚠️  Файл загружен с кодировкой latin-1 вместо utf-8")
                except Exception as e:
                    raise ValueError(f"Ошибка чтения файла: {e}")
            
            if df.empty:
                raise ValueError("Входной CSV файл пуст")
            
            self.logger.info(f"📊 Загружено {len(df)} записей")
            self.logger.info(f"��📋 Колонки: {list(df.columns)}")
            
            # Определяем выходной файл
            if not output_file_path:
                output_file_path = Config.get_output_file_path(
                    os.path.basename(input_file_path)
                )
            
            # Загружаем существующие данные для проверки дубликатов
            if existing_data:
                existing_df, processed_ids, processed_usernames = existing_data
            else:
                existing_df, processed_ids, processed_usernames = self._load_existing_data(output_file_path)
            
            # Инициализируем клиент только если нужно
            if not skip_client_init:
                if not await self.initialize_client():
                    raise Exception("Не удалось инициализировать Telegram клиент")
            else:
                # Если клиент уже инициализирован, проверяем что current_user_id установлен
                if not self.current_user_id and self.client:
                    try:
                        me = await self.client.get_me()
                        if me:
                            self.current_user_id = me.id
                            self.logger.info(f"✅ Установлен current_user_id: {self.current_user_id} (при skip_client_init)")
                    except Exception as e:
                        self.logger.warning(f"⚠️  Не удалось получить current_user_id: {e}")
                elif self.current_user_id:
                    self.logger.debug(f"✅ current_user_id уже установлен: {self.current_user_id}")
                else:
                    self.logger.warning(f"⚠️  current_user_id не установлен и клиент не инициализирован")
            
            # Добавляем новые колонки
            # Оптимизированные колонки - только значимые поля
            new_columns = [
                # Основная информация
                'id', 'actual_title', 'actual_username', 'members_count', 'chat_type',
                'created_date', 'check_date',
                
                # Активность и статистика
                'online_count', 'slow_mode_delay', 'pinned_message_id', 'linked_chat_id',
                
                # Права пользователя
                'can_send_messages',
                
                # Статус обработки (для отладки)
                'access_status', 'error_message'
            ]
            
            for col in new_columns:
                df[col] = None
            
            # Обрабатываем каждую строку с проверкой дубликатов
            self.logger.info("🚀 Начинаем обработку групп...")
            
            successful = 0
            errors = 0
            access_denied = 0
            skipped = 0
            
            # Создаем список для новых данных
            new_rows = []
            
            # Время начала обработки для расчета прогноза
            start_time = time.time()
            total_groups = len(df)
            
            # Настройки для пауз между батчами запросов
            # Случайный интервал между паузами: 50-100 запросов
            pause_interval = random.randint(50, 100)
            # Случайная длительность паузы: 5-10 минут
            pause_minutes = random.randint(5, 10)
            pause_seconds = pause_minutes * 60
            
            # Счетчик запросов к API (только реальные запросы, не пропущенные)
            api_requests_count = 0
            
            self.logger.info("")
            self.logger.info("=" * 70)
            self.logger.info("📊 Настройки пауз между батчами запросов:")
            self.logger.info(f"   Пауза каждые: {pause_interval} запросов")
            self.logger.info(f"   Длительность паузы: {pause_minutes} минут")
            self.logger.info("=" * 70)
            self.logger.info("")
            
            # Функция форматирования времени
            def format_time(seconds):
                if seconds < 60:
                    return f"{int(seconds)}с"
                elif seconds < 3600:
                    return f"{int(seconds // 60)}м {int(seconds % 60)}с"
                else:
                    hours = int(seconds // 3600)
                    minutes = int((seconds % 3600) // 60)
                    return f"{hours}ч {minutes}м"
            
            # Используем tqdm для прогресс-бара
            for index in tqdm(range(len(df)), desc="Обработка групп"):
                row = df.iloc[index]
                current_position = index + 1
                
                # Определяем идентификатор чата
                chat_identifier = self._get_chat_identifier(row)
                
                if not chat_identifier:
                    # Пропускаем строки без валидного идентификатора - они не могут быть обработаны
                    self.logger.warning(f"⚠️  Строка {current_position}/{total_groups}: нет валидного идентификатора (title: '{row.get('title', 'N/A')}') - пропускаем")
                    errors += 1
                    continue
                
                # Проверяем, была ли группа уже обработана
                if self._is_already_processed(chat_identifier, processed_ids, processed_usernames, row=row):
                    group_title = row.get('title', 'Без названия')
                    self.logger.info(f"⏭️  [{current_position}/{total_groups}] Пропускаем '{group_title}' ({chat_identifier}) - уже обработана")
                    skipped += 1
                    continue
                
                # Примечание: для пропущенных групп задержка не применяется (это нормально),
                # задержка применяется только перед реальными запросами к API
                
                # Рассчитываем прогресс и время (только для групп, которые будут обрабатываться)
                remaining_groups = total_groups - current_position
                elapsed_time = time.time() - start_time
                processed_count = current_position - skipped - errors - 1  # -1 потому что текущую еще не обработали
                avg_time_per_group = elapsed_time / processed_count if processed_count > 0 else 0
                estimated_remaining_time = avg_time_per_group * remaining_groups if avg_time_per_group > 0 else 0
                
                # Диагностический вывод
                group_title = row.get('title', 'Без названия')
                group_id = row.get('id', 'N/A')
                group_username = row.get('username', 'N/A')
                
                self.logger.info("")
                self.logger.info(f"📊 Прогресс: {current_position}/{total_groups} групп | Осталось: {remaining_groups}")
                self.logger.info(f"⏱️  Время: прошло {format_time(elapsed_time)} | Осталось ~{format_time(estimated_remaining_time)}")
                self.logger.info(f"📋 Текущая группа: '{group_title}'")
                if group_id and pd.notna(group_id) and str(group_id) != 'nan':
                    self.logger.info(f"   ID: {group_id}")
                if group_username and pd.notna(group_username) and str(group_username) != 'nan':
                    self.logger.info(f"   Username: {group_username}")
                self.logger.info(f"   Идентификатор: {chat_identifier}")
                
                # Проверяем, нужно ли сделать паузу между батчами
                if api_requests_count > 0 and api_requests_count % pause_interval == 0:
                    self.logger.info("")
                    self.logger.info("=" * 70)
                    self.logger.info(f"⏸️  ПАУЗА: обработано {api_requests_count} запросов")
                    self.logger.info(f"   Делаем паузу на {pause_minutes} минут для снижения нагрузки на API")
                    self.logger.info(f"   Это поможет избежать FloodWait и других ограничений")
                    self.logger.info("=" * 70)
                    
                    # Показываем обратный отсчет
                    total_wait_seconds = pause_minutes * 60
                    elapsed_seconds = 0
                    
                    while elapsed_seconds < total_wait_seconds:
                        remaining_seconds = total_wait_seconds - elapsed_seconds
                        remaining_minutes = int(remaining_seconds // 60)
                        remaining_secs = int(remaining_seconds % 60)
                        
                        if remaining_seconds > 60:
                            # Обновляем каждую минуту
                            self.logger.info(f"⏳ Осталось ждать: {remaining_minutes}м {remaining_secs}с")
                            await asyncio.sleep(60)
                            elapsed_seconds += 60
                        else:
                            # Последняя минута - обновляем каждые 30 секунд
                            self.logger.info(f"⏳ Осталось ждать: {remaining_secs}с")
                            sleep_time = min(30, remaining_seconds)
                            await asyncio.sleep(sleep_time)
                            elapsed_seconds += sleep_time
                    
                    self.logger.info("✅ Пауза завершена, продолжаем обработку...")
                    self.logger.info("")
                    
                    # Определяем новый случайный интервал для следующей паузы
                    pause_interval = random.randint(50, 100)
                    pause_minutes = random.randint(5, 10)
                    pause_seconds = pause_minutes * 60
                    self.logger.info(f"📊 Следующая пауза будет через {pause_interval} запросов на {pause_minutes} минут")
                    self.logger.info("")
                
                # Получаем информацию о группе
                try:
                    info = await self.get_chat_info(chat_identifier)
                    # Увеличиваем счетчик после каждого реального запроса к API
                    api_requests_count += 1
                except CriticalFloodWaitError as e:
                    # Критический FloodWait - прерываем обработку
                    self.logger.error("")
                    self.logger.error("=" * 70)
                    self.logger.error("❌ КРИТИЧЕСКАЯ ОШИБКА: FloodWait слишком долгий")
                    self.logger.error(f"   Группа: {e.chat_identifier}")
                    self.logger.error(f"   Требуется ожидание: {e.wait_hours:.1f} часов ({e.wait_days:.1f} дней)")
                    self.logger.error(f"   Это превышает максимально допустимое время ожидания (2 часа)")
                    self.logger.error("")
                    self.logger.error("💡 Рекомендации:")
                    self.logger.error("   1. Прервите обработку (Ctrl+C)")
                    self.logger.error(f"   2. Подождите {e.wait_hours:.1f} часов и попробуйте снова")
                    self.logger.error("   3. Проверьте, не слишком ли часто вы делаете запросы к API")
                    self.logger.error("")
                    self.logger.error("⚠️  Обработка прервана для предотвращения длительного ожидания")
                    self.logger.error("=" * 70)
                    
                    # Сохраняем текущий прогресс перед выходом
                    if new_rows:
                        try:
                            new_df = pd.DataFrame(new_rows)
                            # ... сохранение данных ...
                            self.logger.info(f"💾 Сохранен промежуточный прогресс: {len(new_rows)} групп")
                        except Exception as save_error:
                            self.logger.error(f"Ошибка при сохранении промежуточного прогресса: {save_error}")
                    
                    # Прерываем обработку
                    raise
                
                if info:
                    # Создаем новую строку с данными
                    new_row = row.to_dict()
                    new_row.update(info)
                    new_rows.append(new_row)
                    
                    # Подсчитываем статистику
                    status = info.get('access_status', 'error')
                    if status == 'success':
                        successful += 1
                        self.logger.info(f"✅ Успешно обработана: {info.get('actual_title', group_title)}")
                        if 'members_count' in info:
                            self.logger.info(f"   Участников: {info.get('members_count', 'N/A')}")
                        if 'can_send_messages' in info:
                            self.logger.info(f"   Можно постить: {info.get('can_send_messages', 'N/A')}")
                    elif status == 'access_denied':
                        access_denied += 1
                        self.logger.warning(f"🚫 Доступ запрещен: {group_title}")
                    else:
                        errors += 1
                        error_msg = info.get('error_message', 'Неизвестная ошибка')
                        self.logger.error(f"❌ Ошибка: {error_msg}")
                else:
                    errors += 1
                    self.logger.error(f"❌ Не удалось получить информацию о группе")
                
                # Промежуточное сохранение каждые 10 записей
                if len(new_rows) > 0 and len(new_rows) % 10 == 0:
                    try:
                        temp_df = pd.DataFrame(new_rows)
                        final_df = self._merge_with_existing_data(temp_df, existing_df)
                        
                        # Удаляем колонку Unnamed: 0 если она есть
                        if 'Unnamed: 0' in final_df.columns:
                            final_df = final_df.drop(columns=['Unnamed: 0'])
                        
                        # Конвертируем числовые поля в целые числа (can_send_messages теперь строка)
                        if not final_df.empty:
                            integer_columns = ['id', 'members_count', 'online_count', 'slow_mode_delay', 
                                              'pinned_message_id', 'linked_chat_id']
                            for col in integer_columns:
                                if col in final_df.columns:
                                    final_df[col] = pd.to_numeric(final_df[col], errors='coerce').fillna(0).astype(int)
                        
                        # Проверяем доступность директории для записи
                        output_dir = os.path.dirname(output_file_path)
                        if output_dir and not os.path.exists(output_dir):
                            os.makedirs(output_dir, exist_ok=True)
                        
                        if not os.access(output_dir if output_dir else '.', os.W_OK):
                            raise PermissionError(f"Нет прав на запись в директорию: {output_dir}")
                        
                        final_df.to_csv(output_file_path, index=False, encoding='utf-8')
                        self.logger.debug(f"💾 Промежуточное сохранение: {len(new_rows)} новых записей")
                    except PermissionError as e:
                        self.logger.error(f"❌ Ошибка прав доступа при сохранении: {e}")
                        # Продолжаем работу, но не сохраняем
                    except OSError as e:
                        self.logger.error(f"❌ Ошибка записи файла: {e}")
                        # Продолжаем работу, но не сохраняем
                    except Exception as e:
                        self.logger.error(f"❌ Ошибка при промежуточном сохранении: {e}")
            
            # Создаем DataFrame из новых данных и объединяем с существующими
            if new_rows:
                new_df = pd.DataFrame(new_rows)
                final_df = self._merge_with_existing_data(new_df, existing_df)
            else:
                final_df = existing_df
            
            # Удаляем колонку Unnamed: 0 если она есть
            if 'Unnamed: 0' in final_df.columns:
                final_df = final_df.drop(columns=['Unnamed: 0'])
            
            # Конвертируем числовые поля в целые числа перед сохранением (can_send_messages теперь строка)
            if not final_df.empty:
                integer_columns = ['id', 'members_count', 'online_count', 'slow_mode_delay', 
                                  'pinned_message_id', 'linked_chat_id']
                for col in integer_columns:
                    if col in final_df.columns:
                        final_df[col] = pd.to_numeric(final_df[col], errors='coerce').fillna(0).astype(int)
            
            # Финальное сохранение
            try:
                # Проверяем доступность директории для записи
                output_dir = os.path.dirname(output_file_path)
                if output_dir and not os.path.exists(output_dir):
                    os.makedirs(output_dir, exist_ok=True)
                
                if not os.access(output_dir if output_dir else '.', os.W_OK):
                    raise PermissionError(f"Нет прав на запись в директорию: {output_dir}")
                
                final_df.to_csv(output_file_path, index=False, encoding='utf-8')
                self.logger.info(f"✅ Файл успешно сохранен: {output_file_path}")
            except PermissionError as e:
                self.logger.error(f"❌ Нет прав на запись файла {output_file_path}: {e}")
                raise
            except OSError as e:
                self.logger.error(f"❌ Ошибка записи файла {output_file_path}: {e}")
                raise
            except Exception as e:
                self.logger.error(f"❌ Ошибка при сохранении файла {output_file_path}: {e}")
                raise
            
            # Выводим статистику
            log_separator(self.logger, "РЕЗУЛЬТАТЫ")
            self.logger.info(f"📊 Входных записей: {len(df)}")
            self.logger.info(f"✅ Успешно обработано: {successful}")
            self.logger.info(f"⏭️  Пропущено (уже обработано): {skipped}")
            self.logger.info(f"🚫 Доступ запрещен: {access_denied}")
            self.logger.info(f"❌ Ошибки: {errors}")
            self.logger.info(f"📁 Итого записей в файле: {len(final_df) if 'final_df' in locals() else len(existing_df)}")
            self.logger.info(f"💾 Результат сохранен в: {output_file_path}")
            log_separator(self.logger, "ОБРАБОТКА ЗАВЕРШЕНА")
            self.logger.info("🎉 Обработка файла завершена успешно!")
            
            return {
                'total': len(df),
                'successful': successful,
                'skipped': skipped,
                'access_denied': access_denied,
                'errors': errors,
                'output_file': output_file_path
            }
            
        except Exception as e:
            self.logger.error(f"Критическая ошибка при обработке файла: {e}")
            raise
        
        finally:
            # Отключаем клиент только если это не обработка нескольких файлов
            if not skip_client_init and self.client:
                await self.client.disconnect()
                self.logger.info("Отключение от Telegram")
    
    def _get_chat_identifier(self, row):
        """Извлекает идентификатор чата из строки DataFrame"""
        # Валидация входных данных
        if row is None or not isinstance(row, pd.Series):
            return None
        
        # Пробуем username
        if 'username' in row and pd.notna(row['username']) and row['username']:
            username = str(row['username']).strip()
            # Проверяем, что username не пустой и не 'nan'
            if username and username.lower() not in ('nan', 'none', ''):
                # Убираем лишние @ в начале
                username = username.lstrip('@')
                # Валидация формата username (только буквы, цифры, подчеркивания)
                if username and all(c.isalnum() or c == '_' for c in username) and len(username) <= 32:
                    return f"@{username}"
                else:
                    self.logger.debug(f"Некорректный формат username: {username}")
        
        # Пробуем id
        if 'id' in row and pd.notna(row['id']):
            try:
                # Обрабатываем как float или int
                id_value = row['id']
                if pd.isna(id_value):
                    return None
                # Преобразуем в int (работает с float тоже)
                chat_id = int(float(id_value))
                # Проверяем, что ID положительный (Telegram IDs всегда положительные)
                if chat_id > 0:
                    return chat_id
                else:
                    self.logger.debug(f"Отрицательный или нулевой ID чата: {chat_id}")
            except (ValueError, TypeError, OverflowError) as e:
                self.logger.debug(f"Ошибка преобразования ID {id_value}: {e}")
        
        return None

    async def __aenter__(self):
        """Контекстный менеджер - вход"""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Контекстный менеджер - выход"""
        if self.client:
            await self.client.disconnect()
    
    def _get_file_hash(self, file_path):
        """Вычисляет хеш файла для отслеживания изменений"""
        try:
            with open(file_path, 'rb') as f:
                file_hash = hashlib.md5(f.read()).hexdigest()
            return file_hash
        except Exception as e:
            self.logger.warning(f"Не удалось вычислить хеш файла {file_path}: {e}")
            # Используем имя файла и размер как альтернативу
            try:
                stat = os.stat(file_path)
                return f"{os.path.basename(file_path)}_{stat.st_size}_{stat.st_mtime}"
            except:
                return os.path.basename(file_path)
    
    def _load_processed_files(self):
        """Загружает список обработанных файлов"""
        processed_files = set()
        if os.path.exists(Config.PROCESSED_FILES_LOG):
            try:
                with open(Config.PROCESSED_FILES_LOG, 'r', encoding='utf-8') as f:
                    for line in f:
                        line = line.strip()
                        if line:
                            processed_files.add(line)
            except Exception as e:
                self.logger.warning(f"Ошибка чтения лога обработанных файлов: {e}")
        return processed_files
    
    def _mark_file_as_processed(self, file_path):
        """Отмечает файл как обработанный"""
        try:
            file_hash = self._get_file_hash(file_path)
            # Записываем как абсолютный путь и хеш
            entry = f"{os.path.abspath(file_path)}|{file_hash}"
            
            # Создаем директорию для логов если нужно
            os.makedirs(os.path.dirname(Config.PROCESSED_FILES_LOG), exist_ok=True)
            
            with open(Config.PROCESSED_FILES_LOG, 'a', encoding='utf-8') as f:
                f.write(f"{entry}\n")
        except Exception as e:
            self.logger.warning(f"Ошибка записи в лог обработанных файлов: {e}")
    
    def _is_file_processed(self, file_path, processed_files):
        """Проверяет, был ли файл уже обработан"""
        file_hash = self._get_file_hash(file_path)
        abs_path = os.path.abspath(file_path)
        
        # Проверяем по абсолютному пути и хешу
        for entry in processed_files:
            if '|' in entry:
                path, hash_value = entry.split('|', 1)
                if path == abs_path and hash_value == file_hash:
                    return True
            elif entry == abs_path or entry == file_hash:
                return True
        
        return False
    
    def _get_csv_files_from_input(self, exclude_groups_csv=True):
        """Получает список всех CSV файлов из директории input"""
        csv_files = []
        input_dir = Config.INPUT_DIR
        
        if not os.path.exists(input_dir):
            self.logger.warning(f"Директория {input_dir} не существует")
            return csv_files
        
        # Ищем все CSV файлы
        pattern = os.path.join(input_dir, '*.csv')
        csv_files = glob.glob(pattern)
        
        # Исключаем groups.csv если нужно
        if exclude_groups_csv:
            groups_csv_path = Config.get_input_file_path()
            csv_files = [f for f in csv_files if f != groups_csv_path]
        
        # Сортируем по времени изменения (старые первыми)
        csv_files.sort(key=lambda x: os.path.getmtime(x))
        
        return csv_files
    
    def _detect_csv_separator(self, file_path):
        """Автоматически определяет разделитель CSV файла"""
        separators = [',', ';', '\t']
        best_separator = ','
        max_cols = 0
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                first_line = f.readline()
                # Пробуем разные разделители
                for sep in separators:
                    cols = first_line.split(sep)
                    if len(cols) > max_cols:
                        max_cols = len(cols)
                        best_separator = sep
        except UnicodeDecodeError:
            with open(file_path, 'r', encoding='latin-1') as f:
                first_line = f.readline()
                for sep in separators:
                    cols = first_line.split(sep)
                    if len(cols) > max_cols:
                        max_cols = len(cols)
                        best_separator = sep
        except Exception as e:
            self.logger.debug(f"Ошибка определения разделителя: {e}")
        
        return best_separator
    
    def _normalize_csv_columns(self, df, separator):
        """Нормализует структуру CSV, маппит колонки к стандартному формату"""
        # Возможные названия колонок для маппинга
        column_mapping = {
            'id': ['id', 'ид', 'chat_id', 'group_id'],
            'username': ['username', 'user', 'user_name', 'user_name', 'nick', 'nickname'],
            'title': ['title', 'name', 'group_name', 'chat_name', 'название'],
            'date': ['date', 'created', 'created_at', 'timestamp', 'дата']
        }
        
        # Создаем результирующий DataFrame
        normalized_df = pd.DataFrame()
        
        # Определяем маппинг колонок
        actual_mapping = {}
        for standard_col, possible_names in column_mapping.items():
            for col in df.columns:
                col_lower = str(col).strip().lower()
                if col_lower in [name.lower() for name in possible_names]:
                    actual_mapping[standard_col] = col
                    break
        
        # Если колонок нет, пробуем определить по позиции (для файлов без заголовков)
        if len(actual_mapping) == 0 and len(df.columns) >= 2:
            # Предполагаем формат: id, username, title, date
            if len(df.columns) >= 1:
                actual_mapping['id'] = df.columns[0]
            if len(df.columns) >= 2:
                actual_mapping['username'] = df.columns[1]
            if len(df.columns) >= 3:
                actual_mapping['title'] = df.columns[2]
            if len(df.columns) >= 4:
                actual_mapping['date'] = df.columns[3]
        
        # Копируем данные с переименованием
        for standard_col, source_col in actual_mapping.items():
            if source_col in df.columns:
                normalized_df[standard_col] = df[source_col]
            else:
                normalized_df[standard_col] = None
        
        # Если есть колонки, которые не были замаплены, но содержат данные
        # пробуем их использовать для отсутствующих стандартных колонок
        unused_cols = [col for col in df.columns if col not in actual_mapping.values()]
        
        # Если нет username, но есть другие колонки - пробуем использовать первую текстовую
        if 'username' not in normalized_df.columns or normalized_df['username'].isna().all():
            for col in unused_cols:
                if df[col].dtype == 'object':  # Текстовая колонка
                    sample_val = str(df[col].iloc[0] if len(df) > 0 else '')
                    # Проверяем, похоже ли на username (начинается с @ или содержит буквы/цифры/подчеркивания)
                    if sample_val.startswith('@') or (sample_val and all(c.isalnum() or c == '_' for c in sample_val.replace('@', ''))):
                        normalized_df['username'] = df[col]
                        break
        
        return normalized_df
    
    def _read_csv_with_flexible_format(self, file_path):
        """Читает CSV файл с автоматическим определением разделителя и формата"""
        separators = [',', ';', '\t']
        encodings = ['utf-8', 'latin-1', 'cp1251']
        
        last_error = None
        
        for encoding in encodings:
            for sep in separators:
                try:
                    # Пробуем читать с заголовками
                    df = pd.read_csv(file_path, sep=sep, encoding=encoding, header=0)
                    
                    # Проверяем, есть ли хотя бы одна нужная колонка
                    cols_lower = [str(c).lower().strip() for c in df.columns]
                    has_id = any('id' in c or 'ид' in c for c in cols_lower)
                    has_username = any('username' in c or 'user' in c or 'nick' in c for c in cols_lower)
                    
                    # Если есть нужные колонки или достаточно колонок (>= 2), пробуем нормализовать
                    if has_id or has_username or len(df.columns) >= 2:
                        normalized_df = self._normalize_csv_columns(df, sep)
                        if 'id' in normalized_df.columns or 'username' in normalized_df.columns:
                            self.logger.debug(f"✅ Файл прочитан с разделителем '{sep}' и кодировкой {encoding}")
                            return normalized_df, sep
                    
                    # Если колонок нет или они не подходят, пробуем без заголовков
                    if len(df.columns) >= 2:
                        df_no_header = pd.read_csv(file_path, sep=sep, encoding=encoding, header=None)
                        if len(df_no_header.columns) >= 2:
                            # Переименовываем колонки по позиции
                            column_names = ['id', 'username', 'title', 'date'][:len(df_no_header.columns)]
                            df_no_header.columns = column_names
                            normalized_df = self._normalize_csv_columns(df_no_header, sep)
                            if 'id' in normalized_df.columns or 'username' in normalized_df.columns:
                                self.logger.debug(f"✅ Файл прочитан без заголовков с разделителем '{sep}' и кодировкой {encoding}")
                                return normalized_df, sep
                    
                except pd.errors.EmptyDataError:
                    return pd.DataFrame(), sep
                except Exception as e:
                    last_error = e
                    continue
        
        # Если ничего не сработало, пробуем с автоматическим определением
        try:
            detected_sep = self._detect_csv_separator(file_path)
            # Используем error_bad_lines для старых версий pandas или on_bad_lines для новых
            try:
                df = pd.read_csv(file_path, sep=detected_sep, encoding='utf-8', header=None, on_bad_lines='skip')
            except TypeError:
                # Для старых версий pandas
                df = pd.read_csv(file_path, sep=detected_sep, encoding='utf-8', header=None, error_bad_lines=False, warn_bad_lines=False)
            if len(df.columns) >= 2:
                column_names = ['id', 'username', 'title', 'date'][:len(df.columns)]
                df.columns = column_names
                normalized_df = self._normalize_csv_columns(df, detected_sep)
                self.logger.debug(f"✅ Файл прочитан с автоматически определенным разделителем '{detected_sep}'")
                return normalized_df, detected_sep
        except Exception as e:
            last_error = e
        
        raise Exception(f"Не удалось прочитать файл: {last_error}")
    
    def _merge_csv_files_to_groups(self):
        """Объединяет все CSV файлы из input/ в groups.csv, удаляя дубликаты и исходные файлы"""
        
        log_separator(self.logger, "ОБЪЕДИНЕНИЕ CSV ФАЙЛОВ В GROUPS.CSV")
        
        groups_csv_path = Config.get_input_file_path()
        
        # Загружаем существующий groups.csv если он есть
        if os.path.exists(groups_csv_path):
            try:
                existing_df = pd.read_csv(groups_csv_path, encoding='utf-8')
                self.logger.info(f"📋 Загружен существующий groups.csv: {len(existing_df)} записей")
            except Exception as e:
                self.logger.warning(f"⚠️  Ошибка чтения groups.csv: {e}, создаем новый")
                existing_df = pd.DataFrame()
        else:
            existing_df = pd.DataFrame()
            self.logger.info("📋 groups.csv не существует, будет создан новый")
        
        # Создаем множества для быстрой проверки дубликатов
        existing_ids = set()
        existing_usernames = set()
        
        if not existing_df.empty:
            if 'id' in existing_df.columns:
                # Нормализуем ID: преобразуем float в int, затем в строку
                existing_ids = set()
                for id_val in existing_df['id'].dropna():
                    try:
                        existing_ids.add(str(int(float(id_val))))
                    except (ValueError, TypeError):
                        pass
            if 'username' in existing_df.columns:
                # Нормализуем username: убираем @, https://t.me/, приводим к lowercase
                existing_usernames = set()
                for username_val in existing_df['username'].dropna():
                    username_str = str(username_val).strip()
                    # Убираем https://t.me/ или @ в начале
                    if username_str.startswith('https://t.me/'):
                        username_str = username_str[13:]  # Убираем 'https://t.me/'
                    elif username_str.startswith('@'):
                        username_str = username_str[1:]  # Убираем '@'
                    username_str = username_str.lower()
                    if username_str and username_str not in ('nan', 'none', ''):
                        existing_usernames.add(username_str)
        
        # Получаем все CSV файлы кроме groups.csv
        csv_files = self._get_csv_files_from_input(exclude_groups_csv=True)
        
        if not csv_files:
            self.logger.info("✅ Нет файлов для объединения (кроме groups.csv)")
            return {
                'merged_files': 0,
                'added_groups': 0,
                'skipped_groups': 0,
                'total_groups': len(existing_df) if not existing_df.empty else 0
            }
        
        self.logger.info(f"📂 Найдено {len(csv_files)} CSV файлов для объединения")
        
        merged_files = 0
        total_added = 0
        total_skipped = 0
        new_rows = []
        
        # Обрабатываем каждый файл
        for idx, csv_file in enumerate(csv_files, 1):
            file_name = os.path.basename(csv_file)
            log_separator(self.logger, f"ОБРАБОТКА ФАЙЛА {idx}/{len(csv_files)}: {file_name}")
            
            try:
                # Читаем CSV файл с гибким форматом
                self.logger.info(f"📂 Чтение файла: {file_name}")
                try:
                    df, detected_sep = self._read_csv_with_flexible_format(csv_file)
                    if detected_sep != ',':
                        self.logger.debug(f"🔍 Определен разделитель: '{detected_sep}'")
                except Exception as e:
                    self.logger.error(f"❌ Ошибка при обработке файла {file_name}: {e}")
                    continue
                
                if df.empty:
                    self.logger.warning(f"⚠️  Файл {file_name} пуст, пропускаем")
                    os.remove(csv_file)
                    self.logger.info(f"🗑️  Удален пустой файл: {file_name}")
                    continue
                
                self.logger.info(f"📊 Загружено {len(df)} записей из {file_name}")
                
                # Проверяем наличие необходимых колонок (хотя бы одна должна быть)
                has_id = 'id' in df.columns
                has_username = 'username' in df.columns
                
                if not has_id and not has_username:
                    self.logger.warning(f"⚠️  В файле {file_name} отсутствуют колонки id и username, пропускаем")
                    os.remove(csv_file)
                    self.logger.info(f"🗑️  Удален файл с неверной структурой: {file_name}")
                    continue
                
                added_count = 0
                skipped_count = 0
                
                # Проходим по каждой строке
                for _, row in df.iterrows():
                    # Проверяем, есть ли уже эта группа в groups.csv
                    is_duplicate = False
                    
                    # Проверяем по ID (если есть)
                    if 'id' in df.columns and 'id' in row and pd.notna(row['id']):
                        try:
                            row_id = str(int(float(row['id'])))
                            if row_id in existing_ids:
                                is_duplicate = True
                                skipped_count += 1
                        except (ValueError, TypeError):
                            pass
                    
                    # Проверяем по username если ID не найден или не совпал (если есть)
                    if not is_duplicate and 'username' in df.columns and 'username' in row and pd.notna(row['username']):
                        username = str(row['username']).strip()
                        # Убираем https://t.me/ или @ в начале
                        if username.startswith('https://t.me/'):
                            username = username[13:]  # Убираем 'https://t.me/'
                        elif username.startswith('@'):
                            username = username[1:]  # Убираем '@'
                        username = username.lower()
                        if username and username not in ('nan', 'none', ''):
                            if username in existing_usernames:
                                is_duplicate = True
                                skipped_count += 1
                    
                    # Если не дубликат, добавляем
                    if not is_duplicate:
                        new_row = row.to_dict()
                        new_rows.append(new_row)
                        added_count += 1
                        
                        # Обновляем множества для быстрой проверки
                        if 'id' in df.columns and 'id' in new_row and pd.notna(new_row['id']):
                            try:
                                existing_ids.add(str(int(float(new_row['id']))))
                            except (ValueError, TypeError):
                                pass
                        if 'username' in df.columns and 'username' in new_row and pd.notna(new_row['username']):
                            username = str(new_row['username']).strip()
                            # Убираем https://t.me/ или @ в начале
                            if username.startswith('https://t.me/'):
                                username = username[13:]
                            elif username.startswith('@'):
                                username = username[1:]
                            username = username.lower()
                            if username and username not in ('nan', 'none', ''):
                                existing_usernames.add(username)
                
                self.logger.info(f"✅ Из {file_name}: добавлено {added_count}, пропущено {skipped_count} дубликатов")
                total_added += added_count
                total_skipped += skipped_count
                
                # Удаляем обработанный файл
                os.remove(csv_file)
                self.logger.info(f"🗑️  Удален файл: {file_name}")
                merged_files += 1
                
            except Exception as e:
                self.logger.error(f"❌ Ошибка при обработке файла {file_name}: {e}")
                import traceback
                self.logger.debug(traceback.format_exc())
                # Не удаляем файл при ошибке, чтобы не потерять данные
        
        # Объединяем новые данные с существующими
        if new_rows:
            new_df = pd.DataFrame(new_rows)
            
            # Объединяем с существующими данными
            if existing_df.empty:
                final_df = new_df
            else:
                # Используем concat с подавлением предупреждения
                import warnings
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore", FutureWarning)
                    final_df = pd.concat([existing_df, new_df], ignore_index=True)
            
            # Удаляем дубликаты по ID (если есть)
            if 'id' in final_df.columns:
                before_dedup = len(final_df)
                final_df = final_df.drop_duplicates(subset=['id'], keep='last')
                if len(final_df) < before_dedup:
                    self.logger.info(f"📊 Удалено дубликатов по ID: {before_dedup - len(final_df)}")
            
            # Сохраняем обновленный groups.csv
            final_df.to_csv(groups_csv_path, index=False, encoding='utf-8')
            self.logger.info(f"💾 Обновлен groups.csv: {len(final_df)} записей (было {len(existing_df)}, добавлено {len(new_df)})")
        else:
            final_df = existing_df
            if not existing_df.empty:
                self.logger.info(f"💾 groups.csv без изменений: {len(existing_df)} записей")
        
        log_separator(self.logger, "РЕЗУЛЬТАТЫ ОБЪЕДИНЕНИЯ")
        self.logger.info(f"📁 Обработано файлов: {merged_files}")
        self.logger.info(f"✅ Добавлено новых групп: {total_added}")
        self.logger.info(f"⏭️  Пропущено дубликатов: {total_skipped}")
        self.logger.info(f"📊 Итого групп в groups.csv: {len(final_df)}")
        
        return {
            'merged_files': merged_files,
            'added_groups': total_added,
            'skipped_groups': total_skipped,
            'total_groups': len(final_df)
        }
    
    async def process_all_input_files(self, unified_output_file=None):
        """Обрабатывает все CSV файлы из директории input
        
        Процесс:
        1. Объединяет все CSV файлы (кроме groups.csv) в groups.csv
        2. Удаляет исходные файлы после объединения
        3. Обрабатывает groups.csv через Telegram API
        """
        
        log_separator(self.logger, "ОБРАБОТКА ВСЕХ ФАЙЛОВ ИЗ INPUT/")
        
        # ШАГ 1: Объединяем все CSV файлы в groups.csv
        merge_result = self._merge_csv_files_to_groups()
        
        if merge_result['merged_files'] == 0:
            self.logger.info("ℹ️  Нет файлов для объединения, переходим к обработке groups.csv")
        
        # ШАГ 2: Обрабатываем groups.csv через Telegram API
        groups_csv_path = Config.get_input_file_path()
        
        if not os.path.exists(groups_csv_path):
            self.logger.warning("⚠️  Файл groups.csv не существует после объединения")
            return {
                'merged_files': merge_result['merged_files'],
                'added_groups': merge_result['added_groups'],
                'skipped_groups': merge_result['skipped_groups'],
                'total_groups_in_groups_csv': merge_result['total_groups'],
                'api_processed': False,
                'api_successful': 0,
                'api_access_denied': 0,
                'api_errors': 0,
                'output_file': None
            }
        
        # Определяем единый выходной файл
        if not unified_output_file:
            unified_output_file = Config.get_output_file_path()
        
        log_separator(self.logger, "ОБРАБОТКА GROUPS.CSV ЧЕРЕЗ TELEGRAM API")
        
        # Инициализируем клиент
        if not await self.initialize_client():
            raise Exception("Не удалось инициализировать Telegram клиент")
        
        # Загружаем существующие данные из выходного файла
        existing_df, processed_ids, processed_usernames = self._load_existing_data(unified_output_file)
        
        try:
            # Обрабатываем groups.csv через Telegram API
            api_result = await self.process_csv_file(
                input_file_path=groups_csv_path,
                output_file_path=unified_output_file,
                existing_data=(existing_df, processed_ids, processed_usernames),
                skip_client_init=True  # Клиент уже инициализирован
            )
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка при обработке groups.csv через API: {e}")
            api_result = {
                'total': 0,
                'successful': 0,
                'access_denied': 0,
                'errors': 1,
                'output_file': unified_output_file
            }
        
        # Отключаем клиент
        if self.client:
            await self.client.disconnect()
            self.logger.info("Отключение от Telegram")
        
        log_separator(self.logger, "ОБРАБОТКА ВСЕХ ФАЙЛОВ ЗАВЕРШЕНА")
        self.logger.info("🎉 Обработка всех файлов завершена успешно!")
        
        # Финальная статистика
        log_separator(self.logger, "ИТОГОВАЯ СТАТИСТИКА")
        self.logger.info(f"📁 Объединено файлов: {merge_result['merged_files']}")
        self.logger.info(f"✅ Добавлено новых групп в groups.csv: {merge_result['added_groups']}")
        self.logger.info(f"⏭️  Пропущено дубликатов: {merge_result['skipped_groups']}")
        self.logger.info(f"📊 Всего групп в groups.csv: {merge_result['total_groups']}")
        self.logger.info("")
        self.logger.info(f"📡 Обработка через Telegram API:")
        self.logger.info(f"  - Всего записей: {api_result.get('total', 0)}")
        self.logger.info(f"  - Успешно: {api_result.get('successful', 0)}")
        self.logger.info(f"  - Пропущено (уже обработано): {api_result.get('skipped', 0)}")
        self.logger.info(f"  - Доступ запрещен: {api_result.get('access_denied', 0)}")
        self.logger.info(f"  - Ошибки: {api_result.get('errors', 0)}")
        self.logger.info(f"💾 Результат сохранен в: {unified_output_file}")
        
        return {
            'merged_files': merge_result['merged_files'],
            'added_groups': merge_result['added_groups'],
            'skipped_groups': merge_result['skipped_groups'],
            'total_groups_in_groups_csv': merge_result['total_groups'],
            'api_processed': True,
            'api_total': api_result.get('total', 0),
            'api_successful': api_result.get('successful', 0),
            'api_skipped': api_result.get('skipped', 0),
            'api_access_denied': api_result.get('access_denied', 0),
            'api_errors': api_result.get('errors', 0),
            'output_file': unified_output_file
        }
