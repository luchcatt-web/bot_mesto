"""
Telegram бот для барбершопа с интеграцией YClients.
- Собирает контакты клиентов через QR-код
- Мгновенные уведомления о новых записях
- Уведомления об изменении/отмене записи
- Напоминания за 24 часа и 2 часа до визита
"""

import asyncio
import logging
import sqlite3
import json
import locale
from datetime import datetime, timedelta
from pathlib import Path

# Устанавливаем русскую локаль для дней недели
try:
    locale.setlocale(locale.LC_TIME, 'ru_RU.UTF-8')
except:
    pass

# Дни недели на русском
WEEKDAYS_RU = {
    0: 'понедельник',
    1: 'вторник', 
    2: 'среда',
    3: 'четверг',
    4: 'пятница',
    5: 'суббота',
    6: 'воскресенье'
}

MONTHS_RU = {
    1: 'января', 2: 'февраля', 3: 'марта', 4: 'апреля',
    5: 'мая', 6: 'июня', 7: 'июля', 8: 'августа',
    9: 'сентября', 10: 'октября', 11: 'ноября', 12: 'декабря'
}


def generate_ics_file(record: dict) -> bytes:
    """Генерация .ics файла для добавления в календарь"""
    datetime_str = record.get("datetime", "")
    services_list = record.get("services") or []
    services = ", ".join([s.get("title", "") for s in services_list if isinstance(s, dict)])
    
    staff = record.get("staff") or {}
    staff_name = staff.get("name", "") if isinstance(staff, dict) else ""
    
    record_id = record.get("id", "0")
    
    # Парсим дату
    try:
        if "T" in datetime_str:
            dt_start = datetime.fromisoformat(datetime_str.replace("Z", "+00:00")).replace(tzinfo=None)
        else:
            dt_start = datetime.strptime(datetime_str, "%Y-%m-%d %H:%M:%S")
    except:
        dt_start = datetime.now() + timedelta(days=1)
    
    # Длительность услуги (по умолчанию 1 час)
    duration_minutes = 60
    for s in services_list:
        if isinstance(s, dict) and s.get("length"):
            duration_minutes = s.get("length", 60)
            break
    
    dt_end = dt_start + timedelta(minutes=duration_minutes)
    
    # Форматируем даты для ICS
    dt_format = "%Y%m%dT%H%M%S"
    dt_start_str = dt_start.strftime(dt_format)
    dt_end_str = dt_end.strftime(dt_format)
    dt_now_str = datetime.now().strftime(dt_format)
    
    # Напоминание за 1 час
    ics_content = f"""BEGIN:VCALENDAR
VERSION:2.0
PRODID:-//Место Барбершоп//RU
CALSCALE:GREGORIAN
METHOD:PUBLISH
BEGIN:VEVENT
UID:{record_id}@mesto-barbershop
DTSTAMP:{dt_now_str}
DTSTART:{dt_start_str}
DTEND:{dt_end_str}
SUMMARY:{services}
DESCRIPTION:Мастер: {staff_name}\\nТелефон: {BARBERSHOP_PHONE}
LOCATION:{BARBERSHOP_ADDRESS}
BEGIN:VALARM
TRIGGER:-PT1H
ACTION:DISPLAY
DESCRIPTION:Напоминание: {services} через 1 час
END:VALARM
BEGIN:VALARM
TRIGGER:-PT15M
ACTION:DISPLAY
DESCRIPTION:Напоминание: {services} через 15 минут
END:VALARM
END:VEVENT
END:VCALENDAR"""
    
    return ics_content.encode('utf-8')


# Хранилище записей для callback (временное)
records_cache = {}


def format_record_datetime(datetime_str: str) -> str:
    """Форматирование даты и времени записи: '5 февраля (среда) в 13:30'"""
    if not datetime_str:
        return ""
    
    try:
        if "T" in datetime_str:
            dt = datetime.fromisoformat(datetime_str.replace("Z", "+00:00")).replace(tzinfo=None)
        else:
            dt = datetime.strptime(datetime_str, "%Y-%m-%d")
        
        day = dt.day
        month = MONTHS_RU.get(dt.month, "")
        weekday = WEEKDAYS_RU.get(dt.weekday(), "")
        time_str = dt.strftime("%H:%M")
        
        return f"{day} {month} ({weekday}) в {time_str}"
    except:
        return datetime_str

import aiohttp
from aiogram import Bot, Dispatcher, F
from aiogram.filters import CommandStart, Command
from aiogram.types import (
    Message,
    KeyboardButton,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    CallbackQuery,
    BufferedInputFile,
)
from aiogram.enums import ParseMode

# ==================== НАСТРОЙКИ ====================

# Telegram Bot
BOT_TOKEN = "8579638826:AAHg2YB8IQmc08VOQdS8TS6EVsYRS28ZQgE"

# YClients API
YCLIENTS_PARTNER_TOKEN = "befz68u9gpj6n3ut5zrs"
YCLIENTS_USER_TOKEN = "7ab80bc153a8328ecfae9b339b30b804"
YCLIENTS_COMPANY_ID = "1540716"

# Ссылка на онлайн-запись
YCLIENTS_BOOKING_URL = "https://n1729941.yclients.com"

# Настройки барбершопа
BARBERSHOP_NAME = "Место"
BARBERSHOP_ADDRESS = "ул. Войстроченко, 10"
BARBERSHOP_PHONE = "+7 (4832) 377-888"

# Интервал проверки записей (в секундах)
CHECK_INTERVAL = 5  # Проверяем каждые 5 секунд

# Минимальный перенос для уведомления (в минутах)
MIN_RESCHEDULE_MINUTES = 15

# Секретный код для регистрации сотрудников
STAFF_SECRET_CODE = "mesto2024"

# ==================== ЛОГИРОВАНИЕ ====================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# ==================== S3 ХРАНИЛИЩЕ ====================

S3_ACCESS_KEY = "X0WIIVO2PP8K39TFXMCN"
S3_SECRET_KEY = "btbb2Go6idj7zYxi4o37RMFrlnXXPYSd8E39cPcB"
S3_BUCKET = "09756767-976b-412c-8e2b-dc5b155d26a8"
S3_ENDPOINT = "https://s3.twcstorage.ru"
S3_DB_KEY = "clients.db"

# Резервные данные клиентов (не теряются при перезапуске)
# Формат: telegram_id: phone
BACKUP_CLIENTS = {
    354738973: "+79999798000",  # Твой номер
}

# Резервные данные сотрудников
# Формат: telegram_id: (staff_name, yclients_staff_id)
BACKUP_STAFF = {
    # Добавляй сюда сотрудников
}

# ==================== БАЗА ДАННЫХ ====================

DB_PATH = Path(__file__).parent / "clients.db"


def sync_db_from_s3():
    """Скачать базу данных из S3 при старте"""
    try:
        import boto3
        from botocore.config import Config
        from botocore.exceptions import ClientError
        
        logger.info(f"Подключаюсь к S3: {S3_ENDPOINT}, bucket: {S3_BUCKET}")
        
        s3 = boto3.client(
            's3',
            endpoint_url=S3_ENDPOINT,
            aws_access_key_id=S3_ACCESS_KEY,
            aws_secret_access_key=S3_SECRET_KEY,
            region_name='ru-1',
            config=Config(signature_version='s3v4')
        )
        
        s3.download_file(S3_BUCKET, S3_DB_KEY, str(DB_PATH))
        logger.info("✅ База данных загружена из S3")
        return True
    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', 'Unknown')
        logger.info(f"S3 ClientError ({error_code}): {e}")
        return False
    except Exception as e:
        logger.info(f"S3 ошибка загрузки: {type(e).__name__}: {e}")
        return False


def sync_db_to_s3():
    """Загрузить базу данных в S3"""
    try:
        import boto3
        from botocore.config import Config
        from botocore.exceptions import ClientError
        
        logger.info("Сохраняю базу в S3...")
        
        s3 = boto3.client(
            's3',
            endpoint_url=S3_ENDPOINT,
            aws_access_key_id=S3_ACCESS_KEY,
            aws_secret_access_key=S3_SECRET_KEY,
            region_name='ru-1',
            config=Config(signature_version='s3v4')
        )
        
        s3.upload_file(str(DB_PATH), S3_BUCKET, S3_DB_KEY)
        logger.info("✅ База данных сохранена в S3")
        return True
    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', 'Unknown')
        logger.error(f"S3 ClientError ({error_code}): {e}")
        return False
    except Exception as e:
        logger.error(f"S3 ошибка сохранения: {type(e).__name__}: {e}")
        return False


def init_db():
    """Инициализация базы данных"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Таблица клиентов
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS clients (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            telegram_id INTEGER UNIQUE NOT NULL,
            phone_number TEXT NOT NULL,
            first_name TEXT,
            last_name TEXT,
            username TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Таблица для отслеживания записей (для обнаружения изменений)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS tracked_records (
            record_id INTEGER PRIMARY KEY,
            client_phone TEXT,
            datetime TEXT,
            services TEXT,
            staff_name TEXT,
            status TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Таблица отправленных уведомлений
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS sent_notifications (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            record_id INTEGER NOT NULL,
            notification_type TEXT NOT NULL,
            sent_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(record_id, notification_type)
        )
    """)
    
    # Таблица сотрудников
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS staff (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            telegram_id INTEGER UNIQUE NOT NULL,
            staff_name TEXT NOT NULL,
            yclients_staff_id INTEGER,
            phone_number TEXT,
            is_active INTEGER DEFAULT 1,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Таблица для отслеживания статуса "пришёл"
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS attendance_notified (
            record_id INTEGER PRIMARY KEY,
            notified_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Индекс для быстрого поиска по телефону
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_phone ON clients(phone_number)
    """)
    
    conn.commit()
    conn.close()
    logger.info("База данных инициализирована")


def save_client(telegram_id: int, phone: str, first_name: str = None,
                last_name: str = None, username: str = None):
    """Сохранение клиента"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    phone = normalize_phone(phone)
    
    try:
        cursor.execute("""
            INSERT INTO clients (telegram_id, phone_number, first_name, last_name, username)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(telegram_id) DO UPDATE SET
                phone_number = excluded.phone_number,
                first_name = excluded.first_name,
                last_name = excluded.last_name,
                username = excluded.username
        """, (telegram_id, phone, first_name, last_name, username))
        conn.commit()
        logger.info(f"Клиент сохранён: {phone} (Telegram ID: {telegram_id})")
        
        # Сохраняем базу в S3
        sync_db_to_s3()
        
        return True
    except Exception as e:
        logger.error(f"Ошибка сохранения клиента: {e}")
        return False
    finally:
        conn.close()


def get_telegram_id_by_phone(phone: str):
    """Получение Telegram ID по номеру телефона"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    phone_clean = ''.join(filter(str.isdigit, phone))[-10:]
    
    cursor.execute(
        "SELECT telegram_id FROM clients WHERE phone_number LIKE ?",
        (f"%{phone_clean}",)
    )
    result = cursor.fetchone()
    conn.close()
    return result[0] if result else None


def get_tracked_record(record_id: int):
    """Получить сохранённую запись"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM tracked_records WHERE record_id = ?", (record_id,))
    result = cursor.fetchone()
    conn.close()
    return result


def save_tracked_record(record_id: int, client_phone: str, datetime_str: str, 
                        services: str, staff_name: str, status: str = "active"):
    """Сохранить запись для отслеживания"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO tracked_records (record_id, client_phone, datetime, services, staff_name, status, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(record_id) DO UPDATE SET
            datetime = excluded.datetime,
            services = excluded.services,
            staff_name = excluded.staff_name,
            status = excluded.status,
            updated_at = CURRENT_TIMESTAMP
    """, (record_id, client_phone, datetime_str, services, staff_name, status))
    conn.commit()
    conn.close()


def get_all_tracked_record_ids():
    """Получить все ID отслеживаемых записей"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT record_id, datetime FROM tracked_records WHERE status = 'active'")
    results = cursor.fetchall()
    conn.close()
    return {r[0]: r[1] for r in results}


def mark_record_cancelled(record_id: int):
    """Отметить запись как отменённую"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(
        "UPDATE tracked_records SET status = 'cancelled', updated_at = CURRENT_TIMESTAMP WHERE record_id = ?",
        (record_id,)
    )
    conn.commit()
    conn.close()


def is_notification_sent(record_id: int, notification_type: str) -> bool:
    """Проверка, было ли отправлено уведомление"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(
        "SELECT 1 FROM sent_notifications WHERE record_id = ? AND notification_type = ?",
        (record_id, notification_type)
    )
    result = cursor.fetchone()
    conn.close()
    return result is not None


def mark_notification_sent(record_id: int, notification_type: str):
    """Отметить уведомление как отправленное"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(
        "INSERT OR IGNORE INTO sent_notifications (record_id, notification_type) VALUES (?, ?)",
        (record_id, notification_type)
    )
    conn.commit()
    conn.close()


def save_staff(telegram_id: int, staff_name: str, yclients_staff_id: int = None, phone: str = None):
    """Сохранение сотрудника"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
            INSERT INTO staff (telegram_id, staff_name, yclients_staff_id, phone_number)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(telegram_id) DO UPDATE SET
                staff_name = excluded.staff_name,
                yclients_staff_id = excluded.yclients_staff_id,
                phone_number = excluded.phone_number
        """, (telegram_id, staff_name, yclients_staff_id, phone))
        conn.commit()
        logger.info(f"Сотрудник сохранён: {staff_name} (Telegram ID: {telegram_id})")
        
        # Сохраняем базу в S3
        sync_db_to_s3()
        
        return True
    except Exception as e:
        logger.error(f"Ошибка сохранения сотрудника: {e}")
        return False
    finally:
        conn.close()


def get_all_staff_telegram_ids():
    """Получить Telegram ID всех активных сотрудников"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT telegram_id FROM staff WHERE is_active = 1")
    results = cursor.fetchall()
    conn.close()
    return [r[0] for r in results]


def get_registered_yclients_staff_ids():
    """Получить список YClients ID уже зарегистрированных сотрудников"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT yclients_staff_id FROM staff WHERE is_active = 1 AND yclients_staff_id IS NOT NULL")
    results = cursor.fetchall()
    conn.close()
    return [r[0] for r in results]


def get_staff_by_yclients_id(yclients_staff_id: int):
    """Получить сотрудника по YClients ID"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT telegram_id, staff_name FROM staff WHERE yclients_staff_id = ? AND is_active = 1", 
                   (yclients_staff_id,))
    result = cursor.fetchone()
    conn.close()
    return result


def is_attendance_notified(record_id: int) -> bool:
    """Проверить, было ли уже отправлено уведомление о приходе"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT 1 FROM attendance_notified WHERE record_id = ?", (record_id,))
    result = cursor.fetchone()
    conn.close()
    return result is not None


def mark_attendance_notified(record_id: int):
    """Отметить, что уведомление о приходе отправлено"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("INSERT OR IGNORE INTO attendance_notified (record_id) VALUES (?)", (record_id,))
    conn.commit()
    conn.close()


def normalize_phone(phone: str) -> str:
    """Нормализация номера телефона"""
    digits = ''.join(filter(str.isdigit, phone))
    if len(digits) == 11 and digits.startswith('8'):
        digits = '7' + digits[1:]
    elif len(digits) == 10:
        digits = '7' + digits
    return '+' + digits


# ==================== YCLIENTS API ====================

class YClientsAPI:
    """Клиент для работы с API YClients"""
    
    BASE_URL = "https://api.yclients.com/api/v1"
    
    def __init__(self, partner_token: str, user_token: str, company_id: str):
        self.partner_token = partner_token
        self.user_token = user_token
        self.company_id = company_id
        self._session = None
    
    async def _get_session(self):
        """Получить или создать сессию"""
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=10)
            self._session = aiohttp.ClientSession(timeout=timeout)
        return self._session
    
    def _headers(self) -> dict:
        return {
            "Accept": "application/vnd.yclients.v2+json",
            "Authorization": f"Bearer {self.partner_token}, User {self.user_token}",
            "Content-Type": "application/json"
        }
    
    async def get_records(self, date_from: str, date_to: str) -> list:
        """Получение записей за период"""
        url = f"{self.BASE_URL}/records/{self.company_id}"
        params = {
            "start_date": date_from,
            "end_date": date_to,
            "count": 500
        }
        
        try:
            session = await self._get_session()
            async with session.get(url, headers=self._headers(), params=params) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return data.get("data", [])
                else:
                    logger.error(f"YClients API error: {resp.status}")
                    return []
        except Exception as e:
            logger.error(f"YClients API exception: {e}")
            return []
    
    async def get_upcoming_records(self) -> list:
        """Получение записей на ближайшие 7 дней"""
        today = datetime.now().strftime("%Y-%m-%d")
        week_later = (datetime.now() + timedelta(days=7)).strftime("%Y-%m-%d")
        return await self.get_records(today, week_later)
    
    async def get_staff_list(self) -> list:
        """Получение списка мастеров"""
        url = f"{self.BASE_URL}/company/{self.company_id}/staff"
        
        try:
            session = await self._get_session()
            async with session.get(url, headers=self._headers()) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return data.get("data", [])
                else:
                    logger.error(f"YClients Staff API error: {resp.status}")
                    return []
        except Exception as e:
            logger.error(f"YClients Staff API exception: {e}")
            return []
    
    async def get_visit_link(self, record_id: int) -> str:
        """Получение ссылки на визит для клиента"""
        # Пробуем получить через API визитов
        url = f"{self.BASE_URL}/visits/{self.company_id}/{record_id}"
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=self._headers()) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        visit_data = data.get("data", {})
                        
                        # Ищем ссылку в данных визита
                        for field in ['client_link', 'short_link', 'link', 'url']:
                            if visit_data.get(field):
                                return visit_data.get(field)
        except Exception as e:
            logger.error(f"Ошибка получения ссылки визита: {e}")
        
        return None


# ==================== TELEGRAM BOT ====================

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()
yclients = YClientsAPI(YCLIENTS_PARTNER_TOKEN, YCLIENTS_USER_TOKEN, YCLIENTS_COMPANY_ID)


def get_contact_keyboard():
    """Клавиатура с кнопкой для отправки контакта"""
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="📱 Поделиться номером телефона", request_contact=True)]],
        resize_keyboard=True,
        one_time_keyboard=True
    )


def get_main_keyboard():
    """Главное меню"""
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📅 Мои записи")],
            [KeyboardButton(text="✏️ Записаться"), KeyboardButton(text="📞 Связаться")],
        ],
        resize_keyboard=True
    )


def get_record_keyboard():
    """Inline кнопки для записи"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✏️ Изменить / Отменить", url=YCLIENTS_BOOKING_URL)],
        [InlineKeyboardButton(text="📍 Как добраться", url=f"https://yandex.ru/maps/?text={BARBERSHOP_ADDRESS.replace(' ', '+')}")]
    ])


def get_booking_keyboard():
    """Кнопка записи"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✏️ Записаться онлайн", url=YCLIENTS_BOOKING_URL)]
    ])


# ==================== ОБРАБОТЧИКИ ====================

@dp.message(CommandStart())
async def cmd_start(message: Message):
    """Команда /start"""
    await message.answer(
        f"👋 Добро пожаловать в <b>{BARBERSHOP_NAME}</b>!\n\n"
        "Чтобы получать уведомления о записях, "
        "поделитесь своим номером телефона.\n\n"
        f"📍 {BARBERSHOP_ADDRESS}\n"
        f"📞 {BARBERSHOP_PHONE}\n\n"
        "🔒 Номер используется только для уведомлений.",
        parse_mode=ParseMode.HTML,
        reply_markup=get_contact_keyboard()
    )


@dp.message(Command("myrecords"))
async def cmd_my_records(message: Message):
    await show_my_records(message)


# Временное хранилище для регистрации сотрудников
staff_registration = {}


@dp.message(Command("staff"))
async def cmd_staff(message: Message):
    """Команда /staff для регистрации сотрудников"""
    await message.answer(
        "👨‍💼 <b>Регистрация сотрудника</b>\n\n"
        "Введите секретный код:",
        parse_mode=ParseMode.HTML
    )
    staff_registration[message.from_user.id] = {"step": "code"}


@dp.callback_query(F.data.startswith("staff_select_"))
async def handle_staff_select(callback: CallbackQuery):
    """Обработка выбора мастера при регистрации"""
    staff_id = int(callback.data.replace("staff_select_", ""))
    user_id = callback.from_user.id
    
    if user_id not in staff_registration:
        await callback.answer("❌ Сессия истекла. Начните заново /staff")
        return
    
    # Получаем имя из сохранённого словаря
    staff_names = staff_registration[user_id].get("staff_names", {})
    staff_name = staff_names.get(staff_id, "Мастер")
    
    # Сохраняем сотрудника с YClients ID
    save_staff(
        telegram_id=user_id,
        staff_name=staff_name,
        yclients_staff_id=staff_id,
        phone=None
    )
    
    del staff_registration[user_id]
    
    await callback.message.edit_text(
        f"✅ <b>Вы зарегистрированы!</b>\n\n"
        f"👤 {staff_name}\n\n"
        "Теперь уведомления о приходе ваших клиентов будут приходить вам.",
        parse_mode=ParseMode.HTML
    )
    await callback.answer("✅ Готово!")


@dp.message(F.text == "📅 Мои записи")
async def handle_my_records_button(message: Message):
    await show_my_records(message)


async def show_my_records(message: Message):
    """Показать записи клиента"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT phone_number FROM clients WHERE telegram_id = ?", (message.from_user.id,))
    result = cursor.fetchone()
    conn.close()
    
    if not result:
        await message.answer(
            "❌ Вы ещё не зарегистрированы.\nПоделитесь номером телефона:",
            reply_markup=get_contact_keyboard()
        )
        return
    
    phone = result[0]
    phone_clean = phone[-10:]
    
    try:
        records = await yclients.get_upcoming_records()
    except Exception as e:
        logger.error(f"Ошибка получения записей: {e}")
        await message.answer("😔 Не удалось загрузить записи. Попробуйте позже.", reply_markup=get_main_keyboard())
        return
    
    if not records:
        await message.answer(
            "📅 У вас нет предстоящих записей.",
            reply_markup=get_main_keyboard()
        )
        await message.answer("Хотите записаться?", reply_markup=get_booking_keyboard())
        return
    
    my_records = []
    for r in records:
        if not r or not isinstance(r, dict):
            continue
        client = r.get("client")
        if client and isinstance(client, dict):
            client_phone = client.get("phone", "")
            if client_phone and client_phone[-10:] == phone_clean:
                my_records.append(r)
    
    if not my_records:
        await message.answer(
            "📅 У вас нет предстоящих записей.",
            reply_markup=get_main_keyboard()
        )
        await message.answer("Хотите записаться?", reply_markup=get_booking_keyboard())
        return
    
    # Отправляем каждую запись отдельным сообщением с персональной ссылкой
    await message.answer(f"📅 <b>Ваши записи ({len(my_records)}):</b>", parse_mode=ParseMode.HTML, reply_markup=get_main_keyboard())
    
    for record in my_records:
        datetime_str = record.get("datetime", "")
        formatted_date = format_record_datetime(datetime_str)
        
        services_list = record.get("services") or []
        services = ", ".join([s.get("title", "") for s in services_list if isinstance(s, dict)])
        
        staff = record.get("staff") or {}
        staff_name = staff.get("name", "") if isinstance(staff, dict) else ""
        staff_position = staff.get("specialization", "") if isinstance(staff, dict) else ""
        if not staff_position:
            staff_position = staff.get("position", {}).get("title", "") if isinstance(staff.get("position"), dict) else ""
        
        staff_info = f"{staff_name}, {staff_position}" if staff_position else staff_name
        
        record_link = get_record_link(record)
        
        text = (
            f"🗓 <b>{formatted_date}</b>\n"
            f"✂️ {services}\n"
            f"👤 {staff_info}\n\n"
            f"<a href='{record_link}'>Изменить или отменить</a>"
        )
        
        await message.answer(
            text, 
            parse_mode=ParseMode.HTML,
            reply_markup=get_single_record_keyboard(record)
        )


@dp.message(F.contact)
async def handle_contact(message: Message):
    """Обработчик контакта"""
    contact = message.contact
    
    if contact.user_id != message.from_user.id:
        await message.answer("⚠️ Отправьте свой контакт.", reply_markup=get_contact_keyboard())
        return
    
    phone = contact.phone_number
    if not phone.startswith("+"):
        phone = "+" + phone
    
    if save_client(message.from_user.id, phone, contact.first_name, contact.last_name, message.from_user.username):
        await message.answer(
            f"✅ Отлично, {contact.first_name or 'друг'}!\n\n"
            f"Номер <code>{phone}</code> сохранён.\n\n"
            "Теперь вы будете получать:\n"
            "• 📲 Уведомление сразу при записи\n"
            "• 🔄 Уведомление при изменении/отмене\n"
            "• ⏰ Напоминание за 24 часа\n\n"
            f"Ждём вас в <b>{BARBERSHOP_NAME}</b>! 💈",
            parse_mode=ParseMode.HTML,
            reply_markup=get_main_keyboard()
        )
    else:
        await message.answer("😔 Ошибка. Попробуйте ещё раз.", reply_markup=get_contact_keyboard())


@dp.message(F.text == "✏️ Записаться")
async def handle_booking_button(message: Message):
    await message.answer(
        f"📅 Запишитесь в <b>{BARBERSHOP_NAME}</b>!",
        parse_mode=ParseMode.HTML,
        reply_markup=get_booking_keyboard()
    )


@dp.message(F.text == "📞 Связаться")
async def handle_contact_button(message: Message):
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📞 Позвонить", url=f"tel:+74832377888")],
        [InlineKeyboardButton(text="💬 Написать администратору", url="https://t.me/mesto_adminn")],
        [InlineKeyboardButton(text="📍 На карте", url=f"https://yandex.ru/maps/?text={BARBERSHOP_ADDRESS.replace(' ', '+')}")]
    ])
    await message.answer(
        f"📍 <b>{BARBERSHOP_NAME}</b>\n\n"
        f"🏠 {BARBERSHOP_ADDRESS}\n"
        f"📞 {BARBERSHOP_PHONE}\n\n"
        "Нажмите на кнопку ниже, чтобы написать администратору напрямую.",
        parse_mode=ParseMode.HTML,
        reply_markup=kb
    )


@dp.callback_query(F.data.startswith("calendar_"))
async def handle_calendar_callback(callback: CallbackQuery):
    """Обработчик кнопки Добавить в календарь"""
    record_id = callback.data.replace("calendar_", "")
    
    # Получаем запись из кэша
    record = records_cache.get(record_id)
    
    if not record:
        await callback.answer("⚠️ Запись не найдена. Попробуйте снова через 'Мои записи'.", show_alert=True)
        return
    
    # Генерируем .ics файл
    ics_content = generate_ics_file(record)
    
    # Получаем информацию для имени файла
    services_list = record.get("services") or []
    service_name = services_list[0].get("title", "Запись") if services_list else "Запись"
    
    # Отправляем файл
    ics_file = BufferedInputFile(
        ics_content,
        filename=f"Место_{service_name.replace(' ', '_')}.ics"
    )
    
    await callback.message.answer_document(
        ics_file,
        caption="📅 Откройте файл → добавится в календарь iPhone.\n\n"
                "✅ Напоминания: за 1 час и за 15 минут."
    )
    
    await callback.answer("📅 Файл для Apple календаря!")


@dp.message(F.text)
async def handle_text(message: Message):
    """Обработка текстовых сообщений"""
    user_id = message.from_user.id
    
    # Проверяем, идёт ли регистрация сотрудника
    if user_id in staff_registration:
        reg_data = staff_registration[user_id]
        
        if reg_data.get("step") == "code":
            if message.text == STAFF_SECRET_CODE:
                # Получаем список мастеров из YClients
                staff_list = await yclients.get_staff_list()
                
                if not staff_list:
                    await message.answer("❌ Не удалось загрузить список мастеров. Попробуйте позже.")
                    del staff_registration[user_id]
                    return
                
                # Получаем уже зарегистрированных
                registered_ids = get_registered_yclients_staff_ids()
                
                # Создаём кнопки для выбора мастера (исключаем зарегистрированных)
                buttons = []
                staff_names = {}
                for staff in staff_list:
                    staff_id = staff.get("id")
                    if staff_id in registered_ids:
                        continue  # Пропускаем уже зарегистрированных
                    staff_name = staff.get("name", "Без имени")
                    staff_names[staff_id] = staff_name
                    buttons.append([InlineKeyboardButton(
                        text=f"👤 {staff_name}",
                        callback_data=f"staff_select_{staff_id}"
                    )])
                
                if not buttons:
                    await message.answer("✅ Все мастера уже зарегистрированы!")
                    del staff_registration[user_id]
                    return
                
                staff_registration[user_id] = {"step": "select", "staff_names": staff_names}
                keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
                
                await message.answer(
                    "✅ Код верный!\n\n"
                    "Выберите себя:",
                    parse_mode=ParseMode.HTML,
                    reply_markup=keyboard
                )
            else:
                del staff_registration[user_id]
                await message.answer("❌ Неверный код. Попробуйте /staff заново.")
            return
    
    # Обычная обработка текста
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT 1 FROM clients WHERE telegram_id = ?", (message.from_user.id,))
    result = cursor.fetchone()
    conn.close()
    
    if result:
        await message.answer("Выберите действие:", reply_markup=get_main_keyboard())
    else:
        await message.answer("Поделитесь номером телефона:", reply_markup=get_contact_keyboard())


# ==================== УВЕДОМЛЕНИЯ ====================

def get_record_link(record: dict) -> str:
    """Получить ссылку на изменение записи"""
    # Проверяем все возможные поля со ссылкой
    for field in ['visit_url', 'client_link', 'short_link', 'record_link', 'link', 'links']:
        if record.get(field):
            link = record.get(field)
            if isinstance(link, str) and link.startswith('http'):
                return link
            elif isinstance(link, dict) and link.get('client'):
                return link.get('client')
    
    # Проверяем вложенные объекты
    if record.get("visit") and isinstance(record.get("visit"), dict):
        visit = record.get("visit")
        for field in ['url', 'link', 'short_link']:
            if visit.get(field):
                return visit.get(field)
    
    # Формируем ссылку через ID записи
    record_id = record.get("id")
    visit_id = record.get("visit_id")
    
    # Формат для мобильной версии YClients
    if visit_id:
        return f"{YCLIENTS_BOOKING_URL}/loyalty/record/{visit_id}"
    elif record_id:
        return f"{YCLIENTS_BOOKING_URL}/loyalty/record/{record_id}"
    
    return YCLIENTS_BOOKING_URL


def get_google_calendar_url(record: dict) -> str:
    """Генерация ссылки на Google Calendar"""
    datetime_str = record.get("datetime", "")
    services_list = record.get("services") or []
    services = ", ".join([s.get("title", "") for s in services_list if isinstance(s, dict)])
    
    staff = record.get("staff") or {}
    staff_name = staff.get("name", "") if isinstance(staff, dict) else ""
    
    # Парсим дату
    try:
        if "T" in datetime_str:
            dt_start = datetime.fromisoformat(datetime_str.replace("Z", "+00:00")).replace(tzinfo=None)
        else:
            dt_start = datetime.strptime(datetime_str, "%Y-%m-%d %H:%M:%S")
    except:
        dt_start = datetime.now() + timedelta(days=1)
    
    # Длительность услуги
    duration_minutes = 60
    for s in services_list:
        if isinstance(s, dict) and s.get("length"):
            duration_minutes = s.get("length", 60)
            break
    
    dt_end = dt_start + timedelta(minutes=duration_minutes)
    
    # Формат для Google Calendar: 20260205T133000
    dt_format = "%Y%m%dT%H%M%S"
    dates = f"{dt_start.strftime(dt_format)}/{dt_end.strftime(dt_format)}"
    
    # URL-encode параметры
    from urllib.parse import quote
    
    title = quote(f"{BARBERSHOP_NAME}: {services}")
    details = quote(f"Мастер: {staff_name}\nТелефон: {BARBERSHOP_PHONE}")
    location = quote(BARBERSHOP_ADDRESS)
    
    return f"https://calendar.google.com/calendar/render?action=TEMPLATE&text={title}&dates={dates}&details={details}&location={location}"


def get_single_record_keyboard(record: dict):
    """Кнопки для конкретной записи с персональной ссылкой"""
    record_link = get_record_link(record)
    
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✏️ Изменить / Отменить", url=record_link)],
        [InlineKeyboardButton(text="📍 Как добраться", url=f"https://yandex.ru/maps/?text={BARBERSHOP_ADDRESS.replace(' ', '+')}")]
    ])


async def send_new_record_notification(telegram_id: int, record: dict):
    """Уведомление о новой записи"""
    datetime_str = record.get("datetime", "")
    formatted_date = format_record_datetime(datetime_str)
    
    # Получаем имя клиента
    client = record.get("client") or {}
    client_name = client.get("name", "").split()[0] if isinstance(client, dict) and client.get("name") else ""
    
    services_list = record.get("services") or []
    services = ", ".join([s.get("title", "") for s in services_list if isinstance(s, dict)])
    
    staff = record.get("staff") or {}
    staff_name = staff.get("name", "") if isinstance(staff, dict) else ""
    
    record_link = get_record_link(record)
    
    # Приветствие с именем
    greeting = f"👋 {client_name}, вы" if client_name else "👋 Вы"
    
    text = (
        f"{greeting} записаны в 💈 <b>{BARBERSHOP_NAME.upper()}</b> на услугу\n\n"
        f"◾ {services}\n"
        f"к мастеру {staff_name}\n\n"
        f"👉 на <b>{formatted_date}</b>\n\n"
        f"С нетерпением ждём вашего визита!\n\n"
        f"<a href='{record_link}'>изменение записи</a>"
    )
    
    try:
        await bot.send_message(
            telegram_id, 
            text, 
            parse_mode=ParseMode.HTML,
            reply_markup=get_single_record_keyboard(record),
            disable_web_page_preview=False
        )
        logger.info(f"Уведомление о записи отправлено: {telegram_id}")
        return True
    except Exception as e:
        logger.error(f"Ошибка отправки: {e}")
        return False


async def send_record_changed_notification(telegram_id: int, record: dict, old_datetime: str):
    """Уведомление об изменении записи"""
    datetime_str = record.get("datetime", "")
    formatted_date = format_record_datetime(datetime_str)
    
    services_list = record.get("services") or []
    services = ", ".join([s.get("title", "") for s in services_list if isinstance(s, dict)])
    
    staff = record.get("staff") or {}
    staff_name = staff.get("name", "") if isinstance(staff, dict) else ""
    staff_position = staff.get("specialization", "") if isinstance(staff, dict) else ""
    if not staff_position:
        staff_position = staff.get("position", {}).get("title", "") if isinstance(staff.get("position"), dict) else ""
    
    staff_info = f"{staff_name}, {staff_position}" if staff_position else staff_name
    
    record_link = get_record_link(record)
    
    text = (
        f"Ваша запись перенесена 📅\n\n"
        f"Новое время:\n"
        f"👉 <b>{formatted_date}</b>\n\n"
        f"◾ {services}\n"
        f"к мастеру {staff_info}\n\n"
        f"Ждём вас в 💈 <b>{BARBERSHOP_NAME.upper()}</b>!"
    )
    
    try:
        await bot.send_message(telegram_id, text, parse_mode=ParseMode.HTML, reply_markup=get_single_record_keyboard(record))
        logger.info(f"Уведомление об изменении отправлено: {telegram_id}")
        return True
    except Exception as e:
        logger.error(f"Ошибка отправки: {e}")
        return False


async def send_record_cancelled_notification(telegram_id: int, old_record: dict):
    """Уведомление об отмене записи"""
    datetime_str = old_record.get("datetime", "")
    formatted_date = format_record_datetime(datetime_str) if datetime_str else ""
    
    date_text = f" на {formatted_date}" if formatted_date else ""
    
    text = (
        f"Мы отменили запись{date_text} 😔\n\n"
        f"Будем рады видеть вас снова!\n\n"
        f"Хотите записаться?"
    )
    
    try:
        await bot.send_message(telegram_id, text, parse_mode=ParseMode.HTML, reply_markup=get_booking_keyboard())
        logger.info(f"Уведомление об отмене отправлено: {telegram_id}")
        return True
    except Exception as e:
        logger.error(f"Ошибка отправки: {e}")
        return False


async def send_reminder_24h(telegram_id: int, record: dict):
    """Напоминание за 24 часа"""
    datetime_str = record.get("datetime", "")
    formatted_date = format_record_datetime(datetime_str)
    
    # Получаем только время для короткого напоминания
    time_only = ""
    try:
        if "T" in datetime_str:
            dt = datetime.fromisoformat(datetime_str.replace("Z", "+00:00")).replace(tzinfo=None)
        else:
            dt = datetime.strptime(datetime_str, "%Y-%m-%d %H:%M:%S")
        time_only = dt.strftime("%H:%M")
    except:
        pass
    
    services_list = record.get("services") or []
    services = ", ".join([s.get("title", "") for s in services_list if isinstance(s, dict)])
    
    staff = record.get("staff") or {}
    staff_name = staff.get("name", "") if isinstance(staff, dict) else ""
    
    record_link = get_record_link(record)
    
    text = (
        f"Мы Вас ждём 🤗 завтра в {time_only}\n\n"
        f"◾ {services}\n"
        f"к мастеру {staff_name}\n\n"
        f"📍 {BARBERSHOP_ADDRESS}\n\n"
        f"До встречи в 💈 <b>{BARBERSHOP_NAME.upper()}</b>!"
    )
    
    try:
        await bot.send_message(telegram_id, text, parse_mode=ParseMode.HTML, reply_markup=get_single_record_keyboard(record))
        logger.info(f"Напоминание 24ч отправлено: {telegram_id}")
        return True
    except Exception as e:
        logger.error(f"Ошибка отправки: {e}")
        return False


# ==================== ОТСЛЕЖИВАНИЕ ЗАПИСЕЙ ====================

async def check_records():
    """Проверка записей и отправка уведомлений"""
    logger.info("=== Проверка записей ===")
    
    records = await yclients.get_upcoming_records()
    if not records:
        logger.info("Записей не найдено")
        return
    
    logger.info(f"Найдено {len(records)} записей")
    
    now = datetime.now()
    current_record_ids = set()
    
    for record in records:
        if not record or not isinstance(record, dict):
            continue
        
        record_id = record.get("id")
        if not record_id:
            continue
        
        current_record_ids.add(record_id)
        
        client = record.get("client")
        if not client or not isinstance(client, dict):
            continue
        
        client_phone = client.get("phone", "")
        client_name = client.get("name", "Неизвестно")
        
        if not client_phone:
            logger.info(f"Запись #{record_id}: нет телефона у клиента {client_name}")
            continue
        
        telegram_id = get_telegram_id_by_phone(client_phone)
        if not telegram_id:
            logger.info(f"Запись #{record_id}: клиент {client_name} ({client_phone}) не в боте")
            # Продолжаем обработку даже если клиент не в боте (для отслеживания attendance)
        
        logger.info(f"Запись #{record_id}: {client_name}, телефон {client_phone}, telegram_id={telegram_id}")
        
        datetime_str = record.get("datetime", "")
        services_list = record.get("services") or []
        services = ", ".join([s.get("title", "") for s in services_list if isinstance(s, dict)])
        staff = record.get("staff") or {}
        staff_name = staff.get("name", "") if isinstance(staff, dict) else ""
        
        # Логируем полную структуру первой записи для поиска ссылки
        if len(current_record_ids) == 1:
            logger.info(f"=== ПОЛНЫЕ ДАННЫЕ ЗАПИСИ #{record_id} ===")
            logger.info(json.dumps(record, ensure_ascii=False, indent=2, default=str))
        
        # Получаем сохранённую запись
        tracked = get_tracked_record(record_id)
        
        if tracked is None:
            # Новая запись!
            logger.info(f"Новая запись #{record_id} найдена!")
            
            if telegram_id:
                if not is_notification_sent(record_id, "new"):
                    logger.info(f"Отправляем уведомление клиенту {telegram_id}")
                    if await send_new_record_notification(telegram_id, record):
                        mark_notification_sent(record_id, "new")
                        logger.info(f"Уведомление о записи #{record_id} отправлено!")
                    else:
                        logger.error(f"Не удалось отправить уведомление о записи #{record_id}")
            else:
                logger.info(f"Клиент записи #{record_id} не в боте - уведомление не отправлено")
            
            save_tracked_record(record_id, client_phone, datetime_str, services, staff_name)
        
        else:
            # Проверяем изменение времени
            old_datetime = tracked[2]  # datetime из БД
            
            if telegram_id and old_datetime and datetime_str and old_datetime != datetime_str:
                # Проверяем разницу во времени
                try:
                    old_dt = datetime.fromisoformat(old_datetime.replace("Z", "+00:00")).replace(tzinfo=None)
                    new_dt = datetime.fromisoformat(datetime_str.replace("Z", "+00:00")).replace(tzinfo=None)
                    diff_minutes = abs((new_dt - old_dt).total_seconds() / 60)
                    
                    if diff_minutes >= MIN_RESCHEDULE_MINUTES:
                        notification_key = f"changed_{datetime_str}"
                        if not is_notification_sent(record_id, notification_key):
                            if await send_record_changed_notification(telegram_id, record, old_datetime):
                                mark_notification_sent(record_id, notification_key)
                except:
                    pass
            
            # Обновляем запись
            save_tracked_record(record_id, client_phone, datetime_str, services, staff_name)
        
        # Проверяем напоминание за 24 часа
        if datetime_str:
            try:
                record_time = datetime.fromisoformat(datetime_str.replace("Z", "+00:00")).replace(tzinfo=None)
                hours_until = (record_time - now).total_seconds() / 3600
                
                # Напоминание за 24 часа (окно 23-25 часов)
                if 23 <= hours_until <= 25:
                    if not is_notification_sent(record_id, "reminder_24h"):
                        if await send_reminder_24h(telegram_id, record):
                            mark_notification_sent(record_id, "reminder_24h")
            except:
                pass
        
        # Проверяем статус "пришёл" (attendance)
        attendance = record.get("attendance")
        visit_attendance = record.get("visit_attendance")
        
        # YClients использует attendance=1 или visit_attendance=1 когда клиент пришёл
        if (attendance == 1 or visit_attendance == 1) and not is_attendance_notified(record_id):
            logger.info(f"Клиент пришёл! Запись #{record_id}")
            await notify_staff_client_arrived(record)
            mark_attendance_notified(record_id)
    
    # Проверяем отменённые записи
    tracked_ids = get_all_tracked_record_ids()
    for record_id, old_datetime in tracked_ids.items():
        if record_id not in current_record_ids:
            # Запись исчезла — отменена!
            tracked = get_tracked_record(record_id)
            if tracked:
                client_phone = tracked[1]
                telegram_id = get_telegram_id_by_phone(client_phone)
                
                if telegram_id and not is_notification_sent(record_id, "cancelled"):
                    if await send_record_cancelled_notification(telegram_id, {"datetime": old_datetime}):
                        mark_notification_sent(record_id, "cancelled")
                
                mark_record_cancelled(record_id)


async def records_checker():
    """Планировщик проверки записей"""
    while True:
        try:
            await check_records()
        except Exception as e:
            logger.error(f"Ошибка проверки записей: {e}")
        await asyncio.sleep(CHECK_INTERVAL)


# ==================== УВЕДОМЛЕНИЯ СОТРУДНИКАМ ====================

def mask_phone(phone: str) -> str:
    """Маскировка номера телефона: показываем первые 3 и последние 2 цифры"""
    digits = ''.join(filter(str.isdigit, phone))
    if len(digits) < 6:
        return phone
    # +7 900 *** ** 67
    return f"+{digits[:3]} *** ** {digits[-2:]}"


async def notify_staff_client_arrived(record: dict):
    """Уведомить мастера о приходе его клиента"""
    try:
        # Получаем данные о записи
        client = record.get("client") or {}
        client_name = client.get("name", "Клиент") if isinstance(client, dict) else "Клиент"
        client_phone = client.get("phone", "") if isinstance(client, dict) else ""
        
        # Маскируем номер телефона
        masked_phone = mask_phone(client_phone) if client_phone else ""
        
        services_list = record.get("services") or []
        services = ", ".join([s.get("title", "") for s in services_list if isinstance(s, dict)])
        
        staff_info = record.get("staff") or {}
        staff_name = staff_info.get("name", "Мастер") if isinstance(staff_info, dict) else "Мастер"
        yclients_staff_id = staff_info.get("id") if isinstance(staff_info, dict) else None
        
        datetime_str = record.get("datetime", "")
        time_str = format_record_datetime(datetime_str) if datetime_str else ""
        
        # Формируем сообщение
        msg = (
            f"🔔 <b>К вам пришёл клиент!</b>\n\n"
            f"👤 {client_name}\n"
            f"📞 {masked_phone}\n"
            f"✂️ {services}\n"
            f"🗓 {time_str}"
        )
        
        # Ищем зарегистрированного мастера по YClients ID
        if yclients_staff_id:
            staff_data = get_staff_by_yclients_id(yclients_staff_id)
            
            if staff_data:
                telegram_id = staff_data[0]
                try:
                    await bot.send_message(
                        telegram_id,
                        msg,
                        parse_mode=ParseMode.HTML
                    )
                    logger.info(f"Уведомление о приходе отправлено мастеру {staff_name} ({telegram_id})")
                    return
                except Exception as e:
                    logger.error(f"Ошибка отправки мастеру {telegram_id}: {e}")
            else:
                logger.info(f"Мастер {staff_name} (ID: {yclients_staff_id}) не зарегистрирован в боте")
        else:
            logger.info("В записи нет ID мастера")
                
    except Exception as e:
        logger.error(f"Ошибка notify_staff_client_arrived: {e}")




# ==================== ЗАПУСК ====================

def restore_backup_data():
    """Восстановление резервных данных из кода"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Восстанавливаем клиентов
    for telegram_id, phone in BACKUP_CLIENTS.items():
        cursor.execute("""
            INSERT OR IGNORE INTO clients (telegram_id, phone_number)
            VALUES (?, ?)
        """, (telegram_id, phone))
    
    # Восстанавливаем сотрудников
    for telegram_id, (staff_name, yclients_id) in BACKUP_STAFF.items():
        cursor.execute("""
            INSERT OR IGNORE INTO staff (telegram_id, staff_name, yclients_staff_id)
            VALUES (?, ?, ?)
        """, (telegram_id, staff_name, yclients_id))
    
    conn.commit()
    conn.close()
    logger.info(f"✅ Восстановлено {len(BACKUP_CLIENTS)} клиентов и {len(BACKUP_STAFF)} сотрудников из резерва")


async def main():
    """Запуск бота"""
    # Сначала пробуем загрузить базу из S3
    sync_db_from_s3()
    
    # Инициализируем базу (создаст таблицы если их нет)
    init_db()
    
    # Восстанавливаем резервные данные
    restore_backup_data()
    
    await bot.delete_webhook(drop_pending_updates=True)
    
    # Запускаем проверку записей
    asyncio.create_task(records_checker())
    
    logger.info("🚀 Бот запущен!")
    logger.info(f"⏱ Проверка записей каждые {CHECK_INTERVAL} сек")
    
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
