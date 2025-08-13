import os
import json
import traceback
import datetime
import mimetypes
import shutil
import uuid
import telegram.error
import warnings
from enum import Enum, auto
import urllib.parse
from dotenv import load_dotenv
import pymongo
from pymongo import MongoClient
from bson import ObjectId
from telegram.request import HTTPXRequest
from telegram import (
    Update, ReplyKeyboardMarkup, InlineKeyboardMarkup,
    KeyboardButton, InlineKeyboardButton, ReplyKeyboardRemove
)
from telegram.ext import (
    Application, CommandHandler, MessageHandler, ConversationHandler,
    CallbackQueryHandler, filters, ContextTypes
)

warnings.filterwarnings("ignore", category=UserWarning)

LOG_ENABLED = True  # Логирование (True/False)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATABASE_DIR = os.path.join(BASE_DIR, "Database")
FOLDERS_FILE = os.path.join(BASE_DIR, "folders.json")
USERS_FILE = os.path.join(BASE_DIR, "users.json")
FOLDERS_PER_PAGE = 10
FILES_PER_PAGE = 10
USERS_PER_PAGE = 10

load_dotenv(os.path.join(BASE_DIR, ".env"))
API_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
BOT_API_MODE = os.getenv("TELEGRAM_BOT_API_MODE", "cloud").lower()
BOT_API_URL = os.getenv("TELEGRAM_BOT_API_URL")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
DB_NAME = os.getenv("DB_NAME", "telegram_bot")

try:
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    users_collection = db['users']
    folders_collection = db['folders']
    
    users_collection.create_index("id", unique=True)
    folders_collection.create_index("name", unique=True)
except Exception as e:
    print(f"Ошибка подключения к MongoDB: {e}")
    sys.exit(1)

if BOT_API_MODE == "local": # Тип работы изменяется в .env (cloud - использование облачных серверов Telegram; local - использование Telegram Local Bot API)
    request = HTTPXRequest(api_url_base=BOT_API_URL)
else:
    request = None

class ConversationStates(Enum):
    AUTH = auto()
    FOLDER_NAME = auto()
    ADD_USER_ID = auto()
    ADD_USER_PASSWORD = auto()
    RENAME_FOLDER_NAME = auto()
    ADD_FILES = auto()
    FILES_MENU = auto()
    FILE_RENAME = auto()
    FILE_DELETE_CONFIRM = auto()
    USER_MANAGE_MENU = auto()
    USER_MANAGE_USER = auto()
    USER_ADD_ID = auto()
    USER_ADD_PASS = auto()
    USER_ADD_NAME = auto()
    USER_SEND_MSG = auto()
    USER_SET_LIMIT = auto()
    USER_CONFIRM_SEND_MSG = auto()
    USER_DELETE_CONFIRM = auto()
    CHOOSING_FOLDER = auto()

###########################################
######### ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ #########
###########################################

# Логирует сообщения в консоль и в файл bot.log (если включено логирование).
def log(msg):
    if LOG_ENABLED:
        line = f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][LOG]: {msg}"
        print(line)
        with open(os.path.join(BASE_DIR, "bot.log"), "a", encoding="utf-8") as f:
            f.write(line + "\n")

# Логирует состояние пользователя, сообщение и данные внутри обработчика.
def log_state(update, context, handler_name):
    msg = update.message.text if update and update.message else None
    qd = update.callback_query.data if update and hasattr(update, "callback_query") and update.callback_query else None
    log(f"=== HANDLER: {handler_name} ===")
    log(f"User: {update.effective_user.id if update and update.effective_user else 'None'}")
    log(f"Message: {msg}")
    log(f"CallbackData: {qd}")
    log(f"user_data: {context.user_data}")

# Преобразует размер в байтах в строку с KB, MB или GB.
def format_size(size_bytes):
    kb = size_bytes / 1024
    if kb < 1024:
        return f"{kb:.1f} KB"
    mb = kb / 1024
    if mb < 1024:
        return f"{mb:.1f} MB"
    gb = mb / 1024
    return f"{gb:.1f} GB"

# Проверка работы MongoDB
def check_mongodb_connection():
    try:
        client.admin.command('ping')
        return True
    except Exception as e:
        log(f"Ошибка подключения к MongoDB: {e}")
        return False

# Экранирует спецсимволы для Markdown-разметки.
def escape_md(text):
    return text.replace("\\", "\\\\").replace("_", "\\_").replace("*", "\\*").replace("`", "\\`").replace("[", "\\[").replace("]", "\\]")

# Создаёт файл с дефолтным значением, если файл не существует.
def ensure_file(file_path, default_val):
    if not os.path.exists(file_path):
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(default_val, f)

# Загружает JSON из файла, при ошибке возвращает значение по умолчанию.
def load_json(file_path, default_val):
    ensure_file(file_path, default_val)
    with open(file_path, "r", encoding="utf-8") as f:
        try:
            data = json.load(f)
            if isinstance(data, dict):
                data = [data]
        except Exception as ex:
            log(f"Error reading {file_path}: {ex}\n{traceback.format_exc()}")
            data = default_val
    return data

# Сохраняет объект в JSON-файл.
def save_json(file_path, obj):
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

# Загружает список пользователей из файла.
def load_users():
    return list(users_collection.find())

# Сохраняет список пользователей в файл.
def save_users(users):
    users_collection.delete_many({})
    if users:
        users_collection.insert_many(users)

# Проверяет, существует ли пользователь с данным ID.
def user_exists(user_id: int) -> bool:
    return users_collection.count_documents({"id": user_id}) > 0

# Получает объект пользователя по ID.
def get_user(user_id: int):
    return users_collection.find_one({"id": user_id})

# Добавляет нового пользователя.
def add_user(user_id: int, password: str, status: str = "default", username: str = ""):
    user_data = {
        "id": user_id,
        "password": password,
        "status": status,
        "username": username,
        "created_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "folders": 0,
        "addition": True,
        "download": True,
        "rename": True,
        "delete": True,
        "folders_limit": 10
    }
    users_collection.insert_one(user_data)

# Проверяет пароль пользователя.
def check_password(user_id: int, password: str) -> bool:
    user = get_user(user_id)
    return user and password == user.get("password")

# Возвращает статус пользователя (admin, default, banned).
def get_status(user_id: int) -> str:
    user = get_user(user_id)
    return user.get("status", "default") if user else "default"

# Проверяет, авторизован ли пользователь.
def is_authorized(user_id: int) -> bool:
    user = get_user(user_id)
    return user is not None and user.get("authorized", False) is True

# Устанавливает флаг авторизации для пользователя.
def set_authorized(user_id: int, authorized: bool = True):
    users_collection.update_one(
        {"id": user_id},
        {"$set": {"authorized": authorized}}
    )

# Проверяет, является ли пользователь администратором.
def is_admin(user_id: int) -> bool:
    user = get_user(user_id)
    return user and user.get("status") == "admin"

# Синхронизирует метаданные файлов папки с реальными файлами на диске.
def sync_files_in_folder(folder):
    folder_path = os.path.join(DATABASE_DIR, folder["name"])
    if not os.path.exists(folder_path):
        folder["files"] = []
        return folder
    fs_files = [f for f in os.listdir(folder_path) if os.path.isfile(os.path.join(folder_path, f))]
    files_meta = folder.get("files", [])
    unique_files = {}
    for f in files_meta:
        if f["name"] in fs_files and f["name"] not in unique_files:
            unique_files[f["name"]] = f
    files_meta = list(unique_files.values())
    fs_names_set = set(f["name"] for f in files_meta)
    for fname in fs_files:
        if fname not in fs_names_set:
            files_meta.append({
                "id": str(uuid.uuid4()),
                "name": fname
            })
    folder["files"] = files_meta
    return folder

# Загружает список папок и синхронизирует их с файловой системой.
def load_folders():
    return list(folders_collection.find())

# Сохраняет список папок.
def save_folders(folders):
    folders_collection.delete_many({})
    if folders:
        folders_collection.insert_many(folders)

# Получает папку по имени.
def get_folder_by_name(name):
    folder = folders_collection.find_one({"name": name})
    if folder:
        return sync_files_in_folder(folder)
    return None

# Получает папку по её ID.
def get_folder_by_id(folder_id):
    folder = folders_collection.find_one({"id": folder_id})
    if folder:
        return sync_files_in_folder(folder)
    return None

# Проверяет существование папки по имени.
def folder_exists(name):
    return folders_collection.count_documents({"name": name}) > 0

# Проверяет существование папки по ID.
def folder_exists_by_id(folder_id):
    return get_folder_by_id(folder_id) is not None

# Добавляет новую папку.
def add_folder(name, owner_id, status="public"):
    folder_data = {
        "id": str(uuid.uuid4()),
        "name": name,
        "owner_id": owner_id,
        "status": status,
        "files": []
    }
    folders_collection.insert_one(folder_data)

# Меняет статус папки (private/public) по ID.
def set_folder_status_by_id(folder_id, status):
    folders_collection.update_one(
        {"id": folder_id},
        {"$set": {"status": status}}
    )

# Устанавливает или снимает "заморозку" папки.
def set_folder_freezing_by_id(folder_id, freezing: bool):
    folders = load_folders()
    for folder in folders:
        if folder["id"] == folder_id:
            folder["freezing"] = freezing
            break
    save_folders(folders)

# Проверяет, заморожена ли папка.
def is_folder_frozen_by_id(folder_id):
    folder = get_folder_by_id(folder_id)
    return folder and folder.get("freezing") is True

# Удаляет папку с диска.
def delete_folder_fs(folder_name):
    folder_path = os.path.join(DATABASE_DIR, folder_name)
    if not os.path.exists(folder_path):
        return False, "Папка не найдена."
    try:
        shutil.rmtree(folder_path)
        return True, ""
    except Exception as e:
        return False, str(e)

# Переименовывает папку на диске.
def rename_folder_fs(old_name, new_name):
    old_path = os.path.join(DATABASE_DIR, old_name)
    new_path = os.path.join(DATABASE_DIR, new_name)
    if not os.path.exists(old_path):
        return False, "Source folder not found."
    if os.path.exists(new_path):
        return False, "Folder with this name already exists."
    try:
        os.rename(old_path, new_path)
        return True, ""
    except Exception as e:
        return False, str(e)

# Удаляет папку из базы по ID.
def delete_folder_in_db_by_id(folder_id):
    folders_collection.delete_one({"id": folder_id})

# Переименовывает папку в базе по ID.
def rename_folder_in_db_by_id(folder_id, new_name):
    folders_collection.update_one(
        {"id": folder_id},
        {"$set": {"name": new_name}}
    )

# Формирует список папок для вывода пользователю.
def get_folders_for_list():
    folders = load_folders()
    result = []
    for folder in folders:
        display = folder['name']
        if folder.get("freezing"):
            display += " ❄️"
        elif folder["status"] == "private":
            display += " 🔒"
        result.append({"id": folder["id"], "display": display})
    return result

# Возвращает ID владельца папки.
def get_folder_owner_by_id(folder_id):
    folder = get_folder_by_id(folder_id)
    return folder["owner_id"] if folder else None

# Проверяет, приватная ли папка.
def is_folder_private_by_id(folder_id):
    folder = get_folder_by_id(folder_id)
    return folder and folder["status"] == "private"

# Возвращает статус папки.
def get_folder_status_by_id(folder_id):
    folder = get_folder_by_id(folder_id)
    return folder["status"] if folder else None

# Получает список всех ID папок.
def list_folder_ids():
    return [folder["id"] for folder in load_folders()]

# Очищает имя папки от эмодзи-меток.
def match_real_folder_name(name_with_emoji):
    for mark in (" 🔒", " ❄️"):
        if name_with_emoji.endswith(mark):
            name_with_emoji = name_with_emoji[:-len(mark)].rstrip()
    return name_with_emoji

# Возвращает имя папки с учётом статусов (эмодзи).
def get_actual_folder_name_by_id(folder_id):
    folder = get_folder_by_id(folder_id)
    if folder:
        suffix = ""
        if folder.get("freezing"):
            suffix += " ❄️"
        if folder["status"] == "private":
            suffix += " 🔒"
        return folder["name"] + suffix
    return None

# Синхронизирует папки между файловой системой и базой.
def sync_folders_with_filesystem():
    folders_db = load_folders()
    folders_db_names = [f["name"] for f in folders_db]
    folders_fs = [f for f in os.listdir(DATABASE_DIR) if os.path.isdir(os.path.join(DATABASE_DIR, f))]
    changed = False
    for fs_folder in folders_fs:
        if fs_folder not in folders_db_names:
            folders_db.append({"id": str(uuid.uuid4()), "name": fs_folder, "owner_id": None, "status": "public", "files": []})
            log(f"Added new folder from FS: {fs_folder}")
            changed = True
    valid_folders = []
    for folder in folders_db:
        folder_path = os.path.join(DATABASE_DIR, folder["name"])
        if os.path.isdir(folder_path):
            folder = sync_files_in_folder(folder)
            valid_folders.append(folder)
    if changed:
        save_folders(valid_folders)

# Удаляет из базы папки, которых нет на диске.
def cleanup_nonexistent_folders():
    folders = load_folders()
    valid_folders = []
    for folder in folders:
        folder_path = os.path.join(DATABASE_DIR, folder["name"])
        if os.path.isdir(folder_path):
            folder = sync_files_in_folder(folder)
            valid_folders.append(folder)
    save_folders(valid_folders)

# Получает список ID всех папок.
def get_folders():
    return list_folder_ids()

# Получает количество файлов и общий размер папки.
def get_folder_stats_by_id(folder_id):
    folder = get_folder_by_id(folder_id)
    if not folder:
        return 0, "0 KB"
    folder_path = os.path.join(DATABASE_DIR, folder["name"])
    try:
        files = [f for f in os.listdir(folder_path) if os.path.isfile(os.path.join(folder_path, f))]
    except Exception:
        files = []
    num_files = len(files)
    total_size = sum(os.path.getsize(os.path.join(folder_path, f)) for f in files) if files else 0
    return num_files, format_size(total_size)

# Получает дату создания папки.
def get_folder_created_date_by_id(folder_id):
    folder = get_folder_by_id(folder_id)
    if not folder:
        return "---"
    folder_path = os.path.join(DATABASE_DIR, folder["name"])
    if os.path.exists(folder_path):
        stat = os.stat(folder_path)
        date = datetime.datetime.fromtimestamp(stat.st_ctime)
        return date.strftime("%d.%m.%y")
    return "---"

# Получает общую статистику по базе данных.
def get_database_stats():
    folders = load_folders()
    total_files = 0
    total_size = 0
    for folder in folders:
        folder_path = os.path.join(DATABASE_DIR, folder["name"])
        files = [f for f in os.listdir(folder_path) if os.path.isfile(os.path.join(folder_path, f))]
        total_files += len(files)
        total_size += sum(os.path.getsize(os.path.join(folder_path, f)) for f in files)
    users_count = len([u for u in load_users() if "id" in u])
    return len(folders), total_files, format_size(total_size), users_count

# Возвращает клавиатуру для гостя.
def get_guest_kb():
    return ReplyKeyboardMarkup([[KeyboardButton("📥 Войти в аккаунт")]], resize_keyboard=True)

# Возвращает инлайн-кнопку отмены.
def get_inline_cancel_kb(callback_data="user_list"):
    return InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Отмена", callback_data=callback_data)]])

# Формирует клавиатуру для списка пользователей.
def build_users_list_keyboard(users, current_user_id, page=0, total_pages=1):
    buttons = []
    other_users = [u for u in users if u.get('id') != current_user_id]
    start = page * USERS_PER_PAGE
    end = start + USERS_PER_PAGE
    page_users = other_users[start:end]
    for i in range(0, len(page_users), 2):
        row = []
        for j in range(i, min(i+2, len(page_users))):
            u = page_users[j]
            username = u.get("username") or ''
            caption = f"{u['id']}" + (f" ({username})" if username else "")
            row.append(InlineKeyboardButton(caption, callback_data=f"user_manage:{u['id']}:{page}"))
        buttons.append(row)
    if not page_users:
        buttons.append([InlineKeyboardButton("Пользователей нет 👀", callback_data="no_users")])
    nav_buttons = []
    if total_pages > 1:
        if page > 0:
            nav_buttons.append(InlineKeyboardButton("⬅️", callback_data=f"users_page:{page-1}"))
        if page < total_pages - 1:
            nav_buttons.append(InlineKeyboardButton("➡️", callback_data=f"users_page:{page+1}"))
    if nav_buttons:
        buttons.append(nav_buttons)
    buttons.append([InlineKeyboardButton("👤 Добавить пользователя", callback_data="user_add")])
    return InlineKeyboardMarkup(buttons)

# Формирует строку с информацией о пользователях.
def build_users_list_message(users):
    total = len(users)
    admins = sum(1 for u in users if u.get('status') == 'admin')
    defaults = sum(1 for u in users if u.get('status') != 'admin')
    return (
        f"*⚙️ Управление пользователями*\n\n"
        f"```Информация\n"
        f"👥 Всего записей: {total}\n"
        f"👶 Пользователей: {defaults}\n"
        f"👑 Администраторов: {admins}```\n\n"
        f"*🔎 Выберите пользователя:*"
    )

# Формирует текст для управления пользователем.
def build_user_manage_text(user_data):
    status = user_data.get("status", "default")
    if status == "admin":
        status_str = "👑 Статус: Администратор"
    elif status == "banned":
        status_str = "🚫 Статус: Заблокирован"
    else:
        status_str = "👶 Статус: Пользователь"
    created_at = user_data.get("created_at")
    if created_at:
        try:
            dt = datetime.datetime.strptime(created_at, "%Y-%m-%d %H:%M:%S")
            created_str = dt.strftime("%d.%m.%y")
        except Exception:
            created_str = "---"
    else:
        created_str = "---"
    username = escape_md(str(user_data.get("username", "")))
    folders_count = escape_md(str(user_data.get("folders", 0)))
    folders_limit = user_data.get("folders_limit", 10)
    folders_limit_str = "♾️" if folders_limit == 0 else str(folders_limit)
    user_id = escape_md(str(user_data.get('id')))
    created_str = escape_md(created_str)
    return (
        f"*⚙️ Управление пользователем*\n\n"
        f"```Информация\n"
        f"{status_str}\n"
        f"🆔 ID пользователя: {user_id}\n"
        f"📛 Имя пользователя: {username}\n"
        f"🗓 Дата добавления: {created_str}\n"
        f"🗂 Всего папок: {folders_count} из {folders_limit_str}```"
    )

# Формирует клавиатуру для управления пользователем.
def build_user_manage_keyboard(user_data, page=0):
    status_btn = InlineKeyboardButton("👶 Сделать пользователем" if user_data.get('status') == 'admin' else "👑 Сделать администратором",callback_data=f"user_toggle_status:{user_data['id']}")
    banned = user_data.get("status") == "banned"
    block_btn = InlineKeyboardButton("✅ Разблокировать" if banned else "🚫 Заблокировать",callback_data=f"user_unblock:{user_data['id']}" if banned else f"user_block:{user_data['id']}")

    addition_btn = InlineKeyboardButton(f"📤 Добавление: {'✅' if user_data.get('addition', True) else '❌️'}",callback_data=f"user_toggle_addition:{user_data['id']}")
    download_btn = InlineKeyboardButton(f"📥 Получение: {'✅' if user_data.get('download', True) else '❌️'}",callback_data=f"user_toggle_download:{user_data['id']}")
    rename_btn = InlineKeyboardButton(f"✏️ Смена имени: {'✅️' if user_data.get('rename', True) else '❌️'}",callback_data=f"user_toggle_rename:{user_data['id']}")
    delete_btn = InlineKeyboardButton(f"🗑 Удаление: {'✅️' if user_data.get('delete', True) else '❌️'}",callback_data=f"user_toggle_delete:{user_data['id']}")
    folders_limit_val = user_data.get('folders_limit', 10)
    folders_limit_caption = "Нет" if folders_limit_val == 0 else str(folders_limit_val)
    folders_limit_btn = InlineKeyboardButton(f"📁 Лимит на создание папок: {folders_limit_caption}",callback_data=f"user_set_folders_limit:{user_data['id']}")
    return InlineKeyboardMarkup([
        [addition_btn, download_btn],
        [rename_btn, delete_btn],
        [folders_limit_btn],
        [InlineKeyboardButton("🔑 Изменить пароль", callback_data=f"user_change_pass:{user_data['id']}"),
         block_btn],
        [InlineKeyboardButton("💭 Отправить сообщение", callback_data=f"user_send_msg:{user_data['id']}")],
        [status_btn],
        [InlineKeyboardButton("🗑 Удалить из базы", callback_data=f"user_delete_confirm:{user_data['id']}")],
        [InlineKeyboardButton("🔙 Назад к списку пользователей", callback_data=f"user_list:{page}")]
    ])

# Проверяет, заблокирован ли пользователь.
def is_banned(user_id: int) -> bool:
    user = get_user(user_id)
    return user and user.get("status") == "banned"

# Проверяет, есть ли пользователь в базе.
def is_in_database(user_id: int) -> bool:
    return get_user(user_id) is not None

# Блокирует пользователя.
def admin_block_user(user_id: int):
    users = load_users()
    for u in users:
        if u.get("id") == user_id:
            u["status"] = "banned"
            break
    save_users(users)

# Разблокирует пользователя.
def admin_unblock_user(user_id: int):
    users = load_users()
    for u in users:
        if u.get("id") == user_id:
            u["status"] = "default"
            break
    save_users(users)

# Клавиатура подтверждения удаления пользователя.
def build_user_delete_confirm_keyboard(user_id):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🗑 Удалить", callback_data=f"user_delete:{user_id}"),
         InlineKeyboardButton("🔙 Отмена", callback_data=f"user_delete_cancel:{user_id}")]
    ])

# Клавиатура подтверждения отправки сообщения пользователю.
def build_confirm_send_msg_keyboard(user_id):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅️ Отправить", callback_data=f"user_do_send_msg:{user_id}")]
    ])

# Главная клавиатура для пользователя (основное меню).
def get_main_kb(user_id):
    buttons = [
        [KeyboardButton("➕ Создать папку"), KeyboardButton("🗂 Список папок")]
    ]
    if get_status(user_id) == "admin":
        buttons.append([KeyboardButton("⚙️ Управление пользователями")])
    buttons.append([KeyboardButton("🚪 Выйти из аккаунта")])
    return ReplyKeyboardMarkup(buttons, resize_keyboard=True)

# Клавиатура с кнопкой отмены.
def get_cancel_kb():
    return ReplyKeyboardMarkup([[KeyboardButton("🔙 Отмена")]], resize_keyboard=True)

# Клавиатура отмены для добавления файлов.
def get_files_cancel_kb():
    return ReplyKeyboardMarkup([[KeyboardButton("🔙 Отмена")]], resize_keyboard=True)

# Клавиатура завершения добавления файлов.
def get_files_finish_kb():
    return ReplyKeyboardMarkup([[KeyboardButton("✅ Закончить добавление")]], resize_keyboard=True)

# Клавиатура для списка файлов в папке.
def build_files_keyboard(folder_id, page, total_pages, files):
    buttons = []
    for i in range(0, len(files), 2):
        row = [
            InlineKeyboardButton(
                files[j]["name"][:40] + ("..." if len(files[j]["name"]) > 40 else ""),
                callback_data=f"file_select:{files[j]['id']}:{page}"
            )
            for j in range(i, min(i+2, len(files)))
        ]
        buttons.append(row)
    if not files:
        buttons = [
            [InlineKeyboardButton("Файлов нет 👀", callback_data=f"no_files_info:{folder_id}:{page}")]
        ]
    nav_buttons = []
    if total_pages > 1:
        if page > 0:
            nav_buttons.append(InlineKeyboardButton("⬅️", callback_data=f"files_page:{folder_id}:{page-1}"))
        if page < total_pages - 1:
            nav_buttons.append(InlineKeyboardButton("➡️", callback_data=f"files_page:{folder_id}:{page+1}"))
    if nav_buttons:
        buttons.append(nav_buttons)
    buttons.append([InlineKeyboardButton("🔙 Назад к управлению папкой", callback_data=f"back_to_folder:{folder_id}:{page}")])
    return InlineKeyboardMarkup(buttons)

# Клавиатура для списка папок.
def build_folders_keyboard(page: int, total_pages: int, folders: list):
    buttons = []
    if not folders:
        buttons = [
            [InlineKeyboardButton("Папок нет 👀", callback_data="no_folders_info")]
        ]
        return InlineKeyboardMarkup(buttons)
    for i in range(0, len(folders), 2):
        row = [
            InlineKeyboardButton(folder["display"],callback_data=f"folder_select:{folder['id']}:{page}")
            for folder in folders[i:i+2]
        ]
        buttons.append(row)
    nav_buttons = []
    if total_pages > 1:
        if page > 0:
            nav_buttons.append(InlineKeyboardButton("⬅️", callback_data=f"folders_page:{page-1}"))
        if page < total_pages - 1:
            nav_buttons.append(InlineKeyboardButton("➡️", callback_data=f"folders_page:{page+1}"))
    if nav_buttons:
        buttons.append(nav_buttons)
    return InlineKeyboardMarkup(buttons)

# Клавиатура и текст для управления папкой.
def build_folder_manage_keyboard(folder_id: str, page: int, user_id=None):
    folder = get_folder_by_id(folder_id)
    if not folder:
        return "Папка не найдена.", InlineKeyboardMarkup([[InlineKeyboardButton("Назад", callback_data=f"folders_page:{page}")]])
    num_files, folder_size = get_folder_stats_by_id(folder_id)
    owner_id = folder["owner_id"]
    date_str = get_folder_created_date_by_id(folder_id)
    status = folder["status"]
    freezing = folder.get("freezing", False)
    owner_str = f"{owner_id}" if owner_id else "Console"
    priv_str = "🔒 Тип: Приватная" if status == "private" else "🌎 Тип: Публичная"
    freeze_str = "❄️ Статус: Заморожена" if freezing else "🔥 Статус: Обычный"
    text = (f"*🗂 Управление папкой*\n\n"
            f"```Информация\n"
            f"📛 Имя папки: {escape_md(folder['name'])}\n"
            f"🆔 ID создателя папки: {owner_str}\n"
            f"{priv_str}\n"
            f"{freeze_str}\n"
            f"🗓 Дата создания: {date_str}\n"
            f"📄 Файлов: {num_files}\n"
            f"🗄 Размер: {folder_size}```\n\n")
    status_btn = InlineKeyboardButton("🔓 Сделать публичной" if status == "private" else "🔒 Сделать приватной",callback_data=f"{'folder_public' if status=='private' else 'folder_priv'}:{folder_id}:{page}")
    add_btn = InlineKeyboardButton("📂 Добавить файлы", callback_data=f"folder_add_files:{folder_id}:{page}")
    rename_btn = InlineKeyboardButton("✏️ Изменить имя", callback_data=f"folder_rename:{folder_id}:{page}")
    files_btn = InlineKeyboardButton("📄 Список файлов", callback_data=f"folder_file_list:{folder_id}:{page}")
    delete_btn = InlineKeyboardButton("🗑 Удалить папку", callback_data=f"folder_delete_confirm:{folder_id}:{page}")
    back_btn = InlineKeyboardButton("🔙 Назад к списку папок", callback_data=f"folders_page:{page}")

    admin = is_admin(user_id)
    buttons = [
        [status_btn],
        [add_btn, rename_btn],
        [files_btn],
    ]
    if admin:
        freeze_btn = InlineKeyboardButton("🔥 Разморозить" if freezing else "❄️ Заморозить",callback_data=f"{'folder_unfreeze' if freezing else 'folder_freeze'}:{folder_id}:{page}")
        buttons.append([freeze_btn])
    buttons.append([delete_btn])
    buttons.append([back_btn])

    if freezing and not admin:
        text += "_Папка была заморожена администратором._"
    elif status == "private" and user_id is not None and owner_id != user_id and not admin:
        text += "_Владелец данной папки запретил её изменять._"
    return text, InlineKeyboardMarkup(buttons)

# Клавиатура и текст для управления файлом.
def build_file_manage_keyboard(folder_id, file_id, page):
    folder = get_folder_by_id(folder_id)
    if not folder:
        return "Папка не найдена.", InlineKeyboardMarkup([[InlineKeyboardButton("Назад", callback_data=f"folders_page:{page}")]])
    file_meta = next((f for f in folder["files"] if f["id"] == file_id), None)
    if not file_meta:
        return "Файл не найден.", InlineKeyboardMarkup([[InlineKeyboardButton("Назад", callback_data=f"back_to_file_list:{folder_id}:{page}")]])
    file_path = os.path.join(DATABASE_DIR, folder["name"], file_meta["name"])
    file_exists = os.path.exists(file_path)

    file_type = "📄 Тип файла: Документ"
    if file_exists:
        ext = os.path.splitext(file_meta["name"])[1].lower()
        if ext in [".jpg", ".jpeg", ".png", ".gif", ".bmp", ".webp"]:
            file_type = "🖼 Тип файла: Фото"
        elif ext in [".mp4", ".mov", ".avi", ".mkv", ".webm"]:
            file_type = "📹 Тип файла: Видео"
        else:
            mime, _ = mimetypes.guess_type(file_meta["name"])
            if mime:
                if mime.startswith("image/"):
                    file_type = "🖼 Тип файла: Фото"
                elif mime.startswith("video/"):
                    file_type = "📹 Тип файла: Видео"
    else:
        file_type = "📄 Тип файла: Документ"

    info_text = f"*📄 Управление файлом*\n\n"
    if file_exists:
        size = format_size(os.path.getsize(file_path))
        created_at = file_meta.get("created_at")
        if not created_at and os.path.exists(file_path):
            stat = os.stat(file_path)
            created_at = datetime.datetime.fromtimestamp(stat.st_ctime).strftime('%d.%m.%y')
        elif created_at:
            try:
                dt = datetime.datetime.strptime(created_at, "%Y-%m-%d %H:%M:%S")
                created_at = dt.strftime("%d.%m.%y")
            except Exception:
                pass

        info_text += f"```Информация\n✏️ Имя: {file_meta['name']}\n"
        info_text += f"🗄 Размер: {size}\n"
        info_text += f"{file_type}\n"
        if created_at:
            info_text += f"🗓 Дата создания: {created_at}\n"
        info_text += "```"
    else:
        info_text += f"_Файл уже удален._\n"

    buttons = [
        [InlineKeyboardButton("✏️ Изменить имя", callback_data=f"file_rename:{file_id}:{page}"),
         InlineKeyboardButton("🗑 Удалить файл", callback_data=f"file_delete_confirm:{file_id}:{page}")],
        [InlineKeyboardButton("📥 Получить файл", callback_data=f"file_get:{file_id}:{page}")],
        [InlineKeyboardButton("🔙 Назад к списку файлов", callback_data=f"back_to_file_list:{page}")]
    ]
    return info_text, InlineKeyboardMarkup(buttons)

###############################
######### ОБРАБОТЧИКИ #########
###############################

#Проверяет доступ пользователя для Inline-обработчиков.
async def precheck_inline(update, context):
    user_id = update.effective_user.id if update and update.effective_user else None
    if not is_in_database(user_id) or is_banned(user_id):
        await update.callback_query.answer("Нет доступа.", show_alert=True)
        return True
    return False

# Проверяет доступ пользователя для Reply-обработчиков.
async def precheck_reply(update, context):
    user_id = update.effective_user.id if update and update.effective_user else None
    if not is_in_database(user_id):
        await update.message.reply_text("Войдите через кнопку ниже.", reply_markup=get_guest_kb())
        return True
    if is_banned(user_id):
        await update.message.reply_text("🚫 Функционал бота временно недоступен для вас.", reply_markup=get_guest_kb())
        return True
    return False

# Приветственное сообщение, /start бота
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    log_state(update, context, "start")
    photo_path = os.path.join(BASE_DIR, "start.jpg")
    caption = (
        "*👋 Добро пожаловать!*\n\n"
        "🗄 *Приватный файловый менеджер, работающий прямо через Telegram*\n\n"
        "*1️⃣ Возможности:*\n"
        "✈️ Отправка файлов на сервер\n"
        "📑 Загрузка любых типов файлов\n"
        "✏️ Настройка файлов внутри бота\n"
        "🔒 Ограничение доступка к папкам\n"
        "⚙️ Умное управление пользователями\n\n"
        "*2️⃣ Принципы работы:*\n"
        "⚡ Все действия — простые и быстрые\n"
        "🛡 Безопасность ваших данных\n"
        "✅ Только авторизованные пользователи\n"
        "🤖 Доступно Local Bot API\n\n"
        "*3️⃣ Ссылки:*\n"
        "💭 Автор проекта: [ibuzy](https://t.me/ibuzy)\n"
        "🔗 GitHub репозиторий: [Kol-Dayn](https://github.com/Kol-Dayn/Database)\n\n"
        "`Этот проект распространяется на условиях лицензии Apache-2.0 license`\n\n"
        "*➡️ Для входа — воспользуйтесь кнопкой в меню*"
    )
    if os.path.exists(photo_path):
        with open(photo_path, "rb") as photo_file:
            await update.message.reply_photo(photo=photo_file,caption=caption,parse_mode="Markdown",reply_markup=get_guest_kb())
    else:
        await update.message.reply_text(caption, parse_mode="Markdown", reply_markup=get_guest_kb())

# Обработка авторизации пользователя.
async def auth(update: Update, context: ContextTypes.DEFAULT_TYPE):
    log_state(update, context, "auth")
    user_id = update.effective_user.id
    if not is_in_database(user_id):
        await update.message.reply_text("❓ Вас нет в базе данных пользователей. Доступ ограничен.", reply_markup=get_guest_kb())
        return ConversationHandler.END
    if is_banned(user_id):
        await update.message.reply_text("🚫 Функционал бота временно недоступен для вас.", reply_markup=get_guest_kb())
        return ConversationHandler.END
    password = update.message.text
    if check_password(user_id, password):
        set_authorized(user_id, True)
        await update.message.reply_text("Успешный вход!", reply_markup=get_main_kb(user_id))
        return ConversationHandler.END
    set_authorized(user_id, False)
    await update.message.reply_text("Неверный пароль. Попробуйте снова.", reply_markup=get_guest_kb())
    return ConversationHandler.END

# Меню для неавторизованных пользователей.
async def guest_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    log_state(update, context, "guest_menu")
    user_id = update.effective_user.id
    if not is_in_database(user_id):
        await update.message.reply_text("❓ Вас нет в базе данных пользователей. Доступ ограничен.", reply_markup=get_guest_kb())
        return ConversationHandler.END
    if is_banned(user_id):
        await update.message.reply_text("🚫 Функционал бота временно недоступен для вас.", reply_markup=get_guest_kb())
        return ConversationHandler.END
    user = get_user(user_id)
    if user.get("authorized", False):
        await update.message.reply_text("Вы уже авторизованы.", reply_markup=get_main_kb(user_id))
        return ConversationHandler.END
    await update.message.reply_text("Введите ваш пароль:", reply_markup=ReplyKeyboardRemove())
    return ConversationStates.AUTH

# Главное меню для авторизованных пользователей.
async def main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if await precheck_reply(update, context): return ConversationHandler.END
    log_state(update, context, "main_menu")
    user_id = update.effective_user.id
    text = update.message.text

    if not is_authorized(user_id):
        await update.message.reply_text("Войдите через кнопку ниже.", reply_markup=get_guest_kb())
        return

    if text == "➕ Создать папку":
        user = get_user(user_id)
        limit = user.get("folders_limit", 10)
        folders_created = user.get("folders", 0)
        if limit != 0 and folders_created >= limit:
            await update.message.reply_text(
                "Вы достигли лимита на создание папок. Удалите какую-то папку.",
                reply_markup=get_main_kb(user_id)
            )
            return ConversationHandler.END
        await update.message.reply_text("Введите имя новой папки:", reply_markup=get_cancel_kb())
        return ConversationStates.FOLDER_NAME

    elif text == "🗂 Список папок":
        sync_folders_with_filesystem()
        cleanup_nonexistent_folders()
        folders = get_folders_for_list()
        page = 0
        total_pages = max(1, (len(folders) + FOLDERS_PER_PAGE - 1) // FOLDERS_PER_PAGE)
        page_folders = folders[page * FOLDERS_PER_PAGE : (page + 1) * FOLDERS_PER_PAGE]
        num_folders, total_files, total_size, users_count = get_database_stats()
        stats_message = (
            f"*🗂 Список всех доступных папок в БД*\n\n"
            f"```Информация\n"
            f"📂 Папок: {num_folders}\n"
            f"📄 Всего файлов: {total_files}\n"
            f"🗄 Общий вес базы: {total_size}\n"
            f"👤 Пользователей: {users_count}```\n\n"
            "*🔎 Выберите папку:*"
        )
        await update.message.reply_text(stats_message,parse_mode="Markdown",reply_markup=build_folders_keyboard(page, total_pages, page_folders))
        return ConversationStates.CHOOSING_FOLDER

    elif text == "⚙️ Управление пользователями":
        if get_status(user_id) != "admin":
            await update.message.reply_text("Используйте кнопки меню.",reply_markup=get_main_kb(user_id))
            return
        return await admin_users_menu(update, context)

    elif text == "🚪 Выйти из аккаунта":
        set_authorized(user_id, False)
        await update.message.reply_text("Вы вышли из аккаунта.", reply_markup=get_guest_kb())
        return ConversationHandler.END

# Создание новой папки.
async def create_folder(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if await precheck_reply(update, context): return ConversationHandler.END
    log_state(update, context, "create_folder")
    folder_name = update.message.text.strip()
    user_id = update.effective_user.id

    if folder_name == "🔙 Отмена":
        await update.message.reply_text("Действие отменено.", reply_markup=get_main_kb(user_id))
        return ConversationHandler.END
    if not folder_name or any(c in folder_name for c in r'\/:*?"<>|.'):
        await update.message.reply_text("Недопустимое имя папки. Попробуйте другое.", reply_markup=get_cancel_kb())
        return ConversationStates.FOLDER_NAME
    if folder_exists(folder_name):
        await update.message.reply_text("Папка с таким именем уже есть. Попробуйте другое.", reply_markup=get_cancel_kb())
        return ConversationStates.FOLDER_NAME

    os.makedirs(os.path.join(DATABASE_DIR, folder_name), exist_ok=True)
    add_folder(folder_name, user_id, status="public")

    users = load_users()
    for u in users:
        if u.get("id") == user_id:
            u["folders"] = u.get("folders", 0) + 1
            break
    save_users(users)

    await update.message.reply_text(f"*Папка* `{folder_name}` *создана.*", parse_mode="Markdown", reply_markup=get_main_kb(user_id))
    return ConversationHandler.END

# Обработка нажатий на Inline-кнопки для папок и файлов.
async def folder_button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if await precheck_inline(update, context): return
    log_state(update, context, "folder_button_callback")
    query = update.callback_query
    user_id = query.from_user.id
    data = query.data

    def check_folder_exists_or_back_by_id(folder_id, page, action_text=None):
        if not folder_exists_by_id(folder_id):
            msg = action_text if action_text else f"Папка была удалена другим пользователем."
            return {
                "reply": (msg, InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 Назад к списку папок", callback_data=f"folders_page:{page}")]
                ]))
            }
        return None

    if data == "no_folders_info":
        await query.answer("В базе нет папок.", show_alert=True)
        return ConversationStates.CHOOSING_FOLDER

    if data.startswith("folders_page:"):
        page = int(data.split(":")[1])
        folders = get_folders_for_list()
        total_pages = max(1, (len(folders) + FOLDERS_PER_PAGE - 1) // FOLDERS_PER_PAGE)
        page = max(0, min(page, total_pages - 1))
        page_folders = folders[page * FOLDERS_PER_PAGE : (page + 1) * FOLDERS_PER_PAGE]
        num_folders, total_files, total_size, users_count = get_database_stats()
        stats_message = (
            f"*🗂 Список всех доступных папок в БД*\n\n"
            f"```Информация\n"
            f"📂 Папок: {num_folders}\n"
            f"📄 Всего файлов: {total_files}\n"
            f"🗄 Общий вес базы: {total_size}\n"
            f"👤 Пользователей: {users_count}```\n\n"
            "*🔎 Выберите папку:*"
        )
        await query.edit_message_text(stats_message,parse_mode="Markdown",reply_markup=build_folders_keyboard(page, total_pages, page_folders))
        return ConversationStates.CHOOSING_FOLDER

    if data.startswith("folder_select:"):
        folder_id, page = data.split(":")[1], int(data.split(":")[2])
        folder = get_folder_by_id(folder_id)
        if not folder:
            await query.edit_message_text(
                "Папка была удалена другим пользователем.",
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 Назад к списку папок", callback_data=f"folders_page:{page}")]
                ])
            )
            return ConversationStates.CHOOSING_FOLDER
        text, keyboard = build_folder_manage_keyboard(folder_id, page, user_id)
        await query.edit_message_text(text, parse_mode="Markdown", reply_markup=keyboard)
        return ConversationStates.CHOOSING_FOLDER

    if data.startswith("folder_file_list:"):
        folder_id, page = data.split(":")[1], int(data.split(":")[2])
        context.user_data["current_folder_id"] = folder_id
        folder = get_folder_by_id(folder_id)
        admin = is_admin(user_id)
        is_owner = user_id == get_folder_owner_by_id(folder_id)
        freezing = is_folder_frozen_by_id(folder_id)
        status = get_folder_status_by_id(folder_id)
        if freezing and not admin:
            await query.answer("Папка заморожена администратором.", show_alert=True)
            return ConversationStates.CHOOSING_FOLDER
        if status == "private" and not (admin or is_owner):
            await query.answer("Нет доступа к приватной папке.", show_alert=True)
            return ConversationStates.CHOOSING_FOLDER
        if not folder or not os.path.exists(os.path.join(DATABASE_DIR, folder["name"])):
            await query.edit_message_text("Папка была удалена.",parse_mode="Markdown",reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад к списку папок", callback_data=f"folders_page:{page}")]]))
            return ConversationStates.CHOOSING_FOLDER
        files = folder["files"]
        files = sorted(files, key=lambda f: f["name"])
        total_pages = max(1, (len(files) + FILES_PER_PAGE - 1) // FILES_PER_PAGE)
        page = max(0, min(page, total_pages - 1))
        page_files = files[page * FILES_PER_PAGE : (page + 1) * FILES_PER_PAGE]
        text = f"*📄 Список файлов в папке*"
        await query.edit_message_text(text, parse_mode="Markdown", reply_markup=build_files_keyboard(folder_id, page, total_pages, page_files))
        return ConversationStates.FILES_MENU

    if data.startswith("files_page:"):
        parts = data.split(":")
        folder_id, page = parts[1], int(parts[2])
        folder = get_folder_by_id(folder_id)
        files = sorted(folder["files"], key=lambda f: f["name"])
        total_pages = max(1, (len(files) + FILES_PER_PAGE - 1) // FILES_PER_PAGE)
        page = max(0, min(page, total_pages - 1))
        page_files = files[page * FILES_PER_PAGE : (page + 1) * FILES_PER_PAGE]
        text = f"*📄 Список файлов в папке*"
        await query.edit_message_text(text, parse_mode="Markdown", reply_markup=build_files_keyboard(folder_id, page, total_pages, page_files))
        return ConversationStates.FILES_MENU

    if data.startswith("no_files_info:"):
        _, folder_id, page = data.split(":")
        await query.answer("В этой папке нет файлов.", show_alert=True)
        return ConversationStates.FILES_MENU

    if data.startswith("back_to_folder:"):
        folder_id, page = data.split(":")[1], int(data.split(":")[2])
        text, keyboard = build_folder_manage_keyboard(folder_id, page, user_id)
        await query.edit_message_text(text, parse_mode="Markdown", reply_markup=keyboard)
        return ConversationStates.CHOOSING_FOLDER

    if data.startswith("file_select:"):
        parts = data.split(":")
        file_id, page = parts[1], int(parts[2])
        folder_id = context.user_data.get("current_folder_id")
        folder = get_folder_by_id(folder_id)
        file_meta = next((f for f in folder["files"] if f["id"] == file_id), None)
        admin = is_admin(user_id)
        is_owner = user_id == get_folder_owner_by_id(folder_id)
        freezing = is_folder_frozen_by_id(folder_id)
        status = get_folder_status_by_id(folder_id)
        if freezing and not admin:
            await query.answer("Папка заморожена администратором.", show_alert=True)
            return ConversationStates.FILES_MENU
        if status == "private" and not (admin or is_owner):
            await query.answer("Нет доступа к приватной папке.", show_alert=True)
            return ConversationStates.FILES_MENU
        if not file_meta:
            await query.edit_message_text("Файл не найден.", parse_mode="Markdown")
            return ConversationStates.FILES_MENU
        info_text, keyboard = build_file_manage_keyboard(folder_id, file_id, page)
        await query.edit_message_text(info_text, parse_mode="Markdown", reply_markup=keyboard)
        return ConversationStates.FILES_MENU

    if data.startswith("back_to_file_list:"):
        parts = data.split(":")
        page = int(parts[1])
        folder_id = context.user_data.get("current_folder_id")
        folder = get_folder_by_id(folder_id)
        files = sorted(folder["files"], key=lambda f: f["name"])
        total_pages = max(1, (len(files) + FILES_PER_PAGE - 1) // FILES_PER_PAGE)
        page = max(0, min(page, total_pages - 1))
        page_files = files[page * FILES_PER_PAGE : (page + 1) * FILES_PER_PAGE]
        text = f"*📄 Список файлов в папке*"
        await query.edit_message_text(text, parse_mode="Markdown", reply_markup=build_files_keyboard(folder_id, page, total_pages, page_files))
        return ConversationStates.FILES_MENU

    if data.startswith("file_rename:"):
        parts = data.split(":")
        file_id, page = parts[1], int(parts[2])
        folder_id = context.user_data.get("current_folder_id")
        folder = get_folder_by_id(folder_id)
        user = get_user(user_id)
        admin = is_admin(user_id)
        is_owner = user_id == get_folder_owner_by_id(folder_id)
        freezing = is_folder_frozen_by_id(folder_id)
        status = get_folder_status_by_id(folder_id)

        if not user.get("rename", True):
            await query.answer("Нет доступа.", show_alert=True)
            return ConversationStates.FILES_MENU

        if freezing and not admin:
            await query.answer("Папка заморожена администратором.", show_alert=True)
            return ConversationStates.FILES_MENU

        if status == "private" and not (admin or is_owner):
            await query.answer("Нет доступа к приватной папке.", show_alert=True)
            return ConversationStates.FILES_MENU
        context.user_data["rename_file"] = {"folder_id": folder_id, "file_id": file_id, "page": page}
        await query.edit_message_reply_markup(reply_markup=None)
        file_meta = next((f for f in folder["files"] if f["id"] == file_id), None)
        await query.message.chat.send_message(f"Введите новое имя для файла `{escape_md(file_meta['name'])}`:",parse_mode="Markdown", reply_markup=get_cancel_kb())
        return ConversationStates.FILE_RENAME

    if data.startswith("file_delete_confirm:"):
        parts = data.split(":")
        file_id, page = parts[1], int(parts[2])
        folder_id = context.user_data.get("current_folder_id")
        folder = get_folder_by_id(folder_id)
        user = get_user(user_id)
        admin = is_admin(user_id)
        is_owner = user_id == get_folder_owner_by_id(folder_id)
        freezing = is_folder_frozen_by_id(folder_id)
        status = get_folder_status_by_id(folder_id)

        if not user.get("delete", True):
            await query.answer("Нет доступа.", show_alert=True)
            return ConversationStates.FILES_MENU

        if freezing and not admin:
            await query.answer("Папка заморожена администратором.", show_alert=True)
            return ConversationStates.FILES_MENU

        if status == "private" and not (admin or is_owner):
            await query.answer("Нет доступа к приватной папке.", show_alert=True)
            return ConversationStates.FILES_MENU

        file_meta = next((f for f in folder["files"] if f["id"] == file_id), None)
        confirm_text = (f"Вы действительно хотите удалить файл `{escape_md(file_meta['name'])}`? Это действие *необратимо*.")
        confirm_kb = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("🗑 Удалить", callback_data=f"file_delete:{file_id}:{page}"),
                InlineKeyboardButton("🔙 Отмена", callback_data=f"file_delete_cancel:{file_id}:{page}")
            ]
        ])
        await query.edit_message_text(confirm_text, parse_mode="Markdown", reply_markup=confirm_kb)
        return ConversationStates.FILE_DELETE_CONFIRM

    if data.startswith("file_delete_cancel:"):
        parts = data.split(":")
        file_id, page = parts[1], int(parts[2])
        folder_id = context.user_data.get("current_folder_id")
        info_text, keyboard = build_file_manage_keyboard(folder_id, file_id, page)
        await query.edit_message_text(info_text, parse_mode="Markdown", reply_markup=keyboard)
        return ConversationStates.FILES_MENU

    if data.startswith("file_delete:"):
        parts = data.split(":")
        file_id, page = parts[1], int(parts[2])
        folder_id = context.user_data.get("current_folder_id")
        folder = get_folder_by_id(folder_id)
        user = get_user(user_id)
        admin = is_admin(user_id)
        is_owner = user_id == get_folder_owner_by_id(folder_id)
        freezing = is_folder_frozen_by_id(folder_id)
        status = get_folder_status_by_id(folder_id)

        if not is_in_database(user_id) or is_banned(user_id):
            await query.answer("Нет доступа.", show_alert=True)
            return ConversationStates.FILES_MENU

        if freezing and not admin:
            await query.answer("Папка заморожена администратором.", show_alert=True)
            return ConversationStates.FILES_MENU

        if status == "private" and not (admin or is_owner):
            await query.answer("Нет доступа к приватной папке.", show_alert=True)
            return ConversationStates.FILES_MENU

        if not user.get("delete", True):
            await query.answer("Нет доступа.", show_alert=True)
            return ConversationStates.FILES_MENU

        file_meta = next((f for f in folder["files"] if f["id"] == file_id), None)
        file_path = os.path.join(DATABASE_DIR, folder["name"], file_meta["name"]) if file_meta else None
        if not file_meta or not os.path.exists(file_path):
            await query.edit_message_text(
                f"Файл `{escape_md(file_meta['name'])}` уже удален." if file_meta else "Файл уже удален.",
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад к списку файлов", callback_data=f"back_to_file_list:{page}")]])
            )
            return ConversationStates.FILES_MENU
        try:
            os.remove(file_path)
            folder["files"] = [f for f in folder["files"] if f["id"] != file_id]
            save_folders(load_folders())
            success_text = f"*Файл* `{escape_md(file_meta['name'])}` *удален.*"
            back_kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Назад к списку файлов", callback_data=f"back_to_file_list:{page}")]
            ])
            await query.edit_message_text(success_text, parse_mode="Markdown", reply_markup=back_kb)
        except Exception as e:
            await query.edit_message_text(f"Ошибка удаления файла: {escape_md(str(e))}", parse_mode="Markdown")
            await query.message.chat.send_message("Выберите действие:",reply_markup=get_main_kb(user_id))
        return ConversationStates.FILES_MENU

    if data.startswith("file_get:"):
        parts = data.split(":")
        file_id, page = parts[1], int(parts[2])
        folder_id = context.user_data.get("current_folder_id")
        folder = get_folder_by_id(folder_id)
        user = get_user(user_id)
        admin = is_admin(user_id)
        is_owner = user_id == get_folder_owner_by_id(folder_id)
        freezing = is_folder_frozen_by_id(folder_id)
        status = get_folder_status_by_id(folder_id)

        if not is_in_database(user_id) or is_banned(user_id):
            await query.answer("Нет доступа.", show_alert=True)
            return ConversationStates.FILES_MENU

        if freezing and not admin:
            await query.answer("Папка заморожена администратором.", show_alert=True)
            return ConversationStates.FILES_MENU

        if status == "private" and not (admin or is_owner):
            await query.answer("Нет доступа к приватной папке.", show_alert=True)
            return ConversationStates.FILES_MENU

        if not user.get("download", True):
            await query.answer("Нет доступа.", show_alert=True)
            return ConversationStates.FILES_MENU

        file_meta = next((f for f in folder["files"] if f["id"] == file_id), None)
        file_path = os.path.join(DATABASE_DIR, folder["name"], file_meta["name"]) if file_meta else None
        if not file_meta or not os.path.exists(file_path):
            await query.answer("Файл уже удален.", show_alert=True)
            return ConversationStates.FILES_MENU

        try:
            await query.answer()
            with open(file_path, "rb") as f:
                await query.message.chat.send_document(document=f, filename=file_meta["name"])
        except Exception as e:
            await query.answer(f"Ошибка отправки файла: {str(e)}", show_alert=True)
        return ConversationStates.FILES_MENU

    if data.startswith("folder_freeze:"):
        folder_id, page = data.split(":")[1], int(data.split(":")[2])
        if not is_admin(user_id):
            await query.answer("Только администратор может замораживать папки.", show_alert=True)
            return ConversationStates.CHOOSING_FOLDER
        set_folder_freezing_by_id(folder_id, True)
        text, keyboard = build_folder_manage_keyboard(folder_id, page, user_id)
        await query.edit_message_text(text, parse_mode="Markdown", reply_markup=keyboard)
        return ConversationStates.CHOOSING_FOLDER

    if data.startswith("folder_unfreeze:"):
        folder_id, page = data.split(":")[1], int(data.split(":")[2])
        if not is_admin(user_id):
            await query.answer("Только администратор может разморозить папки.", show_alert=True)
            return ConversationStates.CHOOSING_FOLDER
        set_folder_freezing_by_id(folder_id, False)
        text, keyboard = build_folder_manage_keyboard(folder_id, page, user_id)
        await query.edit_message_text(text, parse_mode="Markdown", reply_markup=keyboard)
        return ConversationStates.CHOOSING_FOLDER

    if data.startswith("folder_priv:"):
        folder_id, page = data.split(":")[1], int(data.split(":")[2])
        owner_id = get_folder_owner_by_id(folder_id)
        admin = is_admin(user_id)
        frozen = is_folder_frozen_by_id(folder_id)
        if owner_id == user_id and frozen and not admin:
            await query.answer("Папка заморожена администратором.", show_alert=True)
            return ConversationStates.CHOOSING_FOLDER
        if owner_id == user_id or admin:
            set_folder_status_by_id(folder_id, "private")
            text, keyboard = build_folder_manage_keyboard(folder_id, page, user_id)
            await query.edit_message_text(text, parse_mode="Markdown", reply_markup=keyboard)
        else:
            await query.answer("Только владелец папки может изменять данный параметр.", show_alert=True)
        return ConversationStates.CHOOSING_FOLDER

    if data.startswith("folder_public:"):
        folder_id, page = data.split(":")[1], int(data.split(":")[2])
        owner_id = get_folder_owner_by_id(folder_id)
        admin = is_admin(user_id)
        frozen = is_folder_frozen_by_id(folder_id)
        if owner_id == user_id and frozen and not admin:
            await query.answer("Папка заморожена администратором.", show_alert=True)
            return ConversationStates.CHOOSING_FOLDER
        if owner_id == user_id or admin:
            set_folder_status_by_id(folder_id, "public")
            text, keyboard = build_folder_manage_keyboard(folder_id, page, user_id)
            await query.edit_message_text(text, parse_mode="Markdown", reply_markup=keyboard)
        else:
            await query.answer("Только владелец папки может изменять данный параметр.", show_alert=True)
        return ConversationStates.CHOOSING_FOLDER

    if data.startswith("folder_add_files:"):
        folder_id, page = data.split(":")[1], int(data.split(":")[2])
        folder = get_folder_by_id(folder_id)
        user = get_user(user_id)
        admin = is_admin(user_id)
        freezing = is_folder_frozen_by_id(folder_id)
        status = get_folder_status_by_id(folder_id)
        owner_id = get_folder_owner_by_id(folder_id)

        if not user.get("addition", True):
            await query.answer("Нет доступа.", show_alert=True)
            return ConversationStates.CHOOSING_FOLDER

        if freezing and not admin:
            await query.answer("Папка заморожена администратором.", show_alert=True)
            return ConversationStates.CHOOSING_FOLDER

        if status == "private" and owner_id != user_id and not admin:
            await query.answer("Нет доступа к приватной папке.", show_alert=True)
            return ConversationStates.CHOOSING_FOLDER
        context.user_data["add_files"] = {"folder_id": folder_id, "page": page, "added": False}
        await query.edit_message_reply_markup(reply_markup=None)
        await query.message.chat.send_message(f"Отправьте файл(ы) для папки `{escape_md(folder['name'])}`:",parse_mode="Markdown", reply_markup=get_files_cancel_kb())
        return ConversationStates.ADD_FILES

    if data.startswith("folder_rename:"):
        folder_id, page = data.split(":")[1], int(data.split(":")[2])
        folder = get_folder_by_id(folder_id)
        user = get_user(user_id)
        admin = is_admin(user_id)
        freezing = is_folder_frozen_by_id(folder_id)
        status = get_folder_status_by_id(folder_id)
        owner_id = get_folder_owner_by_id(folder_id)

        if not user.get("rename", True):
            await query.answer("Нет доступа.", show_alert=True)
            return ConversationStates.CHOOSING_FOLDER

        if freezing and not admin:
            await query.answer("Папка заморожена администратором.", show_alert=True)
            return ConversationStates.CHOOSING_FOLDER

        if status == "private" and owner_id != user_id and not admin:
            await query.answer("Нет доступа к приватной папке.", show_alert=True)
            return ConversationStates.CHOOSING_FOLDER
        context.user_data["rename_folder"] = {"folder_id": folder_id, "page": page}
        await query.edit_message_reply_markup(reply_markup=None)
        await query.message.chat.send_message(f"Введите новое имя для папки `{escape_md(folder['name'])}`:",parse_mode="Markdown", reply_markup=get_cancel_kb())
        return ConversationStates.RENAME_FOLDER_NAME

    if data.startswith("folder_delete_confirm:"):
        folder_id, page = data.split(":")[1], int(data.split(":")[2])
        folder = get_folder_by_id(folder_id)
        user = get_user(user_id)
        admin = is_admin(user_id)
        owner_id = get_folder_owner_by_id(folder_id)
        status = get_folder_status_by_id(folder_id)
        freezing = is_folder_frozen_by_id(folder_id)

        if not user.get("delete", True):
            await query.answer("Нет доступа.", show_alert=True)
            return ConversationStates.CHOOSING_FOLDER

        if freezing and not admin:
            await query.answer("Папка заморожена администратором.", show_alert=True)
            return ConversationStates.CHOOSING_FOLDER

        if status == "private" and owner_id != user_id and not admin:
            await query.answer("Нет доступа к приватной папке.", show_alert=True)
            return ConversationStates.CHOOSING_FOLDER

        confirm_text = (f"Вы действительно хотите удалить папку `{escape_md(folder['name'])}`? Это действие *необратимо*.")
        confirm_kb = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("🗑 Удалить", callback_data=f"folder_delete:{folder_id}:{page}"),
                InlineKeyboardButton("🔙 Отмена", callback_data=f"folder_delete_cancel:{folder_id}:{page}")
            ]
        ])
        await query.edit_message_text(confirm_text, parse_mode="Markdown", reply_markup=confirm_kb)
        return ConversationStates.CHOOSING_FOLDER

    if data.startswith("folder_delete_cancel:"):
        folder_id, page = data.split(":")[1], int(data.split(":")[2])
        text, keyboard = build_folder_manage_keyboard(folder_id, page, user_id)
        await query.edit_message_text(text, parse_mode="Markdown", reply_markup=keyboard)
        return ConversationStates.CHOOSING_FOLDER

    if data.startswith("folder_delete:"):
        folder_id, page = data.split(":")[1], int(data.split(":")[2])
        folder = get_folder_by_id(folder_id)
        user = get_user(user_id)
        admin = is_admin(user_id)
        owner_id = get_folder_owner_by_id(folder_id)
        status = get_folder_status_by_id(folder_id)
        freezing = is_folder_frozen_by_id(folder_id)

        if not is_in_database(user_id) or is_banned(user_id):
            await query.answer("Нет доступа.", show_alert=True)
            return ConversationStates.CHOOSING_FOLDER

        if freezing and not admin:
            await query.answer("Папка заморожена администратором.", show_alert=True)
            return ConversationStates.CHOOSING_FOLDER

        if status == "private" and owner_id != user_id and not admin:
            await query.answer("Нет доступа к приватной папке.", show_alert=True)
            return ConversationStates.CHOOSING_FOLDER

        if not user.get("delete", True):
            await query.answer("Нет доступа.", show_alert=True)
            return ConversationStates.CHOOSING_FOLDER

        if not folder:
            await query.edit_message_text(
                "Папка уже удалена.",
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад к списку папок", callback_data=f"folders_page:{page}")]])
            )
            return ConversationStates.CHOOSING_FOLDER

        if owner_id:
            users = load_users()
            for u in users:
                if u.get("id") == owner_id:
                    u["folders"] = max(0, u.get("folders", 0) - 1)
                    break
            save_users(users)

        success_fs, msg_fs = delete_folder_fs(folder["name"])
        if not success_fs:
            await query.edit_message_text(f"Ошибка удаления папки: {msg_fs}", parse_mode="Markdown")
            return ConversationStates.CHOOSING_FOLDER
        delete_folder_in_db_by_id(folder_id)
        success_text = f"*Папка* `{escape_md(folder['name'])}` *удалена.*"
        back_kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔙 Назад к списку папок", callback_data=f"folders_page:{page}")]
        ])
        await query.edit_message_text(success_text, parse_mode="Markdown", reply_markup=back_kb)
        return ConversationStates.CHOOSING_FOLDER

# Добавление файлов в папку.
async def add_files(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not is_in_database(user_id):
        await update.message.reply_text("Войдите через кнопку ниже.", reply_markup=get_guest_kb())
        return ConversationHandler.END
    if is_banned(user_id):
        await update.message.reply_text("🚫 Функционал бота временно недоступен для вас.", reply_markup=get_guest_kb())
        return ConversationHandler.END

    log_state(update, context, "add_files")
    user = get_user(user_id)
    data = context.user_data.get("add_files")
    if not data:
        await update.message.reply_text("Ошибка данных. Попробуйте снова.", reply_markup=get_main_kb(user_id))
        return ConversationHandler.END

    folder_id = data["folder_id"]
    page = data["page"]
    folder = get_folder_by_id(folder_id)
    if not folder:
        await update.message.reply_text("Папка была удалена другим пользователем.", reply_markup=get_main_kb(user_id))
        context.user_data.pop("add_files", None)
        return ConversationHandler.END

    folder_path = os.path.join(DATABASE_DIR, folder["name"])

    if is_folder_frozen_by_id(folder_id) and not is_admin(user_id):
        await update.message.reply_text("Папка заморожена администратором.", reply_markup=get_main_kb(user_id))
        context.user_data.pop("add_files", None)
        return ConversationStates.CHOOSING_FOLDER

    if not os.path.exists(folder_path):
        await update.message.reply_text(f"Папка была удалена другим пользователем.",parse_mode="Markdown",reply_markup=get_main_kb(user_id))
        context.user_data.pop("add_files", None)
        return ConversationHandler.END

    if is_folder_private_by_id(folder_id) and get_folder_owner_by_id(folder_id) != user_id and not is_admin(user_id):
        await update.message.reply_text("Нет доступа к приватной папке.", reply_markup=get_main_kb(user_id))
        context.user_data.pop("add_files", None)
        return ConversationStates.CHOOSING_FOLDER

    if not user.get("addition", True):
        await update.message.reply_text("Нет доступа.", reply_markup=get_main_kb(user_id))
        context.user_data.pop("add_files", None)
        return ConversationHandler.END

    if update.message.text in ("🔙 Отмена", "✅ Закончить добавление"):
        await update.message.reply_text("Действие отменено." if update.message.text == "🔙 Отмена" else "Файл(ы) были добавлены в папку.",reply_markup=get_main_kb(user_id))
        text, keyboard = build_folder_manage_keyboard(folder_id, page, user_id)
        await update.message.reply_text(text, parse_mode="Markdown", reply_markup=keyboard)
        context.user_data.pop("add_files", None)
        return ConversationStates.CHOOSING_FOLDER

    file_items = []
    ext = None
    file_obj = None
    if update.message.document:
        file_obj = update.message.document
        ext = None
        file_items.append((file_obj, ext))
    elif update.message.photo:
        file_obj = update.message.photo[-1]
        ext = ".jpg"
        file_items.append((file_obj, ext))
    elif update.message.audio:
        file_obj = update.message.audio
        ext = None
        file_items.append((file_obj, ext))
    elif update.message.video:
        file_obj = update.message.video
        ext = ".mp4"
        file_items.append((file_obj, ext))

    if not file_items:
        show_finish = context.user_data["add_files"].get("added", False)
        await update.message.reply_text("Отправьте файл, фото, аудио или видео.",reply_markup=get_files_finish_kb() if show_finish else get_files_cancel_kb())
        return ConversationStates.ADD_FILES

    existing_files = set(os.listdir(folder_path))
    added_files, duplicate_files, processed_file_names, too_big_files = [], set(), set(), []

    for file_obj, ext in file_items:
        file_name = getattr(file_obj, "file_name", None)
        if not file_name:
            extension = ext if ext else ""
            file_name = f"{type(file_obj).__name__}_{file_obj.file_id}{extension}"
        save_path = os.path.join(folder_path, file_name)
        if file_name in existing_files or file_name in processed_file_names:
            duplicate_files.add(file_name)
            continue
        try:
            file = await file_obj.get_file()
            await file.download_to_drive(save_path)
            file_size = os.path.getsize(save_path)
            added_files.append((file_name, file_size))
            processed_file_names.add(file_name)

            folders = load_folders()
            for f in folders:
                if f["id"] == folder_id:
                    f["files"].append({"id": str(uuid.uuid4()), "name": file_name})
                    break
            save_folders(folders)

        except telegram.error.BadRequest as e:
            if "File is too big" in str(e):
                too_big_files.append(file_name)
            else:
                await update.message.reply_text(f"Ошибка загрузки файла `{escape_md(file_name)}`: {escape_md(str(e))}",parse_mode="Markdown")

    if added_files:
        context.user_data["add_files"]["added"] = True
        folder = get_folder_by_id(folder_id)
    show_finish = context.user_data["add_files"].get("added", False)

    reply_messages = []
    if added_files:
        for file_info in added_files:
            reply_messages.append(
                f"*✅ Файл добавлен*\n\nИмя: `{escape_md(file_info[0])}`\n\nРазмер: `{escape_md(format_size(file_info[1]))}`"
            )
    if duplicate_files:
        reply_messages.append(f"*❌ Файл не добавлен*\n\nДанный файл уже есть в папке.")
    if too_big_files:
        reply_messages.append(f"*❌ Файл не добавлен*\n\nОтправленный файл слишком большой (макс 50 MB).")

    await update.message.reply_text("\n\n".join(reply_messages),parse_mode="Markdown",reply_markup=get_files_finish_kb() if show_finish else get_files_cancel_kb())
    return ConversationStates.ADD_FILES

# Переименование папки.
async def rename_folder_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not is_in_database(user_id):
        await update.message.reply_text("Войдите через кнопку ниже.", reply_markup=get_guest_kb())
        return ConversationHandler.END
    if is_banned(user_id):
        await update.message.reply_text("🚫 Функционал бота временно недоступен для вас.", reply_markup=get_guest_kb())
        return ConversationHandler.END

    log_state(update, context, "rename_folder_name")
    text = update.message.text.strip()
    data = context.user_data.get("rename_folder")
    user = get_user(user_id)
    if not data:
        await update.message.reply_text("Ошибка данных.", reply_markup=get_main_kb(user_id))
        return ConversationHandler.END

    folder_id = data["folder_id"]
    page = data["page"]
    folder = get_folder_by_id(folder_id)

    admin = is_admin(user_id)
    freezing = is_folder_frozen_by_id(folder_id)
    status = get_folder_status_by_id(folder_id)
    owner_id = get_folder_owner_by_id(folder_id)

    if freezing and not admin:
        await update.message.reply_text("Папка заморожена администратором.", reply_markup=get_main_kb(user_id))
        context.user_data.pop("rename_folder", None)
        return ConversationStates.CHOOSING_FOLDER

    if not folder:
        await update.message.reply_text("Папка была удалена другим пользователем.",parse_mode="Markdown",reply_markup=get_main_kb(user_id))
        context.user_data.pop("rename_folder", None)
        return ConversationHandler.END

    if status == "private" and owner_id != user_id and not admin:
        await update.message.reply_text("Нет доступа к приватной папке.", reply_markup=get_main_kb(user_id))
        context.user_data.pop("rename_folder", None)
        return ConversationStates.CHOOSING_FOLDER

    if not user.get("rename", True):
        await update.message.reply_text("Нет доступа.", reply_markup=get_main_kb(user_id))
        context.user_data.pop("rename_folder", None)
        return ConversationStates.CHOOSING_FOLDER

    if text == "🔙 Отмена":
        await update.message.reply_text(f"Действие отменено.", reply_markup=get_main_kb(user_id))
        text_reply, keyboard = build_folder_manage_keyboard(folder_id, page, user_id)
        await update.message.reply_text(text_reply, parse_mode="Markdown", reply_markup=keyboard)
        context.user_data.pop("rename_folder", None)
        return ConversationStates.CHOOSING_FOLDER

    if not text or any(c in text for c in r'\/:*?"<>|') or text == folder["name"] or folder_exists(text):
        await update.message.reply_text("Недопустимое или занятое имя папки. Попробуйте другое.", reply_markup=get_cancel_kb())
        return ConversationStates.RENAME_FOLDER_NAME

    success, msg = rename_folder_fs(folder["name"], text)
    if not success:
        await update.message.reply_text(msg, reply_markup=get_cancel_kb())
        return ConversationStates.RENAME_FOLDER_NAME

    rename_folder_in_db_by_id(folder_id, text)
    await update.message.reply_text(f"*Имя папки было сменено на* `{escape_md(text)}`*.*", parse_mode="Markdown", reply_markup=get_main_kb(user_id))
    text_reply, keyboard = build_folder_manage_keyboard(folder_id, page, user_id)
    await update.message.reply_text(text_reply, parse_mode="Markdown", reply_markup=keyboard)
    context.user_data.pop("rename_folder", None)
    return ConversationStates.CHOOSING_FOLDER

# Переименование файла.
async def rename_file_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not is_in_database(user_id):
        await update.message.reply_text("Войдите через кнопку ниже.", reply_markup=get_guest_kb())
        return ConversationHandler.END
    if is_banned(user_id):
        await update.message.reply_text("🚫 Функционал бота временно недоступен для вас.", reply_markup=get_guest_kb())
        return ConversationHandler.END

    log_state(update, context, "rename_file_name")
    text = update.message.text.strip()
    data = context.user_data.get("rename_file")
    user = get_user(user_id)
    if not data:
        await update.message.reply_text("Ошибка данных.", reply_markup=get_main_kb(user_id))
        return ConversationHandler.END

    folder_id = data["folder_id"]
    file_id = data["file_id"]
    page = data["page"]
    folders = load_folders()
    folder = next((f for f in folders if f["id"] == folder_id), None)
    if not folder:
        await update.message.reply_text("Папка уже удалена.", reply_markup=get_main_kb(user_id))
        context.user_data.pop("rename_file", None)
        return ConversationStates.FILES_MENU

    admin = is_admin(user_id)
    freezing = is_folder_frozen_by_id(folder_id)
    status = get_folder_status_by_id(folder_id)
    owner_id = get_folder_owner_by_id(folder_id)
    is_owner = user_id == owner_id

    if freezing and not admin:
        await update.message.reply_text("Папка заморожена администратором.", reply_markup=get_main_kb(user_id))
        context.user_data.pop("rename_file", None)
        return ConversationStates.FILES_MENU

    if status == "private" and not (admin or is_owner):
        await update.message.reply_text("Нет доступа к приватной папке.", reply_markup=get_main_kb(user_id))
        context.user_data.pop("rename_file", None)
        return ConversationStates.FILES_MENU

    if not user.get("rename", True):
        await update.message.reply_text("Нет доступа.", reply_markup=get_main_kb(user_id))
        context.user_data.pop("rename_file", None)
        return ConversationStates.FILES_MENU

    file_meta = next((f for f in folder["files"] if f["id"] == file_id), None)
    if not file_meta:
        await update.message.reply_text(f"Файл уже удален.", reply_markup=get_main_kb(user_id))
        context.user_data.pop("rename_file", None)
        return ConversationStates.FILES_MENU

    old_name = file_meta["name"]
    file_path = os.path.join(DATABASE_DIR, folder["name"], old_name)
    old_ext = os.path.splitext(old_name)[1]

    if text == "🔙 Отмена":
        await update.message.reply_text(f"Действие отменено.", reply_markup=get_main_kb(user_id))
        info_text, keyboard = build_file_manage_keyboard(folder_id, file_id, page)
        await update.message.reply_text(info_text, parse_mode="Markdown", reply_markup=keyboard)
        context.user_data.pop("rename_file", None)
        return ConversationStates.FILES_MENU

    if not os.path.exists(file_path):
        await update.message.reply_text(f"Файл `{escape_md(old_name)}` уже удален.", parse_mode="Markdown", reply_markup=get_main_kb(user_id))
        context.user_data.pop("rename_file", None)
        return ConversationStates.FILES_MENU

    if not os.path.splitext(text)[1] and old_ext:
        text += old_ext

    if not text or any(c in text for c in r'\/:*?"<>|') or text == old_name or os.path.exists(os.path.join(DATABASE_DIR, folder["name"], text)):
        await update.message.reply_text("Недопустимое или занятое имя файла. Попробуйте другое.", reply_markup=get_cancel_kb())
        return ConversationStates.FILE_RENAME

    try:
        os.rename(file_path, os.path.join(DATABASE_DIR, folder["name"], text))
        for f in folders:
            if f["id"] == folder_id:
                for file in f["files"]:
                    if file["id"] == file_id:
                        file["name"] = text
        save_folders(folders)
        folder = next((f for f in folders if f["id"] == folder_id), None)
        file_meta = next((f for f in folder["files"] if f["id"] == file_id), None)
        await update.message.reply_text(f"*Имя файла было сменено на* `{escape_md(text)}`*.*",parse_mode="Markdown",reply_markup=get_main_kb(user_id))
        info_text, keyboard = build_file_manage_keyboard(folder_id, file_id, page)
        await update.message.reply_text(info_text, parse_mode="Markdown", reply_markup=keyboard)
        context.user_data.pop("rename_file", None)
        return ConversationStates.FILES_MENU
    except Exception as e:
        await update.message.reply_text(f"Ошибка переименования файла: {escape_md(str(e))}", reply_markup=get_cancel_kb())
        return ConversationStates.FILE_RENAME

# Меню управления пользователями для администратора.
async def admin_users_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    log_state(update, context, "admin_users_menu")
    users = load_users()
    page = 0
    total_pages = max(1, (len([u for u in users if u.get('id') != update.effective_user.id]) + USERS_PER_PAGE - 1) // USERS_PER_PAGE)
    await update.message.reply_text(build_users_list_message(users),reply_markup=build_users_list_keyboard(users, update.effective_user.id, page, total_pages),parse_mode="Markdown")
    return ConversationStates.USER_MANAGE_MENU

# Обработка нажатий на Inline-кнопки для управления пользователями.
async def user_admin_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if await precheck_inline(update, context): return
    log_state(update, context, "user_admin_callback")
    user_id = update.effective_user.id
    query = update.callback_query
    data = query.data

    if not is_admin(user_id):
        await update.callback_query.answer("Нет доступа.", show_alert=True)
        return

    users = load_users()

    if data.startswith("users_page:"):
        page = int(data.split(":")[1])
        context.user_data['users_page'] = page
        total_pages = max(1, (len([u for u in users if u.get('id') != query.from_user.id]) + USERS_PER_PAGE - 1) // USERS_PER_PAGE)
        page = max(0, min(page, total_pages - 1))
        await query.edit_message_text(build_users_list_message(users),reply_markup=build_users_list_keyboard(users, query.from_user.id, page, total_pages),parse_mode="Markdown")
        return ConversationStates.USER_MANAGE_MENU

    if data.startswith("user_manage:"):
        parts = data.split(":")
        user_id_to_manage = int(parts[1])
        page = int(parts[2]) if len(parts) > 2 else context.user_data.get('users_page', 0)
        context.user_data['users_page'] = page
        user = get_user(user_id_to_manage)
        if user:
            await query.edit_message_text(build_user_manage_text(user),reply_markup=build_user_manage_keyboard(user, page),parse_mode="Markdown")
            context.user_data["current_manage_user"] = user_id_to_manage
        else:
            total_pages = max(1, (len([u for u in users if u.get('id') != query.from_user.id]) + USERS_PER_PAGE - 1) // USERS_PER_PAGE)
            page = max(0, min(page, total_pages - 1))
            await query.edit_message_text(build_users_list_message(users),reply_markup=build_users_list_keyboard(users, query.from_user.id, page, total_pages),parse_mode="Markdown")
        return ConversationStates.USER_MANAGE_USER

    if data.startswith("user_list:"):
        page = int(data.split(":")[1])
        context.user_data['users_page'] = page
        total_pages = max(1, (len([u for u in users if u.get('id') != query.from_user.id]) + USERS_PER_PAGE - 1) // USERS_PER_PAGE)
        page = max(0, min(page, total_pages - 1))
        await query.edit_message_text(build_users_list_message(users),reply_markup=build_users_list_keyboard(users, query.from_user.id, page, total_pages),parse_mode="Markdown")
        return ConversationStates.USER_MANAGE_MENU

    if data == "user_list":
        page = context.user_data.get('users_page', 0)
        total_pages = max(1, (len([u for u in users if u.get('id') != query.from_user.id]) + USERS_PER_PAGE - 1) // USERS_PER_PAGE)
        page = max(0, min(page, total_pages - 1))
        await query.edit_message_text(build_users_list_message(users),reply_markup=build_users_list_keyboard(users, query.from_user.id, page, total_pages),parse_mode="Markdown")
        return ConversationStates.USER_MANAGE_MENU

    if data == "user_add":
        await query.message.edit_reply_markup(reply_markup=None)
        await query.message.chat.send_message("Введите Telegram ID пользователя:", reply_markup=get_cancel_kb())
        return ConversationStates.USER_ADD_ID

    if data == "no_users":
        await query.answer("В базе нет пользователей.", show_alert=True)
        return ConversationStates.USER_MANAGE_MENU

    if data.startswith("user_toggle_status:"):
        target_user_id = int(data.split(":")[1])
        users = load_users()
        for u in users:
            if u.get("id") == target_user_id:
                if u.get("status") != "admin":
                    u["status"] = "admin"
                    u["addition"] = True
                    u["download"] = True
                    u["rename"] = True
                    u["delete"] = True
                    u["folders_limit"] = 0
                    save_users(users)
                    try:
                        await update.get_bot().send_message(
                            target_user_id,
                            f"*🔔 Уведомление*\n\nВас сделали администратором.",
                            parse_mode="Markdown"
                        )
                    except Exception:
                        pass
                else:
                    u["status"] = "default"
                    u["folders_limit"] = 10
                    save_users(users)
                    try:
                        await update.get_bot().send_message(target_user_id,f"*🔔 Уведомление*\n\nУ вас забрали права администратора.",parse_mode="Markdown")
                    except Exception:
                        pass
                break
        user = get_user(target_user_id)
        page = context.user_data.get('users_page', 0)
        await query.edit_message_text(build_user_manage_text(user),reply_markup=build_user_manage_keyboard(user, page),parse_mode="Markdown")
        return ConversationStates.USER_MANAGE_USER

    if data.startswith("user_change_pass:"):
        user_id_to_change = int(data.split(":")[1])
        context.user_data["change_pass_user"] = user_id_to_change
        await query.edit_message_reply_markup(reply_markup=None)
        await query.message.chat.send_message("Введите новый пароль для пользователя:", reply_markup=get_cancel_kb())
        return ConversationStates.USER_ADD_PASS

    if data.startswith("user_block:"):
        user_id_to_block = int(data.split(":")[1])
        admin_block_user(user_id_to_block)
        user = get_user(user_id_to_block)
        page = context.user_data.get('users_page', 0)
        await query.edit_message_text(build_user_manage_text(user),reply_markup=build_user_manage_keyboard(user, page),parse_mode="Markdown")
        try:
            await update.get_bot().send_message(user_id_to_block,"*🔔 Уведомление*\n\nВас заблокировал администратор.",parse_mode="Markdown")
        except Exception:
            pass
        return ConversationStates.USER_MANAGE_USER

    if data.startswith("user_unblock:"):
        user_id_to_unblock = int(data.split(":")[1])
        admin_unblock_user(user_id_to_unblock)
        user = get_user(user_id_to_unblock)
        page = context.user_data.get('users_page', 0)
        await query.edit_message_text(build_user_manage_text(user),reply_markup=build_user_manage_keyboard(user, page),parse_mode="Markdown")
        try:
            await update.get_bot().send_message(user_id_to_unblock,"*🔔 Уведомление*\n\nВас разблокировал администратор.",parse_mode="Markdown")
        except Exception:
            pass
        return ConversationStates.USER_MANAGE_USER

    if data.startswith("user_delete_confirm:"):
        user_id_to_del = int(data.split(":")[1])
        page = context.user_data.get('users_page', 0)
        await query.edit_message_text(f"Вы действительно хотите удалить пользователя `{user_id_to_del}`? Это действие *необратимо*.",parse_mode="Markdown",reply_markup=build_user_delete_confirm_keyboard(user_id_to_del))
        return ConversationStates.USER_DELETE_CONFIRM

    if data.startswith("user_delete_cancel:"):
        user_id_to_cancel = int(data.split(":")[1])
        user = get_user(user_id_to_cancel)
        page = context.user_data.get('users_page', 0)
        await query.edit_message_text(build_user_manage_text(user),reply_markup=build_user_manage_keyboard(user, page),parse_mode="Markdown")
        return ConversationStates.USER_MANAGE_USER

    if data.startswith("user_delete:"):
        user_id_to_delete = int(data.split(":")[1])
        users = [u for u in load_users() if u.get("id") != user_id_to_delete]
        save_users(users)
        page = context.user_data.get('users_page', 0)
        await query.edit_message_text(
            f"Пользователь `{user_id_to_delete}` успешно удалён.",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Назад к списку пользователей", callback_data=f"user_list:{page}")]
            ])
        )
        try:
            await update.get_bot().send_message(user_id_to_delete,"*🔔 Уведомление*\n\nАдминистратор исключил вас из базы.",parse_mode="Markdown")
        except Exception:
            pass
        return ConversationStates.USER_MANAGE_MENU

    if data.startswith("user_send_msg:"):
        user_id_to_send = int(data.split(":")[1])
        context.user_data["send_msg_user"] = user_id_to_send
        await query.edit_message_reply_markup(reply_markup=None)
        await query.message.chat.send_message("Введите сообщение, которое хотите отправить пользователю:", reply_markup=get_cancel_kb())
        return ConversationStates.USER_SEND_MSG

    if data.startswith("user_send_msg_cancel:"):
        user_id_to_cancel = int(data.split(":")[1])
        user = get_user(user_id_to_cancel)
        page = context.user_data.get('users_page', 0)
        await query.message.chat.send_message("Действие отменено.", reply_markup=get_main_kb(update.effective_user.id))
        await query.message.chat.send_message(build_user_manage_text(user),reply_markup=build_user_manage_keyboard(user, page),parse_mode="Markdown")
        return ConversationStates.USER_MANAGE_USER

    if data.startswith("user_do_send_msg:"):
        user_id_to_send = int(data.split(":")[1])
        text_to_send = context.user_data.get("send_msg_text")
        admin_id = query.from_user.id

        if not text_to_send:
            await query.answer("Сообщение уже отправлено.", show_alert=True)
            return ConversationStates.USER_MANAGE_USER

        await query.answer()

        try:
            await update.get_bot().send_message(user_id_to_send,f"*🔔 Уведомление*\n\nАдминистратор отправил вам сообщение.\n_Сообщение: {text_to_send}_",parse_mode="Markdown")
            await query.message.chat.send_message("Сообщение отправлено.", reply_markup=get_main_kb(admin_id))
        except Exception:
            await query.message.chat.send_message("Не удалось отправить сообщение (пользователь, возможно, не запускал бота).", reply_markup=get_main_kb(admin_id))

        user_obj = get_user(user_id_to_send)
        page = context.user_data.get('users_page', 0)
        await query.message.chat.send_message(build_user_manage_text(user_obj),reply_markup=build_user_manage_keyboard(user_obj, page),parse_mode="Markdown")
        context.user_data.pop("send_msg_text", None)
        context.user_data.pop("send_msg_user", None)
        return ConversationStates.USER_MANAGE_USER

    if data.startswith("user_toggle_addition:") or data.startswith("user_toggle_download:") or data.startswith("user_toggle_rename:") or data.startswith("user_toggle_delete:") or data.startswith("user_set_folders_limit:"):
        target_user_id = int(data.split(":")[1])
        user = get_user(target_user_id)
        if user and user.get("status") == "admin":
            await query.answer("Нельзя изменить права администратора.", show_alert=True)
            return ConversationStates.USER_MANAGE_USER

    if data.startswith("user_toggle_addition:"):
        target_user_id = int(data.split(":")[1])
        users = load_users()
        for u in users:
            if u.get("id") == target_user_id:
                u["addition"] = not u.get("addition", True)
                save_users(users)
                break
        user = get_user(target_user_id)
        page = context.user_data.get('users_page', 0)
        await query.answer()
        await query.edit_message_text(build_user_manage_text(user), reply_markup=build_user_manage_keyboard(user, page), parse_mode="Markdown")
        return ConversationStates.USER_MANAGE_USER

    if data.startswith("user_toggle_download:"):
        target_user_id = int(data.split(":")[1])
        users = load_users()
        for u in users:
            if u.get("id") == target_user_id:
                u["download"] = not u.get("download", True)
                save_users(users)
                break
        user = get_user(target_user_id)
        page = context.user_data.get('users_page', 0)
        await query.answer()
        await query.edit_message_text(build_user_manage_text(user), reply_markup=build_user_manage_keyboard(user, page), parse_mode="Markdown")
        return ConversationStates.USER_MANAGE_USER

    if data.startswith("user_toggle_rename:"):
        target_user_id = int(data.split(":")[1])
        users = load_users()
        for u in users:
            if u.get("id") == target_user_id:
                u["rename"] = not u.get("rename", True)
                save_users(users)
                break
        user = get_user(target_user_id)
        page = context.user_data.get('users_page', 0)
        await query.answer()
        await query.edit_message_text(build_user_manage_text(user), reply_markup=build_user_manage_keyboard(user, page), parse_mode="Markdown")
        return ConversationStates.USER_MANAGE_USER

    if data.startswith("user_toggle_delete:"):
        target_user_id = int(data.split(":")[1])
        users = load_users()
        for u in users:
            if u.get("id") == target_user_id:
                u["delete"] = not u.get("delete", True)
                save_users(users)
                break
        user = get_user(target_user_id)
        page = context.user_data.get('users_page', 0)
        await query.answer()
        await query.edit_message_text(build_user_manage_text(user), reply_markup=build_user_manage_keyboard(user, page), parse_mode="Markdown")
        return ConversationStates.USER_MANAGE_USER

    if data.startswith("user_set_folders_limit:"):
        target_user_id = int(data.split(":")[1])
        context.user_data["set_limit_user"] = target_user_id
        await query.edit_message_reply_markup(reply_markup=None)
        await query.message.chat.send_message("Введите лимит для пользователя (0 - Нет лимита):", reply_markup=get_cancel_kb())
        return ConversationStates.USER_SET_LIMIT

# Обработка ввода лимита папок.
async def user_set_limit(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if await precheck_reply(update, context): return ConversationHandler.END
    text = update.message.text.strip()
    user_id = update.effective_user.id
    if text == "🔙 Отмена":
        await update.message.reply_text("Действие отменено.", reply_markup=get_main_kb(user_id))
        limit_user_id = context.user_data.pop("set_limit_user", None)
        user = get_user(limit_user_id)
        page = context.user_data.get('users_page', 0)
        await update.message.reply_text(build_user_manage_text(user),reply_markup=build_user_manage_keyboard(user, page),parse_mode="Markdown")
        return ConversationStates.USER_MANAGE_USER
    try:
        limit = int(text)
        if limit < 0 or limit > 1000:
            raise ValueError
    except ValueError:
        await update.message.reply_text("Введите корректный лимит (от 0 до 1000).", reply_markup=get_cancel_kb())
        return ConversationStates.USER_SET_LIMIT
    limit_user_id = context.user_data.pop("set_limit_user", None)
    users = load_users()
    for u in users:
        if u.get("id") == limit_user_id:
            u["folders_limit"] = limit
            save_users(users)
            break
    await update.message.reply_text("Новый лимит установлен.", reply_markup=get_main_kb(user_id))
    user = get_user(limit_user_id)
    page = context.user_data.get('users_page', 0)
    await update.message.reply_text(build_user_manage_text(user),reply_markup=build_user_manage_keyboard(user, page),parse_mode="Markdown")
    return ConversationStates.USER_MANAGE_USER

# Ввод Telegram ID при добавлении пользователя.
async def user_add_id(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if await precheck_reply(update, context): return ConversationHandler.END
    log_state(update, context, "user_add_id")
    text = update.message.text.strip()
    if text == "🔙 Отмена":
        await update.message.reply_text("Действие отменено.",reply_markup=get_main_kb(update.effective_user.id))
        users = load_users()
        await update.message.reply_text(build_users_list_message(users),reply_markup=build_users_list_keyboard(users, update.effective_user.id),parse_mode="Markdown")
        return ConversationStates.USER_MANAGE_MENU

    try:
        user_id = int(text)
    except ValueError:
        await update.message.reply_text("Введите корректный числовой Telegram ID.", reply_markup=get_cancel_kb())
        return ConversationStates.USER_ADD_ID

    if user_exists(user_id):
        await update.message.reply_text("Пользователь с таким ID уже есть.", reply_markup=get_cancel_kb())
        return ConversationStates.USER_ADD_ID

    context.user_data["add_stage"] = {"id": user_id}

    await update.message.reply_text("Введите пароль для нового пользователя:", reply_markup=get_cancel_kb())
    return ConversationStates.USER_ADD_PASS

# Ввод пароля при добавлении пользователя.
async def user_add_pass(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if await precheck_reply(update, context): return ConversationHandler.END
    log_state(update, context, "user_add_pass")
    password = update.message.text.strip()
    if password == "🔙 Отмена":
        await update.message.reply_text("Действие отменено.", reply_markup=get_main_kb(update.effective_user.id))
        if "change_pass_user" in context.user_data:
            user_id = context.user_data.pop("change_pass_user")
            user = get_user(user_id)
            await update.message.reply_text(build_user_manage_text(user),reply_markup=build_user_manage_keyboard(user),parse_mode="Markdown")
            return ConversationStates.USER_MANAGE_USER
        users = load_users()
        await update.message.reply_text(build_users_list_message(users),reply_markup=build_users_list_keyboard(users, update.effective_user.id),parse_mode="Markdown")
        return ConversationStates.USER_MANAGE_MENU

    if "change_pass_user" in context.user_data:
        user_id = context.user_data.pop("change_pass_user")
        users = load_users()
        for u in users:
            if u.get("id") == user_id:
                u["password"] = password
                break
        save_users(users)
        await update.message.reply_text("Пароль пользователя изменен.", reply_markup=get_main_kb(update.effective_user.id))
        user = get_user(user_id)
        await update.message.reply_text(build_user_manage_text(user),reply_markup=build_user_manage_keyboard(user),parse_mode="Markdown")
        try:
            await update.get_bot().send_message(user_id,f"*🔔 Уведомление*\n\nАдминистратор изменил ваш пароль.\n_Новый пароль: {password}_",parse_mode="Markdown")
        except Exception:
            pass
        return ConversationStates.USER_MANAGE_USER

    add_stage = context.user_data.get("add_stage", {})
    user_id = add_stage.get("id")
    if not user_id:
        await update.message.reply_text("Ошибка добавления пользователя. Начните сначала.", reply_markup=get_main_kb(update.effective_user.id))
        return ConversationStates.USER_MANAGE_MENU

    context.user_data["add_stage"]["password"] = password
    await update.message.reply_text("Введите отображаемое имя пользователя (любое):", reply_markup=get_cancel_kb())
    return ConversationStates.USER_ADD_NAME

# Ввод имени при добавлении пользователя.
async def user_add_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if await precheck_reply(update, context): return ConversationHandler.END
    log_state(update, context, "user_add_name")
    username = update.message.text.strip()
    if username == "🔙 Отмена":
        await update.message.reply_text("Действие отменено.", reply_markup=get_main_kb(update.effective_user.id))
        users = load_users()
        page = 0
        total_pages = max(1, (len([u for u in users if u.get('id') != update.effective_user.id]) + USERS_PER_PAGE - 1) // USERS_PER_PAGE)
        await update.message.reply_text(build_users_list_message(users),reply_markup=build_users_list_keyboard(users, update.effective_user.id, page, total_pages),parse_mode="Markdown")
        return ConversationStates.USER_MANAGE_MENU

    add_stage = context.user_data.get("add_stage", {})
    user_id = add_stage.get("id")
    password = add_stage.get("password")
    if not user_id or not password:
        await update.message.reply_text("Ошибка добавления пользователя. Начните сначала.", reply_markup=get_main_kb(update.effective_user.id))
        return ConversationStates.USER_MANAGE_MENU

    add_user(user_id, password, "default", username)
    context.user_data.pop("add_stage", None)
    await update.message.reply_text("Пользователь добавлен!", reply_markup=get_main_kb(update.effective_user.id))
    try:
        await update.get_bot().send_message(user_id, f"*🔔 Уведомление*\n\nАдминистратор добавил вас в базу данных! Вы можете войти в бота.",parse_mode="Markdown")
    except Exception:
        pass
    users = load_users()
    page = 0
    total_other = len([u for u in users if u.get('id') != update.effective_user.id])
    total_pages = max(1, (total_other + USERS_PER_PAGE - 1) // USERS_PER_PAGE)
    await update.message.reply_text(build_users_list_message(users),reply_markup=build_users_list_keyboard(users, update.effective_user.id, page, total_pages),parse_mode="Markdown")
    return ConversationStates.USER_MANAGE_MENU

# Ввод сообщения для пользователя (для администратора).
async def user_send_msg_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if await precheck_reply(update, context): return ConversationHandler.END
    log_state(update, context, "user_send_msg_text")
    if update.message.text == "🔙 Отмена":
        await update.message.reply_text("Действие отменено.", reply_markup=get_main_kb(update.effective_user.id))
        user_id = context.user_data.get("send_msg_user")
        user = get_user(user_id)
        await update.message.reply_text(build_user_manage_text(user),reply_markup=build_user_manage_keyboard(user),parse_mode="Markdown")
        context.user_data.pop("send_msg_user", None)
        return ConversationStates.USER_MANAGE_USER

    context.user_data["send_msg_text"] = update.message.text
    user_id = context.user_data["send_msg_user"]
    await update.message.reply_text(f"Вы точно хотите отправить данное сообщение пользователю `{user_id}`?\n\n"f"_Сообщение: {update.message.text}_",parse_mode="Markdown",reply_markup=build_confirm_send_msg_keyboard(user_id))
    return ConversationStates.USER_CONFIRM_SEND_MSG

# Отмена отправки сообщения пользователю.
async def cancel_confirm_send_msg(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if await precheck_reply(update, context): return ConversationHandler.END
    log_state(update, context, "cancel_confirm_send_msg")
    user_id = update.effective_user.id
    await update.message.reply_text("Действие отменено.", reply_markup=get_main_kb(user_id))
    managed_user_id = context.user_data.get("send_msg_user")
    user = get_user(managed_user_id)
    if user:
        await update.message.reply_text(build_user_manage_text(user),reply_markup=build_user_manage_keyboard(user),parse_mode="Markdown")
    context.user_data.pop("send_msg_text", None)
    context.user_data.pop("send_msg_user", None)
    return ConversationStates.USER_MANAGE_USER

# Обработка неизвестных сообщений/команд.
async def unknown(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if await precheck_reply(update, context): return ConversationHandler.END
    log_state(update, context, "unknown")
    user_id = update.effective_user.id
    if "rename_folder" in context.user_data:
        data = context.user_data["rename_folder"]
        folder_id = data["folder_id"]
        page = data["page"]
        text_reply, keyboard = build_folder_manage_keyboard(folder_id, page, user_id)
        await update.message.reply_text(text_reply, parse_mode="Markdown", reply_markup=keyboard)
        await update.message.reply_text("Выберите действие:", reply_markup=get_main_kb(user_id))
        context.user_data.pop("rename_folder", None)
        return
    if "rename_file" in context.user_data:
        data = context.user_data["rename_file"]
        folder_id = data["folder_id"]
        file_id = data["file_id"]
        page = data["page"]
        info_text, keyboard = build_file_manage_keyboard(folder_id, file_id, page)
        await update.message.reply_text(info_text, parse_mode="Markdown", reply_markup=keyboard)
        await update.message.reply_text("Выберите действие:", reply_markup=get_main_kb(user_id))
        context.user_data.pop("rename_file", None)
        return
    if not is_authorized(user_id):
        await update.message.reply_text("Войдите через кнопку ниже.", reply_markup=get_guest_kb())
    else:
        await update.message.reply_text("Используйте кнопки меню.", reply_markup=get_main_kb(user_id))

# Игнорирщик сообщений (ничего не делает).
async def ignore_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    log_state(update, context, "ignore_message")
    return

########################################
######### HANDLERS РЕГИСТРАЦИЯ #########
########################################

# Инициализация и запуск.
def main():
    if LOG_ENABLED:
        log("Bot starting...")
    
    if not check_mongodb_connection():
        log("Ошибка подключения к MongoDB. Бот не может быть запущен.")
        return

    os.makedirs(DATABASE_DIR, exist_ok=True)

    main_conv = ConversationHandler(
        entry_points=[MessageHandler(
            filters.Regex("^(➕ Создать папку|🗂 Список папок|⚙️ Управление пользователями|🚪 Выйти из аккаунта)$"),
            main_menu
        )],
        states={
            ConversationStates.FOLDER_NAME: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, create_folder),
                CallbackQueryHandler(folder_button_callback, pattern=r".*")
            ],
            ConversationStates.RENAME_FOLDER_NAME: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, rename_folder_name),
                CallbackQueryHandler(folder_button_callback, pattern=r".*")
            ],
            ConversationStates.ADD_FILES: [
                MessageHandler(
                    filters.Document.ALL | filters.PHOTO | filters.AUDIO | filters.VIDEO | (filters.TEXT & ~filters.COMMAND),
                    add_files
                )
            ],
            ConversationStates.CHOOSING_FOLDER: [
                CallbackQueryHandler(folder_button_callback, pattern=r".*"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, main_menu)
            ],
            ConversationStates.FILES_MENU: [
                CallbackQueryHandler(folder_button_callback, pattern=r".*"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, main_menu)
            ],
            ConversationStates.FILE_RENAME: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, rename_file_name),
                CallbackQueryHandler(folder_button_callback, pattern=r".*")
            ],
            ConversationStates.FILE_DELETE_CONFIRM: [
                CallbackQueryHandler(folder_button_callback, pattern=r".*"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, main_menu)
            ],
            ConversationStates.USER_MANAGE_MENU: [
                CallbackQueryHandler(user_admin_callback, pattern=r".*"),
                MessageHandler(filters.Regex("^(➕ Создать папку|🗂 Список папок|⚙️ Управление пользователями|🚪 Выйти из аккаунта)$"), main_menu),
                MessageHandler(filters.ALL, unknown)
            ],
            ConversationStates.USER_MANAGE_USER: [
                CallbackQueryHandler(user_admin_callback, pattern=r".*"),
                MessageHandler(filters.Regex("^(➕ Создать папку|🗂 Список папок|⚙️ Управление пользователями|🚪 Выйти из аккаунта)$"), main_menu),
                MessageHandler(filters.ALL, unknown)
            ],
            ConversationStates.USER_ADD_ID: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, user_add_id),
                CallbackQueryHandler(user_admin_callback, pattern=r".*"),
                MessageHandler(filters.Regex("^(➕ Создать папку|🗂 Список папок|⚙️ Управление пользователями|🚪 Выйти из аккаунта)$"), main_menu),
                MessageHandler(filters.ALL, unknown)
            ],
            ConversationStates.USER_ADD_PASS: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, user_add_pass),
                CallbackQueryHandler(user_admin_callback, pattern=r".*"),
                MessageHandler(filters.Regex("^(➕ Создать папку|🗂 Список папок|⚙️ Управление пользователями|🚪 Выйти из аккаунта)$"), main_menu),
                MessageHandler(filters.ALL, unknown)
            ],
            ConversationStates.USER_ADD_NAME: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, user_add_name),
                CallbackQueryHandler(user_admin_callback, pattern=r".*"),
                MessageHandler(filters.Regex("^(➕ Создать папку|🗂 Список папок|⚙️ Управление пользователями|🚪 Выйти из аккаунта)$"), main_menu),
                MessageHandler(filters.ALL, unknown)
            ],
            ConversationStates.USER_SEND_MSG: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, user_send_msg_text),
                CallbackQueryHandler(user_admin_callback, pattern=r".*"),
                MessageHandler(filters.ALL, unknown)
            ],
            ConversationStates.USER_CONFIRM_SEND_MSG: [
                CallbackQueryHandler(user_admin_callback, pattern=r".*"),
                MessageHandler(filters.Regex("^🔙 Отмена$"), cancel_confirm_send_msg),
                MessageHandler(filters.ALL, ignore_message),
            ],
            ConversationStates.USER_SET_LIMIT: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, user_set_limit),
                CallbackQueryHandler(user_admin_callback, pattern=r".*"),
                MessageHandler(filters.Regex("^(➕ Создать папку|🗂 Список папок|⚙️ Управление пользователями|🚪 Выйти из аккаунта)$"), main_menu),
                MessageHandler(filters.ALL, unknown)
            ],
            ConversationStates.USER_DELETE_CONFIRM: [
                CallbackQueryHandler(user_admin_callback, pattern=r".*"),
                MessageHandler(filters.ALL, unknown)
            ],
        },
        fallbacks=[CommandHandler("start", start)],
        map_to_parent={ConversationHandler.END: ConversationHandler.END}
    )

    guest_conv = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex("^📥 Войти в аккаунт$"), guest_menu)],
        states={ConversationStates.AUTH: [MessageHandler(filters.TEXT & ~filters.COMMAND, auth)]},
        fallbacks=[CommandHandler("start", start)],
        map_to_parent={ConversationHandler.END: ConversationHandler.END}
    )

    if request:
        app = Application.builder().token(API_TOKEN).request(request).build()
    else:
        app = Application.builder().token(API_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(guest_conv)
    app.add_handler(main_conv)
    app.add_handler(MessageHandler(filters.ALL, unknown))

    if LOG_ENABLED:
        log("Handlers registered, polling starts...")
    app.run_polling()

if __name__ == "__main__":
    main()