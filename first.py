#############################################################
######### ПРОГРАММА ДЛЯ ЗАПИСИ ПЕРВОГО ПОЛЬЗОВАТЕЛЯ #########
#############################################################

import datetime
import os
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
DB_NAME = os.getenv("DB_NAME", "telegram_bot")

def connect_to_mongodb():
    try:
        client = MongoClient(MONGO_URI)
        client.admin.command('ping')
        db = client[DB_NAME]
        return client, db
    except Exception as e:
        print(f"Ошибка подключения к MongoDB: {e}")
        return None, None

def check_user_exists(collection, user_id):
    return collection.count_documents({"id": user_id}) > 0

def create_user(collection, user_data):
    try:
        collection.insert_one(user_data)
        return True
    except Exception as e:
        print(f"Ошибка при создании пользователя: {e}")
        return False

def main():
    print("Добавление администратора в MongoDB\n")

    try:
        client, db = connect_to_mongodb()
        if client is None:
            print("Не удалось подключиться к MongoDB. Проверьте параметры подключения.")
            return

        users_collection = db['users']
        users_collection.create_index("id", unique=True)

        try:
            user_id = int(input("Введите Telegram ID: ").strip())
        except ValueError:
            print("ID должен быть числом!")
            client.close()
            return

        if check_user_exists(users_collection, user_id):
            print("Пользователь с таким ID уже существует.")
            client.close()
            return

        password = input("Введите пароль: ").strip()
        username = input("Введите имя пользователя: ").strip()
        created_at = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        user = {
            "id": user_id,
            "password": password,
            "status": "admin",
            "username": username,
            "authorized": False,
            "folders": 0,
            "created_at": created_at,
            "folders_limit": 0,
            "addition": True,
            "download": True,
            "rename": True,
            "delete": True
        }

        if create_user(users_collection, user):
            print(f"Пользователь {username} (ID: {user_id}) успешно добавлен как администратор.")
        else:
            print("Не удалось добавить пользователя.")

    except Exception as e:
        print(f"Произошла ошибка: {e}")
    
    finally:
        if 'client' in locals():
            client.close()

if __name__ == "__main__":
    main()