#############################################################
######### ПРОГРАММА ДЛЯ ЗАПИСИ ПЕРВОГО ПОЛЬЗОВАТЕЛЯ #########
#############################################################

import json
import os
import datetime

USERS_FILE = 'users.json'

def load_users():
    if not os.path.exists(USERS_FILE):
        return []
    with open(USERS_FILE, 'r', encoding='utf-8') as f:
        try:
            data = json.load(f)
            if isinstance(data, dict):
                data = [data]
            return data
        except Exception:
            return []

def save_users(users):
    with open(USERS_FILE, 'w', encoding='utf-8') as f:
        json.dump(users, f, ensure_ascii=False, indent=2)

def main():
    print("Добавление администратора в users.json\n")
    try:
        user_id = int(input("Введите Telegram ID: ").strip())
    except ValueError:
        print("ID должен быть числом!")
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

    users = load_users()
    for u in users:
        if u.get("id") == user_id:
            print("Пользователь с таким ID уже существует.")
            return

    users.append(user)
    save_users(users)
    print(f"Пользователь {username} (ID: {user_id}) добавлен как администратор.")

if __name__ == "__main__":
    main()