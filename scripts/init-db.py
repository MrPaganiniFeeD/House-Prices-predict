#!/usr/bin/env python
"""
Скрипт инициализации базы данных
Запускается автоматически при первом старте PostgreSQL контейнера
"""
import psycopg2
import os

def init_database():
    """Создает базу данных если её нет"""
    try:
        # Подключаемся к стандартной базе postgres
        conn = psycopg2.connect(
            host="localhost",
            port=5432,
            database="postgres",
            user="postgres",
            password="1324"
        )
        conn.autocommit = True
        cursor = conn.cursor()
        
        # Проверяем существование базы данных
        cursor.execute("SELECT 1 FROM pg_database WHERE datname='training_db'")
        exists = cursor.fetchone()
        
        if not exists:
            cursor.execute("CREATE DATABASE training_db;")
            print("✅ База данных training_db создана!")
        else:
            print("✅ База данных training_db уже существует")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ Ошибка при инициализации БД: {e}")

if __name__ == "__main__":
    init_database()