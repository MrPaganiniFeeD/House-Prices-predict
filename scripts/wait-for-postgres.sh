#!/usr/bin/env python
import sys
import os
import time
import psycopg2

# Добавляем путь к проекту
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from database.connection import db_config

def wait_for_postgres():
    """Ждет пока PostgreSQL станет доступен"""
    print("⌛ Ожидание PostgreSQL...")
    
    for i in range(30):  # 30 попыток по 2 секунды = 60 секунд максимум
        try:
            conn = psycopg2.connect(
                host=db_config.host,
                port=db_config.port,
                database=db_config.name,
                user=db_config.user,
                password=db_config.password
            )
            conn.close()
            print("✅ PostgreSQL доступен!")
            return True
        except Exception as e:
            print(f"⏳ Попытка {i+1}/30: PostgreSQL еще не доступен...")
            time.sleep(2)
    
    print("❌ Не удалось подключиться к PostgreSQL")
    return False

if __name__ == "__main__":
    wait_for_postgres()
    