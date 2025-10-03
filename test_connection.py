import psycopg2
from config.database_conf import db_config

try:
    # Пробуем подключиться
    conn = psycopg2.connect(
        host=db_config.host,
        port=db_config.port,
        database=db_config.name,
        user=db_config.user,
        password=db_config.password
    )
    
    print("✅ Подключение успешно!")
    
    # Проверим список баз данных
    cursor = conn.cursor()
    cursor.execute("SELECT datname FROM pg_database;")
    databases = cursor.fetchall()
    
    print("\n📊 Существующие базы данных:")
    for db in databases:
        print(f"  - {db[0]}")
    
    cursor.close()
    conn.close()
    
except psycopg2.OperationalError as e:
    print(f"❌ Ошибка подключения: {e}")
    print("\nВозможные причины:")
    print("1. PostgreSQL не запущен")
    print("2. Неправильные параметры подключения")
    print("3. База данных не существует")
    print("4. Неверный пароль")
    
except ImportError:
    print("❌ Установите psycopg2: pip install psycopg2-binary")