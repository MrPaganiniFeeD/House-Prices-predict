from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from config.database_conf import db_config
from model.training_data import Base
from contextlib import contextmanager



class DatabaseManager:
    def __init__(self):
        print("db_cofing", db_config)
        self.engine = create_engine(db_config.url)
        self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
    
    def create_tables(self):
        """Создает все таблицы в базе данных"""
        Base.metadata.create_all(bind=self.engine)

    @contextmanager
    def session_scope(self):
        """Контекстный менеджер для сессий"""
        session = self.SessionLocal()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def get_session(self):
        """Возвращает новую сессию для работы с БД"""
        return self.SessionLocal()
    
    def close_connection(self):
        """Закрывает соединение с БД"""
        self.engine.dispose()

# Глобальный экземпляр менеджера БД
db_manager = DatabaseManager()
print(db_manager, "Connection")
