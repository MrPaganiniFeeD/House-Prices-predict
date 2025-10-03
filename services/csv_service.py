from typing import Dict, List, Optional
from sqlalchemy import Table, MetaData
from database.connection import db_manager
import pandas as pd
import logging

logger = logging.getLogger(__name__)


class CSVService:
    def __init__(self):
        self.db_manager = db_manager

    def load_csv_to_table(
        self, 
        csv_path: str, 
        table_name: str,
        chunk_size: int = 10000,
        dtype: Optional[Dict] = None,
        **read_csv_kwargs
    ) -> int:

        total_rows = 0

        try:
            # Чтение CSV файла чанками
            for chunk_number, chunk in enumerate(
                pd.read_csv(csv_path, chunksize=chunk_size, dtype=dtype, **read_csv_kwargs)
            ):
                rows_loaded = self._insert_chunk(chunk, table_name)
                total_rows += rows_loaded

                logger.info(f"Чанк {chunk_number + 1}: загружено {rows_loaded} строк")

            logger.info(f"Всего загружено {total_rows} строк из {csv_path}")
            return total_rows

        except Exception as e:
            logger.error(f"Ошибка при загрузке CSV: {e}")
            raise

    def _insert_chunk(self, chunk: pd.DataFrame, table_name: str) -> int:
        with self.db_manager.session_scope() as session:
            # Используем SQLAlchemy core для эффективной вставки
            chunk.to_sql(
                table_name,
                session.connection(),
                if_exists='append',
                index=False,
                method='multi'
            )
            return len(chunk)

    def create_table_from_csv(
        self,
        csv_path: str,
        table_name: str,
        sample_size: int = 1000,
        **read_csv_kwargs
    ) -> None:

        sample_df = pd.read_csv(csv_path, nrows=sample_size, **read_csv_kwargs)

        with self.db_manager.engine.begin() as conn:
            sample_df.to_sql(
                table_name,
                conn,
                if_exists='fail',
                index=False
            )

        logger.info(f"Создана таблица {table_name} на основе CSV структуры")

csv_service = CSVService()