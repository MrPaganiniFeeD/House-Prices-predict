import pandas as pd
from typing import List, Dict, Any
from sqlalchemy import text
from services.csv_service import CSVService
from model.training_data import TrainingData

from database.connection import db_manager
import numpy as np
import json
from model.training_data import TestData

def safe_json_serialize(obj):
    """Безопасно сериализует объект в JSON"""
    if obj is None:
        return None
    elif isinstance(obj, (int, float)) and np.isnan(obj):
        return None
    elif isinstance(obj, (pd.Timestamp, pd.DatetimeIndex)):
        return obj.isoformat()
    elif hasattr(obj, 'dtype') and np.isscalar(obj) and np.isnan(obj):
        return None
    else:
        return obj

def load_test_data_from_csv(csv_path: str, feature_columns: list):
    """Загружает тестовые данные из CSV в таблицу test_data"""
    
    # Читаем и очищаем данные
    data = pd.read_csv(csv_path)
    data_cleaned = data.replace({np.nan: None})
    data_cleaned = data_cleaned.replace({pd.NaT: None})
    
    with db_manager.session_scope() as session:
        successful_records = 0
        
        for index, row in data_cleaned.iterrows():
            try:
                # Безопасно создаем словарь фичей
                features = {}
                for col in feature_columns:
                    raw_value = row[col]
                    safe_value = safe_json_serialize(raw_value)
                    features[col] = safe_value
                
                # Сохраняем оригинальный Id
                original_id = safe_json_serialize(row['Id']) if 'Id' in row else None
                
                # Проверяем, что features можно сериализовать в JSON
                try:
                    json.dumps(features)
                except (TypeError, ValueError) as e:
                    print(f"Пропущена запись {index}: ошибка сериализации JSON - {e}")
                    continue
                
                test_record = TestData(
                    features=features,
                    original_id=original_id
                )
                session.add(test_record)
                successful_records += 1
                
                # Прогресс для больших файлов
                if (successful_records + 1) % 100 == 0:
                    print(f"Обработано {successful_records + 1} тестовых записей...")
                    
            except Exception as e:
                print(f"Ошибка при обработке тестовой записи {index}: {e}")
                continue
        
        return successful_records

class TrainingDataService:
    def __init__(self):
        self.csv_service = CSVService()

    def load_training_data_from_csv(
        self, 
        csv_path: str,
        feature_columns: List[str],
        target_column: str,
        description_column: str = None
    ) -> int:
        """
        Загружает CSV с данными для обучения в таблицу training_data

        Args:
            csv_path: путь к CSV файлу
            feature_columns: список колонок с фичами
            target_column: колонка с целевой переменной
            description_column: опциональная колонка с описанием
        """

        def process_chunk(chunk: pd.DataFrame) -> List[Dict[str, Any]]:
            """Обрабатывает чанк данных для вставки в training_data"""
            records = []

            for _, row in chunk.iterrows():
                # Формируем features как JSON
                features = {col: row[col] for col in feature_columns}

                record = {
                    "features": features,
                    "target": int(row[target_column])
                }

                # Добавляем описание если есть
                if description_column and description_column in row:
                    record["description"] = str(row[description_column])

                records.append(record)

            return records

        total_rows = 0
        print('db_manager', db_manager)
        # Читаем CSV чанками
        for chunk in pd.read_csv(csv_path, chunksize=5000):
            records = process_chunk(chunk)

            # Вставляем записи в БД
            with db_manager.session_scope() as session:
                session.bulk_insert_mappings(TrainingData, records)
                total_rows += len(records)

        return total_rows

    def load_raw_csv_to_new_table(
        self,
        csv_path: str,
        table_name: str,
        dtype: Dict = None
    ) -> int:
        """
        Загружает CSV как есть в новую таблицу (для сырых данных)
        """
        if dtype is None:
            dtype = {}

        return self.csv_service.load_csv_to_table(
            csv_path=csv_path,
            table_name=table_name,
            dtype=dtype,
            encoding='utf-8'
        )

# Глобальный экземпляр
training_data_service = TrainingDataService()