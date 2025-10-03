import sys
import os
import argparse
from pathlib import Path
import pandas as pd
import numpy as np
from typing import List, Iterable

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from services.csv_service import csv_service
from services.training_data_service import training_data_service
from services.training_data_service import load_test_data_from_csv
from database.connection import db_manager
from database.connection import Base 
from model.training_data import TestData

def create_tables():
    """Создает все таблицы в базе данных"""
    print("Создание таблиц в базе данных...")
    Base.metadata.create_all(db_manager.engine)
    print("Таблицы успешно созданы!")

def create_test_tables():
    """Создает таблицы для тестовых данных"""
    print("Создание таблиц для тестовых данных...")
    Base.metadata.create_all(db_manager.engine, tables=[TestData.__table__])
    print("Таблицы для тестовых данных успешно созданы!")

def load_as_test_data(csv_path: str):
    """Загружает CSV в таблицу test_data"""
    print(f"Загрузка {csv_path} как test_data...")
    
    data_test = pd.read_csv(csv_path)
    data_test = data_test.replace({np.nan: None})
    
    create_test_tables()
    
    feature_columns = data_test.columns.tolist()

    count = load_test_data_from_csv(
        csv_path=csv_path,
        feature_columns=feature_columns
    )
    
    print(f"Успешно загружено {count} тестовых записей")

def load_as_training_data(csv_path: str):
    """Загружает CSV в таблицу training_data"""
    print(f"Загрузка {csv_path} как training_data...")
    data_train = pd.read_csv(csv_path)
    data_train = data_train.replace({np.nan: None})
    data_train = data_train.replace({pd.NaT: None})

    numeric_columns = data_train.select_dtypes(include=[np.number]).columns
    for col in numeric_columns:
        data_train[col] = data_train[col].apply(lambda x: None if pd.isna(x) else x)
    
    # Для строковых колонок
    string_columns = data_train.select_dtypes(include=['object']).columns
    for col in string_columns:
        data_train[col] = data_train[col].apply(lambda x: None if pd.isna(x) else x)
        
    create_tables()
    count = training_data_service.load_training_data_from_csv(
        csv_path=csv_path,
        feature_columns=data_train.drop(columns=["SalePrice"]).columns.tolist(), 
        target_column='SalePrice',                        
        description_column='description'                 
    )
    
    print(f"Успешно загружено {count} записей для обучения")

def main():

    csv_path = r"C:\Users\Егор\Downloads\house-prices-advanced-regression-techniques\train.csv"
    parser = argparse.ArgumentParser(description='Загрузка CSV в PostgreSQL')
    parser.add_argument('csv_path', help='Путь к CSV файлу')
    parser.add_argument('--table-name', help='Имя таблицы для сырой загрузки')
    parser.add_argument('--as-training-data', action='store_true', 
                       help='Загрузить как training_data')
    parser.add_argument('--as-test-data', action='store_true',
                       help='Загрузить как test_data')
    args = parser.parse_args()
    
    # Проверяем существование файла
    if not os.path.exists(args.csv_path):
        print(f"Ошибка: файл {args.csv_path} не найден")
        return
    try:
        if args.as_training_data:
            load_as_training_data(args.csv_path)
        elif args.as_test_data: 
            load_as_test_data(args.csv_path)
        else:
            print("Укажите --table-name или --as-training-data")
            
    except Exception as e:
        print(f"Ошибка при загрузке: {e}")
        raise

if __name__ == "__main__":
    main()

