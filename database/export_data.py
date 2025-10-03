import sys
import os
import pandas as pd
import json
from sqlalchemy import create_engine, text

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from database.connection import db_config

def export_training_data_smart(output_path='data/train_exported.csv'):
    """Умный экспорт - автоматически извлекает все фичи из JSON"""
    
    print("Умный экспорт тренировочных данных...")
    
    engine = create_engine(db_config.url)
    
    # Простой запрос - получаем все данные
    with engine.connect() as conn:
        # Получаем одну запись чтобы узнать структуру JSON
        sample_result = conn.execute(text("SELECT features FROM training_data LIMIT 1"))
        sample_row = sample_result.fetchone()
        
        if sample_row:
            # Парсим JSON чтобы узнать все ключи (фичи)
            sample_features = json.loads(sample_row[0]) if isinstance(sample_row[0], str) else sample_row[0]
            feature_keys = list(sample_features.keys())
            print(f"📊 Обнаружено {len(feature_keys)} фичей")
            print(f"📋 Первые 10 фич: {feature_keys[:10]}")
        
        # Получаем все данные
        query = "SELECT id, target, features FROM training_data"
        df = pd.read_sql(query, conn)
    
    # Автоматически разворачиваем JSON в колонки
    features_df = pd.json_normalize(df['features'].apply(lambda x: json.loads(x) if isinstance(x, str) else x))
    
    # Объединяем с target
    result_df = pd.concat([df[['id', 'target']], features_df], axis=1)
    result_df = result_df.rename(columns={'target': 'SalePrice'})
    
    # Сохраняем
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    result_df.to_csv(output_path, index=False)
    
    print(f"✅ Данные экспортированы в {output_path}")
    print(f"📊 Записей: {len(result_df)}")
    print(f"📈 Колонок: {len(result_df.columns)}")
    
    return result_df

def export_test_data_smart(output_path='data/test_exported.csv'):
    """Умный экспорт тестовых данных"""
    
    print("Умный экспорт тестовых данных...")
    
    engine = create_engine(db_config.url)
    
    # Получаем все данные
    query = "SELECT id, original_id, features FROM test_data"
    df = pd.read_sql(query, engine)
    
    # Автоматически разворачиваем JSON
    features_df = pd.json_normalize(df['features'].apply(lambda x: json.loads(x) if isinstance(x, str) else x))
    
    # Объединяем
    result_df = pd.concat([df[['id', 'original_id']], features_df], axis=1)
    result_df = result_df.rename(columns={'original_id': 'Id'})
    
    # Сохраняем
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    result_df.to_csv(output_path, index=False)
    
    print(f"✅ Тестовые данные экспортированы в {output_path}")
    print(f"📊 Записей: {len(result_df)}")
    
    return result_df

if __name__ == "__main__":
    train_df = export_training_data_smart()
    test_df = export_test_data_smart()
    
    print(f"\nТренировочные данные: {train_df.shape}")
    print(f"Тестовые данные: {test_df.shape}")