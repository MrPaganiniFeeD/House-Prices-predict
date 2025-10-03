import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import RandomForestClassifier
import joblib
import json
import catboost as cb
from sklearn.metrics import mean_squared_error


data = pd.read_csv("data/train_exported.csv")

X = data.drop('SalePrice', axis=1)
y = data['SalePrice']
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

cat_model = cb.CatBoostRegressor(verbose=0, random_state=42)
cat_model.fit(X_train, y_train)
cat_preds = cat_model.predict(X_test)
cat_rmse = np.sqrt(mean_squared_error(y_test, cat_preds))
print("🐱 CatBoost RMSE:", cat_rmse)
