import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import RandomForestRegressor
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
import yaml
import os
import joblib

def fit_model():
    with open('paramsproj.yaml', 'r') as fd:
        params = yaml.safe_load(fd)
    
    data = pd.read_csv('data/initial_dataproj.csv')

    X = data.drop(columns=['price'])
    y = data[params['target_col']]

    num_features = ['kitchen_area','total_area', 'ceiling_height','latitude', 'longitude', 'floor','flats_count','floors_total']
    num_transformer = StandardScaler()

    preprocessor = ColumnTransformer(
        transformers=[
            ('num', num_transformer, num_features),
        ],
        remainder=params['remainder']
    )
	
    model = Pipeline(steps=[
        ('preprocessor', preprocessor),
        ('regressor', RandomForestRegressor(n_estimators=params['n_estimators'], max_depth=params['max_depth'], random_state=params['random_state'], n_jobs=params['n_jobs']))
    ])
	
    model.fit(X, y)

    os.makedirs('models', exist_ok=True)
    with open('models/fitted_flats_model.pkl', 'wb') as fd:
        joblib.dump(model, fd) 

if __name__ == '__main__':
	fit_model()