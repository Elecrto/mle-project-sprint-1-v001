import pandas as pd
import joblib
import yaml
import os
import json
from sklearn.model_selection import KFold, cross_validate

def evaluate_model():
    with open('params.yaml', 'r') as fd:
        params = yaml.safe_load(fd)

    data = pd.read_csv("data/initial_dataproj.csv")
    
    pipeline = joblib.load("models/fitted_flats_model.pkl")

    cv_strategy = KFold(n_splits=5, shuffle=True)

    cv_res = cross_validate(
        pipeline,
        data.drop(columns=['price']),
        data[params['target_col']],
        cv=cv_strategy,
        n_jobs=params['n_jobs'],
        scoring=params['scoring']
    )
    
    averaged_results = {metric: round(values.mean(), 3) for metric, values in cv_res.items() if "test_" in metric}

    os.makedirs('cv_results', exist_ok=True)
    with open("cv_results/cv_res_flats.json", "w") as f:
        json.dump(averaged_results, f, indent=2)

if __name__ == '__main__':
    evaluate_model()
