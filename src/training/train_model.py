"""
Train LightGBM sepsis prediction model using Gold training data from S3.
Saves model, metrics, and SHAP importance locally.
"""

import json
import pickle
import io
import os

import boto3
import numpy as np
import pandas as pd
import lightgbm as lgb
import shap
from sklearn.metrics import roc_auc_score, average_precision_score, classification_report

from src.utils.config import config
from src.utils.s3_utils import get_s3_client

def load_training_data():
    s3 = get_s3_client()
    bucket = config.aws.s3_bucket
    paginator = s3.get_paginator('list_objects_v2')

    keys = []
    for page in paginator.paginate(Bucket=bucket, Prefix='gold/training_data/'):
        for obj in page.get('Contents', []):
            if obj['Key'].endswith('.parquet'):
                keys.append(obj['Key'])

    parts = []
    for key in keys:
        obj = s3.get_object(Bucket=bucket, Key=key)
        parts.append(pd.read_parquet(io.BytesIO(obj['Body'].read())))
    
    df = pd.concat(parts, ignore_index=True)
    print(f"Loaded {len(df):,} rows from {len(keys)} files")
    return df


def train():
    df = load_training_data()

    id_cols = ['window_end', 'stay_id', 'subject_id', 'hadm_id', 'label']
    feature_cols = [c for c in df.columns if c not in id_cols]

    # Patient-level split
    unique_stays = df['stay_id'].unique()
    np.random.seed(42)
    np.random.shuffle(unique_stays)
    split_idx = int(len(unique_stays) * 0.8)
    train_stays = set(unique_stays[:split_idx])

    X_train = df[df['stay_id'].isin(train_stays)][feature_cols]
    y_train = df[df['stay_id'].isin(train_stays)]['label']
    X_test = df[~df['stay_id'].isin(train_stays)][feature_cols]
    y_test = df[~df['stay_id'].isin(train_stays)]['label']
    del df

    print(f"Features: {len(feature_cols)}")
    print(f"Train: {len(X_train):,} rows, {y_train.sum():,} positive ({y_train.mean()*100:.1f}%)")
    print(f"Test:  {len(X_test):,} rows, {y_test.sum():,} positive ({y_test.mean()*100:.1f}%)")

    scale_weight = (y_train == 0).sum() / (y_train == 1).sum()

    model = lgb.LGBMClassifier(
        n_estimators=1000,
        learning_rate=0.05,
        max_depth=6,
        num_leaves=31,
        scale_pos_weight=scale_weight,
        min_child_samples=50,
        subsample=0.8,
        colsample_bytree=0.8,
        random_state=42,
        n_jobs=-1,
        verbose=-1,
    )

    print("\nTraining...")
    model.fit(
        X_train, y_train,
        eval_set=[(X_test, y_test)],
        callbacks=[lgb.log_evaluation(100), lgb.early_stopping(50)],
    )
    print(f"Best iteration: {model.best_iteration_}")

    # Evaluate
    y_proba = model.predict_proba(X_test)[:, 1]
    auroc = roc_auc_score(y_test, y_proba)
    auprc = average_precision_score(y_test, y_proba)

    print(f"\nAUROC: {auroc:.4f}")
    print(f"AUPRC: {auprc:.4f}")

    for thresh in [0.3, 0.4, 0.5, 0.6]:
        y_pred = (y_proba >= thresh).astype(int)
        recall = y_pred[y_test == 1].mean()
        precision = y_test[y_pred == 1].mean() if y_pred.sum() > 0 else 0
        print(f"Threshold {thresh}: precision={precision:.3f}, recall={recall:.3f}")

    report = classification_report(y_test, (y_proba >= 0.5).astype(int),
                                   target_names=['No Sepsis', 'Sepsis'])
    print(f"\n{report}")

    # SHAP
    print("Computing SHAP values...")
    explainer = shap.TreeExplainer(model)
    shap_values = explainer.shap_values(X_test.iloc[:2000])
    if isinstance(shap_values, list):
        shap_vals = shap_values[1]
    else:
        shap_vals = shap_values

    importance = pd.DataFrame({
        'feature': feature_cols,
        'mean_abs_shap': np.abs(shap_vals).mean(axis=0)
    }).sort_values('mean_abs_shap', ascending=False)

    print("\nTop 15 features:")
    for _, row in importance.head(15).iterrows():
        print(f"  {row['feature']}: {row['mean_abs_shap']:.4f}")

    # Save outputs
    os.makedirs('models', exist_ok=True)

    with open('models/lgbm_sepsis_model.pkl', 'wb') as f:
        pickle.dump(model, f)

    metrics = {
        'auroc': round(auroc, 4),
        'auprc': round(auprc, 4),
        'best_iteration': model.best_iteration_,
        'n_features': len(feature_cols),
        'train_rows': len(X_train),
        'test_rows': len(X_test),
        'features': feature_cols,
    }
    with open('models/metrics.json', 'w') as f:
        json.dump(metrics, f, indent=2)

    importance.to_csv('models/shap_importance.csv', index=False)

    print("\nSaved: models/lgbm_sepsis_model.pkl")
    print("Saved: models/metrics.json")
    print("Saved: models/shap_importance.csv")


if __name__ == '__main__':
    train()