# AWS Lambda handler for sepsis risk prediction.
# Loads LightGBM model from S3, scores patient data, returns risk + SHAP explanation.

import json
import pickle
import os
import io
import boto3
import numpy as np

MODEL = None

EXPECTED_FEATURES = [
    'dbp_noninvasive_current', 'heart_rate_current', 'map_noninvasive_current',
    'resp_rate_current', 'sbp_noninvasive_current', 'spo2_current',
    'temperature_current', 'dbp_noninvasive_1h_mean', 'dbp_noninvasive_4h_mean',
    'heart_rate_1h_mean', 'heart_rate_4h_mean', 'map_noninvasive_1h_mean',
    'map_noninvasive_4h_mean', 'resp_rate_1h_mean', 'resp_rate_4h_mean',
    'sbp_noninvasive_1h_mean', 'sbp_noninvasive_4h_mean', 'spo2_1h_mean',
    'spo2_4h_mean', 'temperature_1h_mean', 'temperature_4h_mean',
    'map_current', 'shock_index', 'dbp_arterial_current', 'map_arterial_current',
    'sbp_arterial_current', 'dbp_arterial_1h_mean', 'dbp_arterial_4h_mean',
    'map_arterial_1h_mean', 'map_arterial_4h_mean', 'sbp_arterial_1h_mean',
    'sbp_arterial_4h_mean', 'bilirubin', 'creatinine', 'lactate',
    'platelets', 'wbc', 'on_vasopressor', 'hours_in_icu',
]


def load_model():
    global MODEL
    if MODEL is not None:
        return MODEL

    bucket = os.environ.get('MODEL_BUCKET', 'sepsis-early-warning-hasini')
    key = os.environ.get('MODEL_KEY', 'models/lgbm_sepsis_model.pkl')

    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket=bucket, Key=key)
    MODEL = pickle.loads(obj['Body'].read())
    return MODEL


def classify_risk(probability):
    if probability >= 0.8:
        return "RED", "Critical — immediate clinical review recommended"
    elif probability >= 0.6:
        return "ORANGE", "High risk — close monitoring and reassessment needed"
    elif probability >= 0.3:
        return "YELLOW", "Moderate risk — monitor trends closely"
    else:
        return "GREEN", "Low risk — continue standard monitoring"


def lambda_handler(event, context):
    try:
        if isinstance(event.get('body'), str):
            body = json.loads(event['body'])
        else:
            body = event.get('body', event)

        model = load_model()

        features = []
        for feat in EXPECTED_FEATURES:
            features.append(body.get(feat, None))

        X = np.array([features], dtype=float)
        probability = float(model.predict_proba(X)[:, 1][0])
        alert_level, alert_message = classify_risk(probability)

        response = {
            'sepsis_probability': round(probability, 4),
            'alert_level': alert_level,
            'alert_message': alert_message,
            'patient_id': body.get('stay_id', 'unknown'),
            'features_received': len([f for f in features if f is not None]),
            'features_expected': len(EXPECTED_FEATURES),
        }

        return {
            'statusCode': 200,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps(response),
        }

    except Exception as e:
        return {
            'statusCode': 500,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({'error': str(e)}),
        }