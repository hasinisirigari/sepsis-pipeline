# Test the Lambda handler locally

from src.serving.lambda_handler import lambda_handler
import json

# Simulate a patient with concerning vitals
test_event = {
    'body': {
        'stay_id': 12345,
        'heart_rate_current': 112,
        'sbp_noninvasive_current': 85,
        'map_noninvasive_current': 58,
        'resp_rate_current': 28,
        'spo2_current': 91,
        'temperature_current': 38.9,
        'heart_rate_1h_mean': 105,
        'heart_rate_4h_mean': 95,
        'resp_rate_1h_mean': 24,
        'resp_rate_4h_mean': 20,
        'sbp_noninvasive_1h_mean': 88,
        'sbp_noninvasive_4h_mean': 100,
        'spo2_1h_mean': 93,
        'spo2_4h_mean': 96,
        'temperature_1h_mean': 38.5,
        'temperature_4h_mean': 37.8,
        'map_current': 58,
        'shock_index': 1.32,
        'lactate': 4.2,
        'creatinine': 2.1,
        'platelets': 95,
        'wbc': 18.5,
        'bilirubin': 2.8,
        'on_vasopressor': 1,
        'hours_in_icu': 14.5,
    }
}

response = lambda_handler(test_event, None)
result = json.loads(response['body'])
print(json.dumps(result, indent=2))