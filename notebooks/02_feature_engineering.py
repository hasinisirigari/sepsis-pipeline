# Databricks notebook source
import boto3
import pandas as pd
import numpy as np
import io
import math

AWS_ACCESS_KEY = "AWS_ACCESS_KEY_ID"
AWS_SECRET_KEY = "AWS_SECRET_ACCESS_KEY"
BUCKET = "sepsis-early-warning-hasini"

s3 = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name='us-east-1'
)

paginator = s3.get_paginator('list_objects_v2')
print("S3 connected")

# COMMAND ----------

#Load icustays: reference for which patients to compute features for
obj = s3.get_object(Bucket=BUCKET, Key='bronze/icustays/icustays.parquet')
icustays = pd.read_parquet(io.BytesIO(obj['Body'].read()))
icustays['intime'] = pd.to_datetime(icustays['intime'])
icustays['outtime'] = pd.to_datetime(icustays['outtime'])

print(f"ICU stays: {len(icustays):,}")
print(f"Median LOS: {icustays['los'].median():.1f} days")

# COMMAND ----------

# MAGIC %md
# MAGIC     # Compute clinical features for one ICU stay across all 15-min windows.
# MAGIC     # For each window, we look back at recent vitals:
# MAGIC     # - Current: most recent value within the window
# MAGIC     # - 1h rolling: mean of values in the past 60 minutes
# MAGIC     # - 4h rolling: mean of values in the past 240 minutes
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Strategy: for each Silver vitals file, find the stays in it,
# MAGIC compute features, write to S3

# COMMAND ----------

# Fix: filter out stays with missing timestamps
icustays_clean = icustays.dropna(subset=['intime', 'outtime'])
print(f"Stays before: {len(icustays):,}")
print(f"Stays after dropping null times: {len(icustays_clean):,}")
print(f"Dropped: {len(icustays) - len(icustays_clean)}")

stay_lookup = icustays_clean.set_index('stay_id')

# COMMAND ----------

# MAGIC %md
# MAGIC Fast feature computation — no resampling.
# MAGIC For each 15-min window, find the most recent value 
# MAGIC and compute rolling means directly from raw measurements.
# MAGIC     

# COMMAND ----------

# MAGIC %md
# MAGIC Cap feature computation at 7 days per stay
# MAGIC Rationale: sepsis typically develops within the first few days
# MAGIC A 121-day stay doesn't need 11,646 windows, most are irrelevant
# MAGIC

# COMMAND ----------

# Re-initialize vitals_keys fresh
vitals_keys = []
for page in paginator.paginate(Bucket=BUCKET, Prefix='silver/vitals/'):
    for obj_meta in page.get('Contents', []):
        if obj_meta['Key'].endswith('.parquet'):
            vitals_keys.append(obj_meta['Key'])

print(f"Vitals files: {len(vitals_keys)}")
print(f"First key: {vitals_keys[0]}")

# Also verify stay_lookup is correct
print(f"Stay lookup size: {len(stay_lookup)}")
print(f"Stay lookup index dtype: {stay_lookup.index.dtype}")

# Test one stay manually through the exact loop logic
obj = s3.get_object(Bucket=BUCKET, Key=vitals_keys[0])
test_batch = pd.read_parquet(io.BytesIO(obj['Body'].read()))
test_batch['charttime'] = pd.to_datetime(test_batch['charttime'], format='mixed')

test_sids = test_batch['stay_id'].unique()
print(f"Stays in first file: {len(test_sids)}")

# Try the first 3 stays
for sid in test_sids[:3]:
    if sid not in stay_lookup.index:
        print(f"  {sid}: NOT FOUND")
        continue
    si = stay_lookup.loc[sid]
    sv = test_batch[test_batch['stay_id'] == sid]
    feat = compute_patient_features_capped(sid, si, sv)
    print(f"  {sid}: {len(feat)} windows")

# COMMAND ----------

import time

def compute_features_vectorized(stay_id, stay_info, stay_vitals):
    intime = stay_info['intime']
    outtime = stay_info['outtime']
    
    max_out = intime + pd.Timedelta(days=7)
    if outtime > max_out:
        outtime = max_out
    
    if pd.isna(intime) or pd.isna(outtime) or outtime <= intime:
        return pd.DataFrame()
    
    sv = stay_vitals.copy()
    sv['charttime'] = pd.to_datetime(sv['charttime'], format='mixed')
    
    pivoted = sv.pivot_table(
        index='charttime', columns='vital_name',
        values='valuenum', aggfunc='mean'
    ).sort_index()
    
    if len(pivoted) == 0:
        return pd.DataFrame()
    
    vitals_cols = pivoted.columns.tolist()
    
    windows = pd.date_range(start=intime, end=outtime, freq='15min')
    if len(windows) == 0:
        return pd.DataFrame()
    
    window_df = pd.DataFrame({'window_end': windows})
    pivoted_reset = pivoted.reset_index().rename(columns={'charttime': 'window_end'})
    
    current = pd.merge_asof(window_df, pivoted_reset, on='window_end', direction='backward')
    rename_map = {col: f'{col}_current' for col in vitals_cols}
    current = current.rename(columns=rename_map)
    
    pivoted_1h = pivoted.rolling('1h', min_periods=1).mean()
    pivoted_4h = pivoted.rolling('4h', min_periods=1).mean()
    
    p1_reset = pivoted_1h.reset_index().rename(columns={'charttime': 'window_end'})
    p4_reset = pivoted_4h.reset_index().rename(columns={'charttime': 'window_end'})
    
    rolling_1h = pd.merge_asof(window_df, p1_reset, on='window_end', direction='backward')
    rolling_4h = pd.merge_asof(window_df, p4_reset, on='window_end', direction='backward')
    
    rename_1h = {col: f'{col}_1h_mean' for col in vitals_cols}
    rename_4h = {col: f'{col}_4h_mean' for col in vitals_cols}
    rolling_1h = rolling_1h.rename(columns=rename_1h)
    rolling_4h = rolling_4h.rename(columns=rename_4h)
    
    result = current.copy()
    for col in vitals_cols:
        result[f'{col}_1h_mean'] = rolling_1h[f'{col}_1h_mean']
        result[f'{col}_4h_mean'] = rolling_4h[f'{col}_4h_mean']
    
    sbp_col = 'sbp_arterial_current' if 'sbp_arterial_current' in result.columns else None
    if sbp_col is None:
        sbp_col = 'sbp_noninvasive_current' if 'sbp_noninvasive_current' in result.columns else None
    
    dbp_col = 'dbp_arterial_current' if 'dbp_arterial_current' in result.columns else None
    if dbp_col is None:
        dbp_col = 'dbp_noninvasive_current' if 'dbp_noninvasive_current' in result.columns else None
    
    map_col = 'map_arterial_current' if 'map_arterial_current' in result.columns else None
    if map_col is None:
        map_col = 'map_noninvasive_current' if 'map_noninvasive_current' in result.columns else None
    
    if map_col:
        result['map_current'] = result[map_col]
    elif sbp_col and dbp_col:
        result['map_current'] = (result[sbp_col] + 2 * result[dbp_col]) / 3
    
    hr_col = 'heart_rate_current' if 'heart_rate_current' in result.columns else None
    if hr_col and sbp_col:
        result['shock_index'] = result[hr_col] / result[sbp_col].replace(0, np.nan)
    
    result['stay_id'] = stay_id
    result['subject_id'] = stay_info['subject_id']
    result['hadm_id'] = stay_info['hadm_id']
    
    return result

print("Vectorized feature function defined")

# COMMAND ----------

# List Silver vitals files
vitals_keys = []
for page in paginator.paginate(Bucket=BUCKET, Prefix='silver/vitals/'):
    for obj_meta in page.get('Contents', []):
        if obj_meta['Key'].endswith('.parquet'):
            vitals_keys.append(obj_meta['Key'])
print(f"Silver vitals files: {len(vitals_keys)}")

# Run feature engineering
total_windows = 0
total_stays = 0
total_time = 0

for file_idx, key in enumerate(vitals_keys):
    batch_start = time.time()
    
    obj = s3.get_object(Bucket=BUCKET, Key=key)
    vitals_batch = pd.read_parquet(io.BytesIO(obj['Body'].read()))
    
    stay_ids = vitals_batch['stay_id'].unique()
    
    batch_features = []
    for sid in stay_ids:
        if sid not in stay_lookup.index:
            continue
        stay_info = stay_lookup.loc[sid]
        stay_vitals = vitals_batch[vitals_batch['stay_id'] == sid]
        features = compute_features_vectorized(sid, stay_info, stay_vitals)
        if len(features) > 0:
            batch_features.append(features)
    
    if not batch_features:
        continue
    
    batch_df = pd.concat(batch_features, ignore_index=True)
    
    s3_key = f"silver/features/part_{file_idx:04d}.parquet"
    buffer = io.BytesIO()
    batch_df.to_parquet(buffer, index=False, compression='snappy')
    buffer.seek(0)
    s3.put_object(Bucket=BUCKET, Key=s3_key, Body=buffer.getvalue())
    
    batch_time = time.time() - batch_start
    total_windows += len(batch_df)
    total_stays += len(stay_ids)
    total_time += batch_time
    
    avg_per_file = total_time / (file_idx + 1)
    remaining = avg_per_file * (len(vitals_keys) - file_idx - 1)
    
    print(f"File {file_idx}/{len(vitals_keys)}: {len(stay_ids)} stays -> {len(batch_df):,} windows "
          f"({batch_time:.0f}s, est {remaining/60:.0f}min remaining)")

print(f"\nFEATURE ENGINEERING SUMMARY")
print(f"Total stays: {total_stays:,}")
print(f"Total windows: {total_windows:,}")
print(f"Total time: {total_time/60:.1f} minutes")

# COMMAND ----------

# Verify feature files
feat_keys = []
for page in paginator.paginate(Bucket=BUCKET, Prefix='silver/features/'):
    for obj_meta in page.get('Contents', []):
        if obj_meta['Key'].endswith('.parquet'):
            feat_keys.append(obj_meta['Key'])

print(f"Feature files: {len(feat_keys)}")

# Check one file to confirm schema
obj = s3.get_object(Bucket=BUCKET, Key=feat_keys[0])
sample = pd.read_parquet(io.BytesIO(obj['Body'].read()))
print(f"Sample shape: {sample.shape}")
print(f"Columns: {sample.columns.tolist()}")
print(f"\nNull rates:")
print(sample.isnull().mean().round(3).to_string())