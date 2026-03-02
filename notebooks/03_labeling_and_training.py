# Databricks notebook source
import boto3
import pandas as pd
import numpy as np
import io
import time

AWS_ACCESS_KEY = "AWS_ACCESS_KEY_ID"
AWS_SECRET_KEY = "AWS_SECRET_ACCESS_KEY"
BUCKET = "sepsis-early-warning-hasini"

s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY,
                   aws_secret_access_key=AWS_SECRET_KEY, region_name='us-east-1')
paginator = s3.get_paginator('list_objects_v2')
print("S3 connected")

# COMMAND ----------

obj = s3.get_object(Bucket=BUCKET, Key='silver/medications/antibiotics.parquet')
abx = pd.read_parquet(io.BytesIO(obj['Body'].read()))
abx['starttime'] = pd.to_datetime(abx['starttime'])
abx_first = abx.groupby('hadm_id')['starttime'].min().reset_index()
abx_first.columns = ['hadm_id', 'abx_time']
del abx

obj = s3.get_object(Bucket=BUCKET, Key='silver/medications/blood_cultures.parquet')
blood = pd.read_parquet(io.BytesIO(obj['Body'].read()))
blood['charttime'] = pd.to_datetime(blood['charttime'])
blood_first = blood.groupby('hadm_id')['charttime'].min().reset_index()
blood_first.columns = ['hadm_id', 'culture_time']
del blood

suspected = abx_first.merge(blood_first, on='hadm_id', how='inner')
suspected['time_gap_hours'] = abs((suspected['abx_time'] - suspected['culture_time']).dt.total_seconds() / 3600)
suspected = suspected[suspected['time_gap_hours'] <= 72].copy()
suspected['sepsis_onset'] = suspected[['abx_time', 'culture_time']].min(axis=1)
del abx_first, blood_first

obj = s3.get_object(Bucket=BUCKET, Key='bronze/icustays/icustays.parquet')
icustays = pd.read_parquet(io.BytesIO(obj['Body'].read()))
icustays['intime'] = pd.to_datetime(icustays['intime'])
icustays['outtime'] = pd.to_datetime(icustays['outtime'])
icu_intime = icustays.set_index('stay_id')['intime'].to_dict()

sepsis_stays = icustays.merge(suspected[['hadm_id', 'sepsis_onset']], on='hadm_id', how='inner')
sepsis_stays = sepsis_stays[
    (sepsis_stays['sepsis_onset'] >= sepsis_stays['intime']) &
    (sepsis_stays['sepsis_onset'] <= sepsis_stays['outtime'])
]
sepsis_lookup = sepsis_stays.set_index('stay_id')['sepsis_onset'].to_dict()
del suspected, sepsis_stays, icustays

obj = s3.get_object(Bucket=BUCKET, Key='silver/medications/vasopressors.parquet')
vaso = pd.read_parquet(io.BytesIO(obj['Body'].read()))
vaso['starttime'] = pd.to_datetime(vaso['starttime'])
vaso['endtime'] = pd.to_datetime(vaso['endtime'])
vaso_simple = vaso[['stay_id', 'starttime', 'endtime']].dropna(subset=['starttime', 'endtime'])
del vaso

feat_keys = []
for page in paginator.paginate(Bucket=BUCKET, Prefix='silver/features/'):
    for obj_meta in page.get('Contents', []):
        if obj_meta['Key'].endswith('.parquet'):
            feat_keys.append(obj_meta['Key'])

lab_keys = []
for page in paginator.paginate(Bucket=BUCKET, Prefix='silver/labs/'):
    for obj_meta in page.get('Contents', []):
        if obj_meta['Key'].endswith('.parquet'):
            lab_keys.append(obj_meta['Key'])

PREDICTION_HORIZON_HOURS = 6
print(f"Sepsis stays: {len(sepsis_lookup):,}, Feature files: {len(feat_keys)}, Lab files: {len(lab_keys)}")

# COMMAND ----------

# Process each feature file: add labs, vasopressors, labels, sample, write to S3
for i in range(len(feat_keys)):
    # Load feature windows
    obj = s3.get_object(Bucket=BUCKET, Key=feat_keys[i])
    df = pd.read_parquet(io.BytesIO(obj['Body'].read()))
    df['window_end'] = pd.to_datetime(df['window_end'])
    df['hadm_id'] = df['hadm_id'].astype(float)
    df = df.sort_values('window_end')
    
    # Load matching labs (one file at a time)
    lab_idx = min(i, len(lab_keys) - 1)
    obj = s3.get_object(Bucket=BUCKET, Key=lab_keys[lab_idx])
    labs = pd.read_parquet(io.BytesIO(obj['Body'].read()))
    labs['charttime'] = pd.to_datetime(labs['charttime'])
    labs_wide = labs.pivot_table(
        index=['hadm_id', 'charttime'], columns='lab_name',
        values='valuenum', aggfunc='mean'
    ).reset_index().sort_values('charttime')
    del labs
    
    # Join labs
    hadm_labs = labs_wide[labs_wide['hadm_id'].isin(df['hadm_id'].unique())]
    if len(hadm_labs) > 0:
        df = pd.merge_asof(
            df, hadm_labs.rename(columns={'charttime': 'window_end'}),
            on='window_end', by='hadm_id', direction='backward'
        )
    del labs_wide, hadm_labs
    
    # Vasopressor flag
    df['on_vasopressor'] = 0
    stay_vaso = vaso_simple[vaso_simple['stay_id'].isin(df['stay_id'].unique())]
    if len(stay_vaso) > 0:
        vw = df[['stay_id', 'window_end']].merge(stay_vaso, on='stay_id')
        mask = (vw['window_end'] >= vw['starttime']) & (vw['window_end'] <= vw['endtime'])
        hits = vw.loc[mask, ['stay_id', 'window_end']].drop_duplicates()
        df = df.merge(hits.assign(vf=1), on=['stay_id', 'window_end'], how='left')
        df['on_vasopressor'] = df['vf'].fillna(0).astype(int)
        df = df.drop(columns=['vf'])
        del vw, hits
    
    # Hours in ICU
    df['hours_in_icu'] = df['stay_id'].map(icu_intime)
    df['hours_in_icu'] = (df['window_end'] - df['hours_in_icu']).dt.total_seconds() / 3600
    df['hours_in_icu'] = df['hours_in_icu'].fillna(0)
    
    # Labels
    df['sepsis_onset'] = df['stay_id'].map(sepsis_lookup)
    df['hours_to_onset'] = (df['sepsis_onset'] - df['window_end']).dt.total_seconds() / 3600
    df['label'] = ((df['hours_to_onset'] >= 0) & (df['hours_to_onset'] <= PREDICTION_HORIZON_HOURS)).astype(int)
    df = df.drop(columns=['sepsis_onset', 'hours_to_onset'])
    
    # Sample: all positives + 3x negatives
    pos = df[df['label'] == 1]
    neg = df[df['label'] == 0].sample(n=min(len(pos) * 3, (df['label']==0).sum()), random_state=42)
    sampled = pd.concat([pos, neg], ignore_index=True)
    
    buffer = io.BytesIO()
    sampled.to_parquet(buffer, index=False, compression='snappy')
    buffer.seek(0)
    s3.put_object(Bucket=BUCKET, Key=f'gold/training_data/part_{i:04d}.parquet', Body=buffer.getvalue())
    
    print(f"File {i}/{len(feat_keys)}: {len(pos)} pos, {len(sampled)} total, vaso={sampled['on_vasopressor'].mean()*100:.1f}%")
    del df, sampled, pos, neg

print("\nGold training data written to S3")