# Databricks notebook source
print(spark.version)
print("Spark is working!")

# COMMAND ----------

# MAGIC %pip install boto3

# COMMAND ----------

import boto3
import pandas as pd
import io

AWS_ACCESS_KEY = "AWS_ACCESS_KEY_ID"
AWS_SECRET_KEY = "AWS_SECRET_ACCESS_KEY"
BUCKET = "sepsis-early-warning-hasini"

s3 = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name='us-east-1'
)

# Test: list Bronze layer contents
response = s3.list_objects_v2(Bucket=BUCKET, Prefix='bronze/', Delimiter='/')
for prefix in response.get('CommonPrefixes', []):
    print(prefix['Prefix'])

# COMMAND ----------

spark.conf.set("spark.hadoop.fs.s3.access.key", AWS_ACCESS_KEY)
spark.conf.set("spark.hadoop.fs.s3.secret.key", AWS_SECRET_KEY)
spark.conf.set("spark.hadoop.fs.s3.endpoint", "s3.amazonaws.com")

df = spark.read.parquet(f"s3://{BUCKET}/bronze/icustays/")
print(f"ICU stays: {df.count()}")
df.show(5)

# COMMAND ----------

import pyarrow.parquet as pq

def read_bronze_table(table_name, max_files=None):
    prefix = f"bronze/{table_name}/"
    response = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
    keys = [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith('.parquet')]
    
    if max_files:
        keys = keys[:max_files]
    
    dfs = []
    for key in keys:
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        pdf = pd.read_parquet(io.BytesIO(obj['Body'].read()))
        dfs.append(pdf)
    
    combined = pd.concat(dfs, ignore_index=True)
    return spark.createDataFrame(combined)

# Test with icustays (small table)
icu_df = read_bronze_table('icustays')
print(f"ICU stays: {icu_df.count()} rows")
icu_df.show(5)

# COMMAND ----------

# SILVER LAYER: Vitals Transform
from pyspark.sql import functions as F
from pyspark.sql.types import *

# Vital sign itemids that are need
VITAL_IDS = {
    220045: "heart_rate",
    220050: "sbp_arterial",        # Systolic BP (arterial line)
    220051: "dbp_arterial",        # Diastolic BP (arterial line)
    220052: "map_arterial",        # Mean Arterial Pressure (arterial)
    220179: "sbp_noninvasive",     # Systolic BP (cuff)
    220180: "dbp_noninvasive",     # Diastolic BP (cuff)
    220181: "map_noninvasive",     # MAP (cuff)
    220210: "resp_rate",
    220277: "spo2",
    223761: "temp_fahrenheit",
    223762: "temp_celsius",
}

VITAL_ID_LIST = list(VITAL_IDS.keys())

VALID_RANGES = {
    "heart_rate": (20, 300),
    "sbp_arterial": (30, 300),
    "dbp_arterial": (10, 200),
    "map_arterial": (20, 200),
    "sbp_noninvasive": (30, 300),
    "dbp_noninvasive": (10, 200),
    "map_noninvasive": (20, 200),
    "resp_rate": (2, 60),
    "spo2": (50, 100),
    "temp_fahrenheit": (80, 115),
    "temp_celsius": (25, 45),
}

# COMMAND ----------

#Read chartevents in batches, filter to vitals only
#866 files: processing in groups to manage memory

import math
prefix = "bronze/chartevents/"
paginator = s3.get_paginator('list_objects_v2')
all_keys = []
for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
    for obj in page.get('Contents', []):
        if obj['Key'].endswith('.parquet'):
            all_keys.append(obj['Key'])

print(f"Total chartevents files: {len(all_keys)}")

# Processing in batches of 50 files
BATCH_SIZE = 50
num_batches = math.ceil(len(all_keys) / BATCH_SIZE)
print(f"Will process in {num_batches} batches of {BATCH_SIZE} files")

# COMMAND ----------

#Process chartevents in batches: filter vitals, clean, write to Silver
silver_vitals_parts = []

for batch_num in range(num_batches):
    start = batch_num * BATCH_SIZE
    end = min(start + BATCH_SIZE, len(all_keys))
    batch_keys = all_keys[start:end]
    
    #Reading batch into pandas
    dfs = []
    for key in batch_keys:
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        pdf = pd.read_parquet(io.BytesIO(obj['Body'].read()))
        pdf = pdf[pdf['itemid'].isin(VITAL_ID_LIST)]
        dfs.append(pdf)
    
    if not dfs:
        continue
    
    batch_pdf = pd.concat(dfs, ignore_index=True)
    
    #Converting to Spark df for transformation
    batch_df = spark.createDataFrame(batch_pdf)
    silver_vitals_parts.append(batch_df)
    
    if batch_num % 5 == 0:
        print(f"Batch {batch_num}/{num_batches}: {len(batch_pdf)} vital rows from {len(batch_keys)} files")

#Union all batches into one df
silver_vitals_raw = silver_vitals_parts[0]
for part in silver_vitals_parts[1:]:
    silver_vitals_raw = silver_vitals_raw.unionByName(part)

total = silver_vitals_raw.count()
print(f"\nTotal vital sign rows: {total:,}")

# COMMAND ----------

# Process and write each batch directly to S3 as Silver Parquet
# No need to hold everything in memory at once

import pyarrow as pa
import pyarrow.parquet as pq

# Mapping from itemid to vital name
itemid_to_name = {int(k): v for k, v in VITAL_IDS.items()}

def clean_vitals_batch(pdf):
    # Map itemid to readable name
    pdf['vital_name'] = pdf['itemid'].map(itemid_to_name)
    
    # Convert Fahrenheit to Celsius
    f_mask = pdf['vital_name'] == 'temp_fahrenheit'
    pdf.loc[f_mask, 'valuenum'] = (pdf.loc[f_mask, 'valuenum'] - 32) * 5/9
    pdf.loc[f_mask, 'vital_name'] = 'temperature'
    
    # Rename Celsius entries too
    c_mask = pdf['vital_name'] == 'temp_celsius'
    pdf.loc[c_mask, 'vital_name'] = 'temperature'
    
    # Apply valid range filters — drop impossible values
    clean_rows = []
    for vital_name, (low, high) in VALID_RANGES.items():
        # Map temp_fahrenheit and temp_celsius to temperature
        if vital_name in ('temp_fahrenheit', 'temp_celsius'):
            continue
        subset = pdf[pdf['vital_name'] == vital_name]
        subset = subset[(subset['valuenum'] >= low) & (subset['valuenum'] <= high)]
        clean_rows.append(subset)
    
    # Handle temperature separately (already converted to Celsius)
    temp = pdf[pdf['vital_name'] == 'temperature']
    temp = temp[(temp['valuenum'] >= 25) & (temp['valuenum'] <= 45)]
    clean_rows.append(temp)
    
    result = pd.concat(clean_rows, ignore_index=True)
    
    # Keep only columns we need
    result = result[['subject_id', 'hadm_id', 'stay_id', 'charttime', 
                      'vital_name', 'valuenum', 'valueuom']]
    
    return result

# Process all batches and write directly to S3
total_raw = 0
total_clean = 0

for batch_num in range(num_batches):
    start = batch_num * BATCH_SIZE
    end = min(start + BATCH_SIZE, len(all_keys))
    batch_keys = all_keys[start:end]
    
    # Read and filter to vitals
    dfs = []
    for key in batch_keys:
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        pdf = pd.read_parquet(io.BytesIO(obj['Body'].read()))
        pdf = pdf[pdf['itemid'].isin(VITAL_ID_LIST)]
        dfs.append(pdf)
    
    if not dfs:
        continue
    
    batch_pdf = pd.concat(dfs, ignore_index=True)
    raw_count = len(batch_pdf)
    
    # Clean the batch
    clean_pdf = clean_vitals_batch(batch_pdf)
    clean_count = len(clean_pdf)
    
    # Write to S3 Silver layer
    s3_key = f"silver/vitals/part_{batch_num:04d}.parquet"
    buffer = io.BytesIO()
    clean_pdf.to_parquet(buffer, index=False, compression='snappy')
    buffer.seek(0)
    s3.put_object(Bucket=BUCKET, Key=s3_key, Body=buffer.getvalue())
    
    total_raw += raw_count
    total_clean += clean_count
    
    if batch_num % 3 == 0:
        print(f"Batch {batch_num}/{num_batches}: {raw_count} raw -> {clean_count} clean ({clean_count/raw_count*100:.1f}% kept)")

dropped_pct = (1 - total_clean/total_raw) * 100
print(f"\nVITALS SILVER SUMMARY")
print(f"Total raw vital rows:   {total_raw:,}")
print(f"Total clean vital rows: {total_clean:,}")
print(f"Rows dropped:           {total_raw - total_clean:,} ({dropped_pct:.1f}%)")

# COMMAND ----------

#SILVER LAYER: Labs Transform
SOFA_LAB_IDS = {
    50813: "lactate",
    50885: "bilirubin",
    50912: "creatinine",
    51265: "platelets",
    51301: "wbc",
}

SOFA_LAB_ID_LIST = list(SOFA_LAB_IDS.keys())

LAB_VALID_RANGES = {
    "lactate": (0.1, 30),
    "bilirubin": (0.1, 50),
    "creatinine": (0.1, 20),
    "platelets": (1, 2000),
    "wbc": (0.1, 500),
}

# List labevents files
lab_keys = []
for page in paginator.paginate(Bucket=BUCKET, Prefix="bronze/labevents/"):
    for obj in page.get('Contents', []):
        if obj['Key'].endswith('.parquet'):
            lab_keys.append(obj['Key'])

print(f"Total labevents files: {len(lab_keys)}")
num_lab_batches = math.ceil(len(lab_keys) / BATCH_SIZE)
print(f"Will process in {num_lab_batches} batches")

# Processing labs
total_raw = 0
total_clean = 0

for batch_num in range(num_lab_batches):
    start = batch_num * BATCH_SIZE
    end = min(start + BATCH_SIZE, len(lab_keys))
    batch_keys = lab_keys[start:end]
    
    dfs = []
    for key in batch_keys:
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        pdf = pd.read_parquet(io.BytesIO(obj['Body'].read()))
        pdf = pdf[pdf['itemid'].isin(SOFA_LAB_ID_LIST)]
        dfs.append(pdf)
    
    if not dfs:
        continue
    
    batch_pdf = pd.concat(dfs, ignore_index=True)
    raw_count = len(batch_pdf)
    
    # Mapping itemid to lab name
    batch_pdf['lab_name'] = batch_pdf['itemid'].map(SOFA_LAB_IDS)
    
    # Apply valid ranges
    clean_rows = []
    for lab_name, (low, high) in LAB_VALID_RANGES.items():
        subset = batch_pdf[batch_pdf['lab_name'] == lab_name]
        subset = subset[(subset['valuenum'] >= low) & (subset['valuenum'] <= high)]
        clean_rows.append(subset)
    
    clean_pdf = pd.concat(clean_rows, ignore_index=True)
    clean_pdf = clean_pdf[['subject_id', 'hadm_id', 'charttime', 
                            'lab_name', 'valuenum', 'valueuom']]
    clean_count = len(clean_pdf)
    
    # Write to S3
    s3_key = f"silver/labs/part_{batch_num:04d}.parquet"
    buffer = io.BytesIO()
    clean_pdf.to_parquet(buffer, index=False, compression='snappy')
    buffer.seek(0)
    s3.put_object(Bucket=BUCKET, Key=s3_key, Body=buffer.getvalue())
    
    total_raw += raw_count
    total_clean += clean_count
    
    if batch_num % 2 == 0:
        print(f"Batch {batch_num}/{num_lab_batches}: {raw_count} raw -> {clean_count} clean")

dropped_pct = (1 - total_clean/total_raw) * 100 if total_raw > 0 else 0
print(f"\nLABS SILVER SUMMARY")
print(f"Total raw lab rows:   {total_raw:,}")
print(f"Total clean lab rows: {total_clean:,}")
print(f"Rows dropped:         {total_raw - total_clean:,} ({dropped_pct:.1f}%)")

# COMMAND ----------

#SILVER LAYER: Medications (vasopressors from inputevents)
VASOPRESSOR_NAMES = [
    'norepinephrine', 'vasopressin', 'epinephrine',
    'dopamine', 'dobutamine', 'phenylephrine'
]

#Reading d_items to map itemids to drug names
obj = s3.get_object(Bucket=BUCKET, Key='bronze/d_items/d_items.parquet')
d_items = pd.read_parquet(io.BytesIO(obj['Body'].read()))

#Find vasopressor itemids
vaso_items = d_items[d_items['label'].str.lower().str.contains(
    '|'.join(VASOPRESSOR_NAMES), na=False
)]
print("Vasopressor items found:")
print(vaso_items[['itemid', 'label']].to_string(index=False))
vaso_ids = vaso_items['itemid'].tolist()

#Read inputevents
input_keys = []
for page in paginator.paginate(Bucket=BUCKET, Prefix="bronze/inputevents/"):
    for obj_meta in page.get('Contents', []):
        if obj_meta['Key'].endswith('.parquet'):
            input_keys.append(obj_meta['Key'])

print(f"\nInputevents files: {len(input_keys)}")

#Process
vaso_dfs = []
for key in input_keys:
    obj = s3.get_object(Bucket=BUCKET, Key=key)
    pdf = pd.read_parquet(io.BytesIO(obj['Body'].read()))
    pdf = pdf[pdf['itemid'].isin(vaso_ids)]
    vaso_dfs.append(pdf)

vaso_pdf = pd.concat(vaso_dfs, ignore_index=True)
vaso_pdf['drug_name'] = vaso_pdf['itemid'].map(dict(zip(vaso_items['itemid'], vaso_items['label'].str.lower())))

print(f"Total vasopressor rows: {len(vaso_pdf):,}")
print(vaso_pdf['drug_name'].value_counts())

#Write to Silver
buffer = io.BytesIO()
vaso_pdf.to_parquet(buffer, index=False, compression='snappy')
buffer.seek(0)
s3.put_object(Bucket=BUCKET, Key='silver/medications/vasopressors.parquet', Body=buffer.getvalue())
print("\nVasopressors written to Silver layer")

# COMMAND ----------

#SILVER LAYER: Antibiotics (from prescriptions, for Sepsis-3)
ABX_KEYWORDS = [
    'vancomycin', 'piperacillin', 'tazobactam', 'meropenem',
    'cefepime', 'ceftriaxone', 'ciprofloxacin', 'levofloxacin',
    'metronidazole', 'linezolid', 'daptomycin', 'ceftazidime',
    'ampicillin', 'gentamicin', 'tobramycin', 'azithromycin'
]

#Read prescriptions
rx_keys = []
for page in paginator.paginate(Bucket=BUCKET, Prefix="bronze/prescriptions/"):
    for obj_meta in page.get('Contents', []):
        if obj_meta['Key'].endswith('.parquet'):
            rx_keys.append(obj_meta['Key'])

print(f"Prescriptions files: {len(rx_keys)}")

abx_dfs = []
for key in rx_keys:
    obj = s3.get_object(Bucket=BUCKET, Key=key)
    pdf = pd.read_parquet(io.BytesIO(obj['Body'].read()))
    mask = pdf['drug'].str.lower().str.contains('|'.join(ABX_KEYWORDS), na=False)
    abx_dfs.append(pdf[mask])

abx_pdf = pd.concat(abx_dfs, ignore_index=True)

# Normalize drug names
abx_pdf['drug_normalized'] = abx_pdf['drug'].str.lower().str.strip()

print(f"Total antibiotic rows: {len(abx_pdf):,}")
print(f"Unique drugs: {abx_pdf['drug_normalized'].nunique()}")
print(abx_pdf['drug_normalized'].value_counts().head(10))

#Write to Silver
buffer = io.BytesIO()
abx_pdf.to_parquet(buffer, index=False, compression='snappy')
buffer.seek(0)
s3.put_object(Bucket=BUCKET, Key='silver/medications/antibiotics.parquet', Body=buffer.getvalue())
print("\nAntibiotics written to Silver layer")

# COMMAND ----------

#SILVER LAYER: Blood Cultures (for Sepsis-3 suspected infection)
obj = s3.get_object(Bucket=BUCKET, Key='bronze/microbiologyevents/microbiologyevents.parquet')
micro_pdf = pd.read_parquet(io.BytesIO(obj['Body'].read()))

# Filter to blood cultures only
blood_cultures = micro_pdf[micro_pdf['spec_type_desc'] == 'BLOOD CULTURE'].copy()

print(f"Total blood culture rows: {len(blood_cultures):,}")
print(f"Unique patients: {blood_cultures['subject_id'].nunique():,}")

#Write to Silver
buffer = io.BytesIO()
blood_cultures.to_parquet(buffer, index=False, compression='snappy')
buffer.seek(0)
s3.put_object(Bucket=BUCKET, Key='silver/medications/blood_cultures.parquet', Body=buffer.getvalue())
print("Blood cultures written to Silver layer")

# COMMAND ----------

#Verify Silver layer contents
silver_prefixes = ['silver/vitals/', 'silver/labs/', 'silver/medications/']

for prefix in silver_prefixes:
    response = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
    files = [obj['Key'] for obj in response.get('Contents', [])]
    for f in files:
        print(f)