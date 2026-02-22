#S3 Utility Functions


import io
import json
from typing import Optional

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.exceptions import ClientError

from src.utils.config import config
from src.utils.logging_config import get_logger

log = get_logger("s3_utils")


def get_s3_client():
    return boto3.client("s3", region_name=config.aws.region)


def upload_parquet(
    df: pd.DataFrame,
    s3_key: str,
    bucket: Optional[str] = None,
) -> str:
    """
    Upload a pandas DataFrame to S3 as Parquet.
    
    Args:
        df: DataFrame to upload
        s3_key: S3 object key (e.g., "bronze/chartevents/part-0.parquet")
        bucket: S3 bucket name (defaults to config)
    
    Returns:
        Full S3 URI (s3://bucket/key)
    """
    bucket = bucket or config.aws.s3_bucket
    s3 = get_s3_client()

    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, engine="pyarrow", compression="snappy")
    buffer.seek(0)

    s3.upload_fileobj(buffer, bucket, s3_key)
    uri = f"s3://{bucket}/{s3_key}"

    log.info("Uploaded parquet to S3", s3_uri=uri, rows=len(df), columns=list(df.columns))
    return uri


def read_parquet(s3_key: str, bucket: Optional[str] = None) -> pd.DataFrame:
    #Read a Parquet file from S3 into a pandas DataFrame.
    bucket = bucket or config.aws.s3_bucket
    s3 = get_s3_client()

    buffer = io.BytesIO()
    s3.download_fileobj(bucket, s3_key, buffer)
    buffer.seek(0)

    df = pd.read_parquet(buffer)
    log.info("Read parquet from S3", s3_key=s3_key, rows=len(df))
    return df


def upload_json(data: dict, s3_key: str, bucket: Optional[str] = None) -> str:
    #Upload a JSON object to S3 (for model metadata, configs, etc.).
    bucket = bucket or config.aws.s3_bucket
    s3 = get_s3_client()

    body = json.dumps(data, indent=2, default=str)
    s3.put_object(Bucket=bucket, Key=s3_key, Body=body, ContentType="application/json")

    uri = f"s3://{bucket}/{s3_key}"
    log.info("Uploaded JSON to S3", s3_uri=uri)
    return uri


def upload_bytes(data: bytes, s3_key: str, bucket: Optional[str] = None) -> str:
    #Upload raw bytes to S3 (for model artifacts like .pkl files).
    bucket = bucket or config.aws.s3_bucket
    s3 = get_s3_client()

    s3.put_object(Bucket=bucket, Key=s3_key, Body=data)

    uri = f"s3://{bucket}/{s3_key}"
    log.info("Uploaded bytes to S3", s3_uri=uri, size_bytes=len(data))
    return uri


def list_objects(prefix: str, bucket: Optional[str] = None) -> list:
    """
    List all object keys under a prefix.
    
    S3 doesn't have real folders, it's a flat key-value store.
    "Folders" are just shared key prefixes. This function lists
    all keys that start with the given prefix.
    """
    bucket = bucket or config.aws.s3_bucket
    s3 = get_s3_client()

    keys = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])

    log.info("Listed S3 objects", prefix=prefix, count=len(keys))
    return keys


def check_bucket_exists(bucket: Optional[str] = None) -> bool:
    #Check if the S3 bucket exists and is accessible.
    bucket = bucket or config.aws.s3_bucket
    s3 = get_s3_client()

    try:
        s3.head_bucket(Bucket=bucket)
        return True
    except ClientError:
        return False


def create_bucket(bucket: Optional[str] = None) -> None:
    #Create the S3 bucket if it doesn't exist.
    bucket = bucket or config.aws.s3_bucket
    s3 = get_s3_client()

    try:
        if config.aws.region == "us-east-1":
            s3.create_bucket(Bucket=bucket)
        else:
            s3.create_bucket(
                Bucket=bucket,
                CreateBucketConfiguration={"LocationConstraint": config.aws.region},
            )
        log.info("Created S3 bucket", bucket=bucket)
    except ClientError as e:
        if e.response["Error"]["Code"] == "BucketAlreadyOwnedByYou":
            log.info("Bucket already exists", bucket=bucket)
        else:
            raise
