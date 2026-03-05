# Kafka Producer: Replays MIMIC-IV data as real-time stream.
# Reads from Silver layer, sends to Kafka topics at configurable speed.

import json
import time
import argparse
from datetime import datetime

import pandas as pd
from confluent_kafka import Producer

from src.utils.config import config
from src.utils.logging_config import setup_logging
from src.utils.s3_utils import get_s3_client

log = setup_logging("kafka_producer", json_output=False)


def delivery_report(err, msg):
    if err:
        log.error("Message delivery failed", error=str(err), topic=msg.topic())


def load_silver_vitals_sample(n_stays=100):
    import io

    s3 = get_s3_client()
    bucket = config.aws.s3_bucket

    obj = s3.get_object(Bucket=bucket, Key="silver/vitals/part_0000.parquet")
    df = pd.read_parquet(io.BytesIO(obj['Body'].read()))

    stay_ids = df['stay_id'].unique()[:n_stays]
    df = df[df['stay_id'].isin(stay_ids)].copy()
    df['charttime'] = pd.to_datetime(df['charttime'])
    df = df.sort_values('charttime')

    log.info("Loaded vitals for replay",
             stays=len(stay_ids), rows=len(df))
    return df


def replay_vitals(speed_factor=288, n_stays=100):
    producer = Producer({
        "bootstrap.servers": config.kafka.bootstrap_servers,
        "linger.ms": 50,
        "batch.num.messages": 500,
    })

    df = load_silver_vitals_sample(n_stays=n_stays)

    topic = config.kafka.vitals_topic
    total_sent = 0
    start_time = time.time()
    prev_charttime = None

    for _, row in df.iterrows():
        if prev_charttime is not None:
            real_gap = (row['charttime'] - prev_charttime).total_seconds()
            if real_gap > 0:
                sleep_time = real_gap / speed_factor
                if sleep_time < 2:
                    time.sleep(sleep_time)

        message = {
            "subject_id": int(row['subject_id']),
            "hadm_id": int(row['hadm_id']),
            "stay_id": int(row['stay_id']),
            "charttime": str(row['charttime']),
            "vital_name": row['vital_name'],
            "valuenum": float(row['valuenum']) if pd.notna(row['valuenum']) else None,
            "valueuom": row.get('valueuom', ''),
            "produced_at": datetime.utcnow().isoformat(),
        }

        # Partition by stay_id for per-patient ordering
        producer.produce(
            topic,
            key=str(row['stay_id']).encode('utf-8'),
            value=json.dumps(message).encode('utf-8'),
            callback=delivery_report,
        )

        total_sent += 1

        if total_sent % 1000 == 0:
            producer.poll(0)
            elapsed = time.time() - start_time
            log.info("Producing",
                     messages_sent=total_sent,
                     rate=f"{total_sent/elapsed:.0f} msg/s")

        prev_charttime = row['charttime']

    producer.flush(timeout=30)

    elapsed = time.time() - start_time
    log.info("Replay complete",
             total_messages=total_sent,
             elapsed=f"{elapsed:.1f}s",
             avg_rate=f"{total_sent/elapsed:.0f} msg/s")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--speed", type=int, default=288)
    parser.add_argument("--stays", type=int, default=100)
    args = parser.parse_args()
    replay_vitals(speed_factor=args.speed, n_stays=args.stays)