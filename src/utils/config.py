# Centralized Configuration Management

import os
from dataclasses import dataclass, field
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()


@dataclass
class AWSConfig:
    region: str = field(default_factory=lambda: os.getenv("AWS_DEFAULT_REGION", "us-east-1"))
    s3_bucket: str = field(default_factory=lambda: os.getenv("S3_BUCKET_NAME", "sepsis-pipeline-dev"))

    @property
    def bronze_prefix(self) -> str:
        return "bronze"

    @property
    def silver_prefix(self) -> str:
        return "silver"

    @property
    def model_prefix(self) -> str:
        return "models"


@dataclass
class PostgresConfig:
    host: str = field(default_factory=lambda: os.getenv("POSTGRES_HOST", "localhost"))
    port: int = field(default_factory=lambda: int(os.getenv("POSTGRES_PORT", "5432")))
    database: str = field(default_factory=lambda: os.getenv("POSTGRES_DB", "sepsis_gold"))
    user: str = field(default_factory=lambda: os.getenv("POSTGRES_USER", "sepsis_admin"))
    password: str = field(default_factory=lambda: os.getenv("POSTGRES_PASSWORD", ""))

    @property
    def connection_string(self) -> str:
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"


@dataclass
class KafkaConfig:
    bootstrap_servers: str = field(
        default_factory=lambda: os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    )
    vitals_topic: str = "patient.vitals"
    labs_topic: str = "patient.labs"
    medications_topic: str = "patient.medications"

    @property
    def all_topics(self) -> list:
        return [self.vitals_topic, self.labs_topic, self.medications_topic]


@dataclass
class ClinicalConfig:
    """
    Clinical parameters — domain-specific decisions.
    Separated so a clinician could review these values
    without understanding Python.
    """
    # Sepsis risk alert thresholds
    alert_yellow: float = 0.3
    alert_orange: float = 0.6
    alert_red: float = 0.8

    # Prediction horizon
    prediction_horizon_hours: int = 6

    # Feature window sizes
    rolling_window_1h_minutes: int = 60
    rolling_window_4h_minutes: int = 240
    scoring_interval_minutes: int = 15

    # Replay speed (288x = 24 hours in 5 minutes)
    replay_speed_factor: int = field(
        default_factory=lambda: int(os.getenv("REPLAY_SPEED_FACTOR", "288"))
    )

    # Vital sign valid ranges (for data quality checks)
    hr_range: tuple = (20, 300)
    map_range: tuple = (20, 200)
    spo2_range: tuple = (50, 100)
    temp_range: tuple = (25, 45)
    resp_range: tuple = (4, 60)

    # Lab valid ranges
    lactate_range: tuple = (0.1, 30)
    creatinine_range: tuple = (0.1, 20)
    platelet_range: tuple = (1, 2000)
    bilirubin_range: tuple = (0.1, 50)
    wbc_range: tuple = (0.1, 500)


@dataclass
class PipelineConfig:
    """Master configuration combining all sub-configs."""
    aws: AWSConfig = field(default_factory=AWSConfig)
    postgres: PostgresConfig = field(default_factory=PostgresConfig)
    kafka: KafkaConfig = field(default_factory=KafkaConfig)
    clinical: ClinicalConfig = field(default_factory=ClinicalConfig)
    project_root: Path = field(default_factory=lambda: Path(__file__).parent.parent.parent)


config = PipelineConfig()