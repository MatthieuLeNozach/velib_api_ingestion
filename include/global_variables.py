# --------------- #
# PACKAGE IMPORTS #
# --------------- #

from airflow import Dataset
from pendulum import duration

from minio import Minio
from datetime import timedelta



# ----------------------- #
# Configuration variables #
# ----------------------- #
MY_NAME = "Friend"
# MinIO connection config
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_IP = "host.docker.internal:9000"
ARCHIVE_BUCKET_NAME = "archive"




# DAG default arguments
default_args = {
    "owner": MY_NAME,
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": duration(minutes=5),
    "execution_timeout": timedelta(minutes=180)  # Set execution timeout to 180 minutes
}



# utility functions
def get_minio_client():
    client = Minio(MINIO_IP, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, secure=False)
    return client