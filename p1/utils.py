import os
import boto3
import pandas as pd
from pyspark.sql.types import StructType, StructField, StringType
from botocore.exceptions import ClientError


def read_reference(path="./data/Ref.xlsx") -> pd.DataFrame:
    """Read reference data using pandas"""
    full_path = os.path.join(os.getcwd(), path)
    df_ref = pd.read_excel(full_path, header=0, sheet_name=0)
    print(f"Reference DataFrame: {df_ref.head()}")
    return df_ref


def infer_schema_from_csv_header(folder_path):
    """Use pandas to read only the header from the first CSV in the folder."""
    files = [f for f in os.listdir(folder_path) if f.endswith(".csv")]
    if not files:
        raise ValueError(f"No CSV files found in {folder_path}")
    first_file = os.path.join(folder_path, files[0])
    df = pd.read_csv(first_file, nrows=0)
    fields = [StructField(col, StringType(), True) for col in df.columns]
    return StructType(fields)


def create_s3_bucket():
    """Create a bucket in MinIO if it doesn't exist."""
    MINIO_ENDPOINT = os.getenv("MINIO_URL", "http://127.0.0.1:9000")
    ACCESS_KEY = os.getenv("MINIO_USERNAME", "superuser")
    SECRET_KEY = os.getenv("MINIO_PASSWORD", "superuser")
    BUCKET_NAME = os.getenv("MINIO_BUCKET", "reports-bucket")

    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
    )

    try:
        s3.create_bucket(Bucket=BUCKET_NAME)
        print(f"Bucket '{BUCKET_NAME}' created successfully.")
    except ClientError as e:
        if e.response["Error"]["Code"] == "BucketAlreadyOwnedByYou":
            print(f"Bucket '{BUCKET_NAME}' already exists.")
        else:
            raise
