import os
import sys
import glob
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from utils import read_reference, infer_schema_from_csv_header, create_s3_bucket
from delta import configure_spark_with_delta_pip
from dotenv import load_dotenv

load_dotenv()
BUCKET_NAME = os.getenv("MINIO_BUCKET", "reports-bucket")
MINIO_ENDPOINT = os.getenv("MINIO_URL", "http://minio:9000")
MINIO_USERNAME = os.getenv("MINIO_USERNAME")
MINIO_PASSWORD = os.getenv("MINIO_PASSWORD")
MINIO_SECURE = os.getenv("MINIO_SECURE").lower() == "true"


def create_spark_session(app_name="StructuredStreamingSample"):
    """Create SparkSession with MinIO, Delta Lake, and S3A support."""

    # hadoop_jars = [
    #     "/root/.ivy2/jars/org.apache.hadoop_hadoop-aws-3.3.4.jar",
    #     "/root/.ivy2/jars/aws-java-sdk-bundle-1.12.353.jar",
    # ]

    jars = sorted(glob.glob("/opt/spark/jars/*.jar"))
    if not jars:
        raise RuntimeError(
            "No jars found in /opt/spark/jars. Did you download them into the image?"
        )
    jars_str = ",".join(jars)

    builder = (
        SparkSession.builder.appName("DeltaMinIOExample")
        .master("local[*]")
        # .config("spark.jars", jars_str)
        .config("spark.jars", jars_str)
        # .config("spark.driver.extraClassPath", jars_str)
        # .config("spark.executor.extraClassPath", jars_str)
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.access.key", MINIO_USERNAME)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_PASSWORD)
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.ssl.enabled", MINIO_SECURE)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_csv_stream(spark, folder_path, schema):
    """Read CSV as a streaming DataFrame using an explicit schema."""
    if not os.path.isdir(folder_path):
        raise ValueError(
            f"{folder_path} must be a directory containing CSV files for streaming."
        )
    df_stream = (
        spark.readStream.schema(schema).option("header", "true").csv(folder_path)
    )
    return df_stream


def map_paytype(df_agg: "DataFrame", reference_df: "DataFrame") -> "DataFrame":
    """
    Maps payment types using a reference dataframe.
    Args:
        df_agg: Input DataFrame containing payment type column
        reference_df: Reference DataFrame with mapping values
    Returns:
        DataFrame with mapped payment types
    """
    mapping = dict(zip(reference_df["PayType"], reference_df["value"]))
    payment_column = "Pay_type"

    expr = F.when(
        F.col(payment_column) == list(mapping.keys())[0], list(mapping.values())[0]
    )
    for k, v in list(mapping.items())[1:]:
        expr = expr.when(F.col(payment_column) == k, v)
    expr = expr.otherwise(F.col(payment_column))

    return df_agg.withColumnRenamed("PAYTYPE_515", payment_column).withColumn(
        payment_column, expr
    )


def calculate_total_debit(df_stream, debit_col: str = "DEBIT_AMOUNT_42"):
    df_grouped = (
        df_stream.withColumn("RECORD_DAY", F.to_date("RECORD_DATE"))
        .groupBy("RECORD_DAY")
        .agg((F.sum(debit_col) / 10000).alias("total_debit"))
    )

    query = (
        df_grouped.writeStream.outputMode("complete")
        .format("delta")
        .option("path", f"s3a://{BUCKET_NAME}/total_debit")
        .option("checkpointLocation", f"s3a://{BUCKET_NAME}/checkpoints/total_debit")
        .trigger(once=True)
        .start()
    )
    query.awaitTermination()


def aggregate_debit_stream(
    df_stream: pd.DataFrame,
    window_duration: str = "15 minutes",
    payment_col: str = "PAYTYPE_515",
    debit_col: str = "DEBIT_AMOUNT_42",
    ref_df: pd.DataFrame = None,
) -> pd.DataFrame:
    """
    Aggregates debit amount over time windows and creates two separate output streams.
    """
    df_agg = (
        df_stream.withColumn("RECORD_DATE", F.window("RECORD_DATE", window_duration))
        .groupBy("RECORD_DATE", payment_col)
        .agg(
            (F.sum(debit_col) / 10000).alias("revenue"),
            F.count("*").alias("Record_Count"),
        )
    )

    df_mapped = map_paytype(df_agg, ref_df)
    revenue_only = df_mapped.select("RECORD_DATE", "Pay_type", "revenue")
    full_data = df_mapped

    queries = []

    queries.append(
        revenue_only.writeStream.outputMode("complete")
        .format("delta")
        .option("path", f"s3a://{BUCKET_NAME}/revenue")
        .option("checkpointLocation", f"s3a://{BUCKET_NAME}/checkpoints/revenue")
        .option("truncate", "false")
        .start()
    )

    queries.append(
        full_data.writeStream.outputMode("complete")
        .format("delta")
        .option("path", f"s3a://{BUCKET_NAME}/full")
        .option("checkpointLocation", f"s3a://{BUCKET_NAME}/checkpoints/full")
        .option("truncate", "false")
        .start()
    )

    for query in queries:
        query.awaitTermination(30)


def main(csv_path):
    ref_df = read_reference()
    spark = create_spark_session()
    create_s3_bucket()

    schema = infer_schema_from_csv_header(csv_path)
    df_stream = read_csv_stream(spark, csv_path, schema)
    df_stream = df_stream.withColumn(
        "RECORD_DATE", F.to_timestamp("RECORD_DATE", "yyyy/MM/dd HH:mm:ss")
    )
    calculate_total_debit(df_stream)
    aggregate_debit_stream(df_stream, window_duration="15 minutes", ref_df=ref_df)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python main.py /path/to/csv/folder")
        sys.exit(1)
    main(sys.argv[1])
