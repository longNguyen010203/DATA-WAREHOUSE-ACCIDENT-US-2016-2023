from __future__ import annotations

import logging
from pyspark import SparkContext
from pyspark.sql import SparkSession
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)



BUCKET_NAME="lakehouse"
FILE_PATH="./data/US_Accidents_March23.csv"
S3_KEY="bronze/US_Accidents_March23.csv"

default_args = {
    'owner': 'longdata',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'email': ['longdata.010203@gmail.com']
}


@dag(
    dag_id="ingestion_process_to_snowflake_v111",
    default_args=default_args,
    start_date=datetime(2024, 11, 15),
    schedule_interval="0 23 * * Mon,Wed,Fri",
    tags=["Filesystem", "ETL", "Data Engineer", "Snowflake"],
    catchup=False
)
def etl_ingestion_and_process_to_snowflake() -> None:
    
    #---------------#
    # Create Bucket #
    #---------------#
    @task
    def create_bucket(**context) -> None:
        s3_hook = S3Hook(aws_conn_id="s3_connection_id")
        if not s3_hook.check_for_bucket(BUCKET_NAME):
            s3_hook.create_bucket(BUCKET_NAME)
        else: logger.info(f"Bucket {BUCKET_NAME} already exists.")
        
    
    #---------------------------------------------#
    # Upload file from local filesystem to bucket #
    #---------------------------------------------#
    upload_file_to_bucket = LocalFilesystemToS3Operator(
        task_id="upload_file_to_bucket",
        filename=FILE_PATH,
        dest_key=S3_KEY,
        dest_bucket=BUCKET_NAME,
        aws_conn_id="s3_connection_id",
        replace=True
    )
    
    
    #-----------------------------------#
    # Batch Processing data using Spark #
    #-----------------------------------#
    @task.pyspark(
        conn_id="spark_connection_id",
        config_kwargs={
            "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
            "spark.hadoop.fs.s3a.access.key": "minio",
            "spark.hadoop.fs.s3a.secret.key": "minio123",
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.connection.ssl.enabled": "false",
            "spark.jars.packages": "org.apache.hadoop:hadoop-aws:3.3.6",
            "spark.jars.packages": "com.amazonaws:aws-java-sdk-bundle:1.12.779",
            "spark.jars.packages": "com.amazonaws:aws-java-sdk:1.12.779",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem"
        }
    )
    def batch_process_accident_data(spark: SparkSession, sc: SparkContext) -> None:
        df = spark.read.csv("s3a://lakehouse/bronze/US_Accidents_March23.csv", 
            header=True, inferSchema=True)
        
        logger.info(f"columns number is {len(df.columns)}, records number is {df.count()}")

        
    
    
    create_bucket() >> upload_file_to_bucket >> batch_process_accident_data()
    
etl_ingestion_and_process_to_snowflake()