from __future__ import annotations

import os
import logging
import numpy as np
import pandas as pd
from typing import Any
import pyspark.sql.functions as F
from pyspark import SparkContext
from pyspark.sql import SparkSession
from datetime import datetime, timedelta
from snowflake.snowpark.session import Session

from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)



BUCKET_NAME="lakehouse"
FILE_PATH="/opt/airflow/data/{}/{}"
S3_KEY="{}/{}"

WAREHOUSE_NAME="ACCIDENT_US_DB"
SCHEMA_NAME="STAGING"
SNOWFLAKE_CONN_ID="snowflake_default"


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
    dag_id="ingestion_process_to_snowflake_v1234",
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
    @task()
    def create_bucket(**context) -> None:
        s3_hook = S3Hook(aws_conn_id="s3_connection_id")
        if not s3_hook.check_for_bucket(BUCKET_NAME):
            s3_hook.create_bucket(BUCKET_NAME)
            logger.info(f"Create Bucket {BUCKET_NAME} completed")
        else: logger.info(f"Bucket {BUCKET_NAME} already exists.")
        
    
    #---------------------------------------------#
    # Upload file from local filesystem to bucket #
    #---------------------------------------------#
    upload_accident_file_to_bucket = LocalFilesystemToS3Operator(
        task_id="upload_accident_file_to_bucket",
        filename=FILE_PATH.format("raw", "US_Accidents_March23.csv"),
        dest_key=S3_KEY.format("bronze", "US_Accidents_March23.csv"),
        dest_bucket=BUCKET_NAME,
        aws_conn_id="s3_connection_id",
        replace=True
    )
    
    
    upload_vehicle_file_to_bucket = LocalFilesystemToS3Operator(
        task_id="upload_vehicle_file_to_bucket",
        filename=FILE_PATH.format("raw", "Vehicle_Information.csv"),
        dest_key=S3_KEY.format("bronze", "Vehicle_Information.csv"),
        dest_bucket=BUCKET_NAME,
        aws_conn_id="s3_connection_id",
        replace=True
    )
    
    
    #-----------------------------#
    # Processing data using Spark #
    #-----------------------------#
    @task.pyspark(
        conn_id="spark_connection_id",
        config_kwargs={
            "spark.jars": "/usr/local/spark/jars/aws-java-sdk-bundle-1.12.779.jar, /usr/local/spark/jars/hadoop-aws-3.4.1.jar, /usr/local/spark/jars/s3-2.29.26.jar",
            "spark.driver.memory": "4g",
            "spark.executor.memory": "4g",
            "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
            "spark.hadoop.fs.s3a.access.key": "minio",
            "spark.hadoop.fs.s3a.secret.key": "minio123",
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.connection.ssl.enabled": "false",
            "spark.jars.packages": "org.apache.hadoop:hadoop-aws:3.4.1",
            "spark.jars.packages": "com.amazonaws:aws-java-sdk-bundle:1.12.779",
            "spark.jars.packages": "software.amazon.awssdk:auth:2.29.26",
            "spark.jars.packages": "com.amazonaws:aws-java-sdk-core:1.12.779",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem"
        }
    )
    def transforming_accident_data(spark: SparkSession, sc: SparkContext) -> None:
        try:
            df = spark.read.csv(FILE_PATH.format("raw", "US_Accidents_March23.csv"), header=True, inferSchema=True)
            logger.info(f"columns number is {len(df.columns)}, records number is {df.count()}")
            
            df = df.na.drop(subset=[
                "Street",
                "City", 
                "Zipcode", 
                "Timezone", 
                "Airport_Code", 
                "Weather_Timestamp",
                "Sunrise_Sunset",
                "Civil_Twilight", 
                "Nautical_Twilight", 
                "Astronomical_Twilight", 
            ])
            
            # Fill NA for columns
            df = df.fillna({
                "Temperature(F)": df.select(F.median(F.when(F.col("Temperature(F)").isNull(), 1).otherwise(0))).collect()[0][0],
                "Wind_Chill(F)": df.select(F.median(F.when(F.col("Wind_Chill(F)").isNull(), 1).otherwise(0))).collect()[0][0],
                "Humidity(%)": df.select(F.avg(F.when(F.col("Humidity(%)").isNull(), 1).otherwise(0))).collect()[0][0],
                "Pressure(in)": df.select(F.avg(F.when(F.col("Pressure(in)").isNull(), 1).otherwise(0))).collect()[0][0],
                "Visibility(mi)": df.select(F.avg(F.when(F.col("Visibility(mi)").isNull(), 1).otherwise(0))).collect()[0][0],
                "Wind_Direction": df.select(F.mode(F.when(F.col("Wind_Direction").isNull(), 1).otherwise(0))).collect()[0][0],
                "Wind_Speed(mph)": df.select(F.avg(F.when(F.col("Wind_Speed(mph)").isNull(), 1).otherwise(0))).collect()[0][0],
                "Precipitation(in)": df.select(F.avg(F.when(F.col("Precipitation(in)").isNull(), 1).otherwise(0))).collect()[0][0],
                "Weather_Condition": df.select(F.mode(F.when(F.col("Weather_Condition").isNull(), 1).otherwise(0))).collect()[0][0],
            })
            logger.info(f"columns number is {len(df.columns)}, records number is {df.count()}")
            
            pandas_df = df.toPandas()
            pandas_df.to_csv(FILE_PATH.format("cleaned", "US_Accidents_March23_Cleaned.csv"), header=True)
            # df.coalesce(1).write.mode('overwrite').csv(FILE_PATH.format("cleaned", "US_Accidents_March23_Cleaned.csv"))
            
        except Exception as e:
            logger.error(f"Error processing the file: {str(e)}")
        
        
        
        
    @task.pyspark(
        conn_id="spark_connection_id",
        config_kwargs={
            "spark.driver.memory": "4g",
            "spark.executor.memory": "4g",
        }
    )
    def transforming_vehicle_data(spark: SparkSession, sc: SparkContext) -> None:
        try:
            df = spark.read.csv(FILE_PATH.format("raw", "Vehicle_Information.csv"), header=True, inferSchema=True)
            logger.info(f"columns number is {len(df.columns)}, records number is {df.count()}")
            
            df = df.withColumnRenamed("Engine_Capacity_.CC.", "Engine_Capacity_CC")
            df = df.withColumnRenamed("Vehicle_Location.Restricted_Lane", "Vehicle_Location_Restricted_Lane")
            df = df.fillna({"model": "Unknown"})
            df = df.dropDuplicates()
            logger.info(f"columns number is {len(df.columns)}, records number is {df.count()}")
            
            pandas_df = df.toPandas()
            pandas_df.to_csv(FILE_PATH.format("cleaned", "Vehicle_Information_Cleaned.csv"), header=True)
            # df.coalesce(1).write.mode('overwrite').csv(FILE_PATH.format("cleaned", "Vehicle_Information_Cleaned.csv"))
            
        except Exception as e:
            logger.error(f"Error processing the file: {str(e)}")
            
            
    #-------------------------------#
    # Upload data cleaned to bucket #
    #-------------------------------#
    upload_accident_cleaned_file_to_bucket = LocalFilesystemToS3Operator(
        task_id="upload_accident_cleaned_file_to_bucket",
        filename=FILE_PATH.format("cleaned", "US_Accidents_March23_Cleaned.csv"),
        dest_key=S3_KEY.format("silver", "US_Accidents_March23_Cleaned.csv"),
        dest_bucket=BUCKET_NAME,
        aws_conn_id="s3_connection_id",
        replace=True
    )
    
    
    upload_vehicle_cleaned_file_to_bucket = LocalFilesystemToS3Operator(
        task_id="upload_vehicle_cleaned_file_to_bucket",
        filename=FILE_PATH.format("cleaned", "Vehicle_Information_Cleaned.csv"),
        dest_key=S3_KEY.format("silver", "Vehicle_Information_Cleaned.csv"),
        dest_bucket=BUCKET_NAME,
        aws_conn_id="s3_connection_id",
        replace=True
    )
        
        
    #----------------------------#
    # Create schema at Snowflake #
    #----------------------------#
    create_schema_snowflake = SnowflakeOperator(
        task_id="create_schema_snowflake",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=f"""
            CREATE SCHEMA IF NOT EXISTS {WAREHOUSE_NAME}.{SCHEMA_NAME};
        """
    )
    
    
    #------------------------#
    # Load data to Snowflake #
    #------------------------#
    @task.snowpark
    def load_accident_data_to_snowflake(session: Session) -> None:
        pandas_df = pd.read_csv(FILE_PATH.format("cleaned", "US_Accidents_March23_Cleaned.csv"), chunksize=500000)
        for i, chunk in enumerate(pandas_df, start=1):
            logger.info(f"Processing chunk {i} with {len(chunk)} rows")
            df_session = session.create_dataframe(data=chunk, schema=list(chunk.columns))
            df_session.write.save_as_table(table_name="STAGING_ACCIDENT", mode="append")
            logger.info(f"Loaded chunk {i} into Snowflake")

        
    @task.snowpark
    def load_vehicle_data_to_snowflake(session: Session) -> None:
        pandas_df = pd.read_csv(FILE_PATH.format("cleaned", "Vehicle_Information_Cleaned.csv"), chunksize=500000, encoding='latin-1')
        for i, chunk in enumerate(pandas_df, start=1):
            logger.info(f"Processing chunk {i} with {len(chunk)} rows")
            df_session = session.create_dataframe(data=chunk, schema=list(chunk.columns))
            df_session.write.save_as_table(table_name="STAGING_VEHICLE", mode="append")
            logger.info(f"Loaded chunk {i} into Snowflake")



    create_bucket_task = create_bucket()
    load_accident_task = load_accident_data_to_snowflake()
    load_vehicle_task = load_vehicle_data_to_snowflake()
    transforming_accident_task = transforming_accident_data()
    transforming_vehicle_task = transforming_vehicle_data()
    
    
    create_bucket_task >> upload_accident_file_to_bucket >> transforming_accident_task
    create_bucket_task >> upload_vehicle_file_to_bucket >> transforming_vehicle_task
    transforming_accident_task >> upload_accident_cleaned_file_to_bucket
    transforming_vehicle_task >> upload_vehicle_cleaned_file_to_bucket
    transforming_accident_task >> create_schema_snowflake >> load_accident_task
    transforming_vehicle_task >> create_schema_snowflake >> load_vehicle_task
    
    
etl_ingestion_and_process_to_snowflake()