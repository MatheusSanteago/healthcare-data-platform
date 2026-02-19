from io import BytesIO
import hashlib
from datetime import datetime

from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.sdk.definitions.decorators import dag, task_group

from minio.error import S3Error
from minio import Minio
from minio.commonconfig import CopySource


import polars as pl
from polars import DataFrame


conn = BaseHook.get_connection("my_s3_conn")
folder = "healthcare"


class MinioETL:
    def __init__(self, endpoint, access_key, secret_key, secure=False, bucket="datalake"):
        self.client = Minio(
            endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure
        )

        self.bucket = bucket
        self.access_key = access_key
        self.secret_key = secret_key
        self.endpoint = endpoint

    @staticmethod
    def hash_row(row):
        row_str = "|".join([str(v) if v is not None else "" for v in row])
        return hashlib.sha256(row_str.encode("utf-8")).hexdigest()

    @staticmethod
    def add_metadata_columns(df: DataFrame, filename: str) -> DataFrame:
        batch_id = f"{filename}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        df = df.with_columns(
            pl.lit(datetime.now()).alias("ingestion_timestamp"),
            pl.lit(filename).alias("source_file"),
            pl.lit(batch_id).alias("batch_id"),
            pl.struct([c for c in df.columns])
              .map_elements(lambda x: MinioETL.hash_row(list(x.values())))
              .alias("_record_hash")
        )
        return df

    def move_processed_file(self, filename):
        destination_bucket = self.bucket
        destination_object = f"{folder}/processed_data/{filename.replace(f'{folder}/', '')}"

        source_bucket = self.bucket
        source_object = f"{filename}.csv"

        client = self.client

        try:
            client.copy_object(
                bucket_name=destination_bucket,
                object_name=destination_object,
                source=CopySource(source_bucket, source_object)
            )

            print(f"Copied '{source_object}' to '{destination_object}'.")

            client.remove_object(source_bucket, source_object)

            print("Object moved successfully.")

        except S3Error as e:
            print(f"Error moving object: {e}")
            raise e

    def save_data(self, df: DataFrame, filename: str):
        filename_no_data = filename
        filename = f"{filename}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
        path = f"s3://{self.bucket}/healthcare_data_driven/bronze/{filename}"
        print(f"Saving data to {path}...")

        df.write_parquet(
            path,
            partition_by="batch_id",
            compression="snappy",
            storage_options={
                "aws_access_key_id": self.access_key,
                "aws_secret_access_key": self.secret_key,
                "aws_endpoint_url": f"http://{self.endpoint}",
                "aws_region": "us-east-1"
            }
        )

        self.move_processed_file(filename_no_data)

    def extract_data(self, file_name: str) -> None:
        global res
        try:
            objects = self.client.list_objects(
                self.bucket, prefix=f"{folder}/{file_name}", recursive=True)
            files = [
                obj.object_name for obj in objects if obj.object_name.endswith(".csv")]

            if not files:
                print("Nenhum arquivo encontrado.")

            for file in files:
                print(f"Lendo arquivo: {file}")
                filename = file.replace(".csv", "")

                res = self.client.get_object(self.bucket, file)
                data = res.read()
                df = pl.read_csv(BytesIO(data))
                df_final = self.add_metadata_columns(df, filename)
                self.save_data(df_final, filename)

        except S3Error as e:
            if "NoSuchKey" in str(e):
                print("Arquivo não encontrado, skipping.")
                raise e
            else:
                raise e
        finally:
            try:
                res.close()
                res.release_conn()
            except Exception as e:
                print(e)


def conn_test(client):
    print("Connected to minio")
    for bucket in client.list_buckets():
        print(bucket.name)


@dag(
    dag_id="healthcare_ingestion",
    schedule="@daily",
    start_date=datetime(2026, 2, 1),
    catchup=False,
)
def healthcare_etl():
    files = [
        "specialties",
        "providers",
        "payments",
        "patients",
        "medications",
        "medical_records",
        "lab_test_types",
        "lab_orders",
        "insurance_policies",
        "insurance_companies",
        "icd10_codes",
        "facilities",
        "departments",
        "cpt_codes",
        "claims",
        "claim_line_items",
        "appointments",
        "diagnoses"
    ]

    conn_minio = MinioETL(
        endpoint="host.docker.internal:9000",
        access_key=conn.login,
        secret_key=conn.password,
        secure=False
    )

    @task_group(group_id="connection_status")
    def conn_status():

        conn_staus_report = PythonOperator(
            task_id="conn_test",
            python_callable=conn_test,
            op_kwargs={"client": conn_minio.client}
        )

        check_if_file_exists = S3KeySensor(
            task_id="s3_file_check",
            bucket_name="datalake",
            bucket_key="healthcare/*",
            wildcard_match=True,
            aws_conn_id="my_s3_conn",
            poke_interval=10,
            timeout=180,
            mode="reschedule",
        )

        conn_staus_report >> check_if_file_exists

    @task_group(group_id="healthcare_etl")
    def bronze_etl():
        bronze_extraction_jobs = PythonOperator.partial(
            task_id="bronze_ingestion",
            python_callable=conn_minio.extract_data,
        ).expand(op_kwargs=[{"file_name": f} for f in files])

        bronze_extraction_jobs

    f_conn_check = conn_status()
    f_bronze_ingestion = bronze_etl()

    f_conn_check >> f_bronze_ingestion


healthcare_etl()
