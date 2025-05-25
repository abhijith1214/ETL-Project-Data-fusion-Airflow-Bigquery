import csv
from faker import Faker
import random
import string
from google.cloud import storage
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.datafusion import CloudDataFusionStartPipelineOperator

# Constants
NUM_EMPLOYEES = 100
PASSWORD_CHARACTERS = string.ascii_letters + string.digits + 'm'
FILE_NAME = 'employee_data.csv'
BUCKET_NAME = 'bucket1-new'
DESTINATION_BLOB_NAME = 'employee_data.csv'


# Function to generate employee data
def generate_employee_data():
    fake = Faker()
    with open(FILE_NAME, mode='w', newline='') as file:
        fieldnames = ['first_name', 'last_name', 'job_title', 'department', 'email', 'address', 'phone_number', 'salary', 'password']
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        for _ in range(NUM_EMPLOYEES):
            writer.writerow({
                "first_name": fake.first_name(),
                "last_name": fake.last_name(),
                "job_title": fake.job(),
                "department": fake.job(),  # Simulated department name
                "email": fake.email(),
                "address": fake.city(),
                "phone_number": fake.phone_number(),
                "salary": fake.random_number(digits=5),
                "password": ''.join(random.choice(PASSWORD_CHARACTERS) for _ in range(8))
            })


# Function to upload to GCS
def upload_to_gcs():
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(DESTINATION_BLOB_NAME)
    blob.upload_from_filename(FILE_NAME)
    print(f'File {FILE_NAME} uploaded to {DESTINATION_BLOB_NAME} in {BUCKET_NAME}.')


# Airflow DAG setup
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 12, 18),
    'depends_on_past': False,
    'email': ['vishal.bulbule@techtrapture.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'employee_data_pipeline',
    default_args=default_args,
    description='Generate employee data, upload to GCS, and trigger Data Fusion pipeline',
    schedule_interval='@daily',
    catchup=False
)

# Define Airflow tasks
with dag:
    generate_data_task = PythonOperator(
        task_id='generate_employee_data',
        python_callable=generate_employee_data
    )

    upload_to_gcs_task = PythonOperator(
        task_id='upload_to_gcs',
        python_callable=upload_to_gcs
    )

    start_datafusion_pipeline = CloudDataFusionStartPipelineOperator(
        location="us-central1",
        pipeline_name="etl-pipeline",
        instance_name="datafusion-dev",
        task_id="start_datafusion_pipeline"
    )

    # Task dependencies
    generate_data_task >> upload_to_gcs_task >> start_datafusion_pipeline
