# import all modules
import airflow
from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.python import PythonOperator
from google.cloud import storage
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocStartClusterOperator,
    DataprocStopClusterOperator,
    DataprocSubmitJobOperator,
)

# define the variables
PROJECT_ID = "healthcare-466804"
REGION = "us-east1"
CLUSTER_NAME = "my-demo-cluster2"
COMPOSER_BUCKET = "us-central1-demo-new-instan-aa557158-bucket"

GCS_JOB_FILE_1 = f"gs://{COMPOSER_BUCKET}/data/INGESTION/hospitalA_mysqlToLanding.py"
GCS_JOB_FILE_2 = f"gs://{COMPOSER_BUCKET}/data/INGESTION/hospitalB_mysqlToLanding.py"
GCS_JOB_FILE_3 = f"gs://{COMPOSER_BUCKET}/data/INGESTION/claims.py"
GCS_JOB_FILE_4 = f"gs://{COMPOSER_BUCKET}/data/INGESTION/cpt_codes.py"

PYSPARK_JOB_1 = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": GCS_JOB_FILE_1},
}

PYSPARK_JOB_2 = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": GCS_JOB_FILE_2},
}

PYSPARK_JOB_3 = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": GCS_JOB_FILE_3},
}

PYSPARK_JOB_4 = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": GCS_JOB_FILE_4},
}

def archive_files(**kwargs):
    bucket_name = 'healthcare-bucket202507723'
    hospital_name = kwargs['hospital']
    table_list = kwargs['tables']
    today = datetime.today().strftime('%d%m%Y')
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    for table in table_list:
        prefix = f"landing/{hospital_name}/{table}/{table}_{today}.json"
        source_blob = bucket.blob(prefix)

        if not source_blob.exists():
            continue

        year, month, day = today[-4:], today[2:4], today[:2]
        archive_path = f"landing/{hospital_name}/archive/{table}/{year}/{month}/{day}/{table}_{today}.json"
        dest_blob = bucket.blob(archive_path)

        # Move file
        bucket.copy_blob(source_blob, bucket, archive_path)
        source_blob.delete()

        print(f"✅ Archived: {prefix} ➜ {archive_path}")

ARGS = {
    "owner": "SHAIK FAYAZ",
    "start_date": None,
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": ["***@gmail.com"],
    "email_on_success": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

with DAG(
    dag_id="pyspark_dag",
    schedule_interval=None,
    description="DAG to start a Dataproc cluster, run PySpark jobs, and stop the cluster",
    default_args=ARGS,
    tags=["pyspark", "dataproc", "etl", "marvel"]
) as dag:

    # Start cluster
    start_cluster = DataprocStartClusterOperator(
        task_id="start_cluster",
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name=CLUSTER_NAME,
    )

    # PySpark jobs
    pyspark_task_1 = DataprocSubmitJobOperator(
        task_id="pyspark_task_1", job=PYSPARK_JOB_1, region=REGION, project_id=PROJECT_ID
    )

    pyspark_task_2 = DataprocSubmitJobOperator(
        task_id="pyspark_task_2", job=PYSPARK_JOB_2, region=REGION, project_id=PROJECT_ID
    )

    pyspark_task_3 = DataprocSubmitJobOperator(
        task_id="pyspark_task_3", job=PYSPARK_JOB_3, region=REGION, project_id=PROJECT_ID
    )

    pyspark_task_4 = DataprocSubmitJobOperator(
        task_id="pyspark_task_4", job=PYSPARK_JOB_4, region=REGION, project_id=PROJECT_ID
    )

    # Archive task for hospital A
    archive_task_ha = PythonOperator(
        task_id='archive_files_ha',
        python_callable=archive_files,
        op_kwargs={
            'hospital': 'hospital-a',
            'tables': ['encounters', 'patients', 'departments', 'providers', 'transactions']
        }
    )
    archive_task_ha.trigger_rule = TriggerRule.ALL_SUCCESS

    # Archive task for hospital B
    archive_task_hb = PythonOperator(
        task_id='archive_files_hb',
        python_callable=archive_files,
        op_kwargs={
            'hospital': 'hospital-b',
            'tables': ['encounters', 'patients', 'departments', 'providers', 'transactions']
        }
    )
    archive_task_hb.trigger_rule = TriggerRule.ALL_SUCCESS

    # Stop cluster
    stop_cluster = DataprocStopClusterOperator(
        task_id="stop_cluster",
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name=CLUSTER_NAME,
    )

    # Task dependencies
    start_cluster >> [pyspark_task_1, pyspark_task_2]
    [pyspark_task_1, pyspark_task_2] >> [pyspark_task_3, pyspark_task_4]
    [pyspark_task_3, pyspark_task_4] >> [archive_task_ha, archive_task_hb]
    [archive_task_ha, archive_task_hb] >> stop_cluster
