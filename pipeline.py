from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.operators import BashOperator
from airflow.contrib import BigQueryOperator
from airflow.contrib import GoogleCloudStorageToBigQueryOperator
import os
import data_download, transform_file, delete_after_done

# Define the Python function to upload files to GCS
def upload_to_gcs(data_folder, gcs_path,**kwargs):
    data_folder = data_folder
    bucket_name = 'your-bucket-name'  # Your GCS bucket name
    gcs_conn_id = 'your-gcp-connection-name'
    # List all CSV files in the data folder
    gzip_files = [file for file in os.listdir(data_folder) if file.endswith('.gz')]

    # Upload each CSV file to GCS
    for gzip_file in gzip_files:
        local_file_path = os.path.join(data_folder, gzip_file)
        gcs_file_path = f"{gcs_path}/{gzip_file}"

        upload_task = LocalFilesystemToGCSOperator(
            task_id=f'upload_to_gcs',
            src=local_file_path,
            dst=gcs_file_path,
            bucket=bucket_name,
            gcp_conn_id=gcs_conn_id,
        )
        upload_task.execute(context=kwargs)


#Define the DAGS
default_args={
    'owner': 'airflow'
}, 

with DAG(
    dag_id='upload_files_to_gcs',
    default_args=default_args,
    start_date=datetime(2024, 10, 1, 7, 0),
    schedule_interval='@daily'
) as dag:
# Define the Operator
    upload_to_gcs = PythonOperator(
        task_id='upload_to_gcs',
        python_callable=upload_to_gcs,
        op_args=['/path/to/your/local/data/', 'gcs/destination'],
        provide_context=True
    )
    data_download = BashOperator(
        task_id='data_download', 
        bash_command='/path/to/python /path/to/dags/tasks/data_download.py'
    )
    transform_file = BashOperator(
        task_id='transform_file', 
        bash_command='/path/to/python /path/to/dags/tasks/transform_file.py'
    )
    delete_after_done = BashOperator(
        task_id='delete_after_done', 
        bash_command='/path/to/python /path/to/dags/tasks/delete_after_done.py'
    )
    #GCS to BigQuery task, operator
    create_bq_table = BigQueryOperator(
        task_id='create_bq_table',
        allow_large_results=True,
        sql="CREATE TABLE IF NOT EXIST..." 
    )
    gcs_to_bq_load = GoogleCloudStorageToBigQueryOperator(
        task_id='gcs_to_bq_load',
        bucket='your_gcs_bucket',
        source_objects=['your_source_file'],
        destination_project_dataset_table='your_bigquery_table',
        schema_fields=["your_schema_fields"],
        skip_leading_rows=1,
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE'
    )


# Define DAG dependencies
data_download >> transform_file >> upload_to_gcs >> delete_after_done
data_download >> transform_file >> upload_to_gcs >> create_bq_table >> gcs_to_bq_load