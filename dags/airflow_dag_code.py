from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import boto3
 
# Variables
SNS_TOPIC_ARN = "arn:aws:sns:us-east-1:831645032705:grp-3-glue-job-success-noirframes"
region = 'us-east-1'
# Glue Job 1
job1_name = 'rds_varchasv'
job1_iam = 'rds-varchasv'

# Glue Job 2
job2_name = 'redshift_varchasv'
job2_iam = 'redshift-varchasv'

# QuickSight related
quicksight_account_id = '831645032705'
dataset_1 = '477d4310-50c6-4700-8860-9b89a494f7f4'
dataset_2 = 'ef7d8885-82f6-49f9-8655-243e776b62e9'
dataset_3 = '9979d6ab-9709-4fa6-8593-60124229ed9a'
 
# Function to send SNS notification using boto3
def send_sns_notification(message):
    sns_client = boto3.client("sns", region_name=region)  # Specify AWS region
    sns_client.publish(
        TopicArn=SNS_TOPIC_ARN,
        Message=message,
        Subject="Glue Job Notification"
    )
 
  
# Function to refresh a QuickSight Dataset
def refresh_quicksight_dataset(dataset_id):
    quicksight_client = boto3.client('quicksight', region_name=region)
    response = quicksight_client.create_ingestion(
        AwsAccountId=quicksight_account_id,
        DataSetId=dataset_id,
        IngestionId=f"ingestion-{datetime.now().strftime('%Y%m%d%H%M%S')}"
    )
    print(f"QuickSight dataset {dataset_id} ingestion started: {response}")
 
# Default DAG arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 5, 8),  # Updated start date
    'retries': 2
}
 
# Define the DAG
dag = DAG(
    'Group-3-Varchasv',
    default_args=default_args,
    description='Run multiple AWS Glue jobs sequentially and refresh 3 QuickSight datasets in Airflow',
    schedule_interval='@daily',
    catchup=False
)
 
# Dummy start task
start = DummyOperator(task_id='start', dag=dag)
 
# Task: Send SNS notification when Glue job starts
send_sns_start = PythonOperator(
    task_id="send_sns_start",
    python_callable=lambda: send_sns_notification("Glue job is starting!"),
    dag=dag
)
 
# Glue Job 1: Extract data from S3 and load into RDS
glue_job_1 = GlueJobOperator(
    task_id='glue_job_rds',
    job_name=job1_name,
    iam_role_name=job1_iam,
    region_name=region,
    dag=dag,
   
)
 
# Glue Job 2: Load transformed data from RDS into Redshift (Runs only after Job 1 finishes)
glue_job_2 = GlueJobOperator(
    task_id='glue_job_redshift',
    job_name=job2_name,
    iam_role_name=job2_iam,
    region_name=region,
    dag=dag,
    
)
 
# Task: Send SNS notification when Glue job completes
send_sns_complete = PythonOperator(
    task_id="send_sns_complete",
    python_callable=lambda: send_sns_notification("Glue job has completed!"),
    dag=dag,
)
 
# QuickSight Dataset Refresh Tasks (Sequential Execution)
refresh_quicksight_1 = PythonOperator(
    task_id='refresh_quicksight_1',
    python_callable=lambda: refresh_quicksight_dataset(dataset_1),
    dag=dag
)
 
refresh_quicksight_2 = PythonOperator(
    task_id='refresh_quicksight_2',
    python_callable=lambda: refresh_quicksight_dataset(dataset_2),
    dag=dag
)
 
refresh_quicksight_3 = PythonOperator(
    task_id='refresh_quicksight_3',
    python_callable=lambda: refresh_quicksight_dataset(dataset_3),
    dag=dag
)
 
# Dummy end task
end = DummyOperator(task_id='end', dag=dag)
 
# Enforce Sequential Execution Order
start >> send_sns_start >> glue_job_1 >> glue_job_2 >> send_sns_complete >> refresh_quicksight_1 >> refresh_quicksight_2 >> refresh_quicksight_3 >> end
