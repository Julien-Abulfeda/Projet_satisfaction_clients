from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from src.consume.get_feedbacks_from_openai import get_feedbacks_from_openAI
from src.toolbox.json_functions import clean_json_file
from src.load.postgreSQL import (
    update_comments_analysis_status,
    insert_postrgesql_db_relationnal_feedbacks,
)


# Define default_args for the DAG


# Create the DAG
dag = DAG(
    "save_feedbacks",
    # schedule_interval='*/30 * * * *',  # Run every 30 minutes
    catchup=False,  # Don't backfill past runs
    description="My DAG to for consume",
    tags=["Project"],
    default_args={
        "owner": "airflow",
        "depends_on_past": False,
        "start_date": datetime(2024, 3, 5),
        "retries": 0,
        "retry_delay": timedelta(minutes=5),
    },
)


# Define tasks (operators)
def get_feedbacks_from_AI():
    get_feedbacks_from_openAI()
    
def insert_new_feedbacks_in_db():
    insert_postrgesql_db_relationnal_feedbacks()


def update_comments_analysis_status_in_db():
    update_comments_analysis_status()
    
def clean_feedbacks_json():
    clean_json_file("/app/data/feedbacks.json")


# # Define task dependencies
# get_comments_feedbacks_from_OpenIA_task = PythonOperator(
#     task_id="get_feedbacks_from_openAI",
#     python_callable=get_feedbacks_from_AI,
#     dag=dag
# )
insert_new_feedbacks_in_db_task = PythonOperator(
    task_id="insert_new_feedbacks_in_db",
    python_callable=insert_new_feedbacks_in_db,
    dag=dag,
    trigger_rule='all_done'
)
update_comments_analysis_status_in_db_task = PythonOperator(
    task_id="update_comments_analysis_status_in_db",
    python_callable=update_comments_analysis_status_in_db,
    dag=dag,
    trigger_rule='all_done'
)
clean_feedbacks_json_task = PythonOperator(
    task_id="clean_feedbacks_json",
    python_callable=clean_feedbacks_json,
    dag=dag,
    trigger_rule='all_done'
)


(
    [insert_new_feedbacks_in_db_task, update_comments_analysis_status_in_db_task]
    >> clean_feedbacks_json_task
)

# End of DAG definition
