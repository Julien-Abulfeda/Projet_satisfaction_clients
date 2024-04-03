# import sys
# import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from src.extraction.get_raw_data import get_companies_raw_data
from src.transformation.get_preprocess_data import get_companies_preprocessed_data
from src.consume.get_feedbacks_from_openai import get_feedbacks_from_openAI
from src.toolbox.json_functions import clean_json_file
from src.load.postgreSQL import (
    creation_postrgesql_db_relationnal,
    update_comments_analysis_status,
    creation_postrgesql_db_relationnal_comments,
    creation_postrgesql_db_relationnal_companies_tmp,
    upsert_postrgesql_db_relationnal_companies,
    upsert_postrgesql_db_relationnal_comments,
    creation_postrgesql_db_relationnal_comments_tmp,
    creation_postrgesql_db_relationnal_feedbacks,
    insert_postrgesql_db_relationnal_feedbacks,
)
from src.load.elastic import (
    split_preprocessed_json,
    bulk_index
)

# Define default_args for the DAG


# Create the DAG
dag = DAG(
    "dst_project_demo",
    # schedule_interval='*/30 * * * *',  # Run every 30 minutes
    catchup=False,  # Don't backfill past runs
    description="Demo for dst project",
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
def scrape_raw_data():
    get_companies_raw_data(demo = True)


def preprocess_data():
    get_companies_preprocessed_data()


def create_elasticsearch_query():
    split_preprocessed_json("/app/data/data_preprocessed.json")
    bulk_index(index_name="comment_id", filename="/app/data/comments.json")
    bulk_index(index_name="company_id", filename="/app/data/companies.json")


def create_postrgesql_tables():
    creation_postrgesql_db_relationnal()


def create_postrgesql_tables_comments():
    creation_postrgesql_db_relationnal_comments()


def creation_postrgesql_relationnal_companies_tmp():
    creation_postrgesql_db_relationnal_companies_tmp()


def creation_postrgesql_relationnal_companies_upsert():
    upsert_postrgesql_db_relationnal_companies()


def creation_postrgesql_relationnal_comments_tmp():
    creation_postrgesql_db_relationnal_comments_tmp()


def creation_postrgesql_relationnal_comments_upsert():
    upsert_postrgesql_db_relationnal_comments()


def create_postrgesql_tables_feedbacks():
    creation_postrgesql_db_relationnal_feedbacks()

def get_comments_feedbacks_from_OpenIA():
    get_feedbacks_from_openAI(demo = True)
    
    
def insert_new_feedbacks_in_db():
    insert_postrgesql_db_relationnal_feedbacks()


def update_comments_analysis_status_in_db():
    update_comments_analysis_status()
    
def clean_feedbacks_json():
    clean_json_file("/app/data/feedbacks.json")


# Define task dependencies
scrape_task = PythonOperator(
    task_id="scrape_raw_data",
    python_callable=scrape_raw_data,
    dag=dag
)
preprocess_task = PythonOperator(
    task_id="preprocess_data",
    python_callable=preprocess_data,
    dag=dag
)
elasticsearch_task = PythonOperator(
    task_id="create_elasticsearch_and_query",
    python_callable=create_elasticsearch_query,
    dag=dag,
)
postrgesql_db_relationnal_companies_tmp = PythonOperator(
    task_id="creation_postrgesql_companies_tmp",
    python_callable=creation_postrgesql_relationnal_companies_tmp,
    dag=dag,
)
postrgesql_db_relationnal_companies_upsert = PythonOperator(
    task_id="upsert_postrgesql_companies",
    python_callable=creation_postrgesql_relationnal_companies_upsert,
    dag=dag,
)
postrgesql_db_relationnal_comments_tmp = PythonOperator(
    task_id="creation_postrgesql_comments_tmp",
    python_callable=creation_postrgesql_relationnal_comments_tmp,
    dag=dag,
)
postrgesql_db_relationnal_comments_upsert = PythonOperator(
    task_id="upsert_postrgesql_comments",
    python_callable=creation_postrgesql_relationnal_comments_upsert,
    dag=dag,
)
get_comments_feedbacks_from_OpenIA_task = PythonOperator(
    task_id="get_comments_feedbacks_from_OpenIA",
    python_callable=get_comments_feedbacks_from_OpenIA,
    dag=dag,
)
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

# # Order task run
(
    scrape_task
    >> preprocess_task
    >> elasticsearch_task
)
(
    elasticsearch_task
    >> postrgesql_db_relationnal_companies_tmp
    >> postrgesql_db_relationnal_companies_upsert
)
(
    elasticsearch_task
    >> postrgesql_db_relationnal_comments_tmp
    >> postrgesql_db_relationnal_comments_upsert
    
)
(
    [postrgesql_db_relationnal_companies_upsert, postrgesql_db_relationnal_comments_upsert]
    >> get_comments_feedbacks_from_OpenIA_task
    >> [insert_new_feedbacks_in_db_task, update_comments_analysis_status_in_db_task]
)
(
    [insert_new_feedbacks_in_db_task, update_comments_analysis_status_in_db_task]
    >> clean_feedbacks_json_task
)

# End of DAG definition