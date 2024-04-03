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
    "extract_and_load",
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
    get_companies_raw_data()


def preprocess_data():
    get_companies_preprocessed_data()


def create_elasticsearch_query():
    split_preprocessed_json("/app/data/data_preprocessed.json")
    bulk_index(index_name="comment_id", filename="/app/data/comments.json")
    bulk_index(index_name="company_id", filename="/app/data/companies.json")


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

# # Order task run
(
    scrape_task
    >> preprocess_task
    >> elasticsearch_task
)


# End of DAG definition