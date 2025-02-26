from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'Janis',
    'start_date': days_ago(1)
}

with DAG(
    dag_id='9105411ea84a_dag',
    default_args=default_args,
    schedule_interval='@daily',  # Run daily
    catchup=False,
    description='DAG to trigger a Databricks Notebook run daily'
) as dag:

    # Task to run the Databricks Notebook
    run_databricks_notebook = DatabricksSubmitRunOperator(
        task_id='run_databricks_notebook',
        databricks_conn_id='databricks_default',
        # Use the existing "Pinterest cluster" cluster ID
        existing_cluster_id='1108-162752-8okw8dgg',
        notebook_task={
            'notebook_path': '/Users/gulbisj@gmail.com/9105411ea84a'
        }
    )

    run_databricks_notebook
