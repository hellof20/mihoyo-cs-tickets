from google.cloud import bigquery
from google.cloud.bigquery import LoadJobConfig, WriteDisposition # Added based on main.py imports
from google.cloud.exceptions import NotFound # Added based on main.py imports
import pandas_gbq # Added based on main.py imports
from datetime import datetime
import time
import toml # Import toml to read the config file

# --- Load Configuration ---
def load_config():
    with open("config.toml", "r") as f:
        return toml.load(f)

config = load_config()
bq_config = config.get('bigquery', {})

# --- Constants from Config ---
PROJECT_ID = bq_config.get("project_id", "default_project_id") # Provide defaults or handle missing keys
DATASET_ID = bq_config.get("dataset_id", "default_dataset_id")
TEXT_EMBEDDING_MODEL = bq_config.get("text_embedding_model", "default_embedding_model")
FAQ_TABLE = bq_config.get("faq_table", "default_faq_table")

# --- BigQuery Client ---
client = bigquery.Client(project=PROJECT_ID)


def execute_sql(sql_query, dot_interval=10):  # dot_interval in seconds
    """Executes a BigQuery SQL query and prints progress."""
    try:
        query_job = client.query(sql_query)
        job_id = query_job.job_id
        print(f"Query Job ID: {job_id}")

        start_time = datetime.now()
        print(f"Start time: {start_time}")

        elapsed_time = 0
        while query_job.state != 'DONE':
            time.sleep(1)  # Check every 1 second
            elapsed_time += 1
            query_job.reload()

            if elapsed_time % dot_interval == 0:
                print(".", end="", flush=True)
        
        finish_time = datetime.now()
        print(f"Finish time: {finish_time}")

        if query_job.error_result:
            print(f"Query job failed: {query_job.error_result}")
            return None

        results = query_job.result()
        total_time = finish_time - start_time
        print(f"Total query time: {total_time}")
        return results

    except Exception as e:
        print(f"Error executing SQL query: {e}")
        return None

def read_bigquery_to_dataframe(sql_query):
    """Reads data from BigQuery into a Pandas DataFrame."""
    try:
        query_job = client.query(sql_query)
        results = query_job.result()
        df = results.to_dataframe()
        return df
    except Exception as e:
        print(f"Error reading from BigQuery: {e}")
        return None

def upload_dataframe_to_gbq(dataframe, destination_table, project_id, if_exists='append'):
    """Uploads a Pandas DataFrame to Google BigQuery."""
    try:
        pandas_gbq.to_gbq(dataframe, 
                            f'{project_id}.{DATASET_ID}.{destination_table}', 
                            project_id=project_id, 
                            if_exists=if_exists)
        print(f"Successfully uploaded data to {destination_table}.")
    except Exception as e:
        print(f"Error uploading DataFrame to BigQuery table {destination_table}: {e}")