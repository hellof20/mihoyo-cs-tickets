import toml
from google.cloud import bigquery
import pandas as pd

class BigQueryHandler:
    def __init__(self, config_path: str = "config.toml"):
        try:
            with open(config_path, "r") as f:
                config = toml.load(f)
            
            self.project_id = config['app']['project_id']
            self.dataset_id = config['bigquery']['dataset_id']
            self.config_path = config_path
            print(f"BigQueryHandler initialized for project '{self.project_id}'.")
            
        except FileNotFoundError:
            print(f"Error: Configuration file not found at '{config_path}'")
            raise
        except KeyError as e:
            print(f"Error: Missing key {e} in configuration file.")
            raise

    def _get_client(self):
        """Create a new BigQuery client instance for each operation"""
        return bigquery.Client(project=self.project_id)

    def execute_sql(self, sql_query: str):
        print(f"Executing SQL query...")
        client = self._get_client()
        try:
            query_job = client.query(sql_query)
            query_job.result()  # 等待查询完成
            print("Query executed successfully.")
        except Exception as e:
            print(f"An error occurred while executing the query: {e}")
            raise
        finally:
            client.close()

    def read_gbq_to_dataframe(self, query: str) -> pd.DataFrame:
        """
        执行一个查询并将结果读取到 pandas DataFrame。
        
        Args:
            query (str): 要执行的 SELECT 查询。
        
        Returns:
            pd.DataFrame: 包含查询结果的 DataFrame。
        """
        print("Reading data from BigQuery into DataFrame...")
        client = self._get_client()
        try:
            df = client.query(query).to_dataframe()
            print(f"Successfully read {len(df)} rows into DataFrame.")
            return df
        except Exception as e:
            print(f"An error occurred while reading from BigQuery: {e}")
            raise
        finally:
            client.close()

    def upload_dataframe_to_gbq(self, df: pd.DataFrame, table_id: str, if_exists: str = 'replace'):
        if df.empty:
            print("DataFrame is empty. No data to upload.")
            return
    
        full_table_id = f"{self.project_id}.{self.dataset_id}.{table_id}"
        print(f"Uploading DataFrame to BigQuery table: {full_table_id}...")

        # 根据 if_exists 参数设置写入模式
        write_disposition_map = {
            'replace': bigquery.WriteDisposition.WRITE_TRUNCATE, # 覆盖表
            'append': bigquery.WriteDisposition.WRITE_APPEND    # 追加数据
        }

        job_config = bigquery.LoadJobConfig(
            write_disposition=write_disposition_map.get(if_exists, bigquery.WriteDisposition.WRITE_TRUNCATE),
            create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
            autodetect=True, 
        )

        client = self._get_client()
        try:
            job = client.load_table_from_dataframe(
                df, full_table_id, job_config=job_config
            )
            job.result()  # 等待作业完成
            print(f"DataFrame with {len(df)} rows uploaded successfully to {full_table_id}.")
        except Exception as e:
            print(f"An error occurred during DataFrame upload: {e}")
            raise
        finally:
            client.close()

    def load_csv_from_gcs_to_bq(self, gcs_uri: str, table_id: str, if_exists: str = 'replace'):
        full_table_id = f"{self.project_id}.{self.dataset_id}.{table_id}"
        print(f"Loading data from GCS URI '{gcs_uri}' to BigQuery table '{full_table_id}'...")

        # 根据 if_exists 参数设置写入模式
        write_disposition_map = {
            'replace': bigquery.WriteDisposition.WRITE_TRUNCATE, # 覆盖表
            'append': bigquery.WriteDisposition.WRITE_APPEND    # 追加数据
        }

        job_config = bigquery.LoadJobConfig(
            autodetect=True,
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            write_disposition=write_disposition_map.get(if_exists, bigquery.WriteDisposition.WRITE_TRUNCATE),
            create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
            allow_quoted_newlines=True,
        )

        client = self._get_client()
        try:
            load_job = client.load_table_from_uri(
                gcs_uri, full_table_id, job_config=job_config
            )
            load_job.result()  # 等待加载作业完成
            
            destination_table = client.get_table(full_table_id)
            print(f"Loaded {destination_table.num_rows} rows into {full_table_id}.")
            
        except Exception as e:
            print(f"An error occurred while loading data from GCS: {e}")
            raise
        finally:
            client.close()

    def get_task_status(self, table_id: str, task_id: str) -> pd.DataFrame:
        """
        根据 task_id 从 BigQuery 查询任务状态。
        """
        full_table_id = f"{self.project_id}.{self.dataset_id}.{table_id}"
        query = f"""SELECT * FROM `{full_table_id}` WHERE task_id = '{task_id}'"""
        print(f"Fetching status for task_id: {task_id} from {full_table_id}...")
        try:
            df = self.read_gbq_to_dataframe(query)
            if not df.empty:
                print(f"Found status for task_id {task_id}.")
            else:
                print(f"No status found for task_id {task_id}.")
            return df
        except Exception as e:
            print(f"Error fetching task status for {task_id}: {e}")
            raise

    def list_tasks(self, table_id: str, limit: int = 100, offset: int = 0, lang: str = None, status: str = None) -> pd.DataFrame:
        """
        从 BigQuery 列出所有任务或根据条件筛选任务。
        """
        full_table_id = f"{self.project_id}.{self.dataset_id}.{table_id}"
        query = f"""SELECT * FROM `{full_table_id}`"""
        conditions = []
        if lang:
            conditions.append(f"lang = '{lang}'")
        if status:
            conditions.append(f"status = '{status}'")

        if conditions:
            query += " WHERE " + " AND ".join(conditions)

        query += f" ORDER BY created_at DESC LIMIT {limit} OFFSET {offset}"
        
        print(f"Listing tasks from {full_table_id} with limit {limit}, offset {offset}, lang {lang}, status {status}...")
        try:
            df = self.read_gbq_to_dataframe(query)
            print(f"Successfully listed {len(df)} tasks.")
            return df
        except Exception as e:
            print(f"Error listing tasks: {e}")
            raise

    def get_faq(self, task_id: str) -> pd.DataFrame:
        """
        根据 task_id 从 BigQuery 查询 FAQ 数据。
        """
        query = f"""
        SELECT cluster_id, business, num_tickets, summarized
        FROM `speedy-victory-336109.nap_tickets.issue_faq` 
        WHERE task_id = '{task_id}'
        ORDER BY num_tickets DESC
        """
        print(f"Fetching FAQ data for task_id: {task_id}...")
        try:
            df = self.read_gbq_to_dataframe(query)
            print(f"Successfully fetched {len(df)} FAQ entries.")
            return df
        except Exception as e:
            print(f"Error fetching FAQ data for {task_id}: {e}")
            raise

    def get_cluster_detail(self, cluster_id: str) -> pd.DataFrame:
        """
        根据 cluster_id 从 BigQuery 查询聚类详细信息。
        """
        query = f"""
        SELECT a.ticket_id, ticket_language, dt, player_issue_description, user_issue
        FROM `nap_tickets.issue_summary` a 
        LEFT JOIN `nap_tickets.issue_cluster` b ON a.ticket_id = b.ticket_id
        WHERE b.cluster_id = '{cluster_id}'
        """
        print(f"Fetching cluster details for cluster_id: {cluster_id}...")
        try:
            df = self.read_gbq_to_dataframe(query)
            print(f"Successfully fetched {len(df)} cluster detail entries.")
            return df
        except Exception as e:
            print(f"Error fetching cluster details for {cluster_id}: {e}")
            raise
