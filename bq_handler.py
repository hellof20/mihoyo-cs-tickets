# bq_handler.py

import toml
from google.cloud import bigquery
import pandas as pd

class BigQueryHandler:
    def __init__(self, config_path: str = "config.toml"):
        try:
            with open(config_path, "r") as f:
                config = toml.load(f)
            
            self.project_id = config['bigquery']['project_id']
            self.dataset_id = config['bigquery']['dataset_id']
            self.client = bigquery.Client(project=self.project_id)
            print(f"BigQueryHandler initialized for project '{self.project_id}'.")
            
        except FileNotFoundError:
            print(f"Error: Configuration file not found at '{config_path}'")
            raise
        except KeyError as e:
            print(f"Error: Missing key {e} in configuration file.")
            raise

    def execute_sql(self, sql_query: str):
        print(f"Executing SQL query...")
        try:
            query_job = self.client.query(sql_query)
            query_job.result()  # 等待查询完成
            print("Query executed successfully.")
        except Exception as e:
            print(f"An error occurred while executing the query: {e}")
            raise

    def read_gbq_to_dataframe(self, query: str) -> pd.DataFrame:
        """
        执行一个查询并将结果读取到 pandas DataFrame。
        
        Args:
            query (str): 要执行的 SELECT 查询。
        
        Returns:
            pd.DataFrame: 包含查询结果的 DataFrame。
        """
        print("Reading data from BigQuery into DataFrame...")
        try:
            df = self.client.query(query).to_dataframe()
            print(f"Successfully read {len(df)} rows into DataFrame.")
            return df
        except Exception as e:
            print(f"An error occurred while reading from BigQuery: {e}")
            raise

    def upload_dataframe_to_gbq(self, df: pd.DataFrame, table_id: str, if_exists: str = 'append'):
        """
        将 pandas DataFrame 上传到 BigQuery 表。
        
        Args:
            df (pd.DataFrame): 要上传的 DataFrame。
            table_id (str): 目标表的ID（不包含项目和数据集）。
            if_exists (str): 如果表已存在该如何操作 ('fail', 'replace', 'append')。
        """
        full_table_id = f"{self.project_id}.{self.dataset_id}.{table_id}"
        print(f"Uploading DataFrame to BigQuery table: {full_table_id}...")

        job_config = bigquery.LoadJobConfig(
            write_disposition=f"WRITE_{if_exists.upper()}",
        )

        try:
            job = self.client.load_table_from_dataframe(
                df, full_table_id, job_config=job_config
            )
            job.result()  # 等待作业完成
            print(f"DataFrame with {len(df)} rows uploaded successfully to {full_table_id}.")
        except Exception as e:
            print(f"An error occurred during DataFrame upload: {e}")
            raise
            
    def ensure_table_exists(self, table_id: str, schema_query: str):
        """
        确保一个表存在，如果不存在，则根据提供的 schema_query 创建它。
        
        Args:
            table_id (str): 目标表的ID。
            schema_query (str): 用于创建空表的 `CREATE TABLE ... AS SELECT ... WHERE 1=0` 查询。
        """
        
        try:
            full_table_id = f"{self.project_id}.{self.dataset_id}.{table_id}"
            self.client.get_table(full_table_id)
            print(f"Table {full_table_id} already exists.")
        except Exception: # google.api_core.exceptions.NotFound
            print(f"Table {full_table_id} not found. Creating it now...")
            self.execute_sql(schema_query)

    # --- 新增方法 ---
    def load_csv_from_gcs_to_bq(self, gcs_uri: str, table_id: str, if_exists: str = 'replace'):
        """
        从 Google Cloud Storage (GCS) 加载 CSV 文件到 BigQuery 表。

        Args:
            gcs_uri (str): GCS 中 CSV 文件的 URI，例如 'gs://your-bucket/your-file.csv'。
            table_id (str): 目标 BigQuery 表的ID（不包含项目和数据集）。
            if_exists (str): 如果表已存在该如何操作 ('replace' 或 'append')。
        """
        full_table_id = f"{self.project_id}.{self.dataset_id}.{table_id}"
        print(f"Loading data from GCS URI '{gcs_uri}' to BigQuery table '{full_table_id}'...")

        # 根据 if_exists 参数设置写入模式
        write_disposition_map = {
            'replace': bigquery.WriteDisposition.WRITE_TRUNCATE, # 覆盖表
            'append': bigquery.WriteDisposition.WRITE_APPEND    # 追加数据
        }

        job_config = bigquery.LoadJobConfig(
            # 自动检测表的 schema
            autodetect=True,
            # 指定源文件格式为 CSV
            source_format=bigquery.SourceFormat.CSV,
            # 跳过第一行，通常是 CSV 的 header
            skip_leading_rows=1,
            # 设置写入模式
            write_disposition=write_disposition_map.get(if_exists, bigquery.WriteDisposition.WRITE_TRUNCATE),
            # 允许字段值中包含换行符，增加鲁棒性
            allow_quoted_newlines=True,
        )

        try:
            load_job = self.client.load_table_from_uri(
                gcs_uri, full_table_id, job_config=job_config
            )
            load_job.result()  # 等待加载作业完成
            
            destination_table = self.client.get_table(full_table_id)
            print(f"Loaded {destination_table.num_rows} rows into {full_table_id}.")
            
        except Exception as e:
            print(f"An error occurred while loading data from GCS: {e}")
            raise

