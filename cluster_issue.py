import numpy as np
from datetime import datetime # 导入 datetime
import toml
import hdbscan
from pathlib import Path
from bq_handler import BigQueryHandler

# --- 辅助函数 ---
def get_template(file_path: str) -> str:
    """从文件读取模板内容"""
    try:
        return Path(file_path).read_text(encoding='utf-8')
    except FileNotFoundError:
        print(f"Error: Template file not found at '{file_path}'")
        raise

# --- 加载配置 ---
print("Loading configuration...")
with open("config.toml", "r") as f:
    config = toml.load(f)

app_config = config['app']
bq_config = config['bigquery']
clustering_config = config['clustering']
bq = BigQueryHandler(config_path="config.toml")

# --- 常量 ---
HDBDSCAN_MIN_SAMPLES = clustering_config['hdbscan_min_samples']
PROJECT_ID = app_config['project_id']
DATASET_ID = bq_config['dataset_id']
TASK_STATUS_TABLE = bq_config['task_status_table_name']

# --- 函数定义 ---
def cluster_issues(business, startDate, endDate, lang, task_id):
    print(f"--- Processing clusters for date range: {startDate} to {endDate} ---")
    embedding_table_id = f"{PROJECT_ID}.{DATASET_ID}.{bq_config['embedding_table_name']}"
    cluster_table_name = bq_config['cluster_table_name']

    # 获取该日期的数据
    if lang == "all":
        query = f"SELECT ticket_id, issue_embedding FROM `{embedding_table_id}` WHERE dt between '{startDate}' and '{endDate}' and business = '{business}'"
    else:
        query = f"SELECT ticket_id, issue_embedding FROM `{embedding_table_id}` WHERE dt between '{startDate}' and '{endDate}' and business = '{business}' and ticket_language = '{lang}'"
    print(query)
    df = bq.read_gbq_to_dataframe(query)

    if df is None or df.empty:
        print(f"No embeddings result found. Skipping.")

    # 清理和准备嵌入向量
    df_valid = df.dropna(subset=['issue_embedding'])
    if df_valid.empty:
        print(f"No valid embeddings after dropping NaNs. Skipping.")

    embeddings_matrix = np.stack(df_valid['issue_embedding'].to_numpy())

    # 应用 HDBSCAN
    print(f"Applying HDBSCAN with min_samples={HDBDSCAN_MIN_SAMPLES}...")
    clusterer = hdbscan.HDBSCAN(min_samples=HDBDSCAN_MIN_SAMPLES)
    clusters = clusterer.fit_predict(embeddings_matrix)

    # 准备上传的数据
    df_to_upload = df_valid[['ticket_id']].copy()
    df_to_upload['cluster_id'] = clusters

    # 为 cluster_id 添加UUID后缀，确保每次运行的簇ID唯一
    uuid_str = task_id
    df_to_upload['cluster_id'] = df_to_upload['cluster_id'].astype(str) + "|" + uuid_str
    df_to_upload['id'] = uuid_str

    print(f"Generated {df_to_upload['cluster_id'].nunique()} unique clusters from {startDate} to {endDate}.")

    # 上传到 BigQuery
    bq.upload_dataframe_to_gbq(
        df_to_upload,
        cluster_table_name,
        if_exists='append'
    )

    print("--- Finished: Clustering issues ---")

def run_pipeline(business, startDate, endDate, lang, task_id):
    """主函数，按顺序运行整个数据处理流程"""
    print(f"======== Starting Data Processing Pipeline ========")

    try:
        # 聚类
        print("--- Starting: CLustering tickets ---")
        cluster_issues(business, startDate, endDate, lang, task_id)
        print(f"Task ID: {task_id}")
        
        # 生成 FAQ
        print("--- Starting: Generating FAQ from clusters ---")
        sql = get_template("sql/4_generate_faq.sql").format(
            project_id = PROJECT_ID,
            dataset_id = DATASET_ID,
            faq_table = bq_config['faq_table_name'],
            summary_model = bq_config['summary_model'],
            cluster_table = bq_config['cluster_table_name'],
            summary_table = bq_config['summary_table_name'],
            task_id = task_id,
        )
        bq.execute_sql(sql)
        
        # 更新任务状态为 'success'
        time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        update_status_query = f"""
            UPDATE `{PROJECT_ID}.{DATASET_ID}.{TASK_STATUS_TABLE}`
            SET status = 'success', updated_at = '{time}'
            WHERE task_id = '{task_id}'
        """
        bq.execute_sql(update_status_query)
        print(f"Task {task_id} status updated to 'success'.")
        print(f"======== Pipeline Completed Successfully ========")        

    except Exception as e:
        print(f"======== Pipeline Failed: {e} ========")
        # 更新任务状态为 'failed'
        time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        error_message_escaped = str(e).replace("'", "\\'")
        update_status_query = f"""
            UPDATE `{PROJECT_ID}.{DATASET_ID}.{TASK_STATUS_TABLE}`
            SET status = 'failed', error_message = '{error_message_escaped}', updated_at = '{time}'
            WHERE task_id = '{task_id}'
        """
        bq.execute_sql(update_status_query)
        print(f"Task {task_id} status updated to 'failed'. Error: {e}")

