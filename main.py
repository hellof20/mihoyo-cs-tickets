import numpy as np
import uuid
import toml
import hdbscan
from pathlib import Path
from bq_handler import BigQueryHandler
from pubsub_handler import PubSubHandler
import json

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
gcs_config = config.get('gcs', {}) # 使用 .get 以支持可选配置

# --- 常量 ---
HDBDSCAN_MIN_SAMPLES = clustering_config['hdbscan_min_samples']
PROJECT_ID = app_config['project_id']
DATASET_ID = bq_config['dataset_id']

# --- 函数定义 ---
def cluster_issues(bq: BigQueryHandler, dates_to_process: list[str]):
    """
    在问题嵌入上执行聚类，并将聚类ID上传到BigQuery。
    此函数为传入的日期列表中的每个日期分别执行聚类。

    Args:
        bq (BigQueryHandler): BigQuery 处理器实例。
        dates_to_process (list[str]): 需要处理的日期字符串列表 (格式 'YYYY-MM-DD')。
    """
    print(f"--- Starting: Clustering issues for {len(dates_to_process)} specified dates ---")

    embedding_table_id = f"{PROJECT_ID}.{DATASET_ID}.{bq_config['embedding_table_name']}"
    cluster_table_name = bq_config['cluster_table_name']

    if not dates_to_process:
        print("Date list is empty. Skipping clustering.")
        return

    # 2. 遍历每个日期并进行聚类
    for dt_param in dates_to_process:
        print(f"--- Processing clusters for date: {dt_param} ---")

        # 获取该日期的数据
        query = f"SELECT ticket_id, issue_embedding FROM `{embedding_table_id}` WHERE dt = '{dt_param}'"
        df = bq.read_gbq_to_dataframe(query)

        if df is None or df.empty:
            print(f"No embeddings found for {dt_param}. Skipping.")
            continue

        # 清理和准备嵌入向量
        df_valid = df.dropna(subset=['issue_embedding'])
        if df_valid.empty:
            print(f"No valid embeddings for {dt_param} after dropping NaNs. Skipping.")
            continue

        embeddings_matrix = np.stack(df_valid['issue_embedding'].to_numpy())

        # 应用 HDBSCAN
        print(f"Applying HDBSCAN with min_samples={HDBDSCAN_MIN_SAMPLES}...")
        clusterer = hdbscan.HDBSCAN(
            min_samples=HDBDSCAN_MIN_SAMPLES)
        clusters = clusterer.fit_predict(embeddings_matrix)

        # 准备上传的数据
        df_to_upload = df_valid[['ticket_id']].copy()
        df_to_upload['cluster_id'] = clusters

        # 为 cluster_id 添加UUID后缀，确保每次运行的簇ID唯一
        uuid_str = str(uuid.uuid4())
        df_to_upload['cluster_id'] = df_to_upload['cluster_id'].astype(str) + "|" + uuid_str
        df_to_upload['dt'] = dt_param

        print(f"Generated {df_to_upload['cluster_id'].nunique()} unique clusters for {dt_param}.")

        # 上传到 BigQuery
        bq.upload_dataframe_to_gbq(
            df_to_upload,
            cluster_table_name,
            if_exists='replace'
        )
        print(f"--- Finished clustering for {dt_param} ---")
    
    print("--- Finished: Clustering issues ---")

def run_pipeline():
    """主函数，按顺序运行整个数据处理流程"""
    print(f"======== Starting Data Processing Pipeline ========")
    bq_handler = BigQueryHandler(config_path="config.toml")
    pubsub_handler = PubSubHandler(config_path="config.toml")

    while True:
        resp = pubsub_handler.pull_message()
        if bool(resp):
            msg = resp.received_messages[0].message.data
            ack_id = resp.received_messages[0].ack_id
            msg = json.loads(msg.decode('utf-8'))
            uri = msg['name']
            pubsub_handler.acknowledge_message(ack_id)

            # 从 GCS 加载数据
            print("--- Starting: Loading data from GCS ---")
            gcs_uri = f"gs://{gcs_config['source_bucket']}/{uri}"
            bq_handler.load_csv_from_gcs_to_bq(gcs_uri, bq_config['raw_table_name'])
            print("--- Finished: Loading data ---")

            # 创建视图
            print("--- Starting: Creating raw data view ---")
            sql = get_template("sql/1_create_view.sql").format(
                project_id = PROJECT_ID,
                dataset_id = DATASET_ID, 
                raw_data_view = bq_config['raw_data_view'],
                raw_table_name = bq_config['raw_table_name']
            )
            bq_handler.execute_sql(sql)

            # 从加载的数据中获取所有唯一的日期，作为本次运行的基准
            print("--- Determining dates to process from the raw data ---")
            try:
                dt_query = f"SELECT DISTINCT dt FROM `{PROJECT_ID}.{DATASET_ID}.{bq_config['raw_data_view']}` ORDER BY dt"
                dt_df = bq_handler.read_gbq_to_dataframe(dt_query)
                if dt_df is None or dt_df.empty:
                    print("No dates found in the source data. Exiting pipeline.")
                    return

                # 将日期对象转换为 'YYYY-MM-DD' 格式的字符串列表
                dates_to_process = sorted([
                    dt.strftime('%Y-%m-%d') if hasattr(dt, 'strftime') else str(dt)
                    for dt in dt_df['dt']
                ])
                print(f"Pipeline will run for the following {len(dates_to_process)} dates: {dates_to_process}")
            except Exception as e:
                print(f"Failed to query distinct dates from raw table. Error: {e}")
                raise

            # 摘要和情感分析
            print("--- Starting: Summarizing issues ---")
            sql = get_template("sql/2_summarize_issues.sql").format(
                project_id=PROJECT_ID,
                dataset_id=DATASET_ID,
                summary_table=bq_config['summary_table_name'],
                summary_model=bq_config['summary_model'],
                raw_data_view = bq_config['raw_data_view'],
            )
            bq_handler.execute_sql(sql)

            # 生成 Embedding
            print("--- Starting: Generating embeddings ---")
            sql = get_template("sql/3_generate_embeddings.sql").format(
                project_id=PROJECT_ID,
                dataset_id=DATASET_ID,
                embedding_table=bq_config['embedding_table_name'],
                text_embedding_model=bq_config['text_embedding_model'],
                summary_table=bq_config['summary_table_name'],
            )
            bq_handler.execute_sql(sql)
            
            # 聚类
            print("--- Starting: CLustering tickets ---")
            cluster_issues(bq_handler, dates_to_process)
            
            # 生成 FAQ
            print("--- Starting: Generating FAQ from clusters ---")
            sql = get_template("sql/4_generate_faq.sql").format(
                project_id=PROJECT_ID,
                dataset_id=DATASET_ID,
                faq_table=bq_config['faq_table_name'],
                summary_model=bq_config['summary_model'],
                cluster_table=bq_config['cluster_table_name'],
                summary_table=bq_config['summary_table_name'],
            )
            bq_handler.execute_sql(sql)
        
        print(f"======== Pipeline Completed Successfully ========")
    # 合并最终结果
    # print("--- Starting: Merging final results for each date ---")
    # merge_template = get_template("sql/5_merge_results.sql")
    # for dt_param in dates_to_process:
    #     print(f"--- Processing merge for date: {dt_param} ---")
    #     sql = merge_template.format(
    #         project_id=PROJECT_ID,
    #         dataset_id=DATASET_ID,
    #         result_table=bq_config['result_table_name'],
    #         faq_table=bq_config['faq_table_name'],
    #         cluster_table=bq_config['cluster_table_name'],
    #         dt=dt_param
    #     )
    #     bq_handler.execute_sql(sql)
    #     print(f"--- Finished merging for date: {dt_param} ---")



if __name__ == "__main__":
    run_pipeline()