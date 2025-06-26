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

def run_summary_pipeline():
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

if __name__ == "__main__":
    run_summary_pipeline()