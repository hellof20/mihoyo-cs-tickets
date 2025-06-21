from bq_handler import BigQueryHandler

TEST_GCS_URI = "gs://pwm-lowa/NAP_tickets.csv" 
TEST_DESTINATION_TABLE = "raw_data" 


def run_test():
    """
    执行对 load_csv_from_gcs_to_bq 方法的单独测试。
    """
    print("--- 开始测试 load_csv_from_gcs_to_bq 方法 ---")
    
    try:
        print("正在初始化 BigQueryHandler...")
        bq_handler = BigQueryHandler(config_path="config.toml")
        
        # 2. 调用需要测试的方法
        print(f"\n准备从 GCS 加载文件: {TEST_GCS_URI}")
        print(f"目标 BigQuery 表: {bq_handler.dataset_id}.{TEST_DESTINATION_TABLE}")
        
        bq_handler.load_csv_from_gcs_to_bq(
            gcs_uri=TEST_GCS_URI, 
            destination_table=TEST_DESTINATION_TABLE
        )

        print("\n--- 测试成功完成 ---")
        print("请前往 Google Cloud Console 检查 BigQuery 中的表和数据。")

    except FileNotFoundError:
        print("错误：找不到 'config.toml' 文件。请确保它在正确的路径下。")
    except Exception as e:
        print(f"测试过程中发生错误: {e}")

if __name__ == "__main__":
    run_test()