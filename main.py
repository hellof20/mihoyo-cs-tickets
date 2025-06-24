import asyncio
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from datetime import date, datetime # 导入 datetime
from cluster_issue import run_pipeline
import uuid
from bq_handler import BigQueryHandler # 导入 BigQueryHandler
import toml # 导入 toml

app = FastAPI()

# 加载配置 (这里也需要加载配置来获取 task_status_table_name)
with open("config.toml", "r") as f:
    config = toml.load(f)
bq_config = config['bigquery']
app_config = config['app'] # 确保 app_config 也被加载

class ClusterRequest(BaseModel):
    startDate: date
    endDate: date
    lang: str

@app.post("/cluster_issues")
async def run_cluster_issues(request: ClusterRequest):
    if request.startDate > request.endDate:
        raise HTTPException(status_code=400, detail="Invalid parameter format: startDate cannot be after endDate")
    
    task_id = str(uuid.uuid4())
    
    # 初始化 BigQueryHandler
    bq_handler = BigQueryHandler(config_path="config.toml")
    task_status_table = bq_config['task_status_table_name']

    # 准备初始任务状态数据
    initial_status_data = {
        "task_id": task_id,
        "start_date": request.startDate.strftime("%Y-%m-%d"),
        "end_date": request.endDate.strftime("%Y-%m-%d"),
        "lang": request.lang,
        "status": "running",
        "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "error_message": "",
    }
    import pandas as pd # 导入 pandas
    df_initial_status = pd.DataFrame([initial_status_data])
    
    # 写入初始任务状态到 BigQuery
    try:
        bq_handler.upload_dataframe_to_gbq(df_initial_status, task_status_table, if_exists='append')
        print(f"Task {task_id} initial status 'running' written to BigQuery.")
    except Exception as e:
        print(f"Error writing initial task status to BigQuery: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to record initial task status: {e}")

    asyncio.create_task(run_pipeline(
        request.startDate.strftime("%Y-%m-%d"),
        request.endDate.strftime("%Y-%m-%d"),
        request.lang,
        task_id
    ))
    return {
        "task_id": task_id,
        "start_date": request.startDate.strftime("%Y-%m-%d"),
        "end_date": request.endDate.strftime("%Y-%m-%d"),
        "lang": request.lang,
        "status": "running",
        "date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
