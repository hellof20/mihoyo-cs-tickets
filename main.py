from fastapi import FastAPI, HTTPException, BackgroundTasks
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
bq_handler = BigQueryHandler(config_path="config.toml")
task_status_table = bq_config['task_status_table_name']

class ClusterRequest(BaseModel):
    business: str
    startDate: date
    endDate: date
    lang: str

@app.post("/cluster_issues")
async def run_cluster_issues(request: ClusterRequest, background_tasks: BackgroundTasks):
    if request.startDate > request.endDate:
        raise HTTPException(status_code=400, detail="Invalid parameter format: startDate cannot be after endDate")
    
    task_id = str(uuid.uuid4())

    # 准备初始任务状态数据
    initial_status_data = {
        "task_id": task_id,
        "business": request.business,
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

    # 它将在独立的线程中运行 run_pipeline，不会阻塞主事件循环
    background_tasks.add_task(
        run_pipeline,
        request.business,
        request.startDate.strftime("%Y-%m-%d"),
        request.endDate.strftime("%Y-%m-%d"),
        request.lang,
        task_id
    )

    return {
        "task_id": task_id,
        "start_date": request.startDate.strftime("%Y-%m-%d"),
        "end_date": request.endDate.strftime("%Y-%m-%d"),
        "lang": request.lang,
        "status": "running",
        "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }

@app.get("/tasks/{task_id}")
async def get_task_status(task_id: str):
    """
    获取指定任务ID的任务状态。
    """
    try:
        df_status = bq_handler.get_task_status(task_status_table, task_id)
        if df_status.empty:
            raise HTTPException(status_code=404, detail=f"Task with ID {task_id} not found.")
        # 将 DataFrame 转换为字典列表，并处理日期时间格式
        result = df_status.to_dict(orient='records')[0]
        # 确保日期时间字段是字符串格式以便JSON序列化
        for key, value in result.items():
            if isinstance(value, (date, datetime)):
                result[key] = value.strftime("%Y-%m-%d %H:%M:%S")
        return result
    except Exception as e:
        print(f"Error fetching task status for {task_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch task status: {e}")

@app.get("/tasks")
async def list_all_tasks(
    limit: int = 100,
    offset: int = 0,
    lang: str = None,
    status: str = None
):
    """
    列出所有任务，支持分页、语言和状态过滤。
    """
    try:
        df_tasks = bq_handler.list_tasks(task_status_table, limit, offset, lang, status)
        # 将 DataFrame 转换为字典列表，并处理日期时间格式
        results = df_tasks.to_dict(orient='records')
        for row in results:
            for key, value in row.items():
                if isinstance(value, (date, datetime)):
                    row[key] = value.strftime("%Y-%m-%d %H:%M:%S")
        return results
    except Exception as e:
        print(f"Error listing tasks: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to list tasks: {e}")