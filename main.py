import pandas as pd
import numpy as np
import uuid
import time
from datetime import datetime
import toml # Import toml to read the config file

import hdbscan # Ensure this is installed in your environment, e.g., via requirements.txt

# Import from bq_utils
from bq_utils import (
    PROJECT_ID,
    DATASET_ID,
    execute_sql,
    read_bigquery_to_dataframe,
    upload_dataframe_to_gbq
)


with open("config.toml", "r") as f:
    config = toml.load(f)

main_config = config.get('main', {})
_bq_config = config.get('bigquery', {}) # Renamed to avoid conflict with bq_utils.bq_config if ever imported directly
clustering_config = config.get('clustering', {})

# --- Constants from Config ---
DT = main_config.get('dt', 'YYYY-MM-DD') # Date for processing, provide a default or handle missing
HDBDSCAN_MIN_SAMPLES = clustering_config.get('hdbscan_min_samples', 3) # Default to 3 if not found

# --- Table Names from Config (moved from global constants) ---
RAW_TABLE_NAME = _bq_config.get("raw_table_name", "raw_data_view")
SUMMARY_TABLE_NAME = "issue_summary"
EMBEDDING_TABLE_NAME = "issue_embedding"
CLUSTER_TABLE_NAME = "issue_clustering"
FAQ_TABLE = "issue_faq"
TEXT_EMBEDDING_MODEL = _bq_config.get("text_embedding_model")
SUMMARY_MODEL = _bq_config.get("summary_model")
RESULT_TABLE = _bq_config.get("result_table_name")


summary_query = f"""
    CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_ID}.{SUMMARY_TABLE_NAME}`
    PARTITION BY dt
    AS
    WITH gemini_extract AS (
        SELECT
        TRIM(REGEXP_REPLACE(ml_generate_text_llm_result, r'^```json|```$', '')) AS generated_text,
        *
        FROM
        ML.GENERATE_TEXT(
            MODEL `{DATASET_ID}.{SUMMARY_MODEL}`,
            (
            SELECT
            *,
            CONCAT(
                '<instruction>Your task is to analyze the player question below:',
                '##1. You need to extract and summarize the player question in one sentence, do not add any comments. ',
                '### 1.1 If the player does not clearly describe to issue or it is too general, just output The player is asking for help with an unspecified issue.',
                '##2. You need to output based on the above <output_format>, no extra explanation or notes',
                '##3. Analyze the user sentiment, return positive, neutral or negative, no extra explanation or notes',
                '##4. output should be in Chinese',
                '</instruction>',
                '<output_format>',
                '{{"user_issue": "the user issue, without any double quota(\") sign in the text", "unspecified_issue": "should return true if player is asking for help with an unspecified issue, otherwise return false","user_sentiment":"positive|negative|neutral"}}',
                '</output_format>',
                '<player_question>', player_issue_description, '</player_question>') AS prompt,
            FROM
            `{PROJECT_ID}.{DATASET_ID}.{RAW_TABLE_NAME}`
            WHERE
            player_issue_description IS NOT NULL AND dt = '{DT}'
            ) ,
            STRUCT(TRUE AS flatten_json_output, 0.01 AS temperature, 2048 AS max_output_tokens)
            )
    )
    SELECT
        *,
        JSON_VALUE(parsed_json, '$.user_issue') AS user_issue,
        JSON_VALUE(parsed_json, '$.unspecified_issue') AS unspecified_issue,
        JSON_VALUE(parsed_json, '$.user_sentiment') AS user_sentiment
    FROM
        gemini_extract,
        UNNEST([SAFE.PARSE_JSON(generated_text)]) AS parsed_json;
    """

generate_issue_embeddings_sql = f"""
    CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_ID}.{EMBEDDING_TABLE_NAME}` AS (
      SELECT ticket_id, ml_generate_embedding_result as issue_embedding, ticket_language, business, dt
      FROM ML.GENERATE_EMBEDDING(
          MODEL `{DATASET_ID}.{TEXT_EMBEDDING_MODEL}`,
          (
            SELECT user_issue AS content, *
            FROM `{PROJECT_ID}.{DATASET_ID}.{SUMMARY_TABLE_NAME}`
            WHERE user_issue IS NOT NULL AND unspecified_issue = 'false' AND dt = '{DT}'
          )
      )
    )
    """

generate_faq_sql =  f"""
    CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_ID}.{FAQ_TABLE}`
    PARTITION BY dt
    AS
    WITH summary AS (
        SELECT
        cluster_id, dt, business,
        num_tickets,
        TRIM(REGEXP_REPLACE(ml_generate_text_llm_result, r'^```json|```$', '')) AS generated_text,
        prompt,
        ml_generate_text_llm_result
        FROM
        ML.GENERATE_TEXT( MODEL `{DATASET_ID}.{SUMMARY_MODEL}`,
            (
            SELECT
            t2.dt as dt, business,
            cluster_id,
            count(*) num_tickets,
            CONCAT(
                'please analyze the following group of issues and then summarize them',
                '<output_format>',
                '{{"summarized": "summarized content with Simplified Chinese"}}',
                '</output_format>',
                'Here are the issues you are going to analyze:',
                STRING_AGG(CONCAT('<issue>', user_issue, '</issue>'), "")
            ) AS prompt
            FROM
            `{PROJECT_ID}.{DATASET_ID}.{CLUSTER_TABLE_NAME}` t1
            JOIN
            `{PROJECT_ID}.{DATASET_ID}.{SUMMARY_TABLE_NAME}` t2
            ON
            t1.ticket_id = t2.ticket_id
            WHERE cluster_id NOT LIKE '-1|%' AND t2.dt = '{DT}'
            GROUP BY cluster_id, business, t2.dt),
            STRUCT(TRUE AS flatten_json_output, 1.0 AS temperature, 8192 AS max_output_tokens))
    )
    SELECT
    *,
    JSON_VALUE(parsed_json, '$.summarized') AS summarized
    FROM
    summary,
    UNNEST([SAFE.PARSE_JSON(generated_text)]) AS parsed_json;
    """   

delete_result_sql = f"""
    DELETE FROM `{PROJECT_ID}.{DATASET_ID}.{RESULT_TABLE}` WHERE dt = '{DT}';"""

insert_result_sql = f"""
    BEGIN

    CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{DATASET_ID}.{RESULT_TABLE}`
    partition by dt
    AS(
    SELECT a.cluster_id,ticket_id,a.dt,business,summarized
    FROM `{PROJECT_ID}.{DATASET_ID}.{FAQ_TABLE}` a left join `{PROJECT_ID}.{DATASET_ID}.{CLUSTER_TABLE_NAME}` b on a.cluster_id=b.cluster_id
    WHERE 1=0);

    insert into `{PROJECT_ID}.{DATASET_ID}..{RESULT_TABLE}`(cluster_id,ticket_id,dt,business,summarized) 
    SELECT a.cluster_id,ticket_id,a.dt,business,summarized
    FROM `{PROJECT_ID}.{DATASET_ID}.{FAQ_TABLE}` a left join `{PROJECT_ID}.{DATASET_ID}.{CLUSTER_TABLE_NAME}` b on a.cluster_id=b.cluster_id
    where a.dt = {DT};

    END;"""


def cluster_issues(dt_param):
    """Performs clustering on issue embeddings and uploads cluster IDs to BigQuery."""
    print(f"Performing clustering for date: {dt_param}...")
    uuid_str = str(uuid.uuid4())

    # Export Embeddings from BigQuery
    query = f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.{EMBEDDING_TABLE_NAME}` where dt = '{dt_param}'"
    df = read_bigquery_to_dataframe(query)
    if df is None or df.empty:
        print("No embeddings found to cluster.")
        return

    df['issue_embedding'] = df['issue_embedding'].apply(lambda x: x.tolist() if isinstance(x, np.ndarray) else x) # Ensure it's a list
    df = df[df['issue_embedding'].apply(lambda x: isinstance(x, list) and len(x) > 0)]

    if df.empty:
        print("No valid embeddings found after filtering.")
        return

    embeddings = np.stack(df['issue_embedding'].apply(lambda x: np.array(x)).to_numpy())
    
    # Apply HDBSCAN
    clusterer = hdbscan.HDBSCAN(min_samples=HDBDSCAN_MIN_SAMPLES)
    clusters = clusterer.fit_predict(embeddings)

    # Add cluster labels back to the DataFrame and upload to BigQuery
    df.loc[:, 'cluster_id'] = pd.Series(clusters, index=df.index[:len(clusters)])
    df['cluster_id'] = df['cluster_id'].astype(int).astype(str) + "|" + uuid_str
    
    df_to_upload = df[['ticket_id', 'dt', 'cluster_id']].copy()
    df_to_upload['dt'] = dt_param

    # Use the new upload_dataframe_to_gbq function
    upload_dataframe_to_gbq(df_to_upload, CLUSTER_TABLE_NAME, PROJECT_ID, if_exists='append')
    print("Finished clustering issues and uploading results.")

def main():
    """Main function to run the data processing pipeline."""
    print(f"Starting data processing for date: {DT}...")
    
    execute_sql(summary_query)
    execute_sql(generate_issue_embeddings_sql)
    cluster_issues(DT)
    execute_sql(generate_faq_sql)

    # execute_sql(delete_result_sql)
    # execute_sql(insert_result_sql)
    
    print(f"Data processing completed for date: {DT}.")

if __name__ == "__main__":
    main()