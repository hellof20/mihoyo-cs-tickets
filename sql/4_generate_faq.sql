CREATE TABLE IF NOT EXISTS `{project_id}.{dataset_id}.{faq_table}`
(
  task_id STRING,
  cluster_id STRING,
  business STRING,
  num_tickets INT64,
  generated_text STRING,
  prompt STRING,
  ml_generate_text_llm_result STRING,
  parsed_json JSON,
  summarized STRING
) CLUSTER BY task_id;

INSERT INTO `{project_id}.{dataset_id}.{faq_table}`
WITH summary AS (
    SELECT
    id,cluster_id, business,num_tickets,
    TRIM(REGEXP_REPLACE(ml_generate_text_llm_result, r'^```json|```$', '')) AS generated_text,
    prompt,
    ml_generate_text_llm_result
    FROM
    ML.GENERATE_TEXT( MODEL `{dataset_id}.{summary_model}`,
        (
        SELECT
        t1.id, business,cluster_id,count(*) num_tickets,
        CONCAT(
            'please analyze the following group of issues and then summarize them',
            '<output_format>',
            '{{"summarized": "summarized content with Simplified Chinese"}}',
            '</output_format>',
            'Here are the issues you are going to analyze:',
            STRING_AGG(CONCAT('<issue>', user_issue, '</issue>'), "")
        ) AS prompt
        FROM
        `{project_id}.{dataset_id}.{cluster_table}` t1  
        JOIN
        `{project_id}.{dataset_id}.{summary_table}` t2
        ON
        t1.ticket_id = t2.ticket_id
        WHERE cluster_id NOT LIKE '-1|%' and t1.id = '{task_id}'
        GROUP BY cluster_id, business, t1.id),
        STRUCT(TRUE AS flatten_json_output, 1.0 AS temperature, 8192 AS max_output_tokens))
)
SELECT
*,
JSON_VALUE(parsed_json, '$.summarized') AS summarized
FROM
summary,
UNNEST([SAFE.PARSE_JSON(generated_text)]) AS parsed_json;