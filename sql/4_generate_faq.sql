CREATE TABLE IF NOT EXISTS `{project_id}.{dataset_id}.{faq_table}`
(
  summarized STRING,
  full_response JSON,
  status STRING,  
  task_id STRING,
  business STRING,
  cluster_id STRING,
  num_tickets INT64,
  prompt STRING,
) CLUSTER BY task_id;

INSERT INTO `{project_id}.{dataset_id}.{faq_table}`
SELECT
summarized,full_response,status,id,business,cluster_id,num_tickets,prompt
FROM
AI.GENERATE_TABLE( MODEL `{dataset_id}.{summary_model}`,
    (
    SELECT
    t1.id, business,cluster_id,count(*) num_tickets,
    CONCAT(
        'please analyze the following group of issues and then summarize them.',
        'Output format is json, key is summarized, value is summarized content with Simplified Chinese.',
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
    GROUP BY cluster_id, business, t1.id
    ),
    STRUCT("summarized STRING" AS output_schema, 8192 AS max_output_tokens)
)