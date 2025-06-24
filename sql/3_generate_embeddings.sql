CREATE TABLE IF NOT EXISTS `{project_id}.{dataset_id}.{embedding_table}`
(
  ticket_id INT64,
  issue_embedding ARRAY<FLOAT64>,
  ticket_language STRING,
  business STRING,
  dt DATE
) PARTITION BY dt;

INSERT INTO `{project_id}.{dataset_id}.{embedding_table}`
SELECT
    ticket_id,
    ml_generate_embedding_result AS issue_embedding,
    ticket_language,
    business,
    dt
FROM ML.GENERATE_EMBEDDING(
    MODEL `{project_id}.{dataset_id}.{text_embedding_model}`,
    (
        SELECT user_issue AS content, *
        FROM `{project_id}.{dataset_id}.{summary_table}`
        WHERE user_issue IS NOT NULL
          AND unspecified_issue = 'false'
    )
);