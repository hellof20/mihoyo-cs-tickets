CREATE OR REPLACE TABLE `{project_id}.{dataset_id}.{embedding_table}`
AS
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