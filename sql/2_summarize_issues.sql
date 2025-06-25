CREATE TABLE IF NOT EXISTS `{project_id}.{dataset_id}.{summary_table}`
(
    unspecified_issue STRING,
    user_issue STRING,
    user_sentiment STRING,
    full_response JSON,
    status STRING,
    ticket_id INT64,
    created_at TIMESTAMP,
    ticket_language STRING,
    gamebiz STRING,
    platform STRING,
    player_issue_description STRING,
    business STRING,
    sub_business STRING,
    issue_level_2 STRING,
    issue_level_3 STRING,
    server STRING,
    dt DATE,
    prompt STRING
) PARTITION BY dt;

INSERT INTO `{project_id}.{dataset_id}.{summary_table}`
SELECT 
    unspecified_issue,
    user_issue,
    user_sentiment,
    full_response,
    status,
    ticket_id,
    created_at,
    ticket_language,
    gamebiz,
    platform,
    player_issue_description,
    business,
    sub_business,
    issue_level_2,
    issue_level_3,
    server,
    dt,
    prompt
FROM AI.GENERATE_TABLE(
    MODEL `{project_id}.{dataset_id}.{summary_model}`,
    (
        SELECT
            *,
            CONCAT(
                '<instruction>Your task is to analyze the player question below:',
                '##1. You need to extract and summarize the player question in one sentence, do not add any comments. ',
                '### 1.1 If the player does not clearly describe to issue or it is too general, just output "The player is asking for help with an unspecified issue.". ',
                '##2. You need to output based on the below <output_format>, no extra explanation or notes. ',
                '##3. Analyze the user sentiment, return "positive", "neutral" or "negative", no extra explanation or notes. ',
                '##4. Your entire output must be in Chinese.',
                '</instruction>',
                '<output_format>',
                '{{"user_issue": "用户问题的中文总结，包含问题细节", "unspecified_issue": "如果问题不明确则返回true，否则返回false", "user_sentiment":"positive|negative|neutral"}}',
                '</output_format>',
                '<player_question>',
                player_issue_description,
                '</player_question>'
            ) AS prompt
        FROM `{project_id}.{dataset_id}.{raw_data_view}`
        WHERE player_issue_description IS NOT NULL
    ),
    STRUCT("user_issue STRING, unspecified_issue STRING, user_sentiment STRING" as output_schema, 0.1 AS temperature, 2048 AS max_output_tokens)
)
