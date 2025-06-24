bq mk --connection --location=us-central1 --project_id=speedy-victory-336109 --connection_type=CLOUD_RESOURCE gemini-connection
bq show --connection speedy-victory-336109.us-central1.gemini-connection 
gcloud projects add-iam-policy-binding speedy-victory-336109 --member='serviceAccount:bqcx-279432852451-yesq@gcp-sa-bigquery-condel.iam.gserviceaccount.com' --role='roles/aiplatform.user' --condition=None

CREATE OR REPLACE MODEL `nap_tickets.gemini-25-pro-preview-05-06` REMOTE WITH CONNECTION `us-central1.gemini-connection` OPTIONS(ENDPOINT = 'gemini-2.5-pro-preview-05-06');
CREATE OR REPLACE MODEL `nap_tickets.gemini-25-flash` REMOTE WITH CONNECTION `us-central1.gemini-connection` OPTIONS(ENDPOINT = 'gemini-2.5-flash');
CREATE OR REPLACE MODEL `nap_tickets.gemini-embedding-001` REMOTE WITH CONNECTION `us-central1.gemini-connection` OPTIONS(ENDPOINT = 'gemini-embedding-001');


        CREATE OR REPLACE VIEW nap_tickets.raw_data_view (
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
        dt
        ) AS (
        SELECT
            `工单ID` AS ticket_id,
            `创建时间` AS created_at,
            `工单语言` AS ticket_language,
            `gamebiz` AS gamebiz,
            `所属平台` AS platform,
            `玩家问题描述` AS player_issue_description,
            `业务` AS business,
            `子业务` AS sub_business,
            `问题二级` AS issue_level_2,
            `问题三级` AS issue_level_3,
            `服务器` AS server,
            -- 修正之处：直接将 TIMESTAMP 类型的 '创建时间' 转换为 DATE 类型
            CAST(`创建时间` AS DATE) AS dt
        FROM
            `nap_tickets`.`raw_data`
        );
