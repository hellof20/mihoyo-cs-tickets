CREATE OR REPLACE VIEW `{project_id}.{dataset_id}.{raw_data_view}` (
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
    CAST(`创建时间` AS DATE) AS dt
  FROM
    `{project_id}.{dataset_id}.{raw_table_name}`
);