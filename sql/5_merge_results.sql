CREATE TABLE IF NOT EXISTS `{project_id}.{dataset_id}.{result_table}`
(
  cluster_id STRING,
  ticket_id INTEGER,
  dt DATE,
  business STRING,
  summarized STRING
)
PARTITION BY dt;

-- 步骤1: 原子地删除当天分区的数据。如果分区不存在，此操作不会报错。
-- 这确保了操作的幂等性，可以安全地重跑。
DELETE FROM `{project_id}.{dataset_id}.{result_table}`
WHERE dt = '{dt}';

-- 步骤2: 插入当天从源数据计算出的最新结果。
INSERT INTO `{project_id}.{dataset_id}.{result_table}` (
    cluster_id,
    ticket_id,
    dt,
    business,
    summarized
)
SELECT
    b.cluster_id,
    b.ticket_id,
    a.dt,
    a.business,
    a.summarized
FROM
    `{project_id}.{dataset_id}.{faq_table}` AS a
LEFT JOIN
    `{project_id}.{dataset_id}.{cluster_table}` AS b ON a.cluster_id = b.cluster_id
WHERE
    a.dt = '{dt}';