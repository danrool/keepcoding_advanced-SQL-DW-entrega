CREATE OR REPLACE TABLE `keepcoding.ivr_summary` AS

WITH calls 
  AS (SELECT 
    d.calls_ivr_id AS ivr_id, 
    d.calls_phone_number AS phone_number, 
    d.calls_ivr_result AS ivr_result
    ,CASE WHEN STARTS_WITH(calls_vdn_label, 'ATC') THEN 'FRONT'
        WHEN STARTS_WITH(calls_vdn_label, 'TECH') THEN 'TECH'
        WHEN STARTS_WITH(calls_vdn_label, 'ABSORPTION') THEN 'ABSORPTION'
        ELSE 'RESTO'
    END AS vdn_aggregation
    ,d.calls_start_date AS start_date
    ,d.calls_end_date AS end_date
    ,d.calls_total_duration AS total_duration
    ,d.calls_customer_segment AS customer_segment
    ,d.calls_ivr_language AS ivr_language
    ,d.calls_steps_module AS steps_module
    ,d.calls_module_aggregation as module_aggregation
    , MAX(NULLIF(d.billing_account_id,'NULL')) AS billing_account_id
    , MAX(IF(module_name = 'AVERIA_MASIVA', 1,0)) AS masiva_lg
    , MAX(IF(step_name = 'CUSTOMERINFOBYPHONE.TX' AND step_description_error = 'NULL', 1,0)) AS info_by_phone_lg
    , MAX(IF(step_name = 'CUSTOMERINFOBYDNI.TX' AND step_description_error = 'NULL', 1,0)) AS info_by_dni_lg
  FROM `keepcoding.ivr_detail` d
  GROUP BY d.calls_ivr_id, d.calls_phone_number, d.calls_ivr_result, d.calls_vdn_label, 
  d.calls_start_date, d.calls_end_date, d.calls_total_duration, d.calls_customer_segment, 
  d.calls_ivr_language, calls_steps_module, calls_module_aggregation
), cte_document
AS(
  SELECT calls_ivr_id AS ivr_id, 
    document_identification, document_type
  FROM `keepcoding.ivr_detail`
  WHERE document_identification <> 'NULL'
  QUALIFY ROW_NUMBER() OVER(PARTITION BY CAST(calls_ivr_id AS STRING) ORDER BY calls_end_date DESC) = 1 
), cte_customer_phone 
AS(
  SELECT calls_ivr_id AS ivr_id, 
    customer_phone
  FROM `keepcoding.ivr_detail`
  WHERE customer_phone <> 'NULL'
  QUALIFY ROW_NUMBER() OVER(PARTITION BY CAST(calls_ivr_id AS STRING) ORDER BY calls_end_date DESC) = 1 
), cte_repeated 
AS (
SELECT calls_ivr_id ivr_id,
    IF( DATETIME_DIFF(calls_start_date,
    (LAG(calls_start_date) 
    OVER (PARTITION BY calls_phone_number ORDER BY calls_ivr_id)), MINUTE) <= 1440,1,0) AS repeated_phone_24H
  FROM `keepcoding.ivr_detail`
  GROUP BY calls_ivr_id, calls_phone_number ,calls_start_date
), cte_cause_recall 
AS ( 
  SELECT calls_ivr_id ivr_id,
    IF( DATETIME_DIFF(calls_start_date,
    (LEAD(calls_start_date) 
    OVER (PARTITION BY calls_phone_number ORDER BY calls_ivr_id)), MINUTE) <= 1440,1,0) AS cause_recall_phone_24H
  FROM `keepcoding.ivr_detail`
  GROUP BY calls_ivr_id, calls_phone_number,calls_start_date
)


SELECT c.ivr_id
       ,c.phone_number
       ,c.ivr_result
       ,c.vdn_aggregation
       ,c.start_date
       ,c.end_date
       ,c.total_duration
       ,c.customer_segment
       ,c.ivr_language
       ,c.steps_module
       ,c.module_aggregation
       ,IFNULL(d.document_type,'NULL') document_type
       ,IFNULL(d.document_identification,'NULL') document_identification
       ,IFNULL(cp.customer_phone,'NULL') customer_phone
       ,IFNULL(c.billing_account_id,'NULL') billing_account_id
       ,c.masiva_lg
       ,c.info_by_phone_lg
       ,c.info_by_dni_lg
       ,r.repeated_phone_24H
       ,cr.cause_recall_phone_24H
FROM calls c 
LEFT JOIN cte_document d ON c.ivr_id = d.ivr_id
LEFT JOIN cte_customer_phone cp ON c.ivr_id = cp.ivr_id
LEFT JOIN cte_repeated r ON c.ivr_id = r.ivr_id
LEFT JOIN cte_cause_recall cr on c.ivr_id = cr.ivr_id