WITH date_bounds AS (
  SELECT 
    MIN(date) AS min_date,
    MAX(date) AS max_date
  FROM `mvpd_data.mvpd_spectrum_stg`
),
distinct_dates AS (
  SELECT DISTINCT date FROM `mvpd_data.mvpd_spectrum_stg`
),
cte AS (
  SELECT 
    line_item_id AS dim_plc_id, 
    line_item_start_date, 
    line_item_end_date, 
    product_name 
  FROM `reporting_prod.rpt_us_dim_placement` a
  WHERE product_name = 'Spectrum MVPD'
    AND (
      line_item_start_date <= (SELECT min_date FROM date_bounds)
      AND line_item_end_date >= (SELECT max_date FROM date_bounds)
      OR DATE(line_item_start_date) IN (SELECT date FROM distinct_dates)
      OR DATE(line_item_end_date) IN (SELECT date FROM distinct_dates)
    )
),
cte2 AS (
  SELECT DISTINCT 
    CAST(a.EDI_Order_Number AS STRING) AS EDI_Order_Number,
    product_name AS p_name 
  FROM `mvpd_data.mvpd_spectrum_stg` a
  JOIN `reporting_prod.rpt_us_dim_placement` b 
    ON CAST(a.EDI_Order_Number AS STRING) = CAST(b.line_item_id AS STRING)
),
final_check AS (
  SELECT
    COALESCE(cte.product_name, cte2.p_name) AS product_name,
    COUNT(COALESCE(cte.dim_plc_id, cte2.EDI_Order_Number)) AS error_count,
    CASE 
      WHEN cte.dim_plc_id IS NULL THEN 'Other lines' 
      ELSE 'Missing lines' 
    END AS action,
    STRING_AGG(COALESCE(cte.dim_plc_id, cte2.EDI_Order_Number), ",") AS missing_ids
  FROM cte
  FULL OUTER JOIN cte2 
    ON cte.dim_plc_id = cte2.EDI_Order_Number
  WHERE cte.dim_plc_id IS NULL OR cte2.EDI_Order_Number IS NULL
  GROUP BY 1, 3
)
SELECT
  'MVPD SPECTRUM DMA' AS check,
  error_count,
  CONCAT(
    'Detected ', error_count, ' line item ID(s) classified as "', action, '" ',
    'for product "', product_name, '". ',
    'Affected line item ID(s): ', missing_ids
  ) AS description
FROM final_check;