BEGIN TRANSACTION;
DELETE
FROM
  mvpd_data.mvpd_spectrum_line_item
WHERE
  date IN (
  SELECT
    DISTINCT date
  FROM
    mvpd_data.mvpd_spectrum_line_item_stg );
INSERT INTO
  mvpd_data.mvpd_spectrum_line_item ( 
    advertiser,
    date,
    insertion_order,
    edi_order_number,
    impressions,
    bq_insertion_time,
    dag_id,
    dag_run_id,
    file_name )
SELECT
    advertiser,
    date,
    insertion_order,
    edi_order_number,
    sum(impressions) as impressions,
  CURRENT_TIMESTAMP() AS bq_insertion_time,
  @dag_id AS dag_id,
  @dag_run_id AS dag_run_id,
  ARRAY_REVERSE(SPLIT(_FILE_NAME, '/'))[SAFE_OFFSET(0)] AS file_name
FROM
  mvpd_data.mvpd_spectrum_line_item_stg
  group by 
    advertiser,
    date,
    insertion_order,
    edi_order_number
 ;
COMMIT TRANSACTION;