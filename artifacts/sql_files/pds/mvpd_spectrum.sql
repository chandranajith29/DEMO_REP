BEGIN TRANSACTION;
DELETE
FROM
  mvpd_data.mvpd_spectrum
WHERE
  date IN (
  SELECT
    DISTINCT date
  FROM
    mvpd_data.mvpd_spectrum_stg );
INSERT INTO
  mvpd_data.mvpd_spectrum ( advertiser,
    dma,
    dma_code,
    hour,
    date,
    daypart,
    day_of_week,
    network,
    insertion_order,
    edi_order_number,
    impressions,
    bq_insertion_time,
    dag_id,
    dag_run_id,
    file_name )
SELECT
    advertiser,
    dma,
    dma_code,
    hour,
    date,
    daypart,
    day_of_week,
    network,
    insertion_order,
    cast(edi_order_number as INTEGER),
    impressions,
  CURRENT_TIMESTAMP() AS bq_insertion_time,
  @dag_id AS dag_id,
  @dag_run_id AS dag_run_id,
  ARRAY_REVERSE(SPLIT(_FILE_NAME, '/'))[SAFE_OFFSET(0)] AS file_name
FROM
  mvpd_data.mvpd_spectrum_stg
 ;

update mvpd_data.mvpd_spectrum set edi_order_number=10542873 where edi_order_number=10477531;

INSERT INTO mvpd_data.mvpd_spectrum_dma_log
SELECT DISTINCT insertion_order, date as event_date, ARRAY_REVERSE(SPLIT(_FILE_NAME, '/'))[SAFE_OFFSET(0)] AS file_name,
CURRENT_TIMESTAMP() as log_time
FROM us-pricingtool.mvpd_data.mvpd_spectrum_stg
WHERE edi_order_number is null;

COMMIT TRANSACTION;