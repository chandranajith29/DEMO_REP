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