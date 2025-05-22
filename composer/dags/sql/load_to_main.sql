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
  ARRAY_REVERSE(SPLIT(_FILE_NAME, '/'))[SAFE_OFFSET(0)] AS file_name
FROM
  mvpd_data.mvpd_spectrum_stg
 ;