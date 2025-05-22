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
    bq_insertion_time)
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
  CURRENT_TIMESTAMP() AS bq_insertion_time
FROM
  mvpd_data.mvpd_spectrum_stg
 ;