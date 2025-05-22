BEGIN TRANSACTION;
  CREATE TEMP TABLE tt_mvpd_dma_spectrum_line_item_daily_agg AS (
  SELECT
    edi_order_number AS line_item_id,
    NULL AS ad_server_id,
    date AS event_date,
    SUM(impressions) AS imps
  FROM
    mvpd_data.mvpd_spectrum_stg
  WHERE
    edi_order_number IS NOT NULL
  GROUP BY
    1, 2, 3
    ); 
    
DELETE
FROM
  mvpd_data.mvpd_line_item_daily_agg
WHERE
  partner = 'Spectrum'
  AND event_date IN ( SELECT DISTINCT date FROM mvpd_data.mvpd_spectrum_stg )
  ;

INSERT INTO
  mvpd_data.mvpd_line_item_daily_agg
SELECT
  'Spectrum' AS partner,
  CAST(line_item_id AS string) line_item_id,
  CAST(ad_server_id AS string) ad_server_id,
  event_date,
  imps
FROM
  tt_mvpd_dma_spectrum_line_item_daily_agg
ORDER BY
  event_date;
update mvpd_data.mvpd_line_item_daily_agg set line_item_id='10542873' where line_item_id='10477531' and partner = 'Spectrum';
COMMIT;