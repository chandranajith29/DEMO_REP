BEGIN
  CREATE TEMP TABLE tt_mvpd_dma_spectrum_daily_agg AS (
  SELECT
    edi_order_number AS line_item_id,
    NULL AS ad_server_id,
    dma AS dma_name,
    dma_code AS nielsen_dmaid,
    EXTRACT(YEAR
    FROM
      date) AS week_year,
    EXTRACT(ISOWEEK
    FROM
      date) AS week_no,
    date AS event_date,
    SUM(impressions) AS imps
  FROM
    mvpd_data.mvpd_spectrum_stg
  WHERE
    edi_order_number IS NOT NULL
  GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    7 ); CREATE TEMP TABLE tt_mvpd_dma_spectrum_daily_agg_final AS (
  SELECT
    DS.line_item_id,
    DS.ad_server_id,
    DC.dma_name,
    DS.nielsen_dmaid,
    DS.week_year,
    DS.week_no,
    DS.event_date,
    DS.imps
  FROM
    tt_mvpd_dma_spectrum_daily_agg AS DS
  LEFT JOIN
    us.dmacodes_nielsen_comscore_05_22_2023 AS DC
  ON
    DS.nielsen_dmaid = DC.nielsen_dmaid );
DELETE
FROM
  mvpd_data.mvpd_dma_daily_agg
WHERE
  partner = 'Spectrum'
  AND event_date IN (
  SELECT
    DISTINCT date
  FROM
    mvpd_data.mvpd_spectrum_stg );
INSERT INTO
  mvpd_data.mvpd_dma_daily_agg
SELECT
  'Spectrum' AS partner,
  CAST(line_item_id AS string),
  CAST(ad_server_id AS string),
  dma_name,
  nielsen_dmaid,
  week_year,
  week_no,
  event_date,
  imps
FROM
  tt_mvpd_dma_spectrum_daily_agg_final
ORDER BY
  week_year,
  week_no;
update mvpd_data.mvpd_dma_daily_agg set line_item_id='10542873' where line_item_id='10477531' and partner = 'Spectrum';
END