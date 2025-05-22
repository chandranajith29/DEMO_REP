BEGIN;

USE DATABASE NEXUS_DATAPLATFORM;
USE SCHEMA P_MVPD;

-- Step 1: Aggregate Data
CREATE TEMPORARY TABLE tt_mvpd_dma_spectrum_daily_agg AS
  SELECT
    edi_order_number AS line_item_id,
    NULL AS ad_server_id,
    dma AS dma_name,
    dma_code AS nielsen_dmaid,
    DATE_PART('YEAR', date) AS week_year,
    DATE_PART('WEEKISO', date) AS week_no,
    date AS event_date,
    SUM(impressions) AS imps
  FROM NEXUS_DATAPLATFORM.P_MVPD.P_NEXUS_MVPD_SPECTRUM_STG
  WHERE edi_order_number IS NOT NULL
  GROUP BY 1,2,3,4,5,6,7;

CREATE TEMPORARY TABLE tt_mvpd_dma_spectrum_daily_agg_final AS
  SELECT
    DS.line_item_id,
    DS.ad_server_id,
    DC.dma_name,
    DS.nielsen_dmaid,
    DS.week_year,
    DS.week_no,
    DS.event_date,
    DS.imps
  FROM tt_mvpd_dma_spectrum_daily_agg AS DS
  LEFT JOIN NEXUS_DATAPLATFORM.P_REFERENTIAL.P_NEXUS_DMACODES_NIELSEN_COMSCORE AS DC
  ON DS.nielsen_dmaid = DC.nielsen_dmaid;

-- Step 2: Delete Existing Records
DELETE FROM NEXUS_DATAPLATFORM.P_MVPD.P_NEXUS_MVPD_DMA_DAILY_AGG
WHERE partner = 'Spectrum'
AND event_date IN (
    SELECT DISTINCT date
    FROM NEXUS_DATAPLATFORM.P_MVPD.P_NEXUS_MVPD_SPECTRUM_STG
);

-- Step 3: Insert New Aggregated Data
INSERT INTO NEXUS_DATAPLATFORM.P_MVPD.P_NEXUS_MVPD_DMA_DAILY_AGG
SELECT
  'Spectrum' AS partner,
  CAST(line_item_id AS STRING),
  CAST(ad_server_id AS STRING),
  dma_name,
  nielsen_dmaid,
  week_year,
  week_no,
  event_date,
  imps
FROM tt_mvpd_dma_spectrum_daily_agg_final
ORDER BY week_year, week_no;

COMMIT;