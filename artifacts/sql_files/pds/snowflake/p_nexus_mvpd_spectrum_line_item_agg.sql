BEGIN;

USE DATABASE NEXUS_DATAPLATFORM;
USE SCHEMA P_MVPD;

-- Step 1: Aggregate Data
CREATE TEMPORARY TABLE tt_mvpd_dma_spectrum_line_item_daily_agg AS
  SELECT
    edi_order_number AS line_item_id,
    NULL AS ad_server_id,
    date AS event_date,
    SUM(impressions) AS imps
  FROM NEXUS_DATAPLATFORM.P_MVPD.P_NEXUS_MVPD_SPECTRUM_STG
  WHERE edi_order_number IS NOT NULL
  GROUP BY 1, 2, 3;

-- Step 2: Delete Existing Records
DELETE FROM NEXUS_DATAPLATFORM.P_MVPD.P_NEXUS_MVPD_LINE_ITEM_DAILY_AGG
WHERE partner = 'Spectrum'
AND event_date IN (SELECT DISTINCT date FROM NEXUS_DATAPLATFORM.P_MVPD.P_NEXUS_MVPD_SPECTRUM_STG);

-- Step 3: Insert New Aggregated Data
INSERT INTO NEXUS_DATAPLATFORM.P_MVPD.P_NEXUS_MVPD_LINE_ITEM_DAILY_AGG
SELECT
  'Spectrum' AS partner,
  CAST(line_item_id AS STRING) AS line_item_id,
  CAST(ad_server_id AS STRING) AS ad_server_id,
  event_date,
  imps
FROM tt_mvpd_dma_spectrum_line_item_daily_agg
ORDER BY event_date;

COMMIT;