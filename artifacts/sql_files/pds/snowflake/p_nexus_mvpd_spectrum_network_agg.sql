BEGIN;

USE DATABASE NEXUS_DATAPLATFORM;
USE SCHEMA P_MVPD;

-- Step 1: Aggregate Data
CREATE TEMPORARY TABLE tt_mvpd_network_spectrum_agg AS
  SELECT
    'Spectrum' AS partner,
    dtn.line_item_id,
    COALESCE(nm.objective_name, 'Unknown') AS network,
    dtn.event_date,
    SUM(dtn.imps) AS imps
  FROM (
    SELECT
      edi_order_number AS line_item_id,
      network,
      date AS event_date,
      SUM(impressions) AS imps
    FROM NEXUS_DATAPLATFORM.P_MVPD.P_NEXUS_MVPD_SPECTRUM_STG
    WHERE edi_order_number IS NOT NULL
    GROUP BY 1, 2, 3
  ) AS dtn
  LEFT JOIN (
    SELECT DISTINCT objective_name, spectrum_name
    FROM NEXUS_DATAPLATFORM.P_MVPD.P_NEXUS_NETWORK_MAPPINGS
  ) AS nm
  ON nm.spectrum_name = dtn.network
  GROUP BY 1,2,3,4;

-- Step 2: Delete Existing Records
DELETE FROM NEXUS_DATAPLATFORM.P_MVPD.P_NEXUS_MVPD_NETWORK_AGG
WHERE partner = 'Spectrum'
AND event_date IN (SELECT DISTINCT event_date FROM tt_mvpd_network_spectrum_agg);

-- Step 3: Insert New Aggregated Data
INSERT INTO NEXUS_DATAPLATFORM.P_MVPD.P_NEXUS_MVPD_NETWORK_AGG
SELECT * FROM tt_mvpd_network_spectrum_agg;

COMMIT;