BEGIN

	CREATE TEMP TABLE tt_mvpd_network_spectrum_agg
	AS 
	SELECT 'Spectrum' as partner,    
		dtn.line_item_id, 
		COALESCE(nm.objective_name, 'Unknown') AS network, 
		dtn.event_date, 
		SUM(dtn.imps) AS imps
	FROM (
		SELECT edi_order_number AS line_item_id, network, 
			date as event_date, SUM(impressions) as imps
		FROM mvpd_data.mvpd_spectrum_stg
		WHERE edi_order_number IS NOT NULL
		GROUP BY 1, 2, 3
	) AS dtn
	LEFT JOIN (SELECT DISTINCT objective_name, spectrum_name FROM mvpd_data.network_mappings) AS nm
	ON nm.spectrum_name = dtn.network
	GROUP BY 1,2,3,4;
	
	  
	DELETE FROM mvpd_data.mvpd_network_agg 
	WHERE partner = 'Spectrum' AND event_date IN (SELECT DISTINCT event_date FROM tt_mvpd_network_spectrum_agg);
 
  
	INSERT INTO mvpd_data.mvpd_network_agg 
	SELECT * FROM tt_mvpd_network_spectrum_agg;

END