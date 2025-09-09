INSERT INTO silver.erp_loc_a101 (cid, cntry)

SELECT  
	REPLACE(cid, '-', '') AS cid,
	CASE WHEN UPPER(TRIM(cntry)) IN ('USA', 'UNITED STATES', 'US') THEN 'United States'
		 WHEN UPPER(TRIM(cntry)) IN ('DE', 'GERMANY') THEN 'Germany'
		 WHEN cntry = '' OR cntry IS NULL THEN 'Unknown'
		 ELSE cntry
	END AS cntry
FROM bronze.erp_loc_a101

