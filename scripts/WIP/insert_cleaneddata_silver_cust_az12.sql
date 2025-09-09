INSERT INTO silver.erp_cust_az12 (cid, bdate, gen)

SELECT
	CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
		 ELSE cid
	END AS cid,
	CASE WHEN bdate > GETDATE() THEN NULL
		 ELSE bdate
	END AS bdate,
	CASE WHEN UPPER(TRIM(gen)) IN ('FEMALE', 'F') THEN 'Female'
		 WHEN UPPER(TRIM(gen)) IN ('MALE', 'M') THEN 'Male'
		 ELSE 'Unknown'
	END AS gen
FROM bronze.erp_cust_az12