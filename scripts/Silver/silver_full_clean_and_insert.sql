/*
Script Purpose: Create a stored procedure for cleaning and loading data from the bronze layer
into the silver layer.
Does a truncate and load for each table.
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
	SET @batch_start_time = GETDATE();
		PRINT '========================';
		PRINT 'Loading Silver Layer';
		PRINT '========================';

		PRINT '========================';
		PRINT 'Loading CRM Tables';
		PRINT '========================';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.crm_cust_info';
		TRUNCATE TABLE silver.crm_cust_info;
		PRINT '>> Inserting Data Into: silver.crm_cust_info';
		INSERT INTO silver.crm_cust_info (
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_marital_status,
			cst_gndr,
			cst_create_date)
		SELECT
		cst_id,
		cst_key,
		TRIM(cst_firstname) AS cst_firstname, --Clean first name data of unwanted spaces
		TRIM(cst_lastname) AS cst_lastname, --Clean first name data of unwanted spaces
		CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
			 WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
			 ELSE 'Unknown'
		END cst_marital_status, --Standardize and clean up marital status to readable format
		CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
			 WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
			 ELSE 'Unknown'
		END cst_gndr, --Standardize and clean up gender values to readable format
		cst_create_date
		FROM (
			SELECT
			*,
			ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
			FROM bronze.crm_cust_info
			WHERE cst_id IS NOT NULL
		)t WHERE flag_last = 1; --Selected only most recent record per customer in order to remove duplicates
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '---------------'

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.crm_prd_info';
		TRUNCATE TABLE silver.crm_prd_info;
		PRINT '>> Inserting Data Into: silver.crm_prd_info';
		INSERT INTO silver.crm_prd_info (
			prd_id,
			cat_id,
			prd_key,
			prd_name,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt
		)
		SELECT  
			prd_id,	
			REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id, --Separating out cat_id to facilitate JOINS
			SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key, --Separating out prd_key to facilitate JOINS
			prd_name,
			ISNULL(prd_cost, 0) AS prd_cost, --Removing nulls from cost
			CASE UPPER(TRIM(prd_line)) --Standardizing prd_line data for readability
				 WHEN 'M' THEN 'Mountain'
				 WHEN 'R' THEN 'Road'
				 WHEN 'S' THEN 'Other Sales'
				 WHEN 'T' THEN 'Touring'
				 ELSE 'Unknown'
			END AS prd_line,
			CAST(prd_start_dt AS DATE) AS prd_start_dt, --Changing to DATE format
			CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) AS prd_end_dt --Changing to DATE format and cleaning up time spans
		FROM bronze.crm_prd_info;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '---------------'

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.crm_sales_details';
		TRUNCATE TABLE silver.crm_sales_details;
		PRINT '>> Inserting Data Into: silver.crm_sales_details';
		INSERT INTO silver.crm_sales_details(
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			sls_order_dt,
			sls_ship_dt,
			sls_due_dt,
			sls_sales,
			sls_quantity,
			sls_price
		)
		SELECT 
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			--Following three CASEs are cleaning up dates and transforming from INT to DATE
			CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
			END AS sls_order_dt,
			CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
			END AS sls_order_dt,
			CASE WHEN sls_due_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
			END AS sls_due_dt,
			--Following CASE is cleaning up sales data and removing NULL, negatives, and incorrect calculations
			CASE WHEN sls_sales IS NULL or sls_sales <=0 OR sls_sales != sls_quantity * ABS(sls_price)
					THEN sls_quantity * ABS(sls_price)
				 ELSE sls_sales
			END AS sls_sales,
			sls_quantity, 
			--Following CASE is removing NULLS, negatives, and zeros from sls_price
			CASE WHEN sls_price IS NULL or sls_price <=0
					THEN sls_sales / NULLIF(sls_quantity,0)
				 ELSE sls_price
			END AS sls_price
		FROM bronze.crm_sales_details;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '---------------'

		PRINT '========================';
		PRINT 'Loading ERP Tables';
		PRINT '========================';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_cust_az12';
		TRUNCATE TABLE silver.erp_cust_az12;
		PRINT '>> Inserting Data Into: silver.erp_cust_az12';
		INSERT INTO silver.erp_cust_az12 (cid, bdate, gen)
		SELECT
			--Remove unwanted 'NAS' at beginning of cid
			CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) 
				 ELSE cid
			END AS cid,
			--Remove bdates that are beyond the current date
			CASE WHEN bdate > GETDATE() THEN NULL
				 ELSE bdate
			END AS bdate,
			--Standardize gen data
			CASE WHEN UPPER(TRIM(gen)) IN ('FEMALE', 'F') THEN 'Female'
				 WHEN UPPER(TRIM(gen)) IN ('MALE', 'M') THEN 'Male'
				 ELSE 'Unknown'
			END AS gen
		FROM bronze.erp_cust_az12;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '---------------'

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_loc_a101';
		TRUNCATE TABLE silver.erp_loc_a101;
		PRINT '>> Inserting Data Into: silver.erp_loc_a101';
		INSERT INTO silver.erp_loc_a101 (cid, cntry)
		SELECT  
			REPLACE(cid, '-', '') AS cid, --Removed '-' from cid
			--Standardized country info
			CASE WHEN UPPER(TRIM(cntry)) IN ('USA', 'UNITED STATES', 'US') THEN 'United States'
				 WHEN UPPER(TRIM(cntry)) IN ('DE', 'GERMANY') THEN 'Germany'
				 WHEN cntry = '' OR cntry IS NULL THEN 'Unknown'
				 ELSE cntry
			END AS cntry
		FROM bronze.erp_loc_a101;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '---------------'

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_px_cat_g1v2';
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		PRINT '>> Inserting Data Into: silver.erp_px_cat_g1v2';
		INSERT INTO silver.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
		SELECT
			id,
			cat,
			subcat,
			maintenance
		FROM bronze.erp_px_cat_g1v2;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '---------------'

		SET @batch_end_time = GETDATE();
		PRINT '===============================';
		PRINT '>> Batch Load Duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '===============================';
	END TRY

	BEGIN CATCH
		PRINT '====================================';
		PRINT 'ERROR OCCURRED WHILE LOADING SILVER LAYER';
		PRINT 'Error Message: ' + ERROR_MESSAGE();
		PRINT 'Error Message: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message: ' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '====================================';
	END CATCH
END