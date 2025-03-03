/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze → Silver)
===============================================================================
Purpose:
    - Executes the ETL (Extract, Transform, Load) process to move data from 
      the 'bronze' schema to the 'silver' schema.
    - Cleanses and standardizes data before loading it into Silver tables.

Operations:
    - Clears existing data in Silver tables by truncating them.
    - Loads transformed and validated data from Bronze into Silver.

Parameters:
    - None. This procedure does not take any inputs or return any outputs.

Usage:
    EXEC silver_layer.silver_load;

===============================================================================
*/




CREATE OR ALTER PROCEDURE silver_layer.silver_load AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME;
	DECLARE @proc_start_time DATETIME, @proc_end_time DATETIME;
	BEGIN TRY
		SET @proc_start_time = GETDATE()
		PRINT '================================================';
        PRINT 'Loading Silver Layer';
        PRINT '================================================';

				PRINT '------------------------------------------------';
				PRINT 'Loading CRM Tables';
				PRINT '------------------------------------------------';

				-- Processing Customer Information Table
				SET @start_time = GETDATE();
				PRINT '>> Truncating Table: silver.crm_cust_info';
				TRUNCATE TABLE silver_layer.crm_cust_info;
				PRINT '>> Inserting Data Into: silver.crm_cust_info';
				INSERT INTO silver_layer.crm_cust_info (
					cust_id,
					cust_key,
					cust_firstname,
					cust_lastname,
					cust_maritalstatus,
					cust_gender,
					cust_record_date
					)

				SELECT
					cust_id,
					cust_key,
					TRIM(cust_firstname) AS cust_firstname,
					TRIM(cust_lastname) AS cust_lastname,
					CASE UPPER(TRIM(cust_maritalstatus))
						WHEN 'M' THEN 'Married'
						WHEN 'S' THEN 'Single'
						ELSE 'n/a'
					END AS cust_maritalstatus,
					CASE UPPER(TRIM(cust_gender))
						WHEN 'M' THEN 'Male'
						WHEN 'F' THEN 'Female'
						ELSE 'n/a'
					END AS cust_gender,
					cust_record_date
				FROM(
					SELECT
						*,
						ROW_NUMBER() OVER(PARTITION BY cust_id ORDER BY cust_record_date DESC) AS latest_record
					FROM bronze_layer.crm_cust_info
					WHERE cust_id IS NOT NULL
					)t
				WHERE latest_record = 1
				SET @end_time = GETDATE();
				PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
				PRINT '>> -------------';
				-- This table ensures that only the latest customer records are loaded into Silver Layer.


                -- Processing Product Information Table
				SET @start_time = GETDATE();
				PRINT '>> Truncating Table: silver.crm_prod_info';
				TRUNCATE TABLE silver_layer.crm_prod_info;
				PRINT '>> Inserting Data Into: silver.crm_prod_info';
				INSERT INTO silver_layer.crm_prod_info(
					prod_id,
					cat_id,
					prod_key,
					prod_name,
					prod_cost,
					prod_line,
					prod_start_dt,
					prod_end_dt
				)
				SELECT
					prod_id,
					REPLACE(SUBSTRING(prod_key, 1, 5), '-', '_') AS cat_id,
					SUBSTRING(prod_key, 7, LEN(prod_key)) AS prod_key,
					prod_name,
					ISNULL(prod_cost, 0) AS prod_cost,
					CASE UPPER(TRIM(prod_line))
						WHEN 'M' THEN 'Mountain'
						WHEN 'R' THEN 'Road'
						WHEN 'S' THEN 'Other Sales'
						WHEN 'T' THEN 'Touring'
						ELSE 'n/a'
					END AS prod_line,
					CAST(prod_start_dt AS DATE) AS prd_start_dt,
					CAST(LEAD(prod_start_dt) OVER(PARTITION BY prod_key ORDER BY prod_start_dt) - 1 AS DATE) AS prod_end_dt
				FROM bronze_layer.crm_prod_info
				SET @end_time = GETDATE();
				PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
				PRINT '>> -------------';
				-- This table processes product details, cleans product line abbreviations, 
				-- and ensures cost values are set to 0 if null.

        
				-- Processing Sales Details Table
				SET @start_time = GETDATE();
				PRINT '>> Truncating Table: silver.crm_sales_details';
				TRUNCATE TABLE silver_layer.crm_sales_details;
				PRINT '>> Inserting Data Into: silver.crm_sales_details';
				INSERT INTO silver_layer.crm_sales_details(
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
					CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) < 8 THEN NULL
							ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
					END AS sls_order_dt,
					CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) < 8 THEN NULL
							ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
					END AS sls_ship_dt,
					CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) < 8 THEN NULL
							ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
					END AS sls_due_dt,
					CASE WHEN sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) OR sls_sales IS NULL THEN sls_quantity * ABS(sls_price)
							ELSE sls_sales
					END AS sls_sales,
					sls_quantity,
					CASE WHEN sls_price <= 0 OR sls_price IS NULL THEN sls_sales / NULLIF(sls_quantity ,0)
							ELSE sls_price
					END AS sls_price
				FROM bronze_layer.crm_sales_details
				SET @end_time = GETDATE();
				PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
				PRINT '>> -------------';
				-- This table handles sales transactions, ensuring order dates are valid 
				-- and recalculating sales amounts if incorrect.



				PRINT '------------------------------------------------';
				PRINT 'Loading ERP Tables';
				PRINT '------------------------------------------------';

				-- Processing ERP Customer Data
				SET @start_time = GETDATE();
				PRINT '>> Truncating Table: silver.erp_cust_az12';
				TRUNCATE TABLE silver_layer.erp_cust_az12;
				PRINT '>> Inserting Data Into: silver.erp_cust_az12';
				INSERT INTO silver_layer.erp_cust_az12
				(
					cid,
					bdate,
					gen
				)
				SELECT
					CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
							ELSE cid
					END AS cid,
					CASE WHEN bdate > GETDATE() THEN NULL
							ELSE bdate
					END AS bdate,
					CASE WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
							WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
							ELSE 'n/a'
					END AS gen
				FROM bronze_layer.erp_cust_az12
				SET @end_time = GETDATE();
				PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
				PRINT '>> -------------';
				-- Cleans up customer IDs and ensures gender values are standardized.


				-- Processing ERP Location Data
				SET @start_time = GETDATE();
				PRINT '>> Truncating Table: silver.erp_loc_a101';
				TRUNCATE TABLE silver_layer.erp_loc_a101;
				PRINT '>> Inserting Data Into: silver_layer.erp_loc_a101';
				INSERT INTO silver_layer.erp_loc_a101(
					cid,
					cntry
				)
				SELECT
					REPLACE(cid, '-', '') AS cid,
					CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
							WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
							WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
							ELSE cntry
					END AS cntry
				FROM bronze_layer.erp_loc_a101
				SET @end_time = GETDATE();
				PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
				PRINT '>> -------------';
				-- Normalizes country names and removes unwanted characters.


				-- Processing ERP Product Category Data
				SET @start_time = GETDATE();
				PRINT '>> Truncating Table: silver.erp_px_cat_g1v2';
				TRUNCATE TABLE silver_layer.erp_px_cat_g1v2;
				PRINT '>> Inserting Data Into: silver.erp_px_cat_g1v2';
				INSERT INTO silver_layer.erp_px_cat_g1v2(
					id,
					cat,
					subcat,
					maintenance
				)
				SELECT 
					id,
					cat,
					subcat,
					maintenance
				FROM bronze_layer.erp_px_cat_g1v2;
				SET @end_time = GETDATE();
				PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
				PRINT '>> -------------';

				SET @proc_end_time = GETDATE();
				PRINT '=========================================='
				PRINT 'Loading Silver Layer is Completed';
				PRINT ' - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @proc_start_time, @proc_end_time) AS NVARCHAR) + ' seconds';
				PRINT '=========================================='

	END TRY

	BEGIN CATCH
		PRINT '==========================================';
		PRINT 'ERROR: Bronze Layer Loading Failed!';
		PRINT 'Details: ' + ERROR_MESSAGE();
		PRINT 'Code: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'State: ' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '==========================================';

	END CATCH

END
