/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source → Bronze)
===============================================================================
Purpose:
    - Loads data into 'bronze' schema from external CSV files.
    - Clears existing data before inserting new records.
    - Uses `BULK INSERT` for efficient data loading.

Key Operations:
    - **Truncates Tables:** Ensures fresh data before loading.
    - **BULK INSERT Usage:** Loads data from CSV with optimized performance.
    - **Execution Time Logging:** Captures load duration for each table.
    - **Error Handling:** Logs errors with message, number, and state.

Tables Processed:
    - CRM Data: `crm_cust_info`, `crm_prd_info`, `crm_sales_details`
    - ERP Data: `erp_loc_a101`, `erp_cust_az12`, `erp_px_cat_g1v2`

Usage:
    EXEC bronze_layer.bronze_loading;

Warning:
    - Running this procedure will **delete and reload** data.
===============================================================================
*/



--- Stored Procedure Creation
CREATE OR ALTER PROCEDURE bronze_layer.bronze_loading AS
BEGIN

	--- Declaring variables to track Load start and end times 
	DECLARE @start_time DATETIME, @end_time DATETIME; -- for individual table operations

	DECLARE @proc_start_time DATETIME, @proc_end_time DATETIME; -- for the entire batch operation (BRONZE LAYER)

	--- Using Try-Catch block structure to handle run time errors
	
	BEGIN TRY

			SET @proc_start_time = GETDATE();
			PRINT '======================================================';
			PRINT 'Loading Bronze Layer';
			PRINT '======================================================';


			PRINT '******************************************************';
			PRINT 'Loading tables from CRM';
			PRINT '******************************************************';

			--- table 1
			SET @start_time = GETDATE();
			PRINT '>> Tuncating Table: bronze_layer.crm_cust_info';
			TRUNCATE TABLE bronze_layer.crm_cust_info;
			PRINT '>> Inserting Data into: bronze_layer.crm_cust_info';
			BULK INSERT bronze_layer.crm_cust_info
			FROM 'C:\Users\trina\OneDrive\Desktop\SQL COURSE ABDUL BARRA\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
			WITH(
					FIRSTROW = 2,
					FIELDTERMINATOR = ',',
					TABLOCK
				)
			SET @end_time = GETDATE();
			PRINT '>> Load Duration for the table is: ' + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + ' milliseconds';
			PRINT '>> ===================================';


			--- table 2
			SET @start_time = GETDATE();
			PRINT '>> Tuncating Table: bronze_layer.crm_prod_info';
			TRUNCATE TABLE bronze_layer.crm_prod_info;
			PRINT '>> Inserting Data into: bronze_layer.crm_prod_info';
			BULK INSERT bronze_layer.crm_prod_info
			FROM 'C:\Users\trina\OneDrive\Desktop\SQL COURSE ABDUL BARRA\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
			WITH(
					FIRSTROW = 2,
					FIELDTERMINATOR = ',',
					TABLOCK
				)
			SET @end_time = GETDATE();
			PRINT '>> Load Duration for the table is: ' + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + ' milliseconds';
			PRINT '>> ===================================';


			--- table 3
			SET @start_time = GETDATE();
			PRINT '>> Tuncating Table: bronze_layer.crm_sales_details';
			TRUNCATE TABLE bronze_layer.crm_sales_details;
			PRINT '>> Inserting Data into: bronze_layer.crm_sales_details';
			BULK INSERT bronze_layer.crm_sales_details
			FROM 'C:\Users\trina\OneDrive\Desktop\SQL COURSE ABDUL BARRA\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
			WITH(
					FIRSTROW = 2,
					FIELDTERMINATOR = ',',
					TABLOCK
				)
			SET @end_time = GETDATE();
			PRINT '>> Load Duration for the table is: ' + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + ' milliseconds';
			PRINT '>> ===================================';


			--- table 4
			SET @start_time = GETDATE();
			PRINT '>> Tuncating Table: bronze_layer.erp_loc_a101';
			TRUNCATE TABLE bronze_layer.erp_loc_a101;
			PRINT '>> Inserting Data into: bronze_layer.erp_loc_a101';
			BULK INSERT bronze_layer.erp_loc_a101
			FROM 'C:\Users\trina\OneDrive\Desktop\SQL COURSE ABDUL BARRA\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
			WITH(
					FIRSTROW = 2,
					FIELDTERMINATOR = ',',
					TABLOCK
				)
			SET @end_time = GETDATE();
			PRINT '>> Load Duration for the table is: ' + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + ' milliseconds';
			PRINT '>> ===================================';


			--- table 5
			SET @start_time = GETDATE();
			PRINT '>> Tuncating Table: bronze_layer.erp_cust_az12';
			TRUNCATE TABLE bronze_layer.erp_cust_az12;
			PRINT '>> Inserting Data into: bronze_layer.erp_cust_az12';
			BULK INSERT bronze_layer.erp_cust_az12
			FROM 'C:\Users\trina\OneDrive\Desktop\SQL COURSE ABDUL BARRA\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
			WITH(
					FIRSTROW = 2,
					FIELDTERMINATOR = ',',
					TABLOCK
				)
			SET @end_time = GETDATE();
			PRINT '>> Load Duration for the table is: ' + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + ' milliseconds';
			PRINT '>> ===================================';


			--- table 6
			SET @start_time = GETDATE();
			PRINT '>> Tuncating Table: bronze_layer.erp_px_cat_g1v2';
			TRUNCATE TABLE bronze_layer.erp_px_cat_g1v2;
			PRINT '>> Inserting Data into: bronze_layer.erp_px_cat_g1v2';
			BULK INSERT bronze_layer.erp_px_cat_g1v2
			FROM 'C:\Users\trina\OneDrive\Desktop\SQL COURSE ABDUL BARRA\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
			WITH(
					FIRSTROW = 2,
					FIELDTERMINATOR = ',',
					TABLOCK
				)
			SET @end_time = GETDATE();
			PRINT '>> Load Duration for the table is: ' + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + ' milliseconds';
			PRINT '>> ===================================';

			--- BATCH END TIME
			SET @proc_end_time = GETDATE();
			PRINT '======================================================';
			PRINT 'Loading Bronze Layer completed';
			PRINT '>> Total Load Duration is: ' + CAST(DATEDIFF(millisecond, @proc_start_time, @proc_end_time) AS NVARCHAR) + ' milliseconds'
			PRINT '======================================================';

	END TRY

	BEGIN CATCH
			PRINT '===========================================================';
			PRINT 'An error occurred while loading data into the Bronze Layer.';
			PRINT 'Details: ' + ERROR_MESSAGE();
			PRINT 'Error Code: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
			PRINT 'Error State: ' + CAST(ERROR_STATE() AS NVARCHAR);
			PRINT '===========================================================';
	END CATCH

END
