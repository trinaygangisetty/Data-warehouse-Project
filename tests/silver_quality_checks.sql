/*
===============================================================================
                          DATA CLEANING PROCESS
===============================================================================

Overview:
    - This script ensures data integrity and consistency before loading data into 
      the Silver Layer from the Bronze Layer.
    - The **Bronze Layer Checks** detect errors in raw data, including:
        - Duplicates
        - Unwanted spaces
        - Sanity issues (invalid values, nulls, incorrect formats)
        - Referential integrity violations
    - After detecting issues, the necessary cleaning operations are applied.
    - The **Silver Layer Checks** validate that the cleaned data meets expected 
      business rules and is ready for further processing.

Process Flow:
    1️⃣ **Bronze Layer Checks** (Before Cleaning)
        - Identify data issues in CRM and ERP tables.
        - Ensure column-level integrity (e.g., valid customer IDs, product keys).
        - Detect format inconsistencies in fields like date, gender, and marital status.
    
    2️⃣ **Data Cleaning & Transformation**
        - Based on the identified issues, necessary cleaning actions are applied.
    
    3️⃣ **Silver Layer Checks** (Post-Cleaning)
        - Ensure that no duplicates, nulls, or invalid records exist.
        - Cross-check data consistency between related tables.
        - Run final validation queries to confirm data correctness.

Warning:
    - This script **does not perform cleaning** but identifies data quality issues.
    - All **Silver Layer queries should return zero rows**, confirming that no issues remain.
    - Any unexpected values should be reviewed and corrected before proceeding.

===============================================================================
*/






-- =============================================================================
--								BRONZE LAYER CHECKS
-- =============================================================================

-- =============================================================================
--					Checking 'bronze_layer.crm_cust_info'
-- =============================================================================

SELECT *
FROM bronze_layer.crm_cust_info;

--- checking whether customer_id has any duplicates
SELECT
	cust_id,
	count(*) as count_occurances
FROM bronze_layer.crm_cust_info
GROUP BY cust_id
HAVING count(*) > 1 OR cust_id IS NULL;

--- checking for unwanted spaces in first name
SELECT
	cust_firstname
FROM bronze_layer.crm_cust_info
WHERE cust_firstname != TRIM(cust_firstname)

--- checking for unwanted spaces in last name
SELECT
	cust_lastname
FROM bronze_layer.crm_cust_info
WHERE cust_lastname != TRIM(cust_lastname)

--- checking for sanity errors in marrital status column
SELECT DISTINCT
	cust_maritalstatus
FROM bronze_layer.crm_cust_info

--- checking for sanity errors in gender column
SELECT DISTINCT
	cust_gender
FROM bronze_layer.crm_cust_info

-- =============================================================================
--					Checking 'bronze_layer.crm_prod_info'
-- =============================================================================

SELECT *
FROM bronze_layer.crm_prod_info;

--- checking whether product_id has any duplicates
SELECT
	prod_id,
	count(*) as count_occurances
FROM bronze_layer.crm_prod_info
GROUP BY prod_id
HAVING count(*) > 1 OR prod_id IS NULL;

--- checking prod_key and comparing with cat_id from erm_px_cat_g1v2
SELECT
	prod_key
FROM bronze_layer.crm_prod_info;

--- The first 5 letters of prod_key is matching with id in the category table of erp.
SELECT
	REPLACE(SUBSTRING(prod_key,1,5), '-' , '_') AS prod_key
FROM bronze_layer.crm_prod_info
WHERE REPLACE(SUBSTRING(prod_key,1,5), '-' , '_') NOT IN (
	SELECT DISTINCT
		id
	FROM bronze_layer.erp_px_cat_g1v2
)

--- The next letters after letter 6 is the actual prod_key which could be joined with the table of crm_sales_details
SELECT
	SUBSTRING(prod_key,7,LEN(prod_key)) AS prod_key
FROM bronze_layer.crm_prod_info
WHERE SUBSTRING(prod_key,7,LEN(prod_key)) NOT IN (
	SELECT DISTINCT
		sls_prd_key
	FROM bronze_layer.crm_sales_details
)

--- Checking for Trim spaces in prod_name column
SELECT
	prod_name
FROM bronze_layer.crm_prod_info
WHERE prod_name != TRIM(prod_name);

--- Checking for nulls or negative numbers in prod_cost
SELECT
	prod_cost
FROM bronze_layer.crm_prod_info
WHERE prod_cost < 0 OR prod_cost IS NULL

--- Checking for prod_line column
SELECT DISTINCT
	prod_line
FROM bronze_layer.crm_prod_info --- It is better to ask the source team to ask for abbreviations of unknown acronyms.

--- Checking for last 2 columns sanity
SELECT
	*
FROM bronze_layer.crm_prod_info
WHERE REPLACE(SUBSTRING(prod_key, 1, 5), '-', '_') = 'AC_HE'
order by prod_name

-- =============================================================================
--					Checking 'bronze_layer.crm_sales_details'
-- =============================================================================

SELECT *
FROM bronze_layer.crm_sales_details;

--- Checking for unwanted spaces since sls_order_num is a string
SELECT 
	sls_ord_num
FROM bronze_layer.crm_sales_details
WHERE sls_ord_num != TRIM(sls_ord_num)

--- Checking if sls_prod_key has any issues by comparing it with prd_key from prod_info (in silver layer)
SELECT
	sls_prd_key
FROM bronze_layer.crm_sales_details
WHERE sls_prd_key NOT IN (
	SELECT DISTINCT
		prod_key
	FROM silver_layer.crm_prod_info
)

--- Checking if sls_cust_id has any issues by comparing it with cust_id from cust_info (in silver layer)
SELECT
	sls_cust_id
FROM bronze_layer.crm_sales_details
WHERE sls_cust_id NOT IN (
	SELECT DISTINCT
		cust_id
	FROM silver_layer.crm_cust_info
)

--- Checking for invalid dates
SELECT
	sls_order_dt
FROM bronze_layer.crm_sales_details
WHERE sls_order_dt <= 0
OR LEN(sls_order_dt) != 8
OR sls_order_dt < 19000101
	
--- Also checking if order date is later than shipping date or due date
SELECT
	*
FROM silver_layer.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt

--- Dealing with last three columns (sales, quantity and price)
		--- Business Rules are
		--- 1. Sales = Quantity * Price
		--- 2. No NULLS or No Negatives
SELECT
	sls_sales,
	sls_quantity,
	sls_price
FROM bronze_layer.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_price < 0 OR sls_quantity < 0 OR sls_sales < 0
OR sls_price IS NULL OR sls_quantity IS NULL OR sls_sales IS NULL
ORDER BY sls_sales

-- =============================================================================
--					Checking 'bronze_layer.erp_cust_az12'
-- =============================================================================

SELECT *
FROM bronze_layer.erp_cust_az12;

--- We have to check cid and compare it with cust_key from crm_cust_info table 
SELECT
	cust_key
FROM silver_layer.crm_cust_info;

    -- We have to remove the first three characters from the cid column to match with cust_key
SELECT
	cid
FROM bronze_layer.erp_cust_az12
WHERE cid NOT IN (
	SELECT DISTINCT cust_key FROM silver_layer.crm_cust_info)

--- Checking for out-of-date Birthday ranges (like future dates and very old)
SELECT
	bdate
FROM bronze_layer.erp_cust_az12
WHERE bdate > GETDATE() OR bdate < '1900-01-01'

--- Checking for all values present in Gender column
SELECT DISTINCT
	gen
FROM bronze_layer.erp_cust_az12

-- =============================================================================
--					Checking 'bronze_layer.erp_loc_a101'
-- =============================================================================

SELECT *
FROM bronze_layer.erp_loc_a101;

--- Checking if cid column matches with its foreign reference in table crm_cust_info's cust_key

SELECT
	cust_key
FROM silver_layer.crm_cust_info;
		-- We can see that there is a dash which is not in silver table

SELECT
	cid
FROM bronze_layer.erp_loc_a101
WHERE cid NOT IN (
	SELECT DISTINCT
		cust_key
	FROM silver_layer.crm_cust_info
)

--- Checking for any white spaces and all values
SELECT DISTINCT
	cntry
FROM bronze_layer.erp_loc_a101

-- =============================================================================
--					Checking 'bronze_layer.erp_px_cat_g1v'
-- =============================================================================

SELECT *
FROM bronze_layer.erp_px_cat_g1v2;

--- Checking id column and comparing it to its referencing key cat_id from table crm_prod_info (silver layer)
SELECT
	id 
FROM bronze_layer.erp_px_cat_g1v2
WHERE id NOT IN (
	SELECT DISTINCT cat_id FROM silver_layer.crm_prod_info
)

--- Checking for unwanted spaces and all values
SELECT
	cat
FROM bronze_layer.erp_px_cat_g1v2
WHERE cat != TRIM(cat)

SELECT DISTINCT
	cat
FROM bronze_layer.erp_px_cat_g1v2

--- Checking for unwanted spaces and all values
SELECT
	subcat
FROM bronze_layer.erp_px_cat_g1v2
WHERE subcat != TRIM(subcat)

SELECT DISTINCT
	subcat
FROM bronze_layer.erp_px_cat_g1v2

--- Checking for unwanted spaces and all values
SELECT
	maintenance
FROM bronze_layer.erp_px_cat_g1v2
WHERE maintenance != TRIM(maintenance)

SELECT DISTINCT
	maintenance
FROM bronze_layer.erp_px_cat_g1v2


-- =============================================================================
--								SILVER LAYER CHECKS
-- =============================================================================

-- =============================================================================
--					Checking 'silver_layer.crm_cust_info'
-- =============================================================================


--- All the queries here should return no rows (zero rows) which tells that table is free from errors.

--- checking whether customer_id has any duplicates
SELECT
	cust_id,
	count(*) as count_occurances
FROM silver_layer.crm_cust_info
GROUP BY cust_id
HAVING count(*) > 1 OR cust_id IS NULL;

--- checking for unwanted spaces in first name
SELECT
	cust_firstname
FROM silver_layer.crm_cust_info
WHERE cust_firstname != TRIM(cust_firstname)

--- checking for unwanted spaces in last name
SELECT
	cust_lastname
FROM silver_layer.crm_cust_info
WHERE cust_lastname != TRIM(cust_lastname)

--- checking for sanity errors in marrital status column
SELECT DISTINCT
	cust_maritalstatus
FROM silver_layer.crm_cust_info

--- checking for sanity errors in gender column
SELECT DISTINCT
	cust_gender
FROM silver_layer.crm_cust_info

--- final check
SELECT *
FROM silver_layer.crm_cust_info;

-- =============================================================================
--					Checking 'silver_layer.crm_prod_info'
-- =============================================================================

--- All the queries here should return no rows (zero rows) which tells that table is free from errors.

--- checking whether product_id has any duplicates
SELECT
	prod_id,
	count(*) as count_occurances
FROM silver_layer.crm_prod_info
GROUP BY prod_id
HAVING count(*) > 1 OR prod_id IS NULL;

--- checking prod_key and comparing with cat_id from erm_px_cat_g1v2
SELECT
	prod_key
FROM silver_layer.crm_prod_info;

--- The first 5 letters of prod_key is matching with id in the category table of erp.
SELECT
	cat_id
FROM silver_layer.crm_prod_info
WHERE cat_id NOT IN (
	SELECT DISTINCT
		id
	FROM bronze_layer.erp_px_cat_g1v2
)

--- The next letters after letter 6 is the actual prod_key which could be joined with the table of crm_sales_details
SELECT
	prod_key
FROM silver_layer.crm_prod_info
WHERE prod_key NOT IN (
	SELECT DISTINCT
		sls_prd_key
	FROM bronze_layer.crm_sales_details
)

--- Checking for Trim spaces in prod_name column
SELECT
	prod_name
FROM silver_layer.crm_prod_info
WHERE prod_name != TRIM(prod_name);

--- Checking for nulls or negative numbers in prod_cost
SELECT
	prod_cost
FROM silver_layer.crm_prod_info
WHERE prod_cost < 0 OR prod_cost IS NULL

--- Checking for prod_line column
SELECT DISTINCT
	prod_line
FROM silver_layer.crm_prod_info --- It is better to ask the source team to ask for abbreviations of unknown acronyms.

--- Checking for last 2 columns sanity
SELECT
	*
FROM silver_layer.crm_prod_info
WHERE cat_id = 'AC_HE'
order by prod_name

-- =============================================================================
--					Checking 'silver_layer.crm_sales_details'
-- =============================================================================


--- All the queries here should return no rows (zero rows) which tells that table is free from errors.

--- Checking for unwanted spaces since sls_order_num is a string
SELECT 
	sls_ord_num
FROM silver_layer.crm_sales_details
WHERE sls_ord_num != TRIM(sls_ord_num)

--- Checking if sls_prod_key has any issues by comparing it with prd_key from prod_info (in silver layer)
SELECT
	sls_prd_key
FROM silver_layer.crm_sales_details
WHERE sls_prd_key NOT IN (
	SELECT DISTINCT
		prod_key
	FROM silver_layer.crm_prod_info
)

--- Checking if sls_cust_id has any issues by comparing it with cust_id from cust_info (in silver layer)
SELECT
	sls_cust_id
FROM silver_layer.crm_sales_details
WHERE sls_cust_id NOT IN (
	SELECT DISTINCT
		cust_id
	FROM silver_layer.crm_cust_info
)

--- Also checking if order date is later than shipping date or due date
SELECT
	*
FROM silver_layer.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt

--- Dealing with last three columns (sales, quantity and price)
		--- Business Rules are
		--- 1. Sales = Quantity * Price
		--- 2. No NULLS or No Negatives
SELECT
	sls_sales,
	sls_quantity,
	sls_price
FROM silver_layer.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_price < 0 OR sls_quantity < 0 OR sls_sales < 0
OR sls_price IS NULL OR sls_quantity IS NULL OR sls_sales IS NULL
ORDER BY sls_sales

--- Final check
SELECT *
FROM silver_layer.crm_sales_details;

-- =============================================================================
--					Checking 'silver_layer.erp_cust_az12'
-- =============================================================================


--- All the queries here should return no rows (zero rows) which tells that table is free from errors.

--- We have to check cid and compare it with cust_key from crm_cust_info table 
SELECT
	cust_key
FROM silver_layer.crm_cust_info;

    -- We have to remove the first three characters from the cid column to match with cust_key
SELECT
	cid
FROM silver_layer.erp_cust_az12
WHERE cid NOT IN (
	SELECT DISTINCT cust_key FROM silver_layer.crm_cust_info)

--- Checking for out-of-date Birthday ranges (like future dates and very old)
SELECT
	bdate
FROM silver_layer.erp_cust_az12
WHERE bdate > GETDATE() OR bdate < '1900-01-01'

--- Checking for all values present in Gender column
SELECT DISTINCT
	gen
FROM silver_layer.erp_cust_az12

--- Final check
SELECT *
FROM silver_layer.erp_cust_az12;


-- =============================================================================
--					Checking 'silver_layer.erp_loc_a101'
-- =============================================================================


--- All the queries here should return no rows (zero rows) which tells that table is free from errors.

--- Checking if cid column matches with its foreign reference in table crm_cust_info's cust_key

SELECT
	cust_key
FROM silver_layer.crm_cust_info;
		-- We can see that there is a dash which is not in silver table

SELECT
	cid
FROM silver_layer.erp_loc_a101
WHERE cid NOT IN (
	SELECT DISTINCT
		cust_key
	FROM silver_layer.crm_cust_info
)

--- Checking for any white spaces and all values
SELECT DISTINCT
	cntry
FROM silver_layer.erp_loc_a101

--- Final check
SELECT *
FROM silver_layer.erp_loc_a101;

-- =============================================================================
--					Checking 'silver_layer.erp_px_cat_g1v2'
-- =============================================================================


--- All the queries here should return no rows (zero rows) which tells that table is free from errors.

--- Checking id column and comparing it to its referencing key cat_id from table crm_prod_info (silver layer)
SELECT
	id 
FROM silver_layer.erp_px_cat_g1v2
WHERE id NOT IN (
	SELECT DISTINCT cat_id FROM silver_layer.crm_prod_info
)

--- Checking for unwanted spaces and all values
SELECT
	cat
FROM silver_layer.erp_px_cat_g1v2
WHERE cat != TRIM(cat)

SELECT DISTINCT
	cat
FROM silver_layer.erp_px_cat_g1v2

--- Checking for unwanted spaces and all values
SELECT
	subcat
FROM silver_layer.erp_px_cat_g1v2
WHERE subcat != TRIM(subcat)

SELECT DISTINCT
	subcat
FROM silver_layer.erp_px_cat_g1v2

--- Checking for unwanted spaces and all values
SELECT
	maintenance
FROM silver_layer.erp_px_cat_g1v2
WHERE maintenance != TRIM(maintenance)

SELECT DISTINCT
	maintenance
FROM silver_layer.erp_px_cat_g1v2

--- Final check
SELECT *
FROM silver_layer.erp_px_cat_g1v2;


-- =============================================================================
--										END
-- =============================================================================
