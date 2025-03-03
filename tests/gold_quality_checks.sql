
/*
===============================================================================
                           GOLD LAYER QUALITY CHECKS
===============================================================================
Purpose:
    - This script validates the **integrity, consistency, and accuracy** of the 
      Gold Layer in the data warehouse.
    - These checks ensure that the data model is well-structured for analytics 
      and reporting.

Quality Checks Performed:
    **Uniqueness Validation**: Ensures that surrogate keys in dimension 
       tables (`dim_customers`, `dim_products`) are unique.
    **Referential Integrity**: Validates that fact table (`fact_sales`) 
       correctly references dimension tables.
    **Data Model Validation**: Identifies missing or orphaned records in 
       the `fact_sales` table by ensuring relationships exist with dimensions.

Expected Results:
    - **No duplicates** should be found in `customer_key` and `product_key`.
    - **No NULL values** in `fact_sales` when joining with dimensions.
    - Any issues found should be investigated and resolved.

Usage Notes:
    - If discrepancies are found, analyze the source data and **correct 
      transformations in the Silver Layer** before reloading data.
===============================================================================
*/


-- ====================================================================
-- Checking 'gold_layer.dim_cust'
-- ====================================================================

-- Checking for distinct gender 
SELECT DISTINCT 
	gender
FROM gold_layer.dim_cust;


-- Checking for Uniqueness of Customer Key in gold.dim_customers
-- Expectation: No results
SELECT
	customer_key,
	COUNT(*) AS duplicates
FROM gold_layer.dim_cust
GROUP BY customer_key
HAVING COUNT(*) > 1;
-- ====================================================================
-- Checking 'gold_layer.dim_prod'
-- ====================================================================

-- Check for Uniqueness of Product Key in gold.dim_products
-- Expectation: No results 
SELECT 
    product_key,
    COUNT(*) AS duplicate_count
FROM gold_layer.dim_prod
GROUP BY product_key
HAVING COUNT(*) > 1;

-- ====================================================================
-- Checking 'gold_layer.fact_sales'
-- ====================================================================

-- Checking the data model connectivity between fact and dimensions
SELECT * 
FROM gold_layer.fact_sales f
LEFT JOIN gold_layer.dim_cust c
ON c.customer_key = f.customer_key
LEFT JOIN gold_layer.dim_prod p
ON p.product_key = f.product_key
WHERE p.product_key IS NULL OR c.customer_key IS NULL  
