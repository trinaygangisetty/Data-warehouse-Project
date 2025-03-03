/*
=================================================================================================
                              GOLD LAYER VIEWS - DDL LAYER
=================================================================================================
Purpose:
    - This script defines the Gold Layer views for **fact and dimension tables**.
    - The Gold Layer represents the final **business-ready datasets** using a **Star Schema**.
    - Data is sourced from the Silver Layer, transformed, and structured for analytics.

Schema Overview:
    - **Dimension Tables**: Contain descriptive attributes about key business entities.
      - Examples: Customers, Products
    - **Fact Tables**: Contain transactional or measurable data linked to dimensions.
      - Example: Sales Transactions (fact_sales)
      
Usage:
    - These views can be used for **reporting, business intelligence, and analytics**.
=================================================================================================
*/


-- ==============================================================================================
-- Creating Dimension: gold_layer.dim_cust
-- ==============================================================================================
-- Dimension tables store descriptive information about entities (e.g., Customers).
-- This view enriches customer details by integrating data from CRM and ERP sources.

DROP VIEW IF EXISTS gold_layer.dim_cust;
GO

CREATE VIEW gold_layer.dim_cust AS
SELECT
	ROW_NUMBER() OVER(ORDER BY ccu.cust_id) AS customer_key, -- This is Surrogate key
	ccu.cust_id				AS customer_id,
	ccu.cust_key			AS customer_number,
	ccu.cust_firstname		AS first_name,
	ccu.cust_lastname		AS last_name,
	ccu.cust_maritalstatus	AS marital_status,
	CASE WHEN ccu.cust_gender != 'n/a' THEN ccu.cust_gender -- Because we assume that crm is primary data source
		 ELSE COALESCE(ecu.gen, ccu.cust_gender)
	END						AS gender,
	elo.cntry				AS country,
	ecu.bdate				AS birthdate,
	ccu.cust_record_date	AS created_date
FROM silver_layer.crm_cust_info ccu
LEFT JOIN silver_layer.erp_cust_az12 ecu
ON ccu.cust_key = ecu.cid
LEFT JOIN silver_layer.erp_loc_a101 elo
ON ccu.cust_key = elo.cid
GO

-- ==================================================================================================
-- Creating Dimension: gold_layer.dim_prod
-- ==================================================================================================
-- Dimension tables store product-related attributes and hierarchies.
-- This view structures product details and assigns a surrogate key.

DROP VIEW IF EXISTS gold_layer.dim_prod;
GO

CREATE VIEW gold_layer.dim_prod AS
SELECT
	ROW_NUMBER() OVER(ORDER BY cpr.prod_start_dt, cpr.prod_key) AS product_key, -- This is a Surrogate key
	cpr.prod_id			AS product_id,
	cpr.prod_key		AS product_number,
	cpr.prod_name		AS product_name,
	cpr.cat_id			AS category_id,
	epr.cat				AS category,
	epr.subcat			AS sub_category,
	cpr.prod_line		AS product_line,
	cpr.prod_cost		AS cost,	
	epr.maintenance		AS maintenance,
	cpr.prod_start_dt	AS start_date
FROM silver_layer.crm_prod_info cpr
LEFT JOIN silver_layer.erp_px_cat_g1v2 epr
ON cpr.cat_id = epr.id
WHERE cpr.prod_end_dt IS NULL;  -- This Filters out all the historical data.
GO

-- =================================================================================================
-- Creating Fact Table: gold_layer.fact_sales
-- =================================================================================================
-- Fact tables store transactional data and connect with dimension tables.
-- This view contains sales transactions with references to customers and products.

DROP VIEW IF EXISTS gold_layer.fact_sales;
GO

CREATE VIEW gold_layer.fact_sales AS
SELECT
    sd.sls_ord_num  AS order_number,
    pr.product_key  AS product_key,
    cu.customer_key AS customer_key,
    sd.sls_order_dt AS order_date,
    sd.sls_ship_dt  AS shipping_date,
    sd.sls_due_dt   AS due_date,
    sd.sls_sales    AS sales_amount,
    sd.sls_quantity AS quantity,
    sd.sls_price    AS price
FROM silver_layer.crm_sales_details sd
LEFT JOIN gold_layer.dim_prod pr
    ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold_layer.dim_cust cu
    ON sd.sls_cust_id = cu.customer_id;
GO
