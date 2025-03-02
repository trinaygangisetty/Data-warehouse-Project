/*
===============================================================================
DDL Script: Defining Bronze Tables
===============================================================================
Purpose:
    This script sets up tables within the 'bronze' schema. 
    If any of these tables already exist, they will be dropped and recreated.
    Please execute this script to update the DDL structure of the 'bronze' tables.
===============================================================================
*/

--- TABLE 1
DROP TABLE IF EXISTS bronze_layer.crm_cust_info;
GO

CREATE TABLE bronze_layer.crm_cust_info (
	cust_id				INT,
	cust_key			NVARCHAR(50),
	cust_firstname		NVARCHAR(50),
	cust_lastname		NVARCHAR(50),
	cust_maritalstatus	NVARCHAR(50),
	cust_gender			NVARCHAR(10),
	cust_record_date	DATE
);
GO

--- TABLE 2
DROP TABLE IF EXISTS bronze_layer.crm_prod_info;
GO

CREATE TABLE bronze_layer.crm_prod_info (
	prod_id			INT,
	prod_key		NVARCHAR(50),
	prod_name		NVARCHAR(50),
	prod_cost		INT,
	prod_line		NVARCHAR(50),
	prod_start_dt	DATETIME,
	prod_end_dt		DATETIME
);
GO

--- TABLE 3
DROP TABLE IF EXISTS bronze_layer.crm_sales_details;
GO

CREATE TABLE bronze_layer.crm_sales_details (
	sls_ord_num  NVARCHAR(50),
    sls_prd_key  NVARCHAR(50),
    sls_cust_id  INT,
    sls_order_dt INT,
    sls_ship_dt  INT,
    sls_due_dt   INT,
    sls_sales    INT,
    sls_quantity INT,
    sls_price    INT
);
GO

--- TABLE 4
DROP TABLE IF EXISTS bronze_layer.erp_loc_a101;
GO

CREATE TABLE bronze_layer.erp_loc_a101 (
    cid    NVARCHAR(50),
    cntry  NVARCHAR(50)
);
GO

--- TABLE 5
DROP TABLE IF EXISTS bronze_layer.erp_cust_az12;
GO

CREATE TABLE bronze_layer.erp_cust_az12 (
    cid    NVARCHAR(50),
    bdate  DATE,
    gen    NVARCHAR(50)
);
GO

--- TABLE 6
DROP TABLE IF EXISTS bronze_layer.erp_px_cat_g1v2;
GO

CREATE TABLE bronze_layer.erp_px_cat_g1v2 (
    id           NVARCHAR(50),
    cat          NVARCHAR(50),
    subcat       NVARCHAR(50),
    maintenance  NVARCHAR(50)
);
GO
