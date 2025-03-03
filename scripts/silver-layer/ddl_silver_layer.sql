/*
===============================================================================
DDL Script: Defining Silver Tables
===============================================================================
Purpose:
    - Creates tables in the 'silver' schema.
    - Drops and recreates tables if they already exist.
    - Ensures the correct structure for tables in the silver layer.

Usage:
    - Run this script to reset the DDL structure of 'silver' tables.

Warning:
    - This script will remove existing 'silver' tables before recreating them.
===============================================================================
*/


DROP TABLE IF EXISTS silver_layer.crm_cust_info;
GO

CREATE TABLE silver_layer.crm_cust_info (
	cust_id				INT,
	cust_key			NVARCHAR(50),
	cust_firstname		NVARCHAR(50),
	cust_lastname		NVARCHAR(50),
	cust_maritalstatus	NVARCHAR(50),
	cust_gender			NVARCHAR(10),
	cust_record_date	DATE,
	dwh_create_date     DATETIME2 DEFAULT GETDATE()
);
GO

--- TABLE 2
DROP TABLE IF EXISTS silver_layer.crm_prod_info;
GO

CREATE TABLE silver_layer.crm_prod_info (
	prod_id			INT,
	cat_id			NVARCHAR(50),
	prod_key		NVARCHAR(50),
	prod_name		NVARCHAR(50),
	prod_cost		INT,
	prod_line		NVARCHAR(50),
	prod_start_dt	DATE,
	prod_end_dt		DATE,
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

--- TABLE 3
DROP TABLE IF EXISTS silver_layer.crm_sales_details;
GO

CREATE TABLE silver_layer.crm_sales_details (
	sls_ord_num		NVARCHAR(50),
    sls_prd_key		NVARCHAR(50),
    sls_cust_id		INT,
    sls_order_dt	DATE,
    sls_ship_dt		DATE,
    sls_due_dt		DATE,
    sls_sales		INT,
    sls_quantity	INT,
    sls_price		INT,
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

--- TABLE 4
DROP TABLE IF EXISTS silver_layer.erp_loc_a101;
GO

CREATE TABLE silver_layer.erp_loc_a101 (
    cid				NVARCHAR(50),
    cntry			NVARCHAR(50),
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

--- TABLE 5
DROP TABLE IF EXISTS silver_layer.erp_cust_az12;
GO

CREATE TABLE silver_layer.erp_cust_az12 (
    cid				NVARCHAR(50),
    bdate			DATE,
    gen				NVARCHAR(50),
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

--- TABLE 6
DROP TABLE IF EXISTS silver_layer.erp_px_cat_g1v2;
GO

CREATE TABLE silver_layer.erp_px_cat_g1v2 (
    id				NVARCHAR(50),
    cat				NVARCHAR(50),
    subcat			NVARCHAR(50),
    maintenance		NVARCHAR(50),
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO
