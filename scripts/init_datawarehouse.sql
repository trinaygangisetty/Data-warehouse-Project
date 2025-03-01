/*
=============================================================
Database Creation and Schema Setup
=============================================================
Overview:
    This script initializes a database named 'DataWarehouse'.  
    If the database is already present, it will be removed and recreated.  
    The script also defines three schemas within the database: 'bronze', 'silver', and 'gold'.  

Caution:
    Executing this script will erase the 'DataWarehouse' database if it exists.  
    All existing data will be lost permanently.  
    Ensure that you have necessary backups before proceeding.
*/

--- switching to master database to create a new database
USE master;
GO

--- checking if DataWarehouse database already exists?

--- TRY BLOCK
BEGIN TRY
	IF DATABASEPROPERTYEX('DataWarehouse', 'Version') IS NOT NULL
	BEGIN
		-- Setting to SINGLE_USER mode to terminate active connections
		ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;

		-- Dropping the Database
		DROP DATABASE DataWarehouse;

		PRINT 'Database DataWarehouse has been dropped Successfully.';
	END

	ELSE
	BEGIN
		PRINT 'Database DataWarehouse doenopt exist.';
	END
END TRY

--- CATCH BLOCK
BEGIN CATCH
	PRINT 'An error occured while dropping the Database.';
	PRINT ERROR_MESSAGE(); -- Captures the actual error message
END CATCH;
GO

--- creating 'warehouse' Database inside master
CREATE DATABASE DataWarehouse;
GO

--- switching to newly created database
USE DataWarehouse;
GO

--- creating schemas for three different layers "bronze", "silver", "gold"
CREATE SCHEMA bronze_layer;
GO -- seperator
CREATE SCHEMA silver_layer;
GO
CREATE SCHEMA gold_layer;
GO
