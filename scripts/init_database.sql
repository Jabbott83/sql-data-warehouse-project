/*
Script Purpose:
  This script creates a new database named 'DataWarehouse' and drops any existing ones with the same name. It also sets up three schemas, 'bronze', 'silver', 'gold'
WARNING:
  Running this script with drop the 'DataWarehouse' database if it exists. All data will be deleted. Proceed with caution
*/



USE master;
GO

--Drop and recreate the 'DataWarehouse' database
IF EXISTS (SELECT 1 from sys.databases WHERE name = 'DataWarehouse')
BEGIN
    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWarehouse
END;
GO

--Create the 'DataWarehouse' database
CREATE DATABASE DataWarehouse;
GO

USE DataWarehouse;
GO

--Create Schemas
CREATE SCHEMA bronze;
GO
  
CREATE SCHEMA silver;
GO
  
CREATE SCHEMA gold;
GO
