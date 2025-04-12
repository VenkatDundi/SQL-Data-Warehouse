/*
*****************Create DB****************
Script Purpose: This script creates a new database named 'DataWarehouse'. Then, the script creats three schemas within the database: 'bronze', 'silver', and 'gold'.
*/

USE master;
GO

-- Create the 'DataWarehouse' database
CREATE DATABASE DataWarehouse;
GO

USE DataWarehouse;
GO

-- Create Schemas
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO
