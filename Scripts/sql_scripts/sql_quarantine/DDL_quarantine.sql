/*

**************DDL Script: Create Quarantine Tables****************
Script Purpose:
    This script creates tables in the 'quarantine' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'quarantine' Tables


These tables helps in capturing the Invalid data records to be further investigated by the business
===============================================================================
*/

IF OBJECT_ID('quarantine.Customers', 'U') IS NOT NULL   
    DROP TABLE quarantine.Customers;
GO

CREATE TABLE quarantine.Customers (                      /* Customers Table */
    QuarantineID INT IDENTITY(1,1) PRIMARY KEY,
    CustomerKey INT,
    FieldName VARCHAR(100),
    FieldValue NVARCHAR(MAX),
    ErrorReason VARCHAR(255),
    LoadDate DATETIME DEFAULT GETDATE()
);
GO



IF OBJECT_ID('quarantine.Products', 'U') IS NOT NULL   
    DROP TABLE quarantine.Products;
GO

CREATE TABLE quarantine.Products (                      /* Customers Table */
    QuarantineID INT IDENTITY(1,1) PRIMARY KEY,
    ProductKey INT,
    FieldName VARCHAR(100),
    FieldValue NVARCHAR(MAX),
    ErrorReason VARCHAR(255),
    LoadDate DATETIME DEFAULT GETDATE()
);
GO

