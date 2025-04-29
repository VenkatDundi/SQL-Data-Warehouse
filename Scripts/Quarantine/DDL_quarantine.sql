/*

**************DDL Script: Create Quarantine Tables****************
Script Purpose:
    This script creates tables in the 'quarantine' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'quarantine' Tables
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

