/*

**************DDL Script: Create silver Tables****************
Script Purpose:
    This script creates tables in the 'silver' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'silver' Tables
    
    We have added a meta data column: dwh_createDate to identify the timestamps of silver table creation
===============================================================================
*/

IF OBJECT_ID('silver.crm_Customers', 'U') IS NOT NULL   
    DROP TABLE silver.crm_Customers;
GO

CREATE TABLE silver.crm_Customers (                      /* Customers Table */
    cst_CustomerKey INT,
    cst_Gender NVARCHAR(50),
    cst_Name NVARCHAR(50),
    cst_City NVARCHAR(50),
    cst_StateCode NVARCHAR(10),
    cst_State NVARCHAR(50),
    cst_ZipCode NVARCHAR(20),
    cst_Country NVARCHAR(50),
    cst_Continent NVARCHAR(50),
    cst_Birthday DATE,
    cst_StartDate DATE,
    cst_EndDate DATE,
    cst_IsActive INT,
    dwh_createDate DATETIME2 default GETDATE()
);
GO

IF OBJECT_ID('silver.pos_Sales', 'U') IS NOT NULL              
    DROP TABLE silver.pos_Sales;
GO

CREATE TABLE silver.pos_Sales (                              /* Sales Table */
    sales_OrderNumber INT,
    sales_LineItem INT,
    sales_OrderDate DATE,
    sales_DeliveryDate DATE,
    sales_CustomerKey INT,
    sales_StoreKey INT,
    sales_ProductKey INT,
    sales_Quantity INT,
    sales_CurrencyCode NVARCHAR(10),
    sales_RowKey NVARCHAR(15),
    dwh_createDate DATETIME2 default GETDATE()
);
GO

IF OBJECT_ID('silver.pos_Stores', 'U') IS NOT NULL
    DROP TABLE silver.pos_Stores;
GO

CREATE TABLE silver.pos_Stores (                            /* Stores Table */
    str_StoreKey INT,
    str_Country NVARCHAR(50),
    str_State NVARCHAR(50),
    str_SquareMeters INT,
    str_OpenDate DATE,
    dwh_createDate DATETIME2 default GETDATE()
);
GO

IF OBJECT_ID('silver.fin_ExchangeRates', 'U') IS NOT NULL
    DROP TABLE silver.fin_ExchangeRates;
GO

CREATE TABLE silver.fin_ExchangeRates (                    /* ExchangeRates Table */
    fin_Date DATE,
    fin_Currency NVARCHAR(10),
    fin_Exchange DECIMAL(10, 4),
    dwh_createDate DATETIME2 default GETDATE()
);
GO

IF OBJECT_ID('silver.fin_CurrencyData', 'U') IS NOT NULL
    DROP TABLE silver.fin_CurrencyData;
GO

CREATE TABLE silver.fin_CurrencyData (                     /* CurrencyData Table */
    fin_CurrencyKey INT,
    fin_Country NVARCHAR(100),
    fin_CurrencyName NVARCHAR(100),
    fin_ISOCode CHAR(3),
    fin_CurrencySymbol NVARCHAR(5),
    fin_ActiveStatus NVARCHAR(6),
    fin_DecimalDigits INT,
    dwh_createDate DATETIME2 default GETDATE()
);
GO


IF OBJECT_ID('silver.erp_Products', 'U') IS NOT NULL
    DROP TABLE silver.erp_Products;
GO

CREATE TABLE silver.erp_Products (                        /* Products Table */
    erp_ProductKey INT,
    erp_ProductName NVARCHAR(155),
    erp_Brand NVARCHAR(80),
    erp_Color VARCHAR(50),
    erp_UnitCostUSD DECIMAL(10, 2),
    erp_UnitPriceUSD DECIMAL(10, 2),
    erp_SubcategoryKey INT,
    erp_CategoryKey INT,
    dwh_createDate DATETIME2 default GETDATE()
);
GO




IF OBJECT_ID('silver.erp_SubCategory', 'U') IS NOT NULL
    DROP TABLE silver.erp_SubCategory;
GO

CREATE TABLE silver.erp_SubCategory (                      /* SubCategory Table */
    erp_SubcategoryKey INT,
    erp_Subcategory NVARCHAR(50),
    erp_CategoryKey INT,
    erp_SubcategoryManager NVARCHAR(50),
    erp_TargetMarketSegment NVARCHAR(50),
    erp_ProductCount INT,
    erp_AvgUnitPrice DECIMAL(10, 2),
    erp_SubcategoryStatus VARCHAR(20),
    erp_CreatedDate DATE,
    erp_UpdatedDate DATE,
    dwh_createDate DATETIME2 default GETDATE()
);
GO



IF OBJECT_ID('silver.erp_Category', 'U') IS NOT NULL
    DROP TABLE silver.erp_Category;
GO

CREATE TABLE silver.erp_Category (                        /* Category Table */
    erp_CategoryKey INT,
    erp_CategoryName NVARCHAR(50),
    erp_CategoryManager NVARCHAR(50),
    erp_CategoryType NVARCHAR(50),
    erp_IsSeasonal NVARCHAR(10),
    erp_LaunchYear INT,
    erp_CategoryStatus NVARCHAR(20),
    erp_CreatedDate DATETIME,
    erp_UpdatedDate DATETIME,
    dwh_createDate DATETIME2 default GETDATE()
);
GO
