

/* Products Table Bulk Insert */
TRUNCATE TABLE DataWarehouse.bronze.erp_Products;
BULK INSERT 
	DataWarehouse.bronze.erp_Products
	FROM 'C:\Users\gnani\Downloads\DWH_Gpt\Global_Electronics_Retailer\Products.csv'

	WITH(	ROWTERMINATOR = '\n',
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			FORMAT = 'CSV',
			CODEPAGE = '65001',					/* To keep the formatting of encoding - Special characters */ 
			ERRORFILE = 'C:\Users\gnani\Downloads\DWH_Gpt\Global_Electronics_Retailer\Products',	
												/* Save the Error rows in this file - Catch Errors */
			TABLOCK								/* Lock the table during this operation */
		);

--select * from bronze.erp_Products;



/* Exchange Rates Table Bulk Insert */
TRUNCATE TABLE DataWarehouse.bronze.fin_ExchangeRates;
BULK INSERT 
	DataWarehouse.bronze.fin_ExchangeRates
	FROM 'C:\Users\gnani\Downloads\DWH_Gpt\Global_Electronics_Retailer\ExchangeRates.csv'

	WITH(	ROWTERMINATOR = '\n',
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			FORMAT = 'CSV',
			CODEPAGE = '65001',					/* To keep the formatting of encoding - Special characters */ 
			ERRORFILE = 'C:\Users\gnani\Downloads\DWH_Gpt\Global_Electronics_Retailer\ExchangeRates',	
												/* Save the Error rows in this file - Catch Errors */
			TABLOCK								/* Lock the table during this operation */
		);

--select * from bronze.fin_ExchangeRates;


/*BULK INSERT pos_Stores
	FROM 'C:\Users\gnani\Downloads\DWH_Gpt\Global_Electronics_Retailer\Stores.csv'

	WITH(	ROWTERMINATOR = '\n',
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			FORMAT = 'CSV',
			CODEPAGE = '65001',					/* To keep the formatting of encoding - Special characters */ 
			ERRORFILE = 'C:\Users\gnani\Downloads\DWH_Gpt\Global_Electronics_Retailer\Stores',	/* Save the Error rows in this file - Catch Errors */
			KEEPNULLS,							/* Keep Null values */
			TABLOCK								/* Lock the table during this operation */
		);  ------------ COPY from other database*/

/* Stores Table - Copy of a relation */

BEGIN TRY
    BEGIN TRANSACTION;

    DECLARE @rowcount_before INT = (SELECT COUNT(*) FROM DataWarehouse.bronze.pos_Stores);
    
    TRUNCATE TABLE DataWarehouse.bronze.pos_Stores;

    INSERT INTO DataWarehouse.bronze.pos_Stores
    SELECT * FROM Practicedb.dbo.pos_Stores;

    DECLARE @rowcount_after INT = (SELECT COUNT(*) FROM DataWarehouse.bronze.pos_Stores);

    PRINT 'Rows before: ' + CAST(@rowcount_before AS VARCHAR);
    PRINT 'Rows after: ' + CAST(@rowcount_after AS VARCHAR);

    COMMIT TRANSACTION;
END TRY
BEGIN CATCH
    ROLLBACK TRANSACTION;
    PRINT 'Error: ' + ERROR_MESSAGE();
END CATCH;


--select * from DataWarehouse.bronze.pos_Stores;


alter table bronze.crm_Customers alter column cst_StateCode NVARCHAR(50)

/* Customers Table Bulk Insert */
TRUNCATE TABLE DataWarehouse.bronze.crm_Customers;
BULK INSERT 
	DataWarehouse.bronze.crm_Customers
	FROM 'C:\Users\gnani\Downloads\DWH_Gpt\Global_Electronics_Retailer\Customers.csv'

	WITH(	ROWTERMINATOR = '\n',
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			FORMAT = 'CSV',
			CODEPAGE = '65001',					/* To keep the formatting of encoding - Special characters */ 
			ERRORFILE = 'C:\Users\gnani\Downloads\DWH_Gpt\Global_Electronics_Retailer\Customers',	
			KEEPNULLS,									/* Save the Error rows in this file - Catch Errors */
			TABLOCK								/* Lock the table during this operation */
		);

--select * from bronze.crm_Customers;

Declare @JSON varchar(max)
SELECT @JSON=BulkColumn
FROM OPENROWSET (BULK 'C:\Users\gnani\Downloads\DWH_Gpt\Global_Electronics_Retailer\Category.json', SINGLE_CLOB) import
If (ISJSON(@JSON)=1)
Print 'It is a valid JSON'				/* Check if the file is a valid JSON */
ELSE
Print 'Error in JSON format'


/*
Declare @JSON varchar(max)				/* Transform to Key-Value Pairs */
SELECT @JSON=BulkColumn
FROM OPENROWSET (BULK 'C:\Users\gnani\Downloads\DWH_Gpt\Global_Electronics_Retailer\Category.json', SINGLE_CLOB) import
SELECT *
FROM OPENJSON (@JSON)
*/


/* Category Table Bulk Insert */

TRUNCATE TABLE DataWarehouse.bronze.erp_Category;

DECLARE @json NVARCHAR(MAX);
SELECT @json = BulkColumn						/* Import entire JSON file as a string */
FROM OPENROWSET (
    BULK 'C:\Users\gnani\Downloads\DWH_Gpt\Global_Electronics_Retailer\Category.json',
    SINGLE_CLOB
) AS [import];

-- Insert with proper JSON key mappings
INSERT INTO DataWarehouse.bronze.erp_Category (
    erp_CategoryKey,
    erp_CategoryName,
    erp_CategoryManager,
    erp_CategoryType,
    erp_IsSeasonal,
    erp_LaunchYear,
    erp_CategoryStatus,
    erp_CreatedDate,
    erp_UpdatedDate
)
SELECT *
FROM OPENJSON(@json)									/* Json output from a string to Tabular format */
WITH (
    erp_CategoryKey INT              '$."cat key"',					/* cat Key ---> Name in JSON */
    erp_CategoryName NVARCHAR(50)    '$."cat"',
    erp_CategoryManager NVARCHAR(50) '$."mgr"',
    erp_CategoryType NVARCHAR(50)    '$."cat type"',
    erp_IsSeasonal NVARCHAR(10)      '$."seasonal"',
    erp_LaunchYear INT               '$."Launched"',
    erp_CategoryStatus NVARCHAR(20)  '$."CategoryStatus"',
    erp_CreatedDate DATETIME         '$."Created"',
    erp_UpdatedDate DATETIME         '$."Updated"'
);


--select * from bronze.erp_Category;



select * from bronze.erp_SubCategory;

/* SubCategory Table Insert Procedure */

CREATE PROCEDURE bronze.InsertSubcategory
    @erp_SubcategoryKey INT,
    @erp_Subcategory NVARCHAR(100),
    @erp_CategoryKey INT,
    @erp_SubcategoryManager NVARCHAR(100),
    @erp_TargetMarketSegment NVARCHAR(100),
    @erp_ProductCount INT,
    @erp_AvgUnitPrice DECIMAL(10,2),
    @erp_SubcategoryStatus NVARCHAR(20),
    @erp_CreatedDate DATETIME,
    @erp_UpdatedDate DATETIME
AS
BEGIN
    INSERT INTO bronze.erp_Subcategory (
        erp_SubcategoryKey, erp_Subcategory, erp_CategoryKey,
        erp_SubcategoryManager, erp_TargetMarketSegment, erp_ProductCount,
        erp_AvgUnitPrice, erp_SubcategoryStatus, erp_CreatedDate, erp_UpdatedDate
    )
    VALUES (
        @erp_SubcategoryKey, @erp_Subcategory, @erp_CategoryKey,
        @erp_SubcategoryManager, @erp_TargetMarketSegment, @erp_ProductCount,
        @erp_AvgUnitPrice, @erp_SubcategoryStatus, @erp_CreatedDate, @erp_UpdatedDate
    )
END

select * from bronze.erp_SubCategory;

select * from bronze.fin_CurrencyData;

alter table bronze.fin_CurrencyData alter column fin_CurrencySymbol NVARCHAR(10)


/* Sales Table Insert Procedure */
alter table bronze.pos_Sales add [sales_RowKey] NVARCHAR(15)

select max([sales_OrderNumber]), max(sales_RowKey) from bronze.pos_Sales;


select * from bronze.pos_Sales where sales_OrderNumber > 2243000;

/*truncate table bronze.pos_Sales;*/

select * from bronze.crm_Customers;