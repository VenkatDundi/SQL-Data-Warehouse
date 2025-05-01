/*
Stored Procedure: Load Silver Layer (Bronze -> Silver)
======================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
	
	This file has 8 insert statements in the stored procedure 
	to perform insertion to the Silver layer from Bronze.
===============================================================================
*/

--Exec silver.load_silver;

--drop procedure silver.load_silver

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME; 
    BEGIN TRY
        SET @batch_start_time = GETDATE();

			PRINT '>> Loading Silver Layer';

			SET @start_time = GETDATE();
			PRINT '>> Truncating Table: silver.crm_Customers';
			TRUNCATE TABLE DataWarehouse.silver.crm_Customers;
			PRINT '>> Insert into Table: silver.crm_Customers';
			
			/* Insert Customers table */
			INSERT INTO silver.crm_Customers (				
				cst_CustomerKey,
				cst_Gender,
				cst_Name,
				cst_City,
				cst_StateCode,
				cst_State,
				cst_ZipCode,
				cst_Country,
				cst_Continent,
				cst_Birthday,
				cst_StartDate,
				cst_EndDate,
				cst_IsActive
			)
			select 
					cst_CustomerKey,
					case when lower(cst_Gender) like 'ma%' or lower(cst_Gender) like 'm%' then 'Male'
						when lower(cst_Gender) like 'fe%' or lower(cst_Gender) like 'f%' then 'Female'
						else 'N/A'
					end as cst_Gender,
					cst_Name,
					cst_City,
					case when cst_State = 'Northern Territory' then 'NT'
						when cst_State = 'South Australia' then 'SA'
						when cst_State = 'Victoria' then 'VIC'
						when cst_State = 'Western Australia' then 'WA'
						else cst_StateCode
					end as cst_StateCode,
					case when cst_State COLLATE Latin1_General_BIN like '%[^a-zA-Z0-9 !"#$&''()*+,-./:;<=>?@]%' then 
						TRANSLATE(cst_State, 'øüé�', '    ')
						else cst_State 
					end as cst_State,
					cst_ZipCode,
					cst_Country,
					cst_Continent,
					cst_Birthday,
					cst_StartDate,
					cst_EndDate,
					cst_IsActive
			from bronze.crm_Customers 
			where cst_Birthday BETWEEN '1920-01-01' and GETDATE();

			SET @end_time = GETDATE();
			PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
			PRINT '>> ------------------------------';
	
	
			/* Quarantine BirthDay Errors */
			INSERT INTO quarantine.Customers 
			(CustomerKey, FieldName, FieldValue, ErrorReason)
				SELECT 
					cst_CustomerKey,
					'Birthday',
					CONVERT(NVARCHAR(MAX), cst_Birthday),
					CASE 
						WHEN cst_Birthday < '1920-01-01' THEN 'Birthday before 1900'
						WHEN cst_Birthday > GETDATE() THEN 'Birthday in future'
						else 'Unknown Error'
					END as ErrorReason
				FROM Bronze.crm_Customers
				WHERE 
					cst_Birthday < '1920-01-01' OR cst_Birthday > GETDATE();
	
	
			/* Insert Category table */

			SET @start_time = GETDATE();
			PRINT '>> Truncating Table: silver.erp_Category';
			TRUNCATE TABLE DataWarehouse.silver.erp_Category;
			PRINT '>> Insert into Table: silver.erp_Category';
			
			INSERT INTO silver.erp_Category (				
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
			select erp_CategoryKey,
					TRIM(erp_CategoryName) as erp_CategoryName,
					case when erp_CategoryManager like '%[^A-z0-9 ]%' then
						TRANSLATE(erp_CategoryManager, '@=-', '   ')
						else erp_CategoryManager
					END as erp_CategoryManager,
					erp_CategoryType,
					case when lower(erp_IsSeasonal) like '%n%' then 'No'
							when lower(erp_IsSeasonal) like '%y%' then 'Yes'
							else 'N/A'
					END as erp_IsSeasonal,
					erp_LaunchYear,
					case when erp_CategoryStatus like '%Active%' or erp_CategoryStatus like '%yes%' then 'Active'
							when erp_CategoryStatus like '%Inactive%' or erp_CategoryStatus like '%no%' then 'Inactive'
							else 'N/A'
					END as erp_CategoryStatus,
					erp_CreatedDate,
					erp_UpdatedDate
					from (
					select *, dense_rank() over(partition by erp_CategoryKey order by erp_CreatedDate DESC) as 'ranking' from bronze.erp_Category
					where erp_CategoryName is not NULL and erp_CategoryType is not NULL 
					and erp_CategoryManager not like '%[0-9]%'
					) t
					where t.ranking = 1;
			SET @end_time = GETDATE();
			PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
			PRINT '>> ------------------------------';


			/* Insert Products table */

			SET @start_time = GETDATE();
			PRINT '>> Truncating Table: silver.erp_Products';
			TRUNCATE TABLE DataWarehouse.silver.erp_Products;
			PRINT '>> Insert into Table: silver.erp_Products';
			
			Insert into silver.erp_Products (

				erp_ProductKey,
				erp_ProductName,
				erp_Brand,
				erp_Color,
				erp_UnitCostUSD,
				erp_UnitPriceUSD,
				erp_SubcategoryKey,
				erp_CategoryKey
			)
			select 
				erp_ProductKey,
				erp_ProductName,
				erp_Brand,
				case when erp_color = 'Grey' then 'Gray'
					else erp_color
				END as erp_Color,
				TRY_CAST(REPLACE(REPLACE(erp_UnitCostUSD, '$', '') , ',', '') AS DECIMAL(10,2)) as erp_UnitCostUSD,
				TRY_CAST(REPLACE(REPLACE(erp_UnitPriceUSD, '$', '') , ',', '') AS DECIMAL(10,2)) as erp_UnitPriceUSD,
				erp_SubcategoryKey,
				erp_CategoryKey 
				from bronze.erp_Products
					where TRY_CAST(REPLACE(REPLACE(erp_UnitCostUSD, '$', '') , ',', '') AS DECIMAL(10,2)) IS NOT NULL 
					and TRY_CAST(REPLACE(REPLACE(erp_UnitPriceUSD, '$', '') , ',', '') AS DECIMAL(10,2)) IS NOT NULL;

			SET @end_time = GETDATE();
			PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
			PRINT '>> ------------------------------';


			/* Quarantine Product Price Errors */
			INSERT INTO quarantine.Products 
			(ProductKey, FieldName, FieldValue, ErrorReason)
				SELECT 
					erp_ProductKey,
					'UnitCostUSD',
					erp_UnitCostUSD,
					CASE 
						WHEN substring(erp_UnitCostUSD, 1, len(erp_UnitCostUSD)) like '%(%' THEN 
						'Negative Price has been identified'
						WHEN erp_UnitCostUSD IS NULL THEN 'NULL value for the Price has been identified'
						else 'Unknown Error'
					END as ErrorReason
				FROM Bronze.erp_Products
				WHERE 
					TRY_CAST(REPLACE(REPLACE(erp_UnitCostUSD, '$', ''), ',', '') AS DECIMAL(10,2)) IS NULL;


			/* Insert SubCategory table */

			SET @start_time = GETDATE();
			PRINT '>> Truncating Table: silver.erp_SubCategory';
			TRUNCATE TABLE DataWarehouse.silver.erp_SubCategory;
			PRINT '>> Insert into Table: silver.erp_SubCategory';

			Insert into silver.erp_SubCategory (

				erp_SubcategoryKey,
				erp_Subcategory,
				erp_CategoryKey,
				erp_SubcategoryManager,
				erp_TargetMarketSegment,
				erp_ProductCount,
				erp_AvgUnitPrice,
				erp_SubcategoryStatus,
				erp_CreatedDate,
				erp_UpdatedDate
			)
			select 
				erp_SubcategoryKey,
				erp_Subcategory,
				erp_CategoryKey,
				erp_SubcategoryManager,
				erp_TargetMarketSegment,
				case when erp_SubcategoryKey=205 then 55
					when erp_SubcategoryKey=806 then 51
					else erp_ProductCount
				END as erp_ProductCount,
				erp_AvgUnitPrice,
				erp_SubcategoryStatus,
				erp_CreatedDate,
				erp_UpdatedDate
					from (select *, ROW_NUMBER() over(partition by erp_SubCategoryKey order by erp_CreatedDate DESC) as 'rnum' from bronze.erp_SubCategory
					where erp_CreatedDate IS NOT NULL or erp_UpdatedDate IS NOT NULL) t
				where t.rnum=1;

			SET @end_time = GETDATE();
			PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
			PRINT '>> ------------------------------';


			/* Insert CurrencyData table */

			SET @start_time = GETDATE();
			PRINT '>> Truncating Table: silver.fin_CurrencyData';
			TRUNCATE TABLE DataWarehouse.silver.fin_CurrencyData;
			PRINT '>> Insert into Table: silver.fin_CurrencyData';

			Insert into silver.fin_CurrencyData (

				fin_CurrencyKey,
				fin_Country,
				fin_CurrencyName,
				fin_ISOCode,
				fin_CurrencySymbol,
				fin_ActiveStatus,
				fin_DecimalDigits
			)
			select * from bronze.fin_CurrencyData;
			SET @end_time = GETDATE();
			PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
			PRINT '>> ------------------------------';


			/* Insert ExchangeRates table */

			SET @start_time = GETDATE();
			PRINT '>> Truncating Table: silver.fin_ExchangeRates';
			TRUNCATE TABLE DataWarehouse.silver.fin_ExchangeRates;
			PRINT '>> Insert into Table: silver.fin_ExchangeRates';

			Insert into silver.fin_ExchangeRates (

				fin_Date,
				fin_Currency,
				fin_Exchange
			)
			select * from bronze.fin_ExchangeRates;
			SET @end_time = GETDATE();
			PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
			PRINT '>> ------------------------------';


			/* Insert Stores table  */

			SET @start_time = GETDATE();
			PRINT '>> Truncating Table: silver.pos_Stores';
			TRUNCATE TABLE DataWarehouse.silver.pos_Stores;
			PRINT '>> Insert into Table: silver.pos_Stores';

			Insert into silver.pos_Stores (

				str_StoreKey,
				str_Country,
				str_State,
				str_SquareMeters,
				str_OpenDate
			)
			Select 
				str_StoreKey,
				case when lower(str_Country) like '%can%' then 'Canada'
					when str_Country like '%itl%' then 'Italy'
					when str_Country like '%United Kingdom%' or str_Country like '%UK%' then 'United Kingdom'
					when str_Country like '%United KiStatesngdom%' or str_Country like '%US%' then 'United States'
					else str_Country
				END as str_Country,
				case when str_State COLLATE Latin1_General_BIN like '%[^a-zA-Z0-9 !"#$&''()*+,-./:;<=>?@]%' then 
					TRANSLATE(str_State, 'éü', '  ')
					else str_State 
				END as str_State,
				case when str_StoreKey = 13 then 500
					when str_StoreKey = 59 then 700
					else str_SquareMeters
				END as str_SquareMeters,
				case when str_StoreKey=19 then '2010-09-01'
					else str_OpenDate
				END as str_OpenDate
			from bronze.pos_Stores;
			SET @end_time = GETDATE();
			PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
			PRINT '>> ------------------------------';

			
			/* Insert Sales table */

			SET @start_time = GETDATE();
			PRINT '>> Truncating Table: silver.pos_Sales';
			TRUNCATE TABLE DataWarehouse.silver.pos_Sales;
			PRINT '>> Insert into Table: silver.pos_Sales';

			Insert into silver.pos_Sales (

				sales_OrderNumber,
				sales_LineItem,
				sales_OrderDate,
				sales_DeliveryDate,
				sales_CustomerKey,
				sales_StoreKey,
				sales_ProductKey,
				sales_Quantity,
				sales_CurrencyCode,
				sales_RowKey						-- Unique key to identify during the SCD Implementation
			)
			select * from bronze.pos_Sales;
			SET @end_time = GETDATE();
			PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
			PRINT '>> ------------------------------';

		SET @batch_end_time = GETDATE();
        PRINT '=========================================='
        PRINT 'Loading Silver Layer is Completed';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
        PRINT '=========================================='

	END TRY
	BEGIN CATCH
		PRINT '=========================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=========================================='
	END CATCH
END