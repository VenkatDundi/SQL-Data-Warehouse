/*

Quality Checks and Transformation (Bronze to Silver)
===============================================================================
Script Purpose:
    This script performs various quality checks for data consistency, accuracy, 
    and standardization across the 'bronze' layer. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

	These checks helps in cleansing and transforming data to be ingested to silver layer
===============================================================================
*/


/* Find all column names in a table */

 SELECT name
   FROM sys.columns
   WHERE object_id = OBJECT_ID('silver.pos_Sales')


/* CUSTOMERS TABLE CHECK */

select top 100 * from bronze.crm_Customers;


select * from bronze.crm_Customers where cst_CustomerKey in (select cst_CustomerKey as 'Customer ID'
from bronze.crm_Customers
group by cst_CustomerKey
having count(*) > 1)

-- Duplicate or Null check on Customer ID: No Errors

select cst_CustomerKey as 'Customer ID'
from bronze.crm_Customers
group by cst_CustomerKey
having count(*) > 1 or cst_CustomerKey IS NULL;


-- Distinct Gender Check: Found Inconsistencies

select distinct cst_Gender from bronze.crm_Customers;

-- Name Validation check: No Errors

select * from bronze.crm_Customers where cst_Name != TRIM(cst_Name)
select * from bronze.crm_Customers where cst_City != TRIM(cst_City)
select * from bronze.crm_Customers where cst_StateCode != TRIM(cst_StateCode)
select * from bronze.crm_Customers where cst_State != TRIM(cst_State)
select * from bronze.crm_Customers where cst_Country != TRIM(cst_Country)
select * from bronze.crm_Customers where cst_Continent != TRIM(cst_Continent)

-- State Code check: Found Inconsistencies

select distinct cst_StateCode 
from bronze.crm_Customers 
where left(cst_StateCode, 2) = replicate(left(cst_StateCode, 1), 2);

select cst_State, count(distinct cst_StateCode) as 'count' 
from bronze.crm_Customers
group by cst_State
having count(distinct cst_StateCode) > 1

select distinct cst_StateCode, cst_State from bronze.crm_Customers where cst_State in (select cst_State as 'count' from bronze.crm_Customers
group by cst_State
having count(distinct cst_StateCode) > 1) order by cst_State;

-- State check: Found Inconsistencies

select distinct cst_State 
from bronze.crm_Customers
where cst_State like '%[^a-zA-Z0-9 !"%#$&’’()*+,-./:;<=>?@]%';

SELECT 
    *,
    	REPLACE(
			REPLACE(
				CAST(cst_City AS VARCHAR(100)) COLLATE SQL_Latin1_General_CP1253_CI_AI, '''', ''), '-', ' ')
FROM bronze.crm_Customers where cst_Name like '%Melan%';

SELECT 
    cst_Name,
    	REPLACE(
			REPLACE(
				CAST(cst_Name AS VARCHAR(100)) COLLATE SQL_Latin1_General_CP1253_CI_AI, '''', ''), '-', ' ')
FROM bronze.crm_Customers;

SELECT 
    cst_State,
    	REPLACE(
			REPLACE(
				CAST(cst_State AS VARCHAR(100)) COLLATE SQL_Latin1_General_CP1253_CI_AI, '''', ''), '-', ' ')
FROM bronze.crm_Customers where cst_State like '%[^a-zA-Z0-9 !"%#$&''()*+,-./:;<=>?@]%';

SELECT 
    cst_City,
    	REPLACE(
			REPLACE(
				CAST(cst_City AS VARCHAR(100)) COLLATE SQL_Latin1_General_CP1253_CI_AI, '''', ''), '-', ' ')
FROM bronze.crm_Customers where cst_City like '%[^a-zA-Z0-9 !"%#$&''()*+,-./:;<=>?@]%';


-- Zip Code check: No Errors

select * from bronze.crm_Customers
where cst_ZipCode like '%-%'
or cst_ZipCode LIKE '%[^a-zA-Z0-9 !"#$&''()*+,-./:;<=>?@]%';


-- Country and Continenet check: No Errors

select * from bronze.crm_Customers
where cst_Country like '%[^a-zA-Z0-9 !"#$&''()*+,-./:;<=>?@]%' or cst_Continent like '%[^a-zA-Z0-9 !"#$&''()*+,-./:;<=>?@]%';


-- BirthDay check: Found Inconsistencies - 2 records to be quarantined

select cst_CustomerKey, cst_Birthday from bronze.crm_Customers
where year(cst_Birthday) > 2020 or year(cst_Birthday) < 1925;

-- IsActive check:
select distinct cst_IsActive from bronze.crm_Customers;


-- Update State Name
UPDATE bronze.crm_Customers
SET cst_State = TRANSLATE(cst_State, '-''øüé�', '   ')
WHERE cst_State LIKE '%[^a-zA-Z0-9 !"#$&''()*+,-./:;<=>?@]%';




select 
	cst_CustomerKey,
	case when lower(cst_Gender) like 'ma%' or lower(cst_Gender) like 'm%' then 'Male'
		when lower(cst_Gender) like 'fe%' or lower(cst_Gender) like 'f%' then 'Female'
		else 'N/A'
	end as cst_Gender,

	case when cst_State = 'Northern Territory' then 'NT'
		when cst_State = 'South Australia' then 'SA'
		when cst_State = 'Victoria' then 'VIC'
		when cst_State = 'Western Australia' then 'WA'
		else cst_StateCode
	end as cst_StateCode
	
from bronze.crm_Customers;


------------------------------------------------------------------------------------------------

/* CATEGORY TABLE CHECK */

select top 100 * from bronze.erp_Category;


-- Check Null or Duplicates on Category key: Found Inconsistencies

select erp_CategoryKey as 'count'
from bronze.erp_Category
group by erp_CategoryKey
having count(*) > 1;

select * from bronze.erp_Category 
where erp_CategoryKey in (
			select erp_CategoryKey as 'count'
				from bronze.erp_Category
				group by erp_CategoryKey
				having count(*) > 1);

-- Check Category Name

select * from bronze.erp_Category
where erp_CategoryName != TRIM(erp_CategoryName) or erp_CategoryName IS NULL;

-- Check Category Manager

select * from bronze.erp_Category
where erp_CategoryManager != TRIM(erp_CategoryManager);

select TRANSLATE(erp_categoryManager, '@=-', '   ')
from bronze.erp_Category where erp_categoryManager like '%[^A-z0-9 ]%';

-- Check Category Type

select * from bronze.erp_Category
where erp_CategoryType != TRIM(erp_CategoryType) or erp_CategoryType IS NULL;

-- Check Seasonal

select * from bronze.erp_Category
where erp_IsSeasonal != TRIM(erp_IsSeasonal) or erp_IsSeasonal IS NULL;

select distinct erp_IsSeasonal 
from bronze.erp_Category;



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
		and erp_CategoryManager not like '%[0-9]%') t
		where t.ranking = 1;

------------------------------------------------------------------------------------------------

/* PRODUCTS TABLE CHECK */

select * from bronze.erp_Products;


-- Nulls or duplicates: No Errors

select count(*), erp_ProductKey
from bronze.erp_Products
group by erp_ProductKey
having count(*) > 1;

-- Product Name: No Errors

select * from bronze.erp_Products where erp_ProductName like '%[^A-z0-9 ]%';

select * from bronze.erp_Products where erp_ProductName != TRIM(erp_ProductName)

-- Brand: No Errors

select distinct erp_Brand
from bronze.erp_Products;

select distinct erp_Brand 
from bronze.erp_Products where erp_brand != TRIM(erp_brand);

-- Color: Found Inconsistency

select distinct erp_Color
from bronze.erp_Products;

case when erp_color = 'Grey' then 'Gray'
		else erp_color
	END as erp_Color

-- UnitCost & Unit Price: Many Errors (Negatives, NULL) || Updated (,) separator 

select substring(erp_UnitCostUSD, 2, len(erp_UnitCostUSD)) 
from bronze.erp_Products
where substring(erp_UnitCostUSD, 2, len(erp_UnitCostUSD)) like '%0.0%';

select * from bronze.erp_Products
where substring(erp_UnitCostUSD, 1, len(erp_UnitCostUSD)) like '%(%' 
or substring(erp_UnitCostUSD, 2, len(erp_UnitCostUSD)) like '%0.0%';

select CAST(substring(erp_UnitCostUSD, 2, len(erp_UnitCostUSD)) as Numeric(10,2)) 
from bronze.erp_Products
where substring(erp_UnitCostUSD, 1, len(erp_UnitCostUSD)) not like '%(%' or substring(erp_UnitCostUSD, 2, len(erp_UnitCostUSD)) not like '%0.0%';

select * from bronze.erp_Products
where substring(erp_UnitCostUSD, 2, len(erp_UnitCostUSD)) like '%[^$ . 0-9]%';

select 
	TRY_CAST(REPLACE(REPLACE(erp_UnitPriceUSD, '$', '') , ',', '') AS DECIMAL(10,2))
	from bronze.erp_Products
	where TRY_CAST(REPLACE(REPLACE(erp_UnitPriceUSD, '$', '') , ',', '') AS DECIMAL(10,2)) IS NOT NULL;
		
select * from bronze.erp_Products where erp_UnitCostUSD like '%,%';
select * from bronze.erp_Products where erp_UnitPriceUSD like '%,%';



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


-----------------------------------------------------------------------------------


/* SUB CATEGORY TABLE CHECK */



select top 100 * from bronze.erp_SubCategory;

-- Nulls or Duplicates

select erp_SubCategoryKey
from bronze.erp_SubCategory
where erp_CreatedDate IS NOT NULL or erp_UpdatedDate IS NOT NULL
group by erp_SubCategoryKey
having count(*) > 1 or erp_SubCategoryKey IS NULL

--  SubCategory Check

select erp_Subcategory 
from bronze.erp_SubCategory
where erp_TargetMarketSegment != TRIM(erp_TargetMarketSegment)


-- Product Count

select *
from bronze.erp_SubCategory
where erp_ProductCount <= 0;

-- Avg Unit Price
select *
from bronze.erp_SubCategory
where erp_AvgUnitPrice <= 0;


select 
	erp_SubcategoryKey,
	erp_Subcategory,
	erp_CategoryKey,
	erp_SubcategoryManager,
	erp_TargetMarketSegment,
	case when erp_SubcategoryKey=205 then 55
		when erp_SubcategoryKey=806 then 51
		else erp_SubcategoryKey
	END as erp_SubcategoryKey,
	erp_AvgUnitPrice,
	erp_SubcategoryStatus,
	erp_CreatedDate,
	erp_UpdatedDate
	from (select *, ROW_NUMBER() over(partition by erp_SubCategoryKey order by erp_CreatedDate DESC) as 'rnum' from bronze.erp_SubCategory
	where erp_CreatedDate IS NOT NULL or erp_UpdatedDate IS NOT NULL) t
	where t.rnum=1;



-----------------------------------------------------------------------------------


/* CURRENCY DATA TABLE CHECK */

select * from bronze.fin_CurrencyData;

select * from bronze.fin_ExchangeRates;

select distinct fin_Currency from bronze.fin_ExchangeRates;


-----------------------------------------------------------------------------------


/* STORES DATA TABLE CHECK */

select * from bronze.pos_stores;

select str_StoreKey, count(*) as 'ctr'
from bronze.pos_Stores
group by str_StoreKey
having count(*) > 1 or str_StoreKey IS NULL;


select distinct str_Country
from bronze.pos_Stores;

select str_State 
from bronze.pos_Stores
where str_State COLLATE Latin1_General_BIN LIKE '%[^a-zA-Z0-9 !"#$&''()*+,-./:;<=>?@]%'; 

Select TRANSLATE(str_State, 'éü', '  ')
from bronze.pos_Stores
WHERE str_State COLLATE Latin1_General_BIN LIKE '%[^a-zA-Z0-9 !"#$&''()*+,-./:;<=>?@]%';

select case when str_State COLLATE Latin1_General_BIN like '%[^a-zA-Z0-9 !"#$&''()*+,-./:;<=>?@]%' then 
		TRANSLATE(str_State, 'éü', '  ')
		else str_State 
	end as str_State
	from bronze.pos_Stores

select str_StoreKey, str_SquareMeters
from bronze.pos_Stores
where str_SquareMeters <= 0;

select * from bronze.pos_Stores
where year(str_OpenDate) > 2020 and year(str_OpenDate) < 1990 or str_OpenDate IS NULL;



Select str_StoreKey,
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



-----------------------------------------------------------------------------------

/* SALES DATA TABLE CHECK */

select * from bronze.pos_Sales;

select sales_OrderNumber, count(*)
from bronze.pos_Sales
group by sales_OrderNumber
having count(*) > 1;

select distinct(sales_LineItem)
from bronze.pos_Sales;

select * from bronze.pos_Sales where sales_OrderDate IS NULL;

select * from bronze.pos_Sales 
where sales_StoreKey < 0 or sales_ProductKey < 0 or sales_Quantity <= 0;

select * from bronze.pos_Sales where sales_DeliveryDate is null;

select distinct sales_StoreKey from bronze.pos_Sales
