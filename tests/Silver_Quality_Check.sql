/*

Quality Checks and Transformation (Post Ingestion in Silver Layer)
===============================================================================
Script Purpose:
    This script performs various quality checks for data consistency, accuracy, 
    and standardization across the 'silver' layer. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

	These checks helps in verifying clean ingestion to the silver layer
===============================================================================
*/


select * from quarantine.Products

select * from quarantine.Customers


/* Find all column names in a table */

 SELECT name
   FROM sys.columns
   WHERE object_id = OBJECT_ID('silver.pos_Sales')


/* CUSTOMERS TABLE CHECK */

select top 100 * from silver.crm_Customers order by cst_CustomerKey;


select * from silver.crm_Customers where cst_CustomerKey in (select cst_CustomerKey as 'Customer ID'
from silver.crm_Customers
group by cst_CustomerKey
having count(*) > 1)

-- Duplicate or Null check on Customer ID: No Errors

select cst_CustomerKey as 'Customer ID'
from silver.crm_Customers
group by cst_CustomerKey
having count(*) > 1 or cst_CustomerKey IS NULL;


-- Distinct Gender Check: No Errors

select distinct cst_Gender from silver.crm_Customers;

-- Name Validation check: No Errors

select * from silver.crm_Customers where cst_Name != TRIM(cst_Name)
select * from silver.crm_Customers where cst_City != TRIM(cst_City)
select * from silver.crm_Customers where cst_StateCode != TRIM(cst_StateCode)
select * from silver.crm_Customers where cst_State != TRIM(cst_State)
select * from silver.crm_Customers where cst_Country != TRIM(cst_Country)
select * from silver.crm_Customers where cst_Continent != TRIM(cst_Continent)

-- State Code check: Found Inconsistencies ---> Fixed in  the Silver SP ('North' vs 'Northern')

select distinct cst_StateCode 
from silver.crm_Customers 
where left(cst_StateCode, 2) = replicate(left(cst_StateCode, 1), 2);

select cst_State, count(distinct cst_StateCode) as 'count' 
from silver.crm_Customers
group by cst_State
having count(distinct cst_StateCode) > 1

select distinct cst_StateCode, cst_State from silver.crm_Customers where cst_State in (select cst_State as 'count' from silver.crm_Customers
group by cst_State
having count(distinct cst_StateCode) > 1) order by cst_State;

select * from silver.crm_Customers where cst_State like '%Northern Territory%';

select *, case when cst_State = 'Northern Territory' then 'NT'
		else 'X'
		END as cst_StateCode
		from silver.crm_Customers where cst_State = 'Northern Territory';

select * from silver.crm_Customers
where cst_State != TRIM(cst_State)

-- State check: Found Inconsistencies : Some of the special characters left unchanged

select distinct cst_State 
from silver.crm_Customers
where cst_State like '%[^a-zA-Z0-9 !"%#$&’’()*+,-./:;<=>?@]%';

select 
	case when cst_State COLLATE Latin1_General_BIN like '%[^a-zA-Z0-9 !"#$&''()*+,-./:;<=>?@]%' then 
						TRANSLATE(cst_State, '�', ' ')
						else cst_State 
					end as cst_State
		from silver.crm_Customers
		where cst_State like '%[^a-zA-Z0-9 !"#$&''()*+,-./:;<=>?@]%';


-- Zip Code check: No Errors

select * from silver.crm_Customers
where cst_ZipCode like '%-%'
or cst_ZipCode LIKE '%[^a-zA-Z0-9 !"#$&''()*+,-./:;<=>?@]%';


-- Country and Continenet check: No Errors

select * from silver.crm_Customers
where cst_Country like '%[^a-zA-Z0-9 !"#$&''()*+,-./:;<=>?@]%' or cst_Continent like '%[^a-zA-Z0-9 !"#$&''()*+,-./:;<=>?@]%';


-- BirthDay check: Found Inconsistencies - 2 records to be quarantined

select cst_CustomerKey, cst_Birthday from silver.crm_Customers
where year(cst_Birthday) > 2020 or year(cst_Birthday) < 1925;

-- IsActive check:
select distinct cst_IsActive from silver.crm_Customers;


------------------------------------------------------------------------------------------------

/* CATEGORY TABLE CHECK */

select top 100 * from silver.erp_Category;


-- Check Null or Duplicates on Category key: Found Inconsistencies

select erp_CategoryKey as 'count'
from silver.erp_Category
group by erp_CategoryKey
having count(*) > 1;

select * from silver.erp_Category 
where erp_CategoryKey in (
			select erp_CategoryKey as 'count'
				from silver.erp_Category
				group by erp_CategoryKey
				having count(*) > 1);

-- Check Category Name

select * from silver.erp_Category
where erp_CategoryName != TRIM(erp_CategoryName) or erp_CategoryName IS NULL;

-- Check Category Manager

select * from silver.erp_Category
where erp_CategoryManager != TRIM(erp_CategoryManager);

select TRANSLATE(erp_categoryManager, '@=-', '   ')
from silver.erp_Category where erp_categoryManager like '%[^A-z0-9 ]%';

-- Check Category Type

select * from silver.erp_Category
where erp_CategoryType != TRIM(erp_CategoryType) or erp_CategoryType IS NULL;

-- Check Seasonal

select * from silver.erp_Category
where erp_IsSeasonal != TRIM(erp_IsSeasonal) or erp_IsSeasonal IS NULL;

select distinct erp_IsSeasonal 
from silver.erp_Category;


------------------------------------------------------------------------------------------------

/* PRODUCTS TABLE CHECK */

select * from silver.erp_Products;


-- Nulls or duplicates: No Errors

select count(*), erp_ProductKey
from silver.erp_Products
group by erp_ProductKey
having count(*) > 1;

-- Product Name: No Errors

select * from silver.erp_Products where erp_ProductName like '%[^A-z0-9 ]%';

select * from silver.erp_Products where erp_ProductName != TRIM(erp_ProductName)

-- Brand: No Errors

select distinct erp_Brand
from silver.erp_Products;

select distinct erp_Brand 
from silver.erp_Products where erp_brand != TRIM(erp_brand);

-- Color: No Error

select distinct erp_Color
from silver.erp_Products;

-- UnitCost & Unit Price: No Errors

	
select * from silver.erp_Products where erp_UnitCostUSD like '%,%';
select * from silver.erp_Products where erp_UnitPriceUSD like '%,%';

select * from silver.erp_Products where erp_UnitCostUSD like '%$%';
select * from silver.erp_Products where erp_UnitPriceUSD like '%$%';

-----------------------------------------------------------------------------------


/* SUB CATEGORY TABLE CHECK */



select top 100 * from silver.erp_SubCategory;

-- Nulls or Duplicates

select erp_SubCategoryKey
from silver.erp_SubCategory
where erp_CreatedDate IS NOT NULL or erp_UpdatedDate IS NOT NULL
group by erp_SubCategoryKey
having count(*) > 1 or erp_SubCategoryKey IS NULL

--  SubCategory Check

select erp_Subcategory 
from silver.erp_SubCategory
where erp_TargetMarketSegment != TRIM(erp_TargetMarketSegment)


-- Product Count

select *
from silver.erp_SubCategory
where erp_ProductCount <= 0;

-- Avg Unit Price
select *
from silver.erp_SubCategory
where erp_AvgUnitPrice <= 0;

-----------------------------------------------------------------------------------


/* CURRENCY DATA TABLE CHECK */

select * from silver.fin_CurrencyData;

select * from silver.fin_ExchangeRates;

select distinct fin_Currency from silver.fin_ExchangeRates;


-----------------------------------------------------------------------------------


/* STORES DATA TABLE CHECK */

select * from silver.pos_stores;

select str_StoreKey, count(*) as 'ctr'
from silver.pos_Stores
group by str_StoreKey
having count(*) > 1 or str_StoreKey IS NULL;


select distinct str_Country
from silver.pos_Stores;

select str_State 
from silver.pos_Stores
where str_State COLLATE Latin1_General_BIN LIKE '%[^a-zA-Z0-9 !"#$&''()*+,-./:;<=>?@]%'; 

Select TRANSLATE(str_State, 'éü', '  ')
from silver.pos_Stores
WHERE str_State COLLATE Latin1_General_BIN LIKE '%[^a-zA-Z0-9 !"#$&''()*+,-./:;<=>?@]%';

select case when str_State COLLATE Latin1_General_BIN like '%[^a-zA-Z0-9 !"#$&''()*+,-./:;<=>?@]%' then 
		TRANSLATE(str_State, 'éü', '  ')
		else str_State 
	end as str_State
	from silver.pos_Stores

select str_StoreKey, str_SquareMeters
from silver.pos_Stores
where str_SquareMeters <= 0;

select * from silver.pos_Stores
where year(str_OpenDate) > 2020 and year(str_OpenDate) < 1990 or str_OpenDate IS NULL;

-----------------------------------------------------------------------------------

/* SALES DATA TABLE CHECK */

select * from silver.pos_Sales;

select sales_OrderNumber, count(*)
from silver.pos_Sales
group by sales_OrderNumber
having count(*) > 1;

select distinct(sales_LineItem)
from silver.pos_Sales;

select * from silver.pos_Sales where sales_OrderDate IS NULL;

select * from silver.pos_Sales 
where sales_StoreKey < 0 or sales_ProductKey < 0 or sales_Quantity <= 0;

select * from silver.pos_Sales where sales_DeliveryDate is null;

select distinct sales_StoreKey from silver.pos_Sales
