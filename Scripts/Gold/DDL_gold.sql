/*

DDL Script: Views in Gold Layer 
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    
	The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.	

	These views consist of all required fields along with some calculated columns

	Dimensions: {Products, Customers, Exchange Rates, Stores}
	Fact:		{Sales}
===============================================================================
*/



SELECT name
   FROM sys.columns
   WHERE object_id = OBJECT_ID('silver.pos_Sales')


/* Create Dimension: gold.dim_products  */
IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
    DROP VIEW gold.dim_products;
GO

CREATE VIEW gold.dim_products as
select 
		ROW_NUMBER() over (order by erp_ProductKey) as 'product_surrogate_key',
		p.erp_ProductKey as product_key,
		p.erp_ProductName as product_name,
		p.erp_Brand as brand,
		p.erp_Color as color,
		p.erp_UnitCostUSD as unit_cost_usd,
		p.erp_UnitPriceUSD as unit_price_usd,
		(p.erp_UnitPriceUSD - p.erp_UnitCostUSD) as 'gross_margin',
		case when p.erp_UnitPriceUSD <= 75 then 'Low'
				when p.erp_UnitPriceUSD <= 275 then 'Medium'
				else 'High'
		END as 'price_band',
		pc.erp_CategoryKey as category_key,
		pc.erp_CategoryName as category_name,
		pc.erp_CategoryManager as category_manager,
		pc.erp_CategoryType as category_type,
		pc.erp_IsSeasonal as is_seasonal,
		pc.erp_LaunchYear as launch_year,
		p.erp_SubcategoryKey as subcategory_key,
		ps.erp_Subcategory as subcategory,
		ps.erp_SubcategoryManager as subcategory_manager,
		ps.erp_TargetMarketSegment as target_market_segment,
		ps.erp_AvgUnitPrice as avg_unit_price
	from silver.erp_Products p LEFT JOIN silver.erp_SubCategory ps on p.erp_SubcategoryKey = ps.erp_SubcategoryKey
	LEFT JOIN silver.erp_Category pc on ps.erp_CategoryKey = pc.erp_CategoryKey;


/* Create Dimension: gold.dim_customers  */
IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
    DROP VIEW gold.dim_customers;
GO

CREATE VIEW gold.dim_customers as
select
		ROW_NUMBER() over (order by cst_CustomerKey) as 'customer_surrogate_key',
		cst_CustomerKey as customer_key,
		cst_Gender as customer_gender,
		cst_Name as customer_name,
		cst_City as customer_city,
		cst_StateCode as customer_statecode,
		cst_State as customer_state,
		cst_ZipCode as customer_zipcode,
		cst_Country as customer_country,
		cst_Continent as customer_continent,
		cst_Birthday as customer_birthday,
		cst_StartDate as customer_startdate,
		cst_EndDate as customer_enddate,
		cst_IsActive as customer_isactive
	from silver.crm_Customers;



/* Create Dimension: gold.dim_exchangerates  */
IF OBJECT_ID('gold.dim_exchangerates', 'V') IS NOT NULL
    DROP VIEW gold.dim_exchangerates;
GO
create view gold.dim_exchangerates as
Select 
	ROW_NUMBER() over(order by fin_Date, fin_Exchange) as 'exchange_surrogate_key',
	e.fin_Date as 'exchange_date',
	e.fin_Currency as 'exchange_currency',
	e.fin_Exchange as 'exchange_value',
	c.fin_CurrencyName as 'currency_name',
	c.fin_CurrencySymbol as 'currency_symbol',
	c.fin_Country as 'exchange_country'
	from silver.fin_ExchangeRates e LEFT JOIN silver.fin_CurrencyData c on e.fin_Currency = c.fin_ISOCode;



/* Create Dimension: gold.dim_stores  */
IF OBJECT_ID('gold.dim_stores', 'V') IS NOT NULL
    DROP VIEW gold.dim_stores;
GO

create view gold.dim_stores as
select
	ROW_NUMBER() over(order by str_StoreKey) as 'store_surrogate_key',
	str_StoreKey as 'store_key',
	str_Country as 'store_country',
	str_State as 'store_state',
	case when str_State = 'Online' then 0
		else str_SquareMeters
	END as 'square_meters',
	str_OpenDate as 'store_opendate'
	from silver.pos_Stores;




/* Create Fact: gold.fact_sales  */
IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
    DROP VIEW gold.fact_sales;
GO
create view gold.fact_sales as
Select 
	s.sales_RowKey as 'row_key',
	s.sales_OrderNumber as 'order_number',
	s.sales_LineItem as 'line_item',
	c.customer_surrogate_key as 'customer_key',		-- Surrogate Key
	st.store_surrogate_key as 'store_key',			-- Surrogate Key
	gp.product_surrogate_key as 'product_key',		-- Surrogate Key
	e.exchange_surrogate_key as 'exchange_key',		-- Surrogate Key
	s.sales_OrderDate 'order_date',
	s.sales_DeliveryDate as 'delivery_date',
	s.sales_Quantity as 'quantity',
	gp.unit_cost_usd * s.sales_Quantity as 'product_cost_usd',				-- Calculated column
	gp.unit_price_usd * s.sales_Quantity as 'product_price_usd',			-- Calculated column
	gp.gross_margin * s.sales_Quantity as 'product_gross_margin_usd',		-- Calculated column
	e.currency_name as 'country_currency',
	CAST(round(gp.unit_cost_usd * s.sales_Quantity * e.exchange_value, 2) as numeric(10,2)) as 'product_cost_local_currency',	-- Calculated column [Exchange with Local Currency]
	CAST(round(gp.unit_price_usd * s.sales_Quantity * e.exchange_value, 2) as numeric(10,2)) as 'product_price_local_currency'	-- Calculated column [Exchange with Local Currency]
	from silver.pos_Sales s LEFT JOIN gold.dim_products gp on s.sales_ProductKey = gp.product_key
	LEFT JOIN gold.dim_customers c on s.sales_CustomerKey = c.customer_key
	LEFT JOIN gold.dim_stores st on s.sales_StoreKey = st.store_key
	LEFT JOIN gold.dim_exchangerates e on s.sales_CurrencyCode = e.exchange_currency
	and s.sales_OrderDate = e.exchange_date;