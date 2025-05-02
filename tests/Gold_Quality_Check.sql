/*

Quality Checks (Post Ingestion (Views) in Gold Layer)
===============================================================================
This script performs quality checks to validate the integrity, consistency, 
    and accuracy of the Gold Layer. These checks ensure:
    - Uniqueness of surrogate keys in dimension tables.
    - Referential integrity between fact and dimension tables.
    - Validation of relationships in the data model for analytical purposes.

	These checks helps in verifying clean views in the gold layer
===============================================================================
*/



/* Quality checks for Fact Sales */

/*
=============================================================================================================
NOTE: We have moved 2 Products to Quarantine (205, 706) to demonstrate handling invalid data.				||
																											||
Sales records in the gold layer with NULL Product Key corresponds to those in quarantine.					||
																											||
Below queries helps us in understanding the same. (Match the row_key and check the "sales_productKey" value	||
=============================================================================================================
*/

select * from gold.fact_sales fs						-- Sales Records with ProductKey is NULL
left join gold.dim_customers c
on fs.customer_key = c.customer_surrogate_key
left join gold.dim_products p
on fs.product_key = p.product_surrogate_key
where fs.product_key IS NULL;

select * from silver.pos_Sales where sales_RowKey in (		-- Validate if those ProductKeys are in Quarantine 
		select fs.row_key from gold.fact_sales fs
		left join gold.dim_customers c
		on fs.customer_key = c.customer_surrogate_key
		left join gold.dim_products p
		on fs.product_key = p.product_surrogate_key
		where fs.product_key IS NULL)

select * from quarantine.products;			-- Products in quarantine (Product Keys - 205 and 706)

---------------------------------------------------------------------------------------------------------------

/*
=============================================================================================================
NOTE: We have moved 2 Customer Keys to Quarantine (1642 and 4808) to demonstrate handling invalid data.		||
																											||
Sales records in the gold layer with NULL Customer Key corresponds to those in quarantine.					||
																											||
Below queries helps us in understanding the same. (Match row_key and check the "sales_CustomerKey" value.	||
As there is only one sale record with customer id 1642, it is retrieved.									||
Where as there exists no sale records for the customer - 4808.												||
=============================================================================================================
*/


select * from gold.fact_sales fs						-- Sales Records with CustomerKey is NULL
left join gold.dim_customers c
on fs.customer_key = c.customer_surrogate_key
left join gold.dim_products p
on fs.product_key = p.product_surrogate_key
where fs.customer_key IS NULL;

select * from silver.pos_Sales where sales_RowKey in (		-- Validate if those Customer Keys are in Quarantine 
		select fs.row_key from gold.fact_sales fs
		left join gold.dim_customers c
		on fs.customer_key = c.customer_surrogate_key
		left join gold.dim_products p
		on fs.product_key = p.product_surrogate_key
		where fs.customer_key IS NULL)

select * from quarantine.Customers;			-- Customer Keys in quarantine (Customer Keys - 1642 and 4808)



/* These records should be fixed upon in gold layer upon validating the data in QUARANTINE TABLES */





/* Quality checks for Dimension Customers */

select * from gold.dim_customers;

select customer_surrogate_key, count(*) from gold.dim_customers
group by customer_surrogate_key
having count(*) > 1;


select customer_key, count(*)		--- SCD 2 Implementation records
from gold.dim_customers
group by customer_key
having count(*) > 1;

select distinct customer_gender from gold.dim_customers;

select distinct customer_statecode 
from gold.dim_customers 
where left(customer_statecode, 2) = replicate(left(customer_statecode, 1), 2);

select * from gold.dim_customers  where customer_state like '%Northern Territory%';

select distinct customer_state 
from gold.dim_customers
where customer_state like '%[^a-zA-Z0-9 !"%#$&''()*+,-./:;<=>?@]%';

SELECT N'Baden-Württemberg' AS TestString;

select distinct customer_city 
from gold.dim_customers
where customer_city like '%[^a-zA-Z0-9 !"%#$&''()*+,./:;<=>?@]%';

select * from gold.dim_customers;


/* Quality checks for Dimension Products */

select * from gold.dim_products

select product_surrogate_key, count(*) from gold.dim_products
group by product_surrogate_key having count(*) > 1;

select product_name from gold.dim_products
where product_name like '%[^a-zA-Z0-9 !"%#$&''()*+,-./:;<=>?@]%'

select * from gold.dim_products
where brand is null or color is null;

select * from gold.dim_products
where unit_cost_usd < 0 or unit_price_usd < 0;

select * from gold.dim_products where product_key is null;