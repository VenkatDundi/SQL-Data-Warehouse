# Data Catalog for Gold Layer

## Overview
The Gold Layer is the business-level data representation, structured to support analytical and reporting use cases. 
It consists of **dimension tables** and **fact tables** for specific business metrics.


### 1. **gold.fact_sales**
- **Purpose:** Stores transactional sales facts with customer, product, store, and exchange context for financial and operational analysis.
- **Columns:**

| Column Name                     | Data Type     | Description                                                                                   |
|---------------------------------|---------------|-----------------------------------------------------------------------------------------------|
| row_key                         | NVARCHAR(15)  | Unique row-level surrogate key, typically a combination of order number and line item.        |
| order_number                    | INT           | Sales order number from the transaction source.                                               |
| line_item                       | INT           | Line item number within the sales order.                                                      |
| customer_key                    | INT           | Foreign key to `dim_customers`, identifying the customer.                                     |
| store_key                       | INT           | Foreign key to `dim_stores`, identifying the store location.                                  |
| product_key                     | INT           | Foreign key to `dim_products`, identifying the product sold.                                  |
| exchange_key                    | INT           | Foreign key to `dim_exchangerates`, used for currency conversion.                             |
| order_date                      | DATE          | The date when the order was placed.                                                           |
| delivery_date                   | DATE          | The date when the order was delivered to the customer.                                        |
| quantity                        | INT           | Number of product units sold.                                                                 |
| product_cost_usd                | DECIMAL(21,2) | Cost of the product in USD.                                                                   |
| product_price_usd               | DECIMAL(21,2) | Selling price of the product in USD.                                                          |
| product_gross_margin_usd        | DECIMAL(22,2)  | Gross margin in USD, calculated as price minus cost.                                          |
| country_currency                | NVARCHAR(100) | Currency used in the country where the sale occurred.                                         |
| product_cost_local_currency     | NUMERIC(10,2) | Product cost in local transaction currency.                                                   |
| product_price_local_currency    | NUMERIC(10,2) | Product price in local transaction currency.                                                  |

### 2. **gold.dim_customers**
- **Purpose:** Stores customer master data with attributes relevant for segmentation, personalization, and demographic analysis.
- **Columns:**

| Column Name           | Data Type     | Description                                                      |
|------------------------|---------------|------------------------------------------------------------------|
| customer_surrogate_key | INT           | Surrogate key for slowly changing dimension tracking.            |
| customer_key           | INT           | Business key uniquely identifying a customer.                    |
| customer_gender        | NVARCHAR(50)  | Gender of the customer.                                          |
| customer_name          | NVARCHAR(50)  | Full name of the customer.                                       |
| customer_city          | NVARCHAR(50)  | City where the customer resides.                                 |
| customer_statecode     | NVARCHAR(50)  | Abbreviated state code (e.g., VIC, NSW).                         |
| customer_state         | NVARCHAR(50)  | Full state name.                                                 |
| customer_zipcode       | NVARCHAR(20)  | Postal or ZIP code.                                              |
| customer_country       | NVARCHAR(50)  | Country of residence.                                            |
| customer_continent     | NVARCHAR(50)  | Continent of residence.                                          |
| customer_birthday      | DATE          | Date of birth.                                                   |
| customer_startdate     | DATE          | Date this version of the record became active.                   |
| customer_enddate       | DATE          | End date of this version (NULL if currently active).             |
| customer_isactive      | INT           | 1 if this is the current version, 0 if expired.                  |

### 3. **gold.dim_products**
- **Purpose:** Captures product-level information enriched with pricing, category hierarchy, and segmentation attributes.
- **Columns:**

| Column Name           | Data Type     | Description                                                   |
|------------------------|---------------|---------------------------------------------------------------|
| product_surrogate_key | INT           | Surrogate key for product versioning (SCD).                   |
| product_key           | INT           | Business key for the product.                                 |
| product_name          | NVARCHAR(155) | Descriptive name of the product.                              |
| brand                 | NVARCHAR(80)  | Brand associated with the product.                            |
| color                 | NVARCHAR(50)  | Color of the product.                                         |
| unit_cost_usd         | DECIMAL(10,2) | Unit cost in USD.                                             |
| unit_price_usd        | DECIMAL(10,2) | Unit price in USD.                                            |
| gross_margin          | DECIMAL(10,2) | Gross margin value (price - cost).                            |
| price_band            | VARCHAR(6)    | Categorization of price range (e.g., Low, Mid, Premium).      |
| category_key          | INT           | Foreign key to the product category.                          |
| category_name         | NVARCHAR(50)  | Name of the product category.                                 |
| category_manager      | NVARCHAR(50)  | Person responsible for category performance.                  |
| category_type         | NVARCHAR(50)  | Physical/Digital classification.                              |
| is_seasonal           | NVARCHAR(10)  | Indicates if product is seasonal ('Y', 'N').                  |
| launch_year           | INT           | Year the product was launched.                                |
| subcategory_key       | INT           | Subcategory foreign key.                                      |
| subcategory           | NVARCHAR(50)  | Name of the subcategory.                                      |
| subcategory_manager   | NVARCHAR(50)  | Manager for the subcategory segment.                          |
| target_market_segment | NVARCHAR(50)  | Intended market audience.                                     |
| avg_unit_price        | DECIMAL(10,2) | Average unit price for the subcategory.                       |

### 4. **gold.dim_stores**
- **Purpose:** Stores store metadata including geographic location and operational details for regional sales analysis.
- **Columns:**

| Column Name         | Data Type     | Description                                   |
|----------------------|---------------|-----------------------------------------------|
| store_surrogate_key | INT           | Surrogate key for the store dimension.        |
| store_key           | INT           | Business identifier for the store.            |
| store_country       | NVARCHAR(50)  | Country where the store is located.           |
| store_state         | NVARCHAR(50)  | State or province of the store.               |
| square_meters       | INT           | Size of the store in square meters.           |
| store_opendate      | DATE          | Date when the store began operations.         |

### 5. **gold.dim_exchangerates**
- **Purpose:** Tracks exchange rates for currency conversion used in multi-national financial analysis.
- **Columns:**

| Column Name           | Data Type     | Description                                               |
|------------------------|---------------|-----------------------------------------------------------|
| exchange_surrogate_key | INT           | Surrogate key for versioning exchange rates.             |
| exchange_date         | DATE           | Date when the exchange rate was recorded.                |
| exchange_currency     | NVARCHAR(10)   | Currency code (e.g., USD, EUR, INR).                     |
| exchange_value        | DECIMAL(10,4)  | Conversion value to USD or base currency.                |
| currency_name         | NVARCHAR(100)  | Full name of the currency (e.g., Euro, Yen).             |
| currency_symbol       | NVARCHAR(5)    | Symbol of the currency (e.g., €, ¥, ₹).                  |
| exchange_country      | NVARCHAR(100)  | Country where the currency is used.                      |