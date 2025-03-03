# Data Preparation Processes for Ben's Pizzeria: Order Activity and Inventory Management

The data preparation process ensures raw data is clean, consistent, and ready for analysis. It involves importing the data, fixing errors, and organizing it into a usable structure. This step is crucial for accurate and reliable results.

Here are the steps I followed:
1. [Data Import](#data-import)
2. [Data Cleaning](#data-cleaning)
3. [Data Transformation](#data-transformation)
4. [Create View](#create-view)

## Data Import

The first step is to import the raw CSV data into the database. This ensures the data is loaded correctly and ready for the next steps, such as cleaning, transformation, and creating views.

Below are the SQL queries and results for each import step.
```sql
/* ================================================================================
   STEP 1: DATA IMPORT 
   ================================================================================ */


/* --------------------------------------------------------------------------------
   1.1. Create and Import Table: orders
   -------------------------------------------------------------------------------- */

-- Create table for orders.

CREATE TABLE
        orders(
    	row_id INT,
    	order_id VARCHAR(10),
    	created_at TIMESTAMP,
    	item_id VARCHAR(10),
    	quantity INT,
    	cust_id VARCHAR(10),
    	delivery BOOLEAN,
    	add_id VARCHAR(10)
);


-- Import table from csv file.

COPY 
	orders 
FROM 
	'D:\Downloads\Ben s Pizzeria\orders_data.csv' 
DELIMITER 
	';' 
CSV HEADER
;


/* --------------------------------------------------------------------------------
   1.2. Create and Import Table: customers
   -------------------------------------------------------------------------------- */

-- Create table for customers.

CREATE TABLE 
        customers(
    	cust_id VARCHAR(10) PRIMARY KEY,
    	cust_first_name VARCHAR(50),
    	cust_last_name VARCHAR(50)
);


-- Import table from csv file. 

COPY 
	customers 
FROM 
	'D:\Downloads\Ben s Pizzeria\customers_data.csv' 
DELIMITER 
	';' 
CSV HEADER
;


/* --------------------------------------------------------------------------------
   1.3. Create and Import Table: address
   -------------------------------------------------------------------------------- */

-- Create table for address.

CREATE TABLE 
        address(
    	add_id VARCHAR(10) PRIMARY KEY,
    	delivery_address1 VARCHAR(100),
    	delivery_address2 VARCHAR(50),
    	delivery_city VARCHAR(50),
    	delivery_postcode VARCHAR(50)
);


-- Import table from csv file. 

COPY 
	address 
FROM 
	'D:\Downloads\Ben s Pizzeria\address_data.csv' 
DELIMITER 
	';' 
CSV HEADER
;


/* --------------------------------------------------------------------------------
   1.4. Create and Import Table: item
   -------------------------------------------------------------------------------- */

-- Create table for item.

CREATE TABLE 
        item(
    	item_id VARCHAR(10) PRIMARY KEY,
    	sku VARCHAR(50),
    	item_name VARCHAR(50),
    	item_cat VARCHAR(20),
    	item_size VARCHAR(20),
    	item_price NUMERIC(10,2)
);


-- Import table from csv file. 

COPY 
	item 
FROM 
	'D:\Downloads\Ben s Pizzeria\item_data.csv' 
DELIMITER 
	';' 
CSV HEADER
;


/* --------------------------------------------------------------------------------
   1.5. Create and Import Table: ingredients
   -------------------------------------------------------------------------------- */

-- Create table for ingredients.

CREATE TABLE 
        ingredients(
    	ing_id VARCHAR(10) PRIMARY KEY,
    	ing_name VARCHAR(50),
    	ing_weight INT,
    	ing_meas VARCHAR(20),
    	ing_price NUMERIC(10,2)
);


-- Import table from csv file. 

COPY 
	ingredients 
FROM 
	'D:\Downloads\Ben s Pizzeria\ingredients_data.csv' 
DELIMITER 
	';' 
CSV HEADER
;


/* --------------------------------------------------------------------------------
   1.6. Create and Import Table: recipe
   -------------------------------------------------------------------------------- */

-- Create table for recipe.

CREATE TABLE 
        recipe(
    	row_id INT,
    	recipe_id VARCHAR(50),
    	ing_id VARCHAR(10),
    	quantity INT
);


-- Import table from csv file. 

COPY 
	recipe 
FROM 
	'D:\Downloads\Ben s Pizzeria\recipe_data.csv' 
DELIMITER 
	';' 
CSV HEADER
;


/* --------------------------------------------------------------------------------
   1.7. Create and Import Table: inventory
   -------------------------------------------------------------------------------- */

-- Create table for inventory.

CREATE TABLE 
        inventory(
    	inv_id VARCHAR(50) PRIMARY KEY,
    	ing_id VARCHAR(50),
        quantity INT
);


-- Import table from csv file. 

COPY 
	inventory 
FROM 
	'D:\Downloads\Ben s Pizzeria\inventory_data.csv' 
DELIMITER 
	';' 
CSV HEADER
;
```

## Data Cleaning

After importing the data, the next step is to check for any missing values and duplicates. This helps ensure data integrity before moving on to transformation and analysis.

Below are the SQL queries and results for each Cleaning step.
```sql
/* ================================================================================
   STEP 2: DATA CLEANING
   ================================================================================ */


/* --------------------------------------------------------------------------------
   2.1 Null Handling for orders
   -------------------------------------------------------------------------------- */

SELECT
	COUNT(*)FILTER(WHERE row_id IS NULL) AS row_id_nulls,
	COUNT(*)FILTER(WHERE order_id IS NULL) AS order_id_nulls,
	COUNT(*)FILTER(WHERE created_at IS NULL) AS created_at_nulls,
	COUNT(*)FILTER(WHERE item_id IS NULL) AS item_id_nulls,
	COUNT(*)FILTER(WHERE quantity IS NULL) AS quantity_nulls,
	COUNT(*)FILTER(WHERE cust_id IS NULL) AS cust_id_nulls,
	COUNT(*)FILTER(WHERE delivery IS NULL) AS delivery_nulls,
	COUNT(*)FILTER(WHERE add_id IS NULL) AS add_id_nulls
FROM
	orders
;

-- No NULL values are found.


/* --------------------------------------------------------------------------------
   2.2 Duplicate Check for orders
   -------------------------------------------------------------------------------- */

SELECT
	row_id,
	COUNT(row_id)
FROM
	orders
GROUP BY
	row_id
HAVING
	COUNT(row_id) > 1
ORDER BY
	row_id
;

-- No duplicate records found based on row_id.


/* --------------------------------------------------------------------------------
   2.3 Null Handling for customers
   -------------------------------------------------------------------------------- */

SELECT
	COUNT(*)FILTER(WHERE cust_id IS NULL) AS cust_id_nulls,
	COUNT(*)FILTER(WHERE cust_first_name IS NULL) AS cust_first_name_nulls,
	COUNT(*)FILTER(WHERE cust_last_name IS NULL) AS cust_last_name_nulls
FROM
	customers
;

-- No NULL values are found.


/* --------------------------------------------------------------------------------
   2.4 Duplicate Check for customers
   -------------------------------------------------------------------------------- */

SELECT
	cust_id,
	COUNT(cust_id)
FROM
	customers
GROUP BY
	cust_id
HAVING
	COUNT(cust_id) > 1
ORDER BY
	cust_id
;

-- No duplicate records found based on cust_id.


/* --------------------------------------------------------------------------------
   2.5 Null Handling for address
   -------------------------------------------------------------------------------- */

SELECT
	COUNT(*)FILTER(WHERE add_id IS NULL) AS add_id_nulls,
	COUNT(*)FILTER(WHERE delivery_address1 IS NULL) AS delivery_address1_nulls,
	COUNT(*)FILTER(WHERE delivery_address2 IS NULL) AS delivery_address2_nulls,
	COUNT(*)FILTER(WHERE delivery_city IS NULL) AS delivery_city_nulls,
	COUNT(*)FILTER(WHERE delivery_postcode IS NULL) AS delivery_postcode_nulls
FROM
	address
;

-- No NULL values are found.


/* --------------------------------------------------------------------------------
   2.6 Duplicate Check for address
   -------------------------------------------------------------------------------- */

SELECT
	add_id,
	COUNT(add_id)
FROM
	address
GROUP BY
	add_id
HAVING
	COUNT(add_id) > 1
ORDER BY
	add_id
;

-- No duplicate records found based on add_id.


/* --------------------------------------------------------------------------------
   2.7 Null Handling for item
   -------------------------------------------------------------------------------- */

SELECT
	COUNT(*)FILTER(WHERE item_id IS NULL) AS item_id_nulls,
	COUNT(*)FILTER(WHERE sku IS NULL) AS sku_nulls,
	COUNT(*)FILTER(WHERE item_name IS NULL) AS item_name_nulls,
	COUNT(*)FILTER(WHERE item_cat IS NULL) AS item_cat_nulls,
	COUNT(*)FILTER(WHERE item_size IS NULL) AS item_size_nulls,
	COUNT(*)FILTER(WHERE item_price IS NULL) AS item_price_nulls
FROM
	item
;

-- No NULL values are found.


/* --------------------------------------------------------------------------------
   2.8 Duplicate Check for item
   -------------------------------------------------------------------------------- */

SELECT
	item_id,
	COUNT(item_id)
FROM
	item
GROUP BY
	item_id
HAVING
	COUNT(item_id) > 1
ORDER BY
	item_id
;

-- No duplicate records found based on item_id.


/* --------------------------------------------------------------------------------
   2.9 Null Handling for ingredients
   -------------------------------------------------------------------------------- */

SELECT
	COUNT(*)FILTER(WHERE ing_id IS NULL) AS ing_id_nulls,
	COUNT(*)FILTER(WHERE ing_name IS NULL) AS ing_name_nulls,
	COUNT(*)FILTER(WHERE ing_weight IS NULL) AS ing_weight_nulls,
	COUNT(*)FILTER(WHERE ing_meas IS NULL) AS ing_meas_nulls,
	COUNT(*)FILTER(WHERE ing_price IS NULL) AS ing_price_nulls
FROM
	ingredients
;

-- No NULL values are found.


/* --------------------------------------------------------------------------------
   2.10 Duplicate Check for ingredients
   -------------------------------------------------------------------------------- */

SELECT
	ing_id,
	COUNT(ing_id)
FROM
	ingredients
GROUP BY
	ing_id
HAVING
	COUNT(ing_id) > 1
ORDER BY
	ing_id
;

-- No duplicate records found based on ing_id.


/* --------------------------------------------------------------------------------
   2.11 Null Handling for recipe
   -------------------------------------------------------------------------------- */

SELECT
	COUNT(*)FILTER(WHERE row_id IS NULL) AS row_id_nulls,
	COUNT(*)FILTER(WHERE recipe_id IS NULL) AS recipe_id_nulls,
	COUNT(*)FILTER(WHERE ing_id IS NULL) AS ing_id_nulls,
	COUNT(*)FILTER(WHERE quantity IS NULL) AS quantity_nulls
FROM
	recipe
;

-- No NULL values are found.


/* --------------------------------------------------------------------------------
   2.12 Duplicate Check for recipe
   -------------------------------------------------------------------------------- */

SELECT
	row_id,
	COUNT(row_id)
FROM
	recipe
GROUP BY
	row_id
HAVING
	COUNT(row_id) > 1
ORDER BY
	row_id
;

-- No duplicate records found based on row_id.


/* --------------------------------------------------------------------------------
   2.13 Null Handling for inventory
   -------------------------------------------------------------------------------- */

SELECT
	COUNT(*)FILTER(WHERE inv_id IS NULL) AS inv_id_nulls,
	COUNT(*)FILTER(WHERE ing_id IS NULL) AS ing_id_nulls,
	COUNT(*)FILTER(WHERE quantity IS NULL) AS quantity_nulls
FROM
	inventory
;

-- No NULL values are found.


/* --------------------------------------------------------------------------------
   2.14 Duplicate Check for inventory
   -------------------------------------------------------------------------------- */

SELECT
	inv_id,
	COUNT(inv_id)
FROM
	inventory
GROUP BY
	inv_id
HAVING
	COUNT(inv_id) > 1
ORDER BY
	inv_id
;

-- No duplicate records found based on inv_id.
```

## Data Transformation

In this step, the data is refined to improve consistency and usability. Key transformations include extracting time from timestamps, converting data types, and standardizing time values for better analysis.

Below are the SQL queries and results for each transformation step.
```sql
/* ================================================================================
   STEP 3: DATA TRANSFORMATION
   ================================================================================ */

/* --------------------------------------------------------------------------------
   3.1. Add and Populate 'time_of_order' Column
   -------------------------------------------------------------------------------- */

-- Add a 'time_of_order' column.

ALTER TABLE 
	orders
ADD COLUMN 
	time_of_order TIME
;


-- Populate 'time_of_order' based on 'created_at' column.

UPDATE 
	orders
SET 
	time_of_order = TIMEZONE(created_at)
; 


/* --------------------------------------------------------------------------------
   3.2. Type Conversion and Update 'created_at' Column
   -------------------------------------------------------------------------------- */

-- Convert 'created_at' to DATE type as the time has already been moved to 'time_of_order'.
ALTER TABLE 
	orders
ALTER COLUMN 
	created_at TYPE DATE
;


-- Remove time from the column, retaining only the date.

UPDATE 
	orders
SET 
	created_at = DATE(created_at)
;


/* --------------------------------------------------------------------------------
   3.3. Standarize 'time_of_order' Column Values
   -------------------------------------------------------------------------------- */

-- Simplify 'time_of_order' into standardized time slots for better analysis.

UPDATE 
	orders
SET 
	time_of_order = 
	CASE
	WHEN time_of_order = '00:30:00' THEN '12:30:00'
	WHEN time_of_order = '14:30:00' THEN '13:00:00'
	WHEN time_of_order = '14:45:00' THEN '12:30:00'
	WHEN time_of_order = '15:00:00' THEN '12:30:00'
	WHEN time_of_order = '15:15:00' THEN '13:00:00'
	WHEN time_of_order = '15:45:00' THEN '12:45:00'
	WHEN time_of_order = '16:15:00' THEN '12:15:00'
	WHEN time_of_order = '16:30:00' THEN '12:00:00'
	WHEN time_of_order = '17:00:00' THEN '11:00:00'
	WHEN time_of_order = '17:15:00' THEN '18:45:00'
	WHEN time_of_order = '17:30:00' THEN '20:15:00'
	WHEN time_of_order = '17:45:00' THEN '18:45:00'
	WHEN time_of_order = '18:15:00' THEN '21:30:00'
	WHEN time_of_order = '23:15:00' THEN '22:15:00'
	ELSE time_of_order
	END
;
```

## Create View

To enhance data organization and support analytics, database views are created. These views help simplify reporting and dashboard development by pre-aggregating relevant data.

Below are the SQL queries and results for each transformation step.
```sql
/* ================================================================================
   STEP 4: CREATE VIEW
   ================================================================================ */


/* --------------------------------------------------------------------------------
   4.1. Create View 'order_activity' 
   -------------------------------------------------------------------------------- */

-- This view helps in Dashboard 1 for analyzing:
-- - Total Orders, Sales, Items,
-- - Average Order Value, Sales by Category,
-- - Top Selling Items, Orders by Hour, Orders by Address

CREATE VIEW 
	order_activity AS
SELECT 
	o.order_id,
	o.created_at,
	o.time_of_order,
	a.delivery_city,
	o.delivery,
	i.item_id,
	i.item_name,
	i.item_cat,
	i.item_size,
	i.item_price,
	o.quantity
FROM 
	orders o
	LEFT JOIN item i ON o.item_id = i.item_id
	LEFT JOIN address a ON o.add_id = a.add_id
;


/* --------------------------------------------------------------------------------
   4.2. Create View 'inventory_management_1' 
   -------------------------------------------------------------------------------- */
   
-- This view helps in Dashboard 2 for analyzing:
-- - Total Ingredients Used, Total Cost of Ingredients,
-- - Cost of Pizza (ingredient cost)

CREATE VIEW
	inventory_management_1 AS 
SELECT
	sub1.item_name,
	sub1.ing_id,
	sub1.ing_name,
	sub1.recipe_quantity,
	sub1.total_order_quantity,
	sub1.recipe_quantity * sub1.total_order_quantity AS used_ingredients,
	ROUND((sub1.ing_price / sub1.ing_weight), 5) AS unit_cost,
	ROUND(((sub1.ing_price / sub1.ing_weight) * (sub1.recipe_quantity * sub1.total_order_quantity)), 2) AS ingredients_cost
FROM
	(SELECT 
		o.item_id,
		i.item_name,
		i.item_cat,
		i.item_size,
		ig.ing_id,
		ig.ing_name,
		ig.ing_weight,
		ig.ing_price,
		r.quantity AS recipe_quantity,
		SUM(o.quantity) AS total_order_quantity
	FROM 
		orders o
		LEFT JOIN item i ON o.item_id = i.item_id
		LEFT JOIN recipe r ON i.sku = r.recipe_id
		LEFT JOIN ingredients ig ON r.ing_id = ig.ing_id
	GROUP BY
		o.item_id,
		i.item_name,
		i.item_cat,
		i.item_size,
		ig.ing_id,
		ig.ing_name,
		ig.ing_weight,
		ig.ing_price,
		r.quantity
	ORDER BY
		o.item_id) AS sub1 
GROUP BY 
	sub1.item_name,
	sub1.ing_id,
	sub1.ing_name,
	sub1.ing_weight,
	sub1.ing_price,
	sub1.recipe_quantity,
	sub1.total_order_quantity
ORDER BY 
	sub1.item_name
;


/* --------------------------------------------------------------------------------
   4.3. Create View 'inventory_management_2' 
   -------------------------------------------------------------------------------- */

-- This view helps in Dashboard 2 for analyzing:
-- - Inventory Levels, Stock Remaining,
-- - Identifying which ingredients need reordering

CREATE VIEW 
	inventory_management_2 AS
SELECT
	sub2.ing_name,
	(sub2.total_used_inventory + sub2.inventory_balance) AS total_inventory,
	sub2.total_used_inventory,
	sub2.inventory_balance,
	sub2.measure,
	ROUND((sub2.inventory_balance / (sub2.total_used_inventory + sub2.inventory_balance)), 2) AS remaining_percentage
FROM	
	(SELECT 
		im1.ing_name,
		SUM(im1.used_ingredients) AS total_used_inventory,
		(ig.ing_weight * iv.quantity) AS inventory_balance,
		ig.ing_meas AS measure
	FROM 
		inventory_management_1 im1 
		LEFT JOIN ingredients ig ON im1.ing_id = ig.ing_id
		LEFT JOIN inventory iv ON im1.ing_id = iv.ing_id
	GROUP BY
		im1.ing_name,
		ig.ing_weight,
		iv.quantity,
		ig.ing_meas
	ORDER BY 
		im1.ing_name) AS sub2
GROUP BY
	sub2.ing_name,
	sub2.total_used_inventory,
	sub2.inventory_balance,
	sub2.measure
ORDER BY
	sub2.ing_name
;
```
These queries will produce the final datasets that will be imported into Power BI to uncover insights.

| [⏪ DAX on Power BI](https://mramadhankesapi.github.io/DAX-Processes_for_Bens-Pizzeria...Sales-and-Inventory-Analytics/) | [Ben's Pizzeria: Sales & Inventory Analytics ⏩](https://mramadhankesapi.github.io/Bens-Pizzeria...Sales-and-Inventory-Analytics/) |
|----------------------------------|---------------|
