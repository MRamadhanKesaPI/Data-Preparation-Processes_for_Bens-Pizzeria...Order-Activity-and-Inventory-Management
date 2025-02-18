# MRamadhanKesaPI-Data-Preparation-Process_for_Bens-Pizzeria...Order-Activity-and-Inventory-Analysis

This process cleans and organizes raw data to make it ready for analysis. It involves importing data, fixing errors, and transforming it into a clear structure.

Here are the steps I followed:
1. [Data Import and Exploration](#data-import-and-exploration)
2. [Data Cleaning](#data-cleaning)
3. [Data Transformation](#data-transformation)
4. [Create View](#create-view)

## Data Import and Exploration

Import the raw CSV data into the table and check for initial issues like formatting errors.

Below are the SQL queries and results for each Import and Inspection step.
```sql
/* ================================================================================
   STEP 1: DATA IMPORT AND INSPECTION
   ================================================================================ */


/* --------------------------------------------------------------------------------
   1.1. Create Table: customer_churn
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
