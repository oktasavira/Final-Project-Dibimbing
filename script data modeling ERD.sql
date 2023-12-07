-- Create schema dbo
--
create schema dbo;


-- DATA MODELING --
-- Create the dim_customer dimension table
--
create table dbo.dim_customers
(
customers_key        INT           not null,
first_name          VARCHAR        not null,
last_name           VARCHAR        not null,
gender              VARCHAR        not null,
address             VARCHAR        not null,
zip_code            VARCHAR        not null,

constraint PK_dim_customers primary key (customers_key)
);

-- Populate the dim_customer dimension table with data
INSERT INTO dbo.dim_customers (customers_key, first_name, last_name, gender, address, zip_code)
SELECT 
  id,
  first_name,
  last_name,
  gender,
  address, 
  zip_code
FROM customer;



-- Create the dim_login_attempts dimension table 
--
create table dbo.dim_login_attempts
(
login_attempts_key    INT             not null,
customer_id         INTEGER           not null,
login_successfull   BOOLEAN           not null,
attempted_at        TIMESTAMP         not null,

constraint PK_dim_login_attempts primary key (login_attempts_key)
);

-- Populate the dim_login_attempts dimension table with data
insert into dbo.dim_login_attempts (login_attempts_key, customer_id, login_successfull, attempted_at)
select
id,
customer_id,
login_successful,
attempted_at
from login_attemps;



-- Create the dim_product dimension table
--
create table dbo.dim_product
(
product_key     INT              not null,
name           VARCHAR (6072)   not null,
price          FLOAT            not null,
category_id    INTEGER          not null,
supplier_id    INTEGER          not null,

constraint PK_dim_product primary key (product_key)
);

-- Populate the dim_product dimension table with data
insert into dbo.dim_product (product_key, name, price, category_id, supplier_id)
select
id,
name,
price,
category_id,
supplier_id
from products;



-- Create the dim_product_categories dimension table
--
create table dbo.dim_product_categories
(
product_categories_key    INT      not null,
name                    VARCHAR    not null,

constraint PK_dim_product_categories primary key (product_categories_key)
);

-- Populate the dim_product_categories dimension table with data
insert into dbo.dim_product_categories (product_categories_key, name)
select
id,
name
from product_categories;



-- Create the dim_suppliers dimension table
--
create table dbo.dim_suppliers
(
suppliers_key     INT         not null,
name            VARCHAR       not null,
country         VARCHAR       not null,

constraint PK_dim_suppliers primary key (suppliers_key)
);

-- Populate the dim_suppliers dimension table with data
insert into dbo.dim_suppliers (suppliers_key, name, country)
select
id,
name,
country
from suppliers;



-- Create the dim_orders dimension table
--
create table dbo.dim_orders
(
orders_key      INT       not null,
customer_id    VARCHAR    not null,
status         text       not null,
created_at     TIMESTAMP  not null,

constraint PK_dim_orders primary key (orders_key)
);

-- Populate the dim_orders dimension table with data
insert into dbo.dim_orders (orders_key, customer_id, status, created_at)
select
id,
customer_id,
status,
created_at
from orders;



-- Create the dim_order_items dimension table
--
create table dbo.dim_order_items
(
order_items_key    INT    not null,
order_id         INTEGER  not null,
product_id       INTEGER  not null,
amount           INTEGER  not null,
coupon_id        INTEGER  null,

constraint PK_dim_order_items primary key (order_items_key)
);

-- Populate the dim_order_items dimension table with data
insert into dbo.dim_order_items (order_items_key, order_id, product_id, amount, coupon_id)
select
id,
order_id,
product_id,
amount,
coupon_id
from order_items;



-- Create the dim_coupons dimension table
--
create table dbo.dim_coupons
(
coupons_key        INT     not null,
discount_percent  FLOAT    not null,

constraint PK_dim_coupons primary key (coupons_key)
);

-- Populate the dim_coupons dimension table with data
insert into dbo.dim_coupons (coupons_key, discount_percent)
select
id,
discount_percent
from coupons;



-- Create the dim_date dimension table
--
create table dbo.dim_date 
(
  date_key INTEGER PRIMARY KEY,
  date DATE NOT NULL,
  day_of_week INTEGER NOT NULL,
  week INTEGER NOT NULL,
  month INTEGER NOT NULL,
  quarter INTEGER NOT NULL,
  year INTEGER NOT NULL
);

-- dim_date populate
INSERT INTO dbo.dim_date (date_key, date, day_of_week, week, month, quarter, year)
SELECT 
  TO_CHAR(created_at, 'YYYYMMDD')::integer AS date_key,
  created_at AS date,
  EXTRACT(DOW FROM created_at) AS day_of_week,
  EXTRACT(WEEK FROM created_at) AS week,
  EXTRACT(MONTH FROM created_at) AS month,
  EXTRACT(QUARTER FROM created_at) AS quarter,
  EXTRACT(YEAR FROM created_at) AS year
FROM (
  SELECT DISTINCT created_at
  FROM dbo.dim_orders
) AS subquery;


select * from dbo.dim_date;



-- Create Fact_Sale_Orders Fact Table
CREATE TABLE dbo.fact_sale_orders (
    order_key      INT PRIMARY KEY,
    date_key       INT,
    product_key     INT,
    order_items_key INT,
    sale_amount    DECIMAL (10, 2),
    status_order   TEXT
);


-- Populate the Fact_Sale_Orders fact table with data
  INSERT INTO dbo.fact_sale_orders (order_key, date_key, product_key, order_items_key, sale_amount, status_order)
SELECT 
  dbo.dim_orders.orders_key,
  TO_CHAR(dbo.dim_orders.created_at, 'YYYYMMDD')::integer as date_key,
  dbo.dim_product.product_key as product_key,
  dbo.dim_order_items.order_items_key as order_items_key,
  dbo.dim_order_items.amount as sale_amount,
  dbo.dim_orders.status as status_order
FROM 
  dbo.dim_orders 
LEFT JOIN dbo.dim_product ON dbo.dim_product.product_key = dbo.dim_product.product_key
LEFT JOIN dbo.dim_order_items ON dbo.dim_order_items.order_items_key = dbo.dim_order_items.order_items_key;



-- Create the Fact Category Performance
create table dbo.fact_categories_performance (
category_performance_key    INT             primary key,
product_categories_name     VARCHAR         not null,
total_revenue               DECIMAL (10, 2) not null
);

-- Populate the Fact Category Performance
INSERT INTO dbo.fact_categories_performance (category_performance_key, product_categories_name, total_revenue)
SELECT
    dbo.dim_product_categories.product_categories_key,
    dbo.dim_product_categories.name,
    SUM(dbo.dim_product.price * dbo.dim_order_items.amount) as total_revenue
FROM
    dbo.dim_product_categories
    -- Add the necessary JOIN clauses here to connect the required tables
    LEFT JOIN dbo.dim_product ON dbo.dim_product_categories.product_categories_key = dbo.dim_product.product_key
    LEFT JOIN dbo.dim_order_items ON dbo.dim_product.product_key = dbo.dim_order_items.product_id
GROUP BY
    dbo.dim_product_categories.product_categories_key,
    dbo.dim_product_categories.name;

 

-- Create Customer Activity Fact Table
create table dbo.fact_customer_activity (
customer_activity_key    INT   primary key,
customers_key            INT,
orders_key               BOOLEAN,
login_attempts_key       TIMESTAMP
);

-- Populate Customer Activity Fact Table with data
insert into dbo.fact_customer_activity (customer_activity_key, customers_key, orders_key, login_attempts_key)
select
login_attempts_key,
customer_id,
login_successfull,
attempted_at
from dbo.dim_login_attempts;


-- Table Relasi buat ERD aja
create table dbo.fact_customers_activity (
customers_activity_key    INT   primary key,
customers_key            INT,
orders_key               INT,
login_attempts_key       INT,
FOREIGN KEY (customers_key) REFERENCES dbo.dim_customers(customers_key),
FOREIGN KEY (orders_key) REFERENCES dbo.dim_orders(orders_key),
FOREIGN KEY (login_attempts_key) REFERENCES dbo.dim_login_attempts(login_attempts_key)
);

-- Table Relasi buat ERD aja
CREATE TABLE dbo.fact_sales_orders (
    order_key      INT PRIMARY KEY,
    date_key       INT,
    customers_key   INT,
    product_key     INT,
    order_items_key INT,
    sale_amount    DECIMAL (10, 2),
    status_order   TEXT,
    coupons_key    INT,
    FOREIGN KEY (date_key) REFERENCES dbo.dim_date (date_key),
    FOREIGN KEY (customers_key) REFERENCES dbo.dim_customers(customers_key),
    FOREIGN KEY (product_key) REFERENCES dbo.dim_product(product_key),
    FOREIGN KEY (order_items_key) REFERENCES dbo.dim_order_items(order_items_key),
    FOREIGN KEY (coupons_key) REFERENCES dbo.dim_coupons(coupons_key)
);

-- Table Relasi Buat ERD aja
create table dbo.fact_categories_performances (
category_performance_key    INT             primary key,
product_categories_key      INT,
suppliers_key               INT,
orders_key                  INT,
product_categories_name     VARCHAR         not null,
total_revenue               DECIMAL (10, 2) not null,
FOREIGN KEY (product_categories_key) REFERENCES dbo.dim_product_categories(product_categories_key),
FOREIGN KEY (suppliers_key) REFERENCES dbo.dim_suppliers(suppliers_key),
FOREIGN KEY (orders_key) REFERENCES dbo.dim_orders(orders_key)
);

INSERT INTO dbo.fact_sale_orders (order_key, date_key, product_key, order_items_key, sale_amount, status_order)
SELECT 
  dbo.dim_orders.orders_key,
  TO_CHAR(dbo.dim_orders.created_at, 'YYYYMMDD')::integer as date_key,
  dbo.dim_product.product_key as product_key,
  dbo.dim_order_items.order_items_key as order_items_key,
  dbo.dim_order_items.amount as sale_amount,
  dbo.dim_orders.status as status_order
FROM 
  dbo.dim_orders 
LEFT JOIN dbo.dim_product ON dbo.dim_product.product_key = dbo.dim_product.product_key
LEFT JOIN dbo.dim_order_items ON dbo.dim_order_items.order_items_key = dbo.dim_order_items.order_items_key;
