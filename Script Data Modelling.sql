CREATE SCHEMA dimensional;

CREATE TABLE dimensional.dim_coupons (
  coupon_key INTEGER PRIMARY KEY,
  discount_percent DOUBLE PRECISION
);

INSERT INTO dimensional.dim_coupons (coupon_key, discount_percent)
(SELECT id as coupon_key,
discount_percent
FROM coupons)
ON CONFLICT (coupon_key)
DO UPDATE SET discount_percent = excluded.discount_percent;


CREATE TABLE dimensional.dim_date (
   date_key    BIGINT PRIMARY KEY,
   date        DATE    NOT NULL,
   day_of_week INTEGER NOT NULL,
   week        INTEGER NOT NULL,
   month       INTEGER NOT NULL,
   quarter     INTEGER NOT NULL,
   year        INTEGER NOT NULL
);

INSERT INTO dimensional.dim_date (date_key, date, day_of_week, week, month, quarter, year)
SELECT TO_CHAR(order_date, 'YYYYMMDD')::integer AS date_key,
       order_date                               AS date,
       EXTRACT(DOW FROM order_date)             AS day_of_week,
       EXTRACT(WEEK FROM order_date)            AS week,
       EXTRACT(MONTH FROM order_date)           AS month,
       EXTRACT(QUARTER FROM order_date)         AS quarter,
       EXTRACT(YEAR FROM order_date)            AS year
FROM (SELECT DISTINCT created_at as order_date
      FROM orders) AS subquery
ON CONFLICT (date_key)
    DO UPDATE SET
                  date = excluded.date,
                  day_of_week = excluded.day_of_week,
                  week = excluded.week,
                  month = excluded.month,
                  quarter = excluded.quarter,
                  year = excluded.year;
                 
                 
CREATE TABLE dimensional.dim_customers(
   customers_key BIGINT PRIMARY KEY,
   full_name     TEXT,
   first_name    VARCHAR(150),
   last_name     VARCHAR(150),
   gender        VARCHAR(1),
   address       TEXT,
   zip_code      varchar(8)
);

INSERT INTO dimensional.dim_customers (customers_key, full_name, first_name, last_name, gender, address, zip_code)
    (SELECT id                                                       as customers_key,
            (CONCAT(customer.first_name, ' ', customer.last_name)) as full_name,
            customer.first_name,
            customer.last_name,
            customer.gender,
            customer.address,
            customer.zip_code
     FROM customer)
ON CONFLICT (customers_key)
    DO UPDATE SET full_name  = excluded.full_name,
                  first_name = excluded.first_name,
                  last_name  = excluded.last_name,
                  gender     = excluded.gender,
                  address    = excluded.address,
                  zip_code   = excluded.zip_code;
        
                 
CREATE TABLE dimensional.dim_login_attempts(
   login_key        BIGINT PRIMARY KEY,
   customer_key     BIGINT,
   login_successful BOOLEAN,
   attempted_at     DATE,
   FOREIGN KEY (customer_key) REFERENCES dimensional.dim_customers (customers_key)
);

INSERT INTO dimensional.dim_login_attempts(login_key, customer_key, login_successful, attempted_at)
    (SELECT id           as login_key,
            customer_id  as customer_key,
            login_successful,
            attempted_at as DATE
     FROM login_attemps
     WHERE customer_id < (select MAX(id) FROM customer))
ON CONFLICT (login_key)
    DO UPDATE SET login_successful = excluded.login_successful,
                  attempted_at     = excluded.attempted_at;

                 
CREATE TABLE dimensional.dim_product_categories(
   product_categories_key BIGINT PRIMARY KEY,
   name                   TEXT
);

INSERT INTO dimensional.dim_product_categories(product_categories_key, name)
    (SELECT id as product_categories_key,
            name
     FROM product_categories)
ON CONFLICT (product_categories_key)
    DO UPDATE SET name = excluded.name;
   
   
CREATE TABLE dimensional.dim_suppliers(
   suppliers_key BIGINT PRIMARY KEY,
   name          VARCHAR(150),
   country       VARCHAR(150)
);

INSERT INTO dimensional.dim_suppliers(suppliers_key, name, country)
    (SELECT id as suppliers_key,
            name,
            country
     FROM suppliers)
ON CONFLICT (suppliers_key)
    DO UPDATE SET name    = excluded.name,
                  country = excluded.country;
                 
                 
CREATE TABLE dimensional.dim_products(
   products_key           BIGINT PRIMARY KEY,
   name                   TEXT,
   price                  double precision,
   product_categories_key BIGINT,
   suppliers_key          BIGINT,
   FOREIGN KEY (product_categories_key) REFERENCES dimensional.dim_product_categories (product_categories_key),
   FOREIGN KEY (suppliers_key) REFERENCES dimensional.dim_suppliers (suppliers_key)
);

INSERT INTO dimensional.dim_products(products_key, name, price, product_categories_key, suppliers_key)
    (SELECT id          as products_key,
            name,
            price,
            category_id as product_categories_key,
            supplier_id as suppliers_key
     FROM products)
ON CONFLICT (products_key)
    DO UPDATE SET name                   = excluded.name,
                  price                  = excluded.price,
                  product_categories_key = excluded.product_categories_key;
                 
                 
CREATE TABLE dimensional.dim_orders(
   order_key  BIGINT PRIMARY KEY,
   customers_key BIGINT,
   status     TEXT,
   created_at DATE,
   FOREIGN KEY (customers_key) REFERENCES dimensional.dim_customers(customers_key)
);

INSERT INTO dimensional.dim_orders (order_key, customers_key, status, created_at)
(SELECT
         id as order_key,
         CAST (customer_id AS BIGINT) as customers_key,
         status,
         created_at as DATE
     FROM orders)
ON CONFLICT (order_key)
    DO UPDATE SET
                  status     = excluded.status,
                  created_at = excluded.created_at;
                 
                 
                 
-- Create Fact Table

CREATE TABLE dimensional.fact_sales_orders(
   sale_order_key BIGINT PRIMARY KEY,
   order_key      BIGINT,
   customers_key  BIGINT,
   product_key    BIGINT,
   coupon_key     BIGINT,
   date_key       BIGINT,
   amount         BIGINT,
   price_per_unit DOUBLE PRECISION,
   total          DOUBLE PRECISION,
   discount       DOUBLE PRECISION,
   fixed_total    DOUBLE PRECISION,
   status         TEXT,
   created_at     DATE,
   FOREIGN KEY (order_key) REFERENCES dimensional.dim_orders (order_key),
   FOREIGN KEY (customers_key) REFERENCES dimensional.dim_customers (customers_key),
   FOREIGN KEY (product_key) REFERENCES dimensional.dim_products (products_key),
   FOREIGN KEY (coupon_key) REFERENCES dimensional.dim_coupons (coupon_key),
   FOREIGN KEY (date_key) REFERENCES dimensional.dim_date (date_key)
);

INSERT INTO dimensional.fact_sales_orders (sale_order_key, order_key, customers_key, product_key, coupon_key, date_key, amount, price_per_unit, total, discount, fixed_total, status, created_at)
SELECT
    id as sale_order_key,
    dor.order_key,
    dcus.customers_key,
    dp.products_key as product_key,
    dc.coupon_key,
    TO_CHAR(dor.created_at, 'YYYYMMDD')::integer AS date_key,
    oi.amount,
    dp.price as price_per_unit,
    (oi.amount * dp.price) as total,
    dc.discount_percent as discount,
    (oi.amount * dp.price - ((oi.amount * dp.price) * COALESCE(dc.discount_percent, 0)/100)) fixed_total,
    status,
    created_at
FROM order_items as oi
LEFT JOIN
    dimensional.dim_products AS dp ON dp.products_key = oi.product_id
LEFT JOIN
    dimensional.dim_orders AS dor ON dor.order_key = oi.order_id
LEFT JOIN
    dimensional.dim_customers AS dcus ON dcus.customers_key = dor.customers_key
LEFT JOIN
    dimensional.dim_coupons AS dc ON dc.coupon_key = oi.coupon_id
ON CONFLICT (sale_order_key)
    DO UPDATE SET
                  order_key     = excluded.order_key,
                  customers_key = excluded.customers_key,
                  product_key = excluded.product_key,
                  coupon_key = excluded.coupon_key,
                  date_key = excluded.date_key,
                  amount = excluded.amount,
                  price_per_unit = excluded.price_per_unit,
                  total = excluded.total,
                  discount = excluded.discount,
                  fixed_total = excluded.fixed_total,
                  status = excluded.status,
                  created_at = excluded.created_at;
                 

CREATE TABLE dimensional.fact_product_performance(
   product_key BIGINT PRIMARY KEY,
   name        TEXT,
   count_of_sales      DOUBLE PRECISION,
   total_revenue     DOUBLE PRECISION,
   FOREIGN KEY (product_key) REFERENCES dimensional.dim_products (products_key)
);

INSERT INTO dimensional.fact_product_performance(product_key, name, count_of_sales, total_revenue)
SELECT
	dp.id as product_key,
	MAX(dp.name) as name,
	sum(foi.amount) as count_of_sales,
	sum(foi.fixed_total) as total_revenue
FROM products dp
LEFT JOIN dimensional.fact_sales_orders foi
	ON foi.product_key = product_key
GROUP BY dp.id
ON CONFLICT (product_key)
    DO UPDATE SET
                  product_key = excluded.product_key,
                  name = excluded.name,
                  count_of_sales = excluded.count_of_sales,
                  total_revenue = excluded.total_revenue;
                 

 CREATE TABLE dimensional.fact_product_category_performance(
   category_key      BIGINT PRIMARY KEY,
   name              TEXT,
   count_of_sales    DOUBLE PRECISION,
   total_revenue     DOUBLE PRECISION,
   FOREIGN KEY (category_key) REFERENCES dimensional.dim_product_categories (product_categories_key)
);

INSERT INTO dimensional.fact_product_category_performance(category_key, name, count_of_sales, total_revenue)
SELECT
	dc.product_categories_key as category_key,
	MAX(dc.name) as name,
	sum(foi.amount) as count_of_sales,
	sum(foi.fixed_total) as total_revenue
FROM dimensional.dim_product_categories dc
LEFT JOIN dimensional.dim_products as dp
    ON dp.product_categories_key = dc.product_categories_key
LEFT JOIN dimensional.fact_sales_orders foi
	ON foi.product_key = dp.products_key
GROUP BY dc.product_categories_key
ORDER BY total_revenue DESC, count_of_sales DESC
ON CONFLICT (category_key)
    DO UPDATE SET
                  category_key = excluded.category_key,
                  name = excluded.name,
                  count_of_sales = excluded.count_of_sales,
                  total_revenue = excluded.total_revenue;
          
                 
-- Create Table Presentation
CREATE SCHEMA presentation;
CREATE TABLE presentation.daily_order_revenue_per_product
(
    date         DATE,
    product_name TEXT,
    category   TEXT,
    suppliers    TEXT,
    amount       BIGINT,
    revenue      TEXT
);

INSERT INTO presentation.daily_order_revenue_per_product(date, product_name, category, suppliers, amount, revenue)
SELECT
  dd.date,
  dp.name as product_name,
  dpc.name as category,
  ds.name as suppliers,
  SUM(fso.amount) as amount,
  SUM(fso.fixed_total) AS revenue
FROM
	dimensional.fact_sales_orders as fso
JOIN
	dimensional.dim_date as dd
	ON fso.date_key = dd.date_key
JOIN
	dimensional.dim_products as dp
	ON fso.product_key = dp.products_key
JOIN
	dimensional.dim_suppliers as ds
	ON dp.suppliers_key = ds.suppliers_key
JOIN
    dimensional.dim_product_categories as dpc
    ON dp.product_categories_key = dpc.product_categories_key
GROUP BY
	dd.date, dp.products_key, dpc.product_categories_key, ds.suppliers_key;
	

CREATE TABLE presentation.daily_order_revenue
(
    date        DATE PRIMARY KEY ,
    amount      BIGINT,
    revenue     TEXT
);

INSERT INTO presentation.daily_order_revenue (date, amount, revenue)
SELECT
  dd.date,
  SUM(fso.amount) as amount,
  SUM(fso.fixed_total) AS revenue
FROM
	dimensional.fact_sales_orders as fso
JOIN
	dimensional.dim_date as dd
	ON fso.date_key = dd.date_key
GROUP BY
	dd.date
ON CONFLICT (date)
    DO UPDATE SET
                  amount  = excluded.amount,
                  revenue = excluded.revenue;
                  
                 
CREATE TABLE presentation.daily_order_revenue_per_categories
(
    date         DATE,
    category     TEXT,
    amount       BIGINT,
    revenue      TEXT
);

INSERT INTO presentation.daily_order_revenue_per_categories(date, category, amount, revenue)
SELECT
  dd.date,
  dpc.name as category,
  SUM(fso.amount) as amount,
  SUM(fso.fixed_total) AS revenue
FROM
	dimensional.fact_sales_orders as fso
JOIN
	dimensional.dim_date as dd
	ON fso.date_key = dd.date_key
JOIN
	dimensional.dim_products as dp
	ON fso.product_key = dp.products_key
JOIN
	dimensional.dim_product_categories as dpc
	ON dpc.product_categories_key = dp.product_categories_key
GROUP BY
	dd.date,  dpc.product_categories_key;
	

CREATE TABLE presentation.daily_order_revenue_per_suppliers
(
    date         DATE,
    supplier_name TEXT,
    country      TEXT,
    amount       BIGINT,
    revenue      TEXT
);

INSERT INTO presentation.daily_order_revenue_per_suppliers(date, supplier_name, country, amount, revenue)
SELECT
  dd.date,
  ds.name,
  ds.country,
  SUM(fso.amount) as amount,
  SUM(fso.fixed_total) AS revenue
FROM
	dimensional.fact_sales_orders as fso
JOIN
	dimensional.dim_date as dd
	ON fso.date_key = dd.date_key
JOIN
	dimensional.dim_products as dp
	ON fso.product_key = dp.products_key
JOIN
	dimensional.dim_suppliers as ds
	ON dp.suppliers_key = dp.suppliers_key
GROUP BY
	dd.date,  ds.suppliers_key;