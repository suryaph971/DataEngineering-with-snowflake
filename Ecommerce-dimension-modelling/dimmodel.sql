create database ecommerce_project;
create schema ec_schema;


create or replace stage instacart_stage
url = 's3://instacartecommerce/instacart/'
credentials=(AWS_KEY_ID='AKIASJCIWEZASPMULHBP' AWS_SECRET_KEY='e31A5mz9kQatOH+pk0HratLpCJGxF8joYLE4OZEJ');

list @instacart_stage;


create or replace FILE FORMAT MY_CSV_FILEFORMAT
type='CSV'
FIELD_DELIMITER=','
SKIP_HEADER=1
FIELD_OPTIONALLY_ENCLOSED_BY='"'
RECORD_DELIMITER='\n'
TRIM_SPACE=TRUE;


create table aisles
(aisle_id INTEGER PRIMARY KEY,
aisle VARCHAR);

copy into aisles from @instacart_stage/aisles.csv
file_Format=(Format_name=MY_CSV_FILEFORMAT);

select * from aisles;


create table department
(department_id integer Primary key,
department varchar);


copy into department from @instacart_stage/departments.csv
file_Format=(Format_name=MY_CSV_FILEFORMAT);

select * from department;



create or replace table product
(
product_id integer,
product_name varchar,
aisle_id integer,
department integer
);


copy into product from @instacart_stage/products.csv
file_Format=(Format_name=MY_CSV_FILEFORMAT);


select * from product;



create or replace table orders
(
order_id INTEGER,
user_id integer,
eval_set string,
order_number INTEGER,
order_dow INTEGER,
order_hour_of_day INTEGER,
days_since_prior_order INTEGER);

copy into orders from @instacart_stage/orders.csv
file_Format=(Format_name=MY_CSV_FILEFORMAT);


select * from orders;

create or replace table order_products
(
order_id INTEGER,
product_id INTEGER,
add_to_cart_order INTEGER,
reordered INTEGER,
primary key(order_id,product_id)
);

copy into order_products from @instacart_stage/order_products__prior.csv
file_Format=(Format_name=MY_CSV_FILEFORMAT);

select * from order_products;


--Creating fact and dimension tables

create or replace table dim_user as (select user_id from orders);

select * from dim_user;

create or replace table dim_products as (Select product_id,product_name from product);

select * from dim_products;


create or replace table dim_aisles as (Select aisle_id,aisle from aisles);

select * from dim_aisles;


create or replace table dim_department as (Select department_id,department from department);

select * from dim_department;


create or replace table dim_orders as (Select order_id,order_number,order_dow,order_hour_of_day,days_since_prior_order from orders);

select * from dim_orders;


create or replace table fact_order_product as (
select 
op.order_id,
op.product_id,
o.user_id,
p.aisle_id,
p.department as department_id,
op.add_to_cart_order,
op.reordered
from 
order_products op join orders o on o.order_id=op.order_id
JOIN product p on op.product_id = p.product_id);

select * from FACT_ORDER_PRODUCT;
