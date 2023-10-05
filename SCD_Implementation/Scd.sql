create database SCD;
create schema scd_schema;


CREATE TABLE customer (
   customer_id INT,
   customer_name VARCHAR(50),
   customer_email VARCHAR(50),
   customer_phone VARCHAR(15),
   load_date DATE,
	 customer_address VARCHAR(255)
);

INSERT INTO customer VALUES
   (1, 'John Doe', 'john.doe@example.com', '123-456-7890', '2022-01-01', '123 Main St'),
   (2, 'Jane Doe', 'jane.doe@example.com', '987-654-3210', '2022-01-01', '456 Elm St'),
   (3, 'Bob Smith', 'bob.smith@example.com', '555-555-5555', '2022-01-01', '789 Oak St');


select * from customer;

--SCD type 1
UPDATE customer SET customer_address = '789 Maple St' WHERE customer_id = 2;

select * from customer;

--SCD type 2

ALTER TABLE customer ADD COLUMN customer_segment VARCHAR(20);

ALTER TABLE customer ADD COLUMN start_date DATE;
ALTER TABLE customer ADD COLUMN end_date DATE;

ALTER TABLE customer ADD COLUMN version BIGINT DEFAULT 1;


select * from customer;

update customer set customer_segment='Gold',start_date = '2022-02-01', end_date = '9999-12-31' WHERE customer_id = 2;

select * from customer;


INSERT INTO customer (customer_id, customer_name, customer_email, customer_phone, customer_address, customer_segment, start_date, end_date, version, load_date)
   SELECT customer_id, customer_name, customer_email, customer_phone, customer_address, 'Platinum', '2022-03-01', '9999-12-31', version + 1, '2022-03-01' FROM customer WHERE customer_id = 2;

select * from customer;


update customer set end_date='2022-02-28' where customer_id=2 and version=1;

select * from customer where customer_id=2;


--SCD type 3

ALTER TABLE customer ADD COLUMN customer_status VARCHAR(10);
ALTER TABLE customer ADD COLUMN prev_customer_status VARCHAR(10);


select * from customer;

UPDATE customer SET customer_status = 'Active', prev_customer_status = 'Inactive' WHERE customer_id = 2;


select * from customer;


Alter table CUSTOMER add column Previous_segment varchar(100);


insert into customer (customer_id, customer_name, customer_email, customer_phone, customer_address, customer_segment, start_date, end_date, version, load_date,Previous_segment)
select customer_id, customer_name, customer_email, customer_phone, customer_address, 'Silver', '2022-03-01', '9999-12-31',version+1, '2022-03-01',customer_segment FROM customer WHERE customer_id = 2 and version=2;


select * from customer;






