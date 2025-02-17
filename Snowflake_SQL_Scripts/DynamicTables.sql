CREATE or replace TABLE cust_info_table (
cust_id CHAR(10),
FST_NAME VARCHAR(50),
LST_NAME VARCHAR(50),
email VARCHAR(100),
phone VARCHAR(20),
load_date DATE);


INSERT INTO cust_info_table (cust_id, FST_NAME, LST_NAME, email, phone, load_date)
VALUES 
('C1', 'John', 'Doe', 'john.doe@email.com', '123-456-7890', '2024-02-13'),
('C2', 'Alice', 'Smith', 'alice.smith@email.com', '987-654-3210', '2024-02-13'),
('C3', 'Bob', 'Johnson', 'bob.johnson@email.com', '555-123-4567', '2024-02-13'),
('C4', 'Emily', 'Williams', 'emily.williams@email.com', '333-888-9999', '2024-02-13'),
('C5', 'Michael', 'Brown', 'michael.brown@email.com', '777-444-2222', '2024-02-13');




create or replace dynamic table customer_history_dynamic
TARGET_LAG='1 MINUTE'
warehouse = COMPUTE_WH
as

select cust_id,FST_NAME,LST_NAME,email,phone,load_date,
row_number() over (partition by cust_id order by load_date desc) rn,
case when rn=1 then True else False end as current_flag,
case when rn=1 then null else current_date() end as cur_date
from cust_info_table


select * from customer_history_dynamic


insert into cust_info_table(cust_id, FST_NAME, LST_NAME, email, phone, load_date)
values (
'C6','Subbarao','where are you amma','subbaraopullarao@gmail.com','9848032919','2025-02-02'
)

select * from cust_info_table


select * from customer_history_dynamic

insert into cust_info_table(cust_id, FST_NAME, LST_NAME, email, phone, load_date) values ('C1','John','Doe','johndoe@ymail.com','123-456-7890','2025-02-02')

insert into cust_info_table(cust_id, FST_NAME, LST_NAME, email, phone, load_date) values ('C1','John','Doe','johndoe@gmail.com','123-456-7892','2025-02-03')