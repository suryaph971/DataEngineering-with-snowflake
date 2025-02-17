CREATE OR REPLACE TABLE staging_customer_data (
    customer_id INT,            
    first_name STRING,
    last_name STRING,
    city STRING,
    start_date DATE,
    end_date DATE
);


CREATE OR REPLACE TABLE customer_history (
    customer_surrogate_key INT AUTOINCREMENT,  
    customer_id INT,                          
    first_name STRING,
    last_name STRING,
    city STRING,
    start_date DATE,
    end_date DATE,
    record_effective_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  
    record_end_date TIMESTAMP DEFAULT NULL,                      
    current_flag BOOLEAN DEFAULT TRUE,                            
    PRIMARY KEY (customer_surrogate_key)
);



INSERT INTO staging_customer_data (customer_id, first_name, last_name, city, start_date, end_date)
VALUES
(1, 'John', 'Doe', 'New York', '2020-01-01', NULL),
(2, 'Jane', 'Smith', 'Los Angeles', '2020-03-15', NULL),
(3, 'Alice', 'Johnson', 'Chicago', '2020-05-20', NULL),
(4, 'Bob', 'Brown', 'Houston', '2020-02-10', NULL),
(5, 'Charlie', 'Davis', 'Phoenix', '2020-07-01', NULL);




INSERT INTO customer_history (customer_id, first_name, last_name, city, start_date, end_date, current_flag)
VALUES
(1, 'John', 'Doe', 'New York', '2020-01-01', NULL, TRUE),
(2, 'Jane', 'Smith', 'Los Angeles', '2020-03-15', NULL, TRUE),
(3, 'Alice', 'Johnson', 'Chicago', '2020-05-20', NULL, TRUE),
(4, 'Bob', 'Brown', 'Houston', '2020-02-10', NULL, TRUE),
(6, 'David', 'Williams', 'Dallas', '2020-08-10', NULL, TRUE);



CREATE OR REPLACE STREAM staging_stream
ON TABLE staging_customer_data ;
update staging_customer_data set city='Chicago' where customer_id=1;


select * from staging_stream





create or replace view customer_change_data
as
select customer_id,first_name,last_name,city,start_date,end_date,current_flag,'I' as dml_type
from (select customer_id,first_name,last_name,city,start_date,lag(start_date) over(partition by customer_id order by start_date desc) as end_time_raw,
CASE when end_time_raw is NULL then '9999-12-31'::timestamp_ntz else end_time_raw end as end_date,
case when end_time_raw is NULL then 1 else 0 end as current_flag
from (select customer_id,first_name,last_name,city,start_date from staging_stream where METADATA$ACTION='INSERT' AND METADATA$ISUPDATE='FALSE'))

UNION

select customer_id,first_name,last_name,city,start_date,end_date,current_flag,dml_type
from (select customer_id,first_name,last_name,city,start_date,lag(start_date) over (partition by customer_id order by start_date desc) as end_time_raw,
case when end_time_raw is NULL then '9999-12-31'::timestamp_ntz else end_time_raw end as end_date,
case when end_time_raw is NULL then 1 else 0  end as current_flag,dml_type
from (select customer_id,first_name,last_name,city,start_date,'I' as dml_type
from staging_stream where METADATA$ACTION='INSERT' AND METADATA$ISUPDATE='TRUE'
UNION
select customer_id,NULL,NULL,NULL,start_date,'U' as dml_type
from customer_history where customer_id in (Select distinct customer_id from staging_stream where METADATA$ACTION='INSERT' AND METADATA$ISUPDATE='TRUE') and current_flag=1 
))

UNION

select s.customer_id,NULL,NULL,NULL,h.start_date,current_timestamp(),NULL,'D' as dml_type
from customer_history h
join staging_stream s
on h.customer_id = s.customer_id
WHERE METADATA$ACTION='DELETE' AND METADATA$ISUPDATE='FALSE'
AND h.current_flag=TRUE


select * from customer_change_data



/*MERGE INTO customer_history a 
USING
customer_change_data b
on a.customer_id = b.customer_id
and a.start_date = b.start_date
WHEN MATCHED and b.dml_type='U' THEN UPDATE
set a.end_date=b.end_date,
current_flag = FALSE

WHEN MATCHED and b.dml_type='D' THEN UPDATE
set a.end_date=b.end_date,
current_flag = FALSE

WHEN NOT MATCHED and b.dml_type='I' then
insert (customer_id,first_name,last_name,city,start_date,end_date,current_flag)
values (b.customer_id,b.first_name,b.last_name,b.city,b.start_date,b.end_date,TRUE)*/


select  system$stream_has_data('staging_stream')


MERGE INTO customer_history a 
USING (Select * from staging_stream) b
on a.customer_id = b.customer_id
and a.first_name = b.first_name
and a.last_name = b.last_name
and a.city = b.city
when matched and (b.METADATA$ACTION='DELETE')
then update set a.end_date = current_timestamp,
a.current_flag = 0
when not matched and (b.METADATA$ACTION='INSERT')
then insert (customer_id,first_name,last_name,city,start_date,end_date,current_flag)
values (b.customer_id,b.first_name,b.last_name,b.city,b.start_date,NULL,TRUE)

select * from customer_history


