create or replace external table user_data(
date varchar(255) as split_part(METADATA$FILENAME,'/',2),
registration_dttm timestamp_ntz as (value:registration_dttm::timestamp_ntz),
id number as (value:id::number),
first_name varchar(255) as (value:first_name::varchar),
last_name  varchar(255) as (value:last_name::varchar),
email 	varchar(255) as (value:email::varchar),
gender varchar(255) as (value:gender::varchar),
ip_address 	varchar(255) as (value:ip_address::varchar),
cc varchar(255) as (value:cc::varchar),
country varchar(255) as (value:country::varchar),
birthdate varchar(255) as (value:birthdate::varchar),
salary float as (value:salary::float),
title varchar(255) as (value:title::varchar),
comments varchar(255) as (value:comments::varchar)
)
PARTITION BY (date)
WITH LOCATION = @parquetstage
FILE_FORMAT = (TYPE='PARQUET')


list @parquetstage

select split_part(METADATA$FILENAME,'/',2) from user_data limit 10


show external tables like '%user_data%'


select * from user_data