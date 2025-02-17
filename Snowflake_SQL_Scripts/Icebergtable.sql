create database test

create schema test

create or replace stage parquetstage
url='s3://snowflakedatapractice/parquet/'
credentials=(AWS_KEY_ID='' AWS_SECRET_KEY='')


list @parquetstage



create or replace external table user_data(
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
WITH LOCATION = @parquetstage
FILE_FORMAT = (TYPE='PARQUET')

select * from user_data


CREATE OR REPLACE EXTERNAL VOLUME iceberg_external_volume
   STORAGE_LOCATIONS =
      (
         (
            NAME = 'snowflakedatapractice'
            STORAGE_PROVIDER = 'S3'
            STORAGE_BASE_URL = 's3://snowflakedatapractice/parquet/'
            STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::084828575424:role/snowflakeicebergrole'
            STORAGE_AWS_EXTERNAL_ID = 'iceberg_table'
         )
      );


DESCRIBE External volume iceberg_external_volume


select system$VERIFY_EXTERNAL_VOLUME('iceberg_external_volume')



create or replace ICEBERG table user_data_iceberg(
registration_dttm timestamp_ntz,
id int,
first_name string,
last_name  string,
email 	string,
gender string,
ip_address 	string,
cc string,
country string,
birthdate string,
salary float,
title string,
comments string
)
CATALOG='SNOWFLAKE'
EXTERNAL_VOLUME='iceberg_external_volume'
BASE_LOCATION='s3://snowflakedatapractice/parquet/'
COMMENT='ICEBERG TABLE CREATION'



