create or replace storage integration storage_s3_integration
type=external_stage
storage_provider=s3
enabled=true
storage_aws_role_arn='arn:aws:iam::084828575424:role/snowflakeStorage_integration'
storage_allowed_locations=('s3://snowflakedatapractice')


describe integration storage_s3_integration



create or replace stage snowpipe_stage
storage_integration=storage_s3_integration
url = 's3://snowflakedatapractice'
file_format=(type='parquet')


list @snowpipe_stage


create or replace table user_data_new(
parquet_data variant,
load_data timestamp_ntz
)

create or replace pipe datapipe
AUTO_INGEST=TRUE
as
copy into user_data_new
from (select $1,current_timestamp from @snowpipe_stage)


describe pipe datapipe


select SYSTEM$PIPE_STATUS('datapipe')


select * from user_data_new

Alter pipe datapipe refresh

Alter pipe datapipe set pipe_execution_paused=True

show pipes




create or replace table user_data_historical(
registration_dttm timestamp_ntz,
id number,
first_name varchar(255),
last_name  varchar(255),
email 	varchar(255),
gender varchar(255),
ip_address 	varchar(255),
cc varchar(255),
country varchar(255),
birthdate varchar(255),
salary float,
title varchar(255),
comments varchar(255)
)




MERGE INTO user_data_historical hist
using (
select parquet_data:registration_dttm::timestamp_ntz as registration_dttm,
parquet_data:id::INT as id,
parquet_data:first_name::varchar(255) as first_name,
parquet_data:last_name::varchar(255) as last_name,
parquet_data:email::varchar(255) as email,
parquet_data:gender::varchar(255) as gender,
parquet_data:ip_address::varchar(255) as ip_address,
parquet_data:cc::varchar(255) as cc,
parquet_data:country::varchar(255) as country,
parquet_data:birthdate::varchar(255) as birthdate,
parquet_data:salary:: float as salary,
parquet_data:title:: varchar(255) as title,
parquet_data:comments:: varchar(255) as comments
from user_data_new
) as staging
on hist.id = staging.id
when matched and (hist.first_name=staging.first_name and hist.last_name = staging.last_name) then update
set hist.registration_dttm = staging.registration_dttm,
hist.first_name = staging.first_name,
hist.last_name = staging.last_name,
hist.email = staging.email,
hist.gender = staging.gender,
hist.ip_address = staging.ip_address,
hist.cc = staging.cc,
hist.country = staging.country,
hist.birthdate = staging.birthdate,
hist.salary = staging.salary,
hist.title = staging.title,
hist.comments = staging.comments

when not matched then insert (registration_dttm,id,first_name,last_name,email,gender,ip_address,cc,country,birthdate,salary,title,comments) values (staging.registration_dttm,
staging.id,staging.first_name,staging.last_name,staging.email,staging.gender,staging.ip_address,staging.cc,staging.country,staging.birthdate,staging.salary,staging.title,staging.comments)


select * from user_data_historical



create or replace task parquet_data_task
warehouse = COMPUTE_WH
SCHEDULE = 'USING CRON */10 * * * * UTC'
AS
MERGE INTO user_data_historical hist
using (
select parquet_data:registration_dttm::timestamp_ntz as registration_dttm,
parquet_data:id::INT as id,
parquet_data:first_name::varchar(255) as first_name,
parquet_data:last_name::varchar(255) as last_name,
parquet_data:email::varchar(255) as email,
parquet_data:gender::varchar(255) as gender,
parquet_data:ip_address::varchar(255) as ip_address,
parquet_data:cc::varchar(255) as cc,
parquet_data:country::varchar(255) as country,
parquet_data:birthdate::varchar(255) as birthdate,
parquet_data:salary:: float as salary,
parquet_data:title:: varchar(255) as title,
parquet_data:comments:: varchar(255) as comments
from user_data_new
) as staging
on hist.id = staging.id
when matched and (hist.first_name=staging.first_name and hist.last_name = staging.last_name) then update
set hist.registration_dttm = staging.registration_dttm,
hist.first_name = staging.first_name,
hist.last_name = staging.last_name,
hist.email = staging.email,
hist.gender = staging.gender,
hist.ip_address = staging.ip_address,
hist.cc = staging.cc,
hist.country = staging.country,
hist.birthdate = staging.birthdate,
hist.salary = staging.salary,
hist.title = staging.title,
hist.comments = staging.comments

when not matched then insert (registration_dttm,id,first_name,last_name,email,gender,ip_address,cc,country,birthdate,salary,title,comments) values (staging.registration_dttm,
staging.id,staging.first_name,staging.last_name,staging.email,staging.gender,staging.ip_address,staging.cc,staging.country,staging.birthdate,staging.salary,staging.title,staging.comments)

show tasks




Alter task parquet_data_task RESUME


select * from TABLE(information_schema.task_history()) where name='PARQUET_DATA_TASK'

select id,count(*) as count from user_data_historical group by id having count>1

delete from user_data_historical a USING (select id,first_name,last_name,
row_number() over (partition by id order by id) as rnk from user_data_historical where id =1001) b
where  a.first_name = b.first_name and a.last_name = b.last_name and b.rnk>1

select * from user_data_historical where id is null

select max(id) from user_data_historical
update user_data_historical set id = 1001 where id is null

select * from user_data_historical where id = 1001


Alter task parquet_data_task SUSPEND

truncate table user_data_new

select * from user_data_new


