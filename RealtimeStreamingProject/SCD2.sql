show streams;

select * from CUSTOMER_TABLE_CHANGES;

select DISTINCT METADATA$ACTION from CUSTOMER_TABLE_CHANGES;


select * from CUSTOMER;

insert into CUSTOMER values(223136,'Jessica','Arnold','tanner39@smith.com','595 Benjamin Forge Suite 124','Michaelstad','Connecticut'
                            ,'Cape Verde',current_timestamp());

update CUSTOMER SET FIRST_NAME='JESSICA',update_timestamp=current_timestamp()::timestamp_ntz where CUSTOMER_ID=72;

delete from CUSTOMER where customer_id =73;


select * from  customer where customer_id in (72,73,223136);

select * from customer_table_changes where customer_id in (72,73,223136);


create or replace view view_customer_change_data
as
select CUSTOMER_ID,FIRST_NAME,LAST_NAME,EMAIL,STREET,CITY,STATE,COUNTRY,START_TIME,END_TIME,IS_CURRENT,'I' as dml_type
from
(select CUSTOMER_ID,FIRST_NAME,LAST_NAME,EMAIL,STREET,CITY,STATE,COUNTRY,UPDATE_TIMESTAMP as START_TIME,lag(UPDATE_TIMESTAMP) OVER (PARTITION BY CUSTOMER_ID ORDER BY UPDATE_TIMESTAMP DESC) as END_TIME_RAW,
CASE when END_TIME_RAW IS NULL THEN '9999-12-31'::timestamp_ntz else END_TIME_RAW END AS END_TIME,
CASE WHEN END_TIME_RAW IS NULL THEN TRUE ELSE FALSE END AS IS_CURRENT
FROM (SELECT CUSTOMER_ID,FIRST_NAME,LAST_NAME,EMAIL,STREET,CITY,STATE,COUNTRY,UPDATE_TIMESTAMP FROM CUSTOMER_TABLE_CHANGES WHERE METADATA$ACTION='INSERT' AND METADATA$ISUPDATE='FALSE'))

UNION

SELECT CUSTOMER_ID,FIRST_NAME,LAST_NAME,EMAIL,STREET,CITY,STATE,COUNTRY,START_TIME,END_TIME,IS_CURRENT,'I' AS dml_type
from(SELECT CUSTOMER_ID,FIRST_NAME,LAST_NAME,EMAIL,STREET,CITY,STATE,COUNTRY,UPDATE_TIMESTAMP AS START_TIME,lag(UPDATE_TIMESTAMP) OVER (PARTITION BY CUSTOMER_ID ORDER BY UPDATE_TIMESTAMP DESC) as END_TIME_RAW,
CASE WHEN END_TIME_RAW IS NULL THEN '9999-12-31'::timestamp_ntz ELSE END_TIME_RAW END AS END_TIME,
CASE WHEN END_TIME_RAW IS NULL THEN TRUE ELSE FALSE END AS IS_CURRENT
FROM (SELECT CUSTOMER_ID,FIRST_NAME,LAST_NAME,EMAIL,STREET,CITY,STATE,COUNTRY,UPDATE_TIMESTAMP FROM CUSTOMER_TABLE_CHANGES WHERE METADATA$ACTION='INSERT' AND METADATA$ISUPDATE='TRUE'))

UNION
SELECT CUSTOMER_ID,null,null,null,null,null,null,null,start_time,CURRENT_TIMESTAMP()::timestamp_ntz,null,'U' as dml_type
from CUSTOMER_HISTORY where CUSTOMER_ID in (SELECT DISTINCT CUSTOMER_ID FROM CUSTOMER_TABLE_CHANGES where METADATA$ACTION='DELETE' and METADATA$ISUPDATE='TRUE' AND IS_CURRENT=TRUE)

UNION
SELECT CTC.CUSTOMER_ID,null,null,null,null,null,null,null,ch.start_time,current_timestamp()::timestamp_ntz,null,'D' as dml_type
from CUSTOMER_HISTORY ch inner join CUSTOMER_TABLE_CHANGES CTC on CTC.CUSTOMER_ID=ch.CUSTOMER_ID where CTC.METADATA$ACTION='DELETE' and CTC.METADATA$ISUPDATE='FALSE' AND ch.is_current=TRUE;



select * from view_customer_change_data;

select * from CUSTOMER_HISTORY;

CREATE OR REPLACE TASK STREAMING_SCD2_TASK SCHEDULE='2 Minute'
ERROR_ON_NONDETERMINISTIC_MERGE=FALSE
AS

MERGE INTO CUSTOMER_HISTORY ch using view_customer_change_data vd
on ch.CUSTOMER_ID=vd.CUSTOMER_ID and ch.START_TIME=vd.START_TIME
WHEN MATCHED and vd.dml_type='U' then UPDATE
SET ch.END_TIME=vd.END_TIME,
ch.IS_CURRENT=FALSE
WHEN MATCHED AND vd.dml_type='D' then UPDATE
SET ch.END_TIME=vd.END_TIME,
ch.IS_CURRENT=FALSE
WHEN NOT MATCHED AND vd.dml_type='I' then INSERT(ch.CUSTOMER_ID,ch.FIRST_NAME,ch.LAST_NAME,ch.EMAIL,ch.STREET,ch.CITY,ch.STATE,ch.COUNTRY,ch.START_TIME,ch.END_TIME,ch.IS_CURRENT) VALUES (vd.CUSTOMER_ID,vd.FIRST_NAME,vd.LAST_NAME,vd.EMAIL,vd.STREET,vd.CITY,vd.STATE,vd.COUNTRY,vd.START_TIME,vd.END_TIME,vd.IS_CURRENT);

show tasks;

ALTER TASK STREAMING_SCD2_TASK SUSPEND;

select * from table(information_schema.task_history(TASK_NAME=>'STREAMING_SCD2_TASK'));


insert into customer values(223136,'Jessica','Arnold','tanner39@smith.com','595 Benjamin Forge Suite 124','Michaelstad','Connecticut'
                            ,'Cape Verde',current_timestamp());
update customer set FIRST_NAME='Jessica' where customer_id=7523;
delete from customer where customer_id =136 and FIRST_NAME = 'Kim';

select * from customer_history where customer_id =223136;
select * from customer_history where IS_CURRENT=TRUE;


select * from customer_history where IS_CURRENT=FALSE;
