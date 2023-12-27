MERGE INTO CUSTOMER C USING CUSTOMER_RAW CR ON C.CUSTOMER_ID=CR.CUSTOMER_ID
WHEN MATCHED AND C.CUSTOMER_ID <> CR.CUSTOMER_ID OR
                 C.FIRST_NAME <> CR.FIRST_NAME OR
                 C.LAST_NAME <> CR.LAST_NAME OR
                 C.EMAIL <> CR.EMAIL OR
                 C.STREET <> CR.STREET OR
                 C.CITY <> CR.CITY OR
                 C.STATE <> CR.STATE OR
                 C.COUNTRY <> CR.COUNTRY THEN UPDATE

                 SET C.CUSTOMER_ID=CR.CUSTOMER_ID,
                     C.FIRST_NAME=CR.FIRST_NAME,
                     C.LAST_NAME=CR.LAST_NAME,
                     C.EMAIL=CR.EMAIL,
                     C.STREET=CR.STREET,
                     C.CITY=CR.CITY,
                     C.STATE=CR.STATE,
                     C.COUNTRY=CR.COUNTRY,
                     C.UPDATE_TIMESTAMP=CURRENT_TIMESTAMP()

WHEN NOT MATCHED THEN INSERT(C.CUSTOMER_ID,C.FIRST_NAME,C.LAST_NAME,C.EMAIL,C.STREET,C.CITY,C.STATE,C.COUNTRY) VALUES (CR.CUSTOMER_ID,CR.FIRST_NAME,CR.LAST_NAME,CR.EMAIL,CR.STREET,CR.CITY,CR.STATE,CR.COUNTRY);


TRUNCATE TABLE CUSTOMER_RAW;

CREATE OR REPLACE PROCEDURE SCD1_PROCEDURE()
RETURNS STRING NOT NULL
LANGUAGE JAVASCRIPT
as
$$
var cmd = `MERGE INTO CUSTOMER C USING CUSTOMER_RAW CR ON C.CUSTOMER_ID=CR.CUSTOMER_ID
WHEN MATCHED AND C.CUSTOMER_ID <> CR.CUSTOMER_ID OR
                 C.FIRST_NAME <> CR.FIRST_NAME OR
                 C.LAST_NAME <> CR.LAST_NAME OR
                 C.EMAIL <> CR.EMAIL OR
                 C.STREET <> CR.STREET OR
                 C.CITY <> CR.CITY OR
                 C.STATE <> CR.STATE OR
                 C.COUNTRY <> CR.COUNTRY THEN UPDATE

                 SET C.CUSTOMER_ID=CR.CUSTOMER_ID,
                     C.FIRST_NAME=CR.FIRST_NAME,
                     C.LAST_NAME=CR.LAST_NAME,
                     C.EMAIL=CR.EMAIL,
                     C.STREET=CR.STREET,
                     C.CITY=CR.CITY,
                     C.STATE=CR.STATE,
                     C.COUNTRY=CR.COUNTRY,
                     C.UPDATE_TIMESTAMP=CURRENT_TIMESTAMP()

WHEN NOT MATCHED THEN INSERT(C.CUSTOMER_ID,C.FIRST_NAME,C.LAST_NAME,C.EMAIL,C.STREET,C.CITY,C.STATE,C.COUNTRY) VALUES (CR.CUSTOMER_ID,CR.FIRST_NAME,CR.LAST_NAME,CR.EMAIL,CR.STREET,CR.CITY,CR.STATE,CR.COUNTRY);`

var cmd2="truncate table CUSTOMER_RAW;"
var query1=snowflake.createStatement({sqlText:cmd});
var query2=snowflake.createStatement({sqlText:cmd2});
var result1 = query1.execute();
var result2 = query2.execute();
return cmd+'\n'+cmd2;
$$;

call SCD1_PROCEDURE();


select count(*) from CUSTOMER;
select count(*) from CUSTOMER_RAW;


CREATE OR REPLACE TASK STREAMING_SCD1_TASK SCHEDULE='2 minute'
ERROR_ON_NONDETERMINISTIC_MERGE=FALSE

as 
call SCD1_PROCEDURE();

show tasks;


ALTER TASK STREAMING_SCD1_TASK SUSPEND;


select * from table(information_schema.task_history());
