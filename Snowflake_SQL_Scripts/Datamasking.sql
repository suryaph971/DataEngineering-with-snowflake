create role maskingadmin

show roles

grant create masking policy on schema TEST  to role maskingadmin

grant apply masking policy on account to role maskingadmin


show roles


grant role maskingadmin to role ACCOUNTADMIN


grant USAGE on warehouse COMPUTE_WH to role ORGADMIN
grant usage on schema TEST TO role ORGADMIN
grant usage on DATABASE TEST TO role ORGADMIN
grant select on table user_data to role ORGADMIN


select * from user_data limit 10

==================
--creating a masking policy


create or replace masking policy SALARY_MASK as (val float) returns float ->
case when 
current_role() in ('ACCOUNTADMIN') then val
else 0.0
end;


select current_role()


show tables

Alter table user_data modify column salary set masking policy SALARY_MASK

Alter table user_data modify column salary Unset masking policy

select * from user_data limit 10