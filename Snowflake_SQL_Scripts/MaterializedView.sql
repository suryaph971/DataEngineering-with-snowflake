select * from user_data limit 10



select concat_ws(' ',FIRST_NAME,LAST_NAME) as Username,Email,Country,Salary,registration_dttm,Gender from USER_DATA where BIRTHDATE>'01/01/1971'

select split_part(email,'@',-1) as email from user_data limit 10

create or replace materialized view userdata_view
as 
select concat_ws(' ',FIRST_NAME,LAST_NAME) as Username,Email,Country,Salary,registration_dttm,Gender from USER_DATA where BIRTHDATE>'01/01/1971'


select * from userdata_view