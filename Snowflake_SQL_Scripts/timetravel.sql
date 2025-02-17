create or replace table time_travel_practice(
id number,
product varchar(255),
category varchar(255),
price float,
launch_date timestamp_ntz,
updated_date timestamp_ntz
)
WITH DATA_RETENTION_TIME_IN_DAYS=60;


show tables

INSERT INTO time_travel_practice (id, product, category, price, launch_date, updated_date) VALUES 
(1, 'Iphone', 'Mobiles', 85945.0, '2025-01-27 22:00:00', '2025-01-27 22:00:00'),
(2, 'Samsung Galaxy', 'Mobiles', 74999.0, '2025-01-27 22:00:00', '2025-01-27 22:00:00'),
(3, 'MacBook Pro', 'Laptops', 129999.0, '2025-01-27 22:00:00', '2025-01-27 22:00:00'),
(4, 'Dell XPS 13', 'Laptops', 99999.0, '2025-01-27 22:00:00', '2025-01-27 22:00:00'),
(5, 'Sony WH-1000XM4', 'Headphones', 29999.0, '2025-01-27 22:00:00', '2025-01-27 22:00:00'),
(6, 'Bose QuietComfort 35 II', 'Headphones', 24999.0, '2025-01-27 22:00:00', '2025-01-27 22:00:00'),
(7, 'Apple Watch Series 7', 'Wearables', 42999.0, '2025-01-27 22:00:00', '2025-01-27 22:00:00'),
(8, 'Fitbit Charge 5', 'Wearables', 19999.0, '2025-01-27 22:00:00', '2025-01-27 22:00:00'),
(9, 'Samsung QLED TV', 'Televisions', 89990.0, '2025-01-27 22:00:00', '2025-01-27 22:00:00'),
(10, 'LG OLED TV', 'Televisions', 119990.0, '2025-01-27 22:00:00', '2025-01-27 22:00:00');



select * from time_travel_practice


update time_travel_practice set category='Televisions' where id=2

select * from time_travel_practice


update time_travel_practice set category='Mobiles' where id=9


select * from time_travel_practice

drop table time_travel_practice

undrop table time_travel_practice


select * from time_travel_practice BEFORE(OFFSET=>-60*10)


update time_travel_practice a set a.category=b.category from (select * from time_travel_practice BEFORE(OFFSET=>-60*10)) b 
where a.id = b.id 

select * from time_travel_practice
