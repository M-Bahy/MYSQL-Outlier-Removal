create database bahy
drop database bahy
use bahy
select count(*) from original where value < 30 or value >50
select count(*) from original where (value < 30 or value >50) and server =1
select count(*) from original where (value < 30 or value >50) and server =2
select count(*) from original where (value < 30 or value >50) and server =3
truncate table compact
select * from rn_qos_data_0018
select sum(Value) from original where time < "2024-01-01 01:00:00" and server = 1
select * from original where time < "2024-01-01 01:00:00" and server = 1

select sum(Value) from original where time >= "2024-01-01 01:00:00" and  time <= "2024-01-01 01:55:00" and server = 1
select * from original where time >= "2024-01-01 01:00:00" and  time <= "2024-01-01 01:55:00" and server = 1


select * from compact_0018
truncate table original
drop table original 
truncate table compact
ALTER TABLE original
ADD CONSTRAINT pk_server_time PRIMARY KEY (Server, Time);
