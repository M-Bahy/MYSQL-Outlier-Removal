create database bahy
drop database bahy
use bahy
select count(*) from rn_qos_data_0018 where samplevalue < 30 or samplevalue >50
select count(*) from rn_qos_data_0018 where (samplevalue < 30 or samplevalue >50) and table_id =1
select count(*) from rn_qos_data_0018 where (samplevalue < 30 or samplevalue >50) and table_id =2
select count(*) from rn_qos_data_0018 where (samplevalue < 30 or samplevalue >50) and table_id =3
truncate table compact_0018
select * from rn_qos_data_0018
select table_id , samplesampletime , samplesamplevalue from rn_qos_data_0018
select sum(samplevalue) from rn_qos_data_0018 where sampletime < "2024-01-01 01:00:00" and table_id = 1
select * from rn_qos_data_0018 where sampletime < "2024-01-01 01:00:00" and table_id = 1

select sum(samplevalue) from rn_qos_data_0018 where sampletime >= "2024-01-01 01:00:00" and  sampletime <= "2024-01-01 01:55:00" and table_id = 1
select * from rn_qos_data_0018 where sampletime >= "2024-01-01 01:00:00" and  sampletime <= "2024-01-01 01:55:00" and table_id = 1


select * from compact_0018
truncate table rn_qos_data_0018
drop table rn_qos_data_0018 
truncate table compact_0018
ALTER TABLE rn_qos_data_0018