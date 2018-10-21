0、创建库
create database lda;
use lda;

创建表，映射原始数据:
create external table if not exists logs(
s_time string,
en string,
ver string,
u_ud string,
u_mid string,
u_sid string,
c_time string,
language  string,
b_iev string,
b_rst string,
p_url string,
p_ref string,
tt string,
pl string,
o_id string,
`on` string,
cut string,
cua string,
pt string,
ca string,
ac string,
kv_ string,
du string,
os string,
os_v string,
browser string,
browser_v string,
country string,
province string,
city string
)
partitioned by(month String,day string)
row format delimited fields terminated by '\001'
;

load data inpath '/ods/month=07/day=27' into table logs partition(month=07,day=27);


1、创建事件的基础维度类，不需要使用集合维度对象

2、创建获取维度id的udf函数，并测试
create function date_convert as 'com.qianfeng.analystic.hive.DateDimensionUdf' using jar "hdfs://qianfeng/logs/udfjars/GP1706LogAnalystic-1.0.jar";
create function platform_convert as 'com.qianfeng.analystic.hive.PlatformDimensionUdf' using jar "hdfs://qianfeng/logs/udfjars/GP1706LogAnalystic-1.0.jar";
create function event_convert as 'com.qianfeng.analystic.hive.EventDimensionUdf' using jar "hdfs://qianfeng/logs/udfjars/GP1706LogAnalystic-1.0.jar";

3、创建hive表映射每一天的数据，创建成分区表
create table if not exists dw_en(
s_time bigint,
pl string,
ca string,
ac string
)
partitioned by(month int,day int)
row format delimited fields terminated by ' '
stored as orc;


导入数据：
from logs
insert into table dw_en partition(month=7,day=27)
select s_time,pl,ca,ac
where month = 7
and day = 27;



4、创建最终结果表
CREATE external TABLE if not exists stats_event (
  `platform_dimension_id` int,
  `date_dimension_id` int,
  `event_dimension_id` int,
  `times` int,
  `created` string
);


4、写hql

-d 2018-07-27

select
 from_unixtime(cast(de.s_time/1000 as bigint),"yyyy-MM-dd"),
 de.pl,
 de.ca,
 de.ac,
 count(1)
 from dw_en de
 where pl is not null
 and de.month = 7
 and de.day = 27
 group by de.s_time,de.pl,de.ca,de.ac
 ;




  1532593870123	website	null	null	1
  1532593870456	java_server	null	null	1

   1532593870123	website	null	all	1
   1532593870456	java_server	null	all	1


  1532593870123	all	null	null	1
  1532593870456	all	null	null	1


  1532593870123	all	null	all	1
  1532593870456	all	null	all	1


5、扩展维度,并将结果集导出到最终结果表中。union all

with tmp as(
select
from_unixtime(cast(de.s_time/1000 as bigint),"yyyy-MM-dd") dt,
de.pl pl,
de.ca ca,
de.ac ac
from dw_en de
where de.pl is not null
and de.month = 7
and de.day = 27
)
from (
select pl as pl,dt,ca as ca,ac as ac,count(1) as ct from tmp group by pl,dt,ca,ac union all
select pl as pl,dt,ca as ca,'all' as ac,count(1) as ct from tmp group by pl,dt,ca union all
select pl as pl,dt,'all' as ca,'all' as ac,count(1) as ct from tmp group by pl,dt union all
select 'all' as pl,dt,ca as ca,ac as ac,count(1) as ct from tmp group by dt,ca,ac union all
select 'all' as pl,dt,ca as ca,'all' as ac,count(1) as ct from tmp group by dt,ca union all
select 'all' as pl,dt,'all' as ca,'all' as ac,count(1) as ct  from tmp group by dt
) as tmp2
insert into stats_event
select date_convert(dt),platform_convert(pl),event_convert(ca,ac),sum(ct),dt
group by pl,dt,ca,ac
;



6、使用sqoop语句将结果导出到mysql中
sqoop export --connect jdbc:mysql://hadoop01:3306/result \
--username root --password root --table stats_event \
--export-dir hdfs://qianfeng/user/hive/warehouse/lda.db/stats_event/* \
--input-fields-terminated-by "\\001" --update-mode allowinsert \
--update-key platform_dimension_id,date_dimension_id,event_dimension_id \
;

7、将整个封装成shell脚本
判断时间，当时间没有则默认使用昨天的时间在执行。


