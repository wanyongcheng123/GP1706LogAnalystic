1、创建hive表映射每一天的数据，创建成分区表
create table if not exists dw_dp(
s_time bigint,
pl string,
p_url string,
u_ud string,
u_sid string
)
partitioned by(month int,day int)
row format delimited fields terminated by ' '
stored as orc;


导入数据：
from logs
insert overwrite table dw_dp partition(month=7,day=27)
select s_time,pl,p_url,u_ud,u_sid
where month = 7
and day = 27
;


and en = 'e_pv'

######################################
create table if not exists dwa_dp(
pl string,
dt string,
pv string,
u_ud string)
;

创建临时表：
create table if not exists dwa_dp(
pl string,
dt string,
col string,
ct int
)
;

最终临时表：
CREATE TABLE IF NOT EXISTS stats_view_depth (
`platform_dimension_id` int,
`data_dimension_id` int,
`kpi_dimension_id` int,
`pv1` int,
`pv2` int,
`pv3` int,
`pv4` int,
`pv5_10` int,
`pv10_30` int,
`pv30_60` int,
`pv60pluss` int,
`created` string
);


from(
select
from_unixtime(cast(dd.s_time/1000 as bigint),"yyyy-MM-dd") dt,
dd.pl pl,
(case
when count(dd.p_url) = 1 then 'pv1'
when count(dd.p_url) = 2 then 'pv2'
when count(dd.p_url) = 3 then 'pv3'
when count(dd.p_url) = 4 then 'pv4'
when count(dd.p_url) <= 10  then 'pv5_10'
when count(dd.p_url) <= 30 then 'pv10_30'
when count(dd.p_url) <= 60 then 'pv30_60'
else 'pv60pluss'
end) as pv,
dd.u_ud u_ud
from dw_dp dd
where dd.pl is not null
and dd.p_url is not null
and dd.month = 7
and dd.day = 27
group by pl,from_unixtime(cast(dd.s_time/1000 as bigint),"yyyy-MM-dd"),u_ud
) as tmp
insert overwrite table dwa_dp
select pl,dt,pv,count(u_ud)
group by pl,dt,pv
;

dt pl u_ud pv
1  1  123  pv2
1  1  123  pv2
1  1  345  pv3

1 1 pv2 2
1 1 pv3 1

1 1 2 0 2 1 0 0 0 0 2018-07-27

扩展all维度：
with tmp as(
select pl as pl,dt,ct as pv1,0 as pv2,0 as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60pluss from dwa_dp where col='pv1' union all
select pl as pl,dt,0 as pv1,ct as pv2,0 as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60pluss from dwa_dp where col='pv2' union all
select pl as pl,dt,0 as pv1,0 as pv2,ct as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60pluss from dwa_dp where col='pv3' union all
select pl as pl,dt,0 as pv1,0 as pv2,0 as pv3,ct as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60pluss from dwa_dp where col='pv4' union all
select pl as pl,dt,0 as pv1,0 as pv2,0 as pv3,0 as pv4,ct as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60pluss from dwa_dp where col='pv5_10' union all
select pl as pl,dt,0 as pv1,0 as pv2,0 as pv3,0 as pv4,0 as pv5_10,ct as pv10_30,0 as pv30_60,0 as pv60pluss from dwa_dp where col='pv10_30' union all
select pl as pl,dt,0 as pv1,0 as pv2,0 as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,ct as pv30_60,0 as pv60pluss from dwa_dp where col='pv30_60' union all
select pl as pl,dt,0 as pv1,0 as pv2,0 as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,ct as pv60pluss from dwa_dp where col='pv60pluss' union all
select 'all' as pl,dt,ct as pv1,0 as pv2,0 as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60pluss from dwa_dp where col='pv1' union all
select 'all' as pl,dt,0 as pv1,ct as pv2,0 as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60pluss from dwa_dp where col='pv2' union all
select 'all' as pl,dt,0 as pv1,0 as pv2,ct as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60pluss from dwa_dp where col='pv3' union all
select 'all' as pl,dt,0 as pv1,0 as pv2,0 as pv3,ct as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60pluss from dwa_dp where col='pv4' union all
select 'all' as pl,dt,0 as pv1,0 as pv2,0 as pv3,0 as pv4,ct as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60pluss from dwa_dp where col='pv5_10' union all
select 'all' as pl,dt,0 as pv1,0 as pv2,0 as pv3,0 as pv4,0 as pv5_10,ct as pv10_30,0 as pv30_60,0 as pv60pluss from dwa_dp where col='pv10_30' union all
select 'all' as pl,dt,0 as pv1,0 as pv2,0 as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,ct as pv30_60,0 as pv60pluss from dwa_dp where col='pv30_60' union all
select 'all' as pl,dt,0 as pv1,0 as pv2,0 as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,ct as pv60pluss from dwa_dp where col='pv60pluss'
)
from tmp
insert overwrite table stats_view_depth
select date_convert(dt),platform_convert(pl),2,sum(pv1),sum(pv2),sum(pv3),sum(pv4),sum(pv5_10),sum(pv10_30),sum(pv30_60),sum(pv60pluss),dt
group by pl,dt
;


sqoop语句：
sqoop export --connect jdbc:mysql://hadoop01:3306/result \
--username root --password root --table stats_view_depth \
--export-dir hdfs://qianfeng/user/hive/warehouse/lda.db/stats_view_depth/* \
--input-fields-terminated-by "\\001" --update-mode allowinsert \
--update-key platform_dimension_id,date_dimension_id,kpi_dimension_id \
;


####################session depth-view ################################


