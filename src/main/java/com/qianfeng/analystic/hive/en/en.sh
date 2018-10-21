#!/bin/bash

#./en.sh -n -m -d 2018-07-27
run_date=
until [ $# -eq 0 ]
do
if [ $1'x' = '-dx' ]
shift
run_date=$1
fi
shift
done


if [ ${#run_date} != 10 ]
then
echo "$run_date"
else
run_date=`date -d "1 days ago" "+%Y-%m-%d"`
fi

month=`date -d "$run_date" "+%m"`
day=`date -d "$run_date" "+%d"`
echo "final running date is:${run_date},${month},${day}"

###############################################
########run hql statment
########
##############################################

hive --datebase lda -e "
with tmp as(
select
from_unixtime(cast(de.s_time/1000 as bigint),'yyy-MM-dd') dt,
de.pl pl,
de.ca ca,
de.ac ac
from dw_en de
where de.pl is not null
and de.month = "${month}"
and de.day = "${day}"
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
"

echo "run sqoop statment..."
sqoop export --connect jdbc:mysql://hadoop01:3306/result \
--username root --password root --table stats_event \
--export-dir hdfs://qianfeng/user/hive/warehouse/lda.db/stats_event/* \
--input-fields-terminated-by "\\001" --update-mode allowinsert \
--update-key platform_dimension_id,date_dimension_id,event_dimension_id \
;

echo "event job is finished."
