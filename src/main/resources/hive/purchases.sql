--create external hive purchases table from flume events data
drop table if exists obila.ppurchases;

create external table obila.ppurchases (
    pname string,
    pprice double,
    pdatetime timestamp,
    pcategory string,
    pip string)
partitioned by (pdate string)
row format delimited fields terminated by ','
stored as TEXTFILE
location '/flume/events/';

drop view if exists obila.ppurchases_splt;
create view obila.ppurchases_splt as
select pname, pprice, pip, split(pip, '\\.') as splt from obila.ppurchases;


--create partitions manually
ALTER TABLE obila.ppurchases ADD PARTITION (pdate = '2019-08-01')
LOCATION '/flume/events/2019/08/01';
ALTER TABLE obila.ppurchases ADD PARTITION (pdate = '2019-08-02')
LOCATION '/flume/events/2019/08/02';
ALTER TABLE obila.ppurchases ADD PARTITION (pdate = '2019-08-03')
LOCATION '/flume/events/2019/08/03';
ALTER TABLE obila.ppurchases ADD PARTITION (pdate = '2019-08-04')
LOCATION '/flume/events/2019/08/04';
ALTER TABLE obila.ppurchases ADD PARTITION (pdate = '2019-08-05')
LOCATION '/flume/events/2019/08/05';
ALTER TABLE obila.ppurchases ADD PARTITION (pdate = '2019-08-06')
LOCATION '/flume/events/2019/08/06';
ALTER TABLE obila.ppurchases ADD PARTITION (pdate = '2019-08-07')
LOCATION '/flume/events/2019/08/07';



--5.1
drop table if exists obila.result51;

create table obila.result51 as
select pcategory as category, count(*) as cnt from obila.ppurchases GROUP BY pcategory order by cnt DESC LIMIT 10;


--5.2
drop table if exists obila.result52;

create table obila.result52 as
select pname as name, pcategory as category, cnt
from
    (select DISTINCT pname, pcategory, cnt, row_number() over(partition by pcategory order by cnt DESC) as row
     from
        (select pname, pcategory, count(*) over (partition by pcategory, pname) as cnt
         from obila.ppurchases) p1) t1
where row <= 10;