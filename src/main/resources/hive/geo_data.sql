------------------------------------------------------------------------------------------------------------------------
--Drop & create geo_cidr table:
------------------------------------------------------------------------------------------------------------------------
drop table if exists obila.geo_cidr;

create external table obila.geo_cidr (
    cidr VARCHAR(30),
    geoname_id int,
    req_country_id int,
    rep_country_id int,
    is_anon_proxy int,
    is_satellite int)
row format delimited fields terminated by ','
stored as TEXTFILE
location '/user/obila/geo_cidr'
tblproperties ("skip.header.line.count"="1");
------------------------------------------------------------------------------------------------------------------------



------------------------------------------------------------------------------------------------------------------------
--Drop & create geo_country table:
------------------------------------------------------------------------------------------------------------------------
drop table if exists obila.geo_country;

create external table obila.geo_country (
    geoname_id VARCHAR(30),
    locale VARCHAR(2),
    continent_code VARCHAR(2),
    continent VARCHAR(30),
    country_code VARCHAR(2),
    country VARCHAR(30),
    is_eu int)
row format delimited fields terminated by ','
stored as TEXTFILE
location '/user/obila/geo_country'
tblproperties ("skip.header.line.count"="1","serialization.null.format"="");
------------------------------------------------------------------------------------------------------------------------



------------------------------------------------------------------------------------------------------------------------
--Drop & create cidr_joined table:
------------------------------------------------------------------------------------------------------------------------
drop table if exists obila.cidr_joined;

create table obila.cidr_joined as
select cidr, coalesce(geoname_id, req_country_id, rep_country_id) as geo_id
from obila.geo_cidr;
------------------------------------------------------------------------------------------------------------------------



------------------------------------------------------------------------------------------------------------------------
--Drop & create geo_joined table:
------------------------------------------------------------------------------------------------------------------------
drop table if exists obila.geo_joined;

create table obila.geo_joined
row format delimited
fields terminated by ','
stored as textfile as
    select cidr, country from
    (select gr.cidr, coalesce(gc.country, gc.continent) as country
    from obila.geo_country gc join obila.cidr_joined gr on gc.geoname_id = gr.geo_id) t;
------------------------------------------------------------------------------------------------------------------------



------------------------------------------------------------------------------------------------------------------------
--Delete & create temporary function to extract country from ip address:
------------------------------------------------------------------------------------------------------------------------
delete jar /home/obila/Udf-1.0-SNAPSHOT.jar;

create temporary function extractCountry as 'CountryByIp' using jar '/home/obila/Udf-1.0-SNAPSHOT.jar';
------------------------------------------------------------------------------------------------------------------------



------------------------------------------------------------------------------------------------------------------------
--Drop & create table of countries together with ips:
------------------------------------------------------------------------------------------------------------------------
drop table if exists obila.country_ip;

create table obila.country_ip as
select extractCountry(pip, '/user/hive/warehouse/obila.db/geo_joined/000000_0') as country, pip, pprice
from obila.ppurchases;
------------------------------------------------------------------------------------------------------------------------



------------------------------------------------------------------------------------------------------------------------
--6.4: Select top 10 countries with the highest money spending:
------------------------------------------------------------------------------------------------------------------------
drop table if exists obila.result64;

create table obila.result64 as
select sum(pprice) as spending, country from obila.country_ip
where country is not null group by country order by spending desc limit 10;
------------------------------------------------------------------------------------------------------------------------