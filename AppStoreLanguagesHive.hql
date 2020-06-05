SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=2000;
SET hive.exec.max.dynamic.partitions.pernode=500;
USE wawrzynimaci;

DROP TABLE IF EXISTS  `temp_iosdata` PURGE;

CREATE TEMPORARY EXTERNAL TABLE IF NOT EXISTS `temp_iosdata`(
  `_id` string, 
  `ios_app_id` int, 
  `developer_ios_id` int, 
  `ios_store_url` string, 
  `seller_official_website` string, 
  `age_rating` string, 
  `total_average_rating` double, 
  `total_number_of_ratings` int, 
  `average_rating_for_version` double, 
  `number_of_ratings_for_version` int, 
  `original_release_date` string, 
  `current_version_release_date` string, 
  `price_usd` double, 
  `primary_genre` string, 
  `all_genres` string, 
  `languages` string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ( "separatorChar" = ",", "quoteChar" = "\"")  
STORED AS TEXTFILE
LOCATION '/user/wawrzynimaci/data'
TBLPROPERTIES ('skip.header.line.count'='1', 'colelction.delim'=',');
	  
DROP TABLE IF EXISTS `app_avg` PURGE;

CREATE TEMPORARY TABLE IF NOT EXISTS `app_avg` as (
  SELECT temp_iosdata.primary_genre as genre, AVG(temp_iosdata.price_usd) as avg_val
	from temp_iosdata
	where temp_iosdata.price_usd is not null or temp_iosdata.price_usd >= 0
	group by temp_iosdata.primary_genre
  );

DROP TABLE IF EXISTS `app_data` PURGE;

CREATE TABLE `app_data` (
  `price_usd` double, 
  `languages` string)
COMMENT 'mobile application database from appstore, when the application price is higher than the average price for a given category'
PARTITIONED BY(primary_genre string)
CLUSTERED BY(price_usd) INTO 8 BUCKETS
STORED AS ORC;
  
INSERT OVERWRITE TABLE `app_data` PARTITION(primary_genre)
	SELECT temp_iosdata.price_usd, temp_iosdata.languages, temp_iosdata.primary_genre
	from temp_iosdata 
	where temp_iosdata.price_usd >= (
  		SELECT app_avg.avg_val 
 		from app_avg 
  		where app_avg.genre = temp_iosdata.primary_genre);
  
select w.primary_genre as kategoria, w.lang as Jezyk, w.counts as Liczba_wystapien, w.rank
from (
	select *, rank() over ( partition by q.primary_genre order by q.primary_genre, q.counts desc) as rank 
	from (   
	   select * 
	   from (
		 select d.primary_genre, d.lang, COUNT(1) as counts 
		  from (
			SELECT  t.primary_genre, regexp_replace( t.lang, " | ", "") as lang
			from ( 
			  SELECT app_data.primary_genre, lang
			  FROM app_data
			  LATERAL VIEW  explode(split(regexp_replace(app_data.languages,"[\\[|\\'|\\]]",""),","))  adTable AS lang
			) as t
		  ) as d
		  group by d.primary_genre, d.lang
		  order by d.primary_genre, counts desc
		 ) as z
	   where z.primary_genre <> ""
	 ) as q
) as w
where w.rank <= 3
  	
  
  

  
