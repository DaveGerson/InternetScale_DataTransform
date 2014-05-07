--DATE is a reserved word so tran-date is used in this instance
Create table kaggle_transactions_file as (
	(id STRING,
	chain STRING,
	dept STRING,
	category STRING,
	company STRING,
	brand STRING,
	tran_date STRING,
	productSize STRING,
	productMeasure STRING,
	purchaseQuantity DOUBLE,
	spend DOUBLE)
PARTITIONED BY (tran_date STRING)
ROW FORMAT DELIMITTED
FIELDS TERMINATED BY ','
STORED AS textfile
LOCATION 's3n://dgersonbucket/Kaggle/transactions/';

--To differentiate from pig script rolling complete 3 months logic will be implemented using hive 11 syntax
create view sales_by_customer as 
SELECT 
	id  as unique_customer,
	COUNT(DISTINCT chain) as stores_shopped,
	count(DISTINCT tran_date) as trips,
	SUM(spend) as salesDollars,
	SUM(purchaseQuantity) as itemsPurchased,
	(SUM(spend)/count(DISTINCT tran_date)) as average_spend,
	(SUM(spend)/SUM(purchaseQuantity)) as average_itemcost
FROM kaggle_transactions_file main
JOIN (
SELECT last_4m.months
FROM 
(SELECT DISTINCT substr(tran_date,0,8) as months --Will bring in only yyyy-mm
FROM  kaggle_transactions_file
ORDER BY substr(tran_date,0,8) desc 
LIMIT 4) last_4m
ORDER BY last_4m.months asc
LIMIT 3) last_3completem 
last_3_dates on (subst(main.tran_date,0,8) = last_4m.months)
group by id
 
--Winsoring Using Hive 12 functions. I don't have a private hive 12 instace so this is untested.
create table winsored_customer_metrics as 
	SELECT
		id  as unique_customer,
		trip_measure.tripcounts as tripcounts,
		avgSpend_measure.avgSpend as avgSpend,	
	FROM 
		sales_by_customer main
			JOIN(
				SELECT
					unique_customer,
					IF(trip_identification.trips_tile > 195,maxTrips,IF(trip_identification.trips_tile < 6,minTrips,trip_identification.trips)) as tripcounts
				FROM(
					SELECT 
					unique_customer,
					trips,
					NTILE(200) OVER (ORDER BY trips	DESC) as trips_tile
					trip_builder
					from 
					sales_by_customer
					) trip_identification
				JOIN	
					(
					select
					max(trips) as maxTrips,
					min(trips)  as minTrips
					from
					(
					Select
					unique_customer,
					trips,
					NTILE(200) OVER (ORDER BY trips	DESC) as trips_tile
					trip_builder
					from 
					sales_by_customer
					) trip_limit
					) trip_bounds
					where trips_tile between 195 and 6
				) trip_measure
			ON main.unique_customer = trip_measure.unique_customer
			
			JOIN(
				SELECT
					unique_customer,
					IF(avgSpend_identification.avgspnd_tile > 195,maxSpend,IF(avgSpend_identification.avgspnd_tile < 6,minSpend,avgSpend_identification.trips)) as avgSpend
				FROM(
					SELECT 
					unique_customer,
					average_spend,
					NTILE(200) OVER (ORDER BY trips	DESC) as avgspnd_tile
					trip_builder
					FROM 
					sales_by_customer
					) avgSpend_identification
				JOIN	
					(
					SELECT
						max(average_spend) as maxSpend,
						min(average_spend)  as minSpend
					FROM
					(
					SELECT
						unique_customer,
						average_spend,
						NTILE(200) OVER (ORDER BY average_spend	DESC) as avgspnd_tile
						avgSpend_builder
					FROM 
					sales_by_customer
					) avgSpend_limit
					) avgSpend_bounds
					where avgspnd_tile between 195 and 6
				) avgSpend_measure
			ON main.unique_customer = avgSpend_measure.unique_customer
			
			JOIN(
				SELECT
					unique_customer,
					IF(avg_itemcost_identification.avg_itemcost_tile > 195,maxavg_itemcost,IF(avg_itemcost_identification.avg_itemcost_tile < 6,minavg_itemcost,avg_itemcost_identification.avg_itemcost)) as avg_itemcostcounts
				FROM(
					SELECT 
					unique_customer,
					avg_itemcost,
					NTILE(200) OVER (ORDER BY avg_itemcost	DESC) as avg_itemcost_tile
					avg_itemcost_builder
					from 
					sales_by_customer
					) avg_itemcost_identification
				JOIN	
					(
					select
					max(avg_itemcost) as maxavg_itemcost,
					min(avg_itemcost)  as minavg_itemcost
					from
					(
					Select
					unique_customer,
					avg_itemcost,
					NTILE(200) OVER (ORDER BY avg_itemcost	DESC) as avg_itemcost_tile
					avg_itemcost_builder
					from 
					sales_by_customer
					) avg_itemcost_limit
					) avg_itemcost_bounds
					where avg_itemcost_tile between 195 and 6
				) avg_itemcost_measure
			ON main.unique_customer = trip_measure.unique_customer
				
	
	
	







