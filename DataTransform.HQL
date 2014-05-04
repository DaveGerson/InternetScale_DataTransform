Create table kaggle_transactions_file as (
	(id STRING,
	chain STRING,
	dept STRING,
	category STRING,
	company STRING,
	brand STRING,
	date STRING,
	productsize STRING,
	productmeasure STRING,
	purchasequantity DOUBLE,
	spend DOUBLE)
PARTITIONED BY (DATE STRING)
ROW FORMAT DELIMITTED
FIELDS TERMINATED BY ','
STORED AS textfile
LOCATION 's3n://dgersonbucket/Kaggle/transactions/';
