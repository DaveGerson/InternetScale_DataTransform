-- Giant Datafile being Input
/*
    *id - A unique id representing a customer
    *chain - An integer representing a store chain
    *dept - An aggregate grouping of the Category (e.g. water)
    *category - The product category (e.g. sparkling water)
    *company - An id of the company that sells the item
    *brand - An id of the brand to which the item belongs
    *date - The date of purchase
    *productsize - The amount of the product purchase (e.g. 16 oz of water)
    *productmeasure - The units of the product purchase (e.g. ounces)
    *purchasequantity - The number of units purchased
    *spend - The dollar amount of the purchase
*/



DEFINE sales_by_customer(Transactions)
RETURNS customer_sales {
    customer_grouping       =   GROUP $Transactions BY id;
    $customer_sales        =    FOREACH customer_grouping { 
        						Unique_chains = DISTINCT $Transactions.chain;
								Unique_trips = DISTINCT $Transactions.date;
                                GENERATE
								FLATTEN(group) as (id), 
								COUNT(Unique_chains) AS stores_shopped,
                                COUNT(Unique_trips) AS trips,
								SUM($Transactions.spend) AS salesDollars,
								SUM($Transactions.purchasequantity) AS itemsPurchased,
								SUM($Transactions.spend)/COUNT(Unique_trips) AS average_spend,
                                SUM($Transactions.spend)/SUM($Transactions.purchasequantity) AS average_itemcost;
    }
								;
	};

DEFINE item_granular_info(Transactions)
RETURNS item_granular_data {


    item_grouping         =   GROUP $Transactions BY (chain,dept,category,date,company, brand);
    $item_granular_data        =    FOREACH item_grouping {
                                    distinct_cust = DISTINCT $Transactions.id;
                                    GENERATE FLATTEN(group) as (chain,dept,category,date,company, brand),
        						COUNT(distinct_cust) as tripcount,
								SUM($Transactions.spend) AS salesDollars,
								SUM($Transactions.purchasequantity) AS itemsPurchased,	SUM($Transactions.spend)/SUM($Transactions.purchasequantity) AS average_itemcost;};
				
								
	};	
	
DEFINE company_granular_info(Transactions)
RETURNS company_granular_data {

	companies				   =   GROUP $Transactions BY company;
    $company_granular_data     =    FOREACH companies { 
								Unique_chains = DISTINCT $Transactions.chain;
								Unique_brands = DISTINCT $Transactions.brand;
                                GENERATE
								FLATTEN(group) as (company), 
								COUNT(Unique_chains) AS stores_shopped,
                                COUNT(Unique_brands) AS brands,
								SUM($Transactions.salesDollars) AS salesDollars,
								SUM($Transactions.itemsPurchased) AS itemsPurchased,
								SUM($Transactions.tripcount) AS trips,
								SUM($Transactions.salesDollars)/SUM($Transactions.itemsPurchased) AS average_itemcost
								;
                                };							
	};		
	
DEFINE store_granular_info(Transactions)
RETURNS store_granular_data {

	chainstore				   =   GROUP $Transactions BY chain;
    $store_granular_data     =    FOREACH chainstore { 
								Unique_dept = DISTINCT $Transactions.dept;
								Unique_categories = DISTINCT $Transactions.category;
								Unique_brands = DISTINCT $Transactions.brand;
                                GENERATE
								FLATTEN(group) as (chain), 
								COUNT(Unique_dept) AS departments,
								COUNT(Unique_categories) AS categories,
                                COUNT(Unique_brands) AS brands,
								SUM($Transactions.salesDollars) AS salesDollars,
								SUM($Transactions.itemsPurchased) AS itemsPurchased,
								SUM($Transactions.tripcount) AS trips,
								SUM($Transactions.salesDollars)/SUM($Transactions.itemsPurchased) AS average_itemcost
								;
                                };
	};		

kaggle_transactions_file = LOAD 's3n://dgersonbucket/Kaggle/transactions/'
Using PigStorage(',')
   as (id:chararray,chain:chararray,dept:chararray,category:chararray,company:chararray,brand:chararray,date:chararray,productsize:chararray,productmeasure:chararray,purchasequantity:chararray,spend:chararray);



kaggle_transactions_castfile_unfiltered =
    FOREACH kaggle_transactions_file
    GENERATE
    id,
    chain,
    dept,
    category,
    company,
    brand,
    date,
    productsize,
    productmeasure,
        (DOUBLE) REGEX_EXTRACT(purchasequantity,'(-?\\d+(\\.\\d+)?)',1) as purchasequantity,
        (DOUBLE) REGEX_EXTRACT(spend,'(-?\\d+(\\.\\d+)?)',1) as spend;

    --Filter out Returns
    kaggle_transactions_castfile = FILTER kaggle_transactions_castfile_unfiltered BY spend > 0;
   
customer_info = sales_by_customer(kaggle_transactions_castfile);
item_info = item_granular_info(kaggle_transactions_castfile);
company_info = company_granular_info(item_info);
store_info = store_granular_info(item_info);
   
   
   
STORE customer_info INTO 's3n://dgersonbucket/Kaggle/customerFile/' Using 
     org.apache.pig.piggybank.storage.CSVExcelStorage() ;

STORE item_info INTO 's3n://dgersonbucket/Kaggle/itemFile/' Using 
   org.apache.pig.piggybank.storage.CSVExcelStorage() ;

STORE company_info INTO 's3n://dgersonbucket/Kaggle/companyFile/' Using 
   org.apache.pig.piggybank.storage.CSVExcelStorage() ;

STORE store_info INTO 's3n://dgersonbucket/Kaggle/storeFile/' Using 
   org.apache.pig.piggybank.storage.CSVExcelStorage() ;   
  

