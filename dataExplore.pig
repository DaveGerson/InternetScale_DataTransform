kaggle_transactions_gzfile = LOAD 's3n://dgersonbucket/Kaggle/transactions.csv.gz'
Using PigStorage();

STORE kaggle_transactions_gzfile INTO 's3n://dgersonbucket/Kaggle/transactions/'

kaggle_transactions_file = LOAD 's3n://dgersonbucket/Kaggle/transactions/'
Using org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'YES_MULTILINE')
   as (id:chararray,chain:chararray,dept:chararray,category:chararray,brand:chararray,date:chararray,productsize:chararray,productmeasure:chararray,purchasequantity:chararray,spend:chararray);

limited_output = limit kaggle_transactions_castfile 1000;

kaggle_transactions_file = LOAD 's3n://dgersonbucket/Kaggle/transactions/'
Using PigStorage(',')
   as (id:chararray,chain:chararray,dept:chararray,category:chararray,company:chararray,brand:chararray,date:chararray,productsize:chararray,productmeasure:chararray,purchasequantity:chararray,spend:chararray);

uncast_limited_output = limit kaggle_transactions_file 1000;
STORE uncast_limited_output INTO 's3n://dgersonbucket/Kaggle/out/uncast_limited'   ;    

kaggle_transactions_castfile =
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


 kaggle_transactions_group = group kaggle_transactions_castfile all;
    data_counts        =    FOREACH kaggle_transactions_group {
                                Unique_customer = DISTINCT kaggle_transactions_castfile.id;
        						Unique_chain = DISTINCT kaggle_transactions_castfile.chain;
                                Unique_dept = DISTINCT kaggle_transactions_castfile.dept;
                                Unique_category = DISTINCT kaggle_transactions_castfile.category;
                                Unique_company = DISTINCT kaggle_transactions_castfile.company;
                        		Unique_brand = DISTINCT kaggle_transactions_castfile.brand;
                                GENERATE
                                COUNT(Unique_customer) as cust_count,
								COUNT(Unique_chain) as chain_count,
                                COUNT(Unique_dept) as dept_count,
                                COUNT(Unique_company) as company_count,
                                COUNT(Unique_category) as cat_count,
                                COUNT(Unique_brand) as brand_count;								
								};
                                
                                
spend_and_qty =  FOREACH (GROUP kaggle_transactions_castfile ALL) GENERATE
SUM(kaggle_transactions_castfile.spend) as spend,
SUM(kaggle_transactions_castfile.purchasequantity) as qty;                    

limited_output = limit kaggle_transactions_castfile 1000;


STORE data_counts INTO  's3n://dgersonbucket/Kaggle/out/data_counts';
STORE spend_and_qty INTO 's3n://dgersonbucket/Kaggle/out/spend_and_qty'   ;    
STORE limited_output INTO 's3n://dgersonbucket/Kaggle/out/limited_output'; 



 kaggle_transactions_group = group kaggle_transactions_castfile all;
    data_counts        =    FOREACH kaggle_transactions_group {
                                Unique_customer = DISTINCT kaggle_transactions_castfile.id;
        						Unique_chain = DISTINCT kaggle_transactions_castfile.chain;
                                Unique_dept = DISTINCT kaggle_transactions_castfile.dept;
                                Unique_category = DISTINCT kaggle_transactions_castfile.category;
                        		Unique_brand = DISTINCT kaggle_transactions_castfile.brand;
                                GENERATE
                                COUNT(Unique_customer) as cust_count,
								COUNT(Unique_chain) as chain_count,
                                COUNT(Unique_dept) as dept_count,
                                COUNT(Unique_category) as cat_count,
                                COUNT(Unique_brand) as brand_count;								
								};
                                
                                
spend_and_qty =  FOREACH (GROUP kaggle_transactions_castfile ALL) GENERATE
SUM(kaggle_transactions_castfile.spend) as spend,
SUM(kaggle_transactions_castfile.purchasequantity) as qty;                    

limited_output = limit kaggle_transactions_castfile 1000;

STORE data_counts INTO  's3n://dgersonbucket/Kaggle/out_cast/data_counts';
STORE spend_and_qty INTO 's3n://dgersonbucket/Kaggle/out_cast/spend_and_qty'   ;          
STORE limited_output INTO 's3n://dgersonbucket/Kaggle/out_cast/limited_output'; 
