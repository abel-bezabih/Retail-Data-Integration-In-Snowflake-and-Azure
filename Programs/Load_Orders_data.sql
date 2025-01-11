--CREATING PARQUET FILE FORMAT
CREATE OR REPLACE FILE FORMAT PARQUET_FORMAT
TYPE='PARQUET'
COMPRESSION=AUTO;

--CREATE A TABLE IN ORDER TO LOAD ORDERS DATA
CREATE TABLE IF NOT EXISTS RAW_ORDERS_DATA(
customer_id  INT,
payment_method STRING,
product_id INT,
quantity INT,
store_type STRING,
total_amount DOUBLE,
transaction_date DATE,
transaction_id STRING,
source_file_name STRING,
source_file_row_number INT,
created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
);

--CREATING A TASK TO LOAD DATA FROM ADLS TO ABOVE TABLE
CREATE TASK IF NOT EXISTS ORDERS_DATA_TASK
WAREHOUSE=COMPUTE_WH
SCHEDULE = 'USING CRON 0 21 * * * UTC'
AS 
    COPY INTO RAW_ORDERS_DATA(
    customer_id  ,
payment_method ,
product_id ,
quantity ,
store_type ,
total_amount ,
transaction_date ,
transaction_id ,
source_file_name,
source_file_row_number
)
FROM (
SELECT 
$1: customer_id :: INT,
$1: payment_method :: STRING,
$1: product_id :: INT,
$1: quantity :: INT,
$1: store_type :: STRING,
$1: total_amount :: DOUBLE,
$1: transaction_date :: DATE,
$1: transaction_id :: STRING, 
metadata$filename,
metadata$file_row_number,
FROM @ADLS_STAGE/Orders/
)
FILE_FORMAT = (FORMAT_NAME='PARQUET_FORMAT')
ON_ERROR = 'CONTINUE'
PATTERN='.*[.]parquet';

-- INORDER TO EXECUTE THE TASK
ALTER TASK ORDERS_DATA_TASK RESUME;

--FOR IMMEDIATLE EXECUTION
EXECUTE TASK ORDERS_DATA_TASK;
