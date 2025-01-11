--CREATING FILE FORMAT FOR JSON DATA 
CREATE OR REPLACE FILE FORMAT JSON_FORMAT
TYPE = 'JSON'
STRIP_OUTER_ARRAY=TRUE
IGNORE_UTF8_ERRORS=TRUE
COMPRESSION=AUTO;

-- CREATING TABLE TO LOAD PRODUCT DATA
CREATE TABLE IF NOT EXISTS RAW_PRODUCT_DATA (
product_id INT,
name VARCHAR,
category VARCHAR,
brand VARCHAR,
price FLOAT,
stock_quantity INT,
rating FLOAT,
is_active BOOLEAN,
source_file_name VARCHAR,
source_file_row_number INT,
created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);


--CREATING TASK TO LOAD DATA FROM ADLS INTO ABOVE TABLE
CREATE TASK IF NOT EXISTS PRODUCT_DATA_TASK
WAREHOUSE = COMPUTE_WH
SCHEDULE = 'USING CRON 0 21 * * * UTC'
AS
    COPY INTO RAW_PRODUCT_DATA(
    product_id ,
    name ,
    category ,
    brand ,
    price ,
    stock_quantity ,
    rating ,
    is_active, 
    source_file_name ,
    source_file_row_number,
    created_at 
    )
FROM (
SELECT 
$1:product_id :: INT,
$2:name:: STRING,
$3:category :: STRING,
$4:brand :: STRING,
$5:price:: FLOAT,
$6:stock_quantity :: INT,
$7:rating :: FLOAT,
$8:is_active :: BOOLEAN,
metadata$filename,
metadata$file_row_number
FROM @ADLS_STAGE/Products/
)
FILE_FORMAT=(FORMAT_NAME='JSON_FORMAT')
ON_ERROR='CONTINUE'
PATTERN = '.*[.]json';

-- IN ORDER TO RUN THE TASK WE NEED TO START IT
ALTER TASK PRODUCT_DATA_TASK RESUME;

EXECUTE TASK PRODUCT_DATA_TASK;

