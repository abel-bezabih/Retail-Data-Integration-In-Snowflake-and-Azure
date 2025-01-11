CREATE OR REPLACE PROCEDURE CUSTOMERS_TABLE_TRANSFORMATION()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
 no_of_rows_inserted INT;
 no_of_rows_updated INT;
BEGIN
    MERGE INTO SILVER.CUSTOMERS AS target
    USING(
    SELECT
        customer_id,
        
        --NAME STANDARDIZATION
        CASE
            WHEN name IS NULL THEN 'UNKNOWN'
            ELSE name
        END AS name,
        
        --EMAIL STANDARDIZATION
        CASE
            WHEN email IS NULL THEN 'N/A'
            ELSE email
        END AS email,
        
        --COUNTRY STANDRADIZATION
        CASE
            WHEN country IS NULL THEN 'N/A'
            ELSE country
        END AS country,
        
        --CUSTOMER TYPE STANDARDIZATION
        CASE
            WHEN TRIM(UPPER(customer_type)) IN ('REGULAR','REG','R') THEN 'Regular'
            WHEN TRIM(UPPER(customer_type)) IN ('PREMIUM','PREM','P') THEN 'Premium'
            WHEN TRIM(UPPER(customer_type)) IN ('V.I.P','V','VIP') THEN 'VIP'
            ELSE 'UNKNOWN'
        END AS customer_type,
        
        registration_date,
        
        -- AGE STANDARDIZATION
        CASE 
            WHEN age BETWEEN 15 AND 130 THEN age 
            ELSE NULL
        END AS age,
        
        -- GENDER STANDARDIZATION
        CASE
            WHEN TRIM(UPPER(gender)) IN ('MALE','M','1') THEN 'Male'
            WHEN TRIM(UPPER(gender)) IN ('FEMALE','F','0') THEN 'Female'
            ELSE 'Other'
        END AS gender,

        --TOTAL PURCHASE STANDARDIZATION
        CASE 
            WHEN total_purchases >= 0 THEN total_purchases
            ELSE 0
        END AS total_purchases,
        CURRENT_TIMESTAMP AS created_at
    FROM BRONZE.CUSTOMER_DATA_STREAM
    WHERE customer_id IS NOT NULL AND email IS NOT NULL
        ) AS source
        ON target.customer_id = source.customer_id
    WHEN MATCHED THEN
        UPDATE SET
        name = source.name,
        email = source.email,
        country=source.country,
        customer_type=source.customer_type,
        registration_date=source.registration_date,
        age=source.age,
        gender=source.gender,
        total_purchases=source.total_purchases,
        created_at = source.created_at
    WHEN NOT MATCHED THEN
    INSERT
        (customer_id,
        name,
        email,
        country,
        customer_type,
        registration_date,
        age,
        gender,
        total_purchases,
        created_at)
    VALUES
        (source.customer_id,
        source.name,
        source.email,
        source.country,
        source.customer_type,
        source.registration_date,
        source.age,
        source.gender,
        source.total_purchases,
        source.created_at);
RETURN 'CUSTOMERS DONE';
END;
$$;

--CREATING A TASK TO PERFORM ABOVE PROCEDURE
CREATE OR REPLACE TASK CUSTOMER_MERGE_TASK
WAREHOUSE = COMPUTE_WH
SCHEDULE = 'USING CRON 0 21 * * * UTC'
AS
CALL CUSTOMERS_TABLE_TRANSFORMATION();

-- TO EXECUTE THE TASK
ALTER TASK CUSTOMER_MERGE_TASK RESUME;

EXECUTE TASK CUSTOMER_MERGE_TASK;

        