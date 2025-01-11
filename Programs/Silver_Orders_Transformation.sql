CREATE OR REPLACE PROCEDURE ORDERS_TABLE_TRANSFORMATION()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    MERGE INTO SILVER.ORDERS AS target 
    USING (
        SELECT
            customer_id,
            CASE 
                WHEN payment_method IS NULL THEN 'UNKNOWN'
                ELSE payment_method
            END AS payment_method,
            product_id,
            CASE 
                WHEN quantity < 0 THEN 0
                ELSE quantity
            END AS quantity,
            CASE 
                WHEN store_type IS NULL THEN 'UNKNOWN'
                ELSE store_type
            END AS store_type,
            CASE 
                WHEN total_amount < 0 THEN 0
                ELSE total_amount
            END AS total_amount,
            transaction_date,
            transaction_id,
            CURRENT_TIMESTAMP AS created_at
        FROM BRONZE.ORDERS_DATA_STREAM
        WHERE product_id IS NOT NULL 
          AND transaction_id IS NOT NULL 
          AND customer_id IS NOT NULL
    ) AS source
    ON target.transaction_id = source.transaction_id
    WHEN MATCHED THEN
        UPDATE SET
            customer_id = source.customer_id,
            payment_method = source.payment_method,
            product_id = source.product_id,
            quantity = source.quantity,
            store_type = source.store_type,
            total_amount = source.total_amount,
            transaction_date = source.transaction_date,
            created_at = source.created_at
    WHEN NOT MATCHED THEN
        INSERT (
            customer_id,
            payment_method,
            product_id,
            quantity,
            store_type,
            total_amount,
            transaction_date,
            created_at
        ) 
        VALUES (
            source.customer_id,
            source.payment_method,
            source.product_id,
            source.quantity,
            source.store_type,
            source.total_amount,
            source.transaction_date,
            source.created_at
        );

    RETURN 'ORDERS PROCESSING DONE';
END;
$$;


-- CREATING A TASK TO RUN ABOVE PROCEDURE
CREATE OR REPLACE TASK ORDERS_MERGE_TASK
WAREHOUSE = COMPUTE_WH
SCHEDULE = 'USING CRON 0 21 * * * UTC'
AS 
CALL ORDERS_TABLE_TRANSFORMATION();

-- TO EXECUTE THE TASK
ALTER TASK ORDERS_MERGE_TASK RESUME;

EXECUTE TASK ORDERS_MERGE_TASK;
