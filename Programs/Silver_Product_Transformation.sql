--CREATING PROCEDURE FOR PRODUCT DATA
CREATE OR REPLACE PROCEDURE PRODUCTS_TABLE_TRANSFORMATION()
RETURNS STRING
LANGUAGE SQL
AS 
$$
BEGIN
    MERGE INTO SILVER.PRODUCTS AS target 
    USING (
    SELECT
    product_id,
    CASE
        WHEN name IS NULL THEN 'UNKNOWN'
        ELSE name
    END AS name,
    CASE
        WHEN category IS NULL THEN 'UNKNOWN'
        ELSE category
    END AS category,
    CASE
        WHEN brand IS NULL THEN 'UNKNOWN'
        ELSE brand
    END AS brand,
    CASE 
        WHEN price >= 0 THEN price 
        ELSE 0
    END AS price,
    CASE 
        WHEN stock_quantity >=0 THEN stock_quantity
        ELSE 0
    END AS stock_quantity,
    CASE
        WHEN rating BETWEEN 0 AND 5 THEN rating
        ELSE 0
    END AS rating,
    is_active,
    CURRENT_TIMESTAMP AS created_at
    FROM BRONZE.PRODUCTS_DATA_STREAM
    WHERE product_id IS NOT NULL
    ) AS source
    ON target.product_id=source.product_id
    WHEN MATCHED 
    THEN 
        UPDATE SET
        name =source.name,
        category=source.category ,
        brand=source.brand ,
        price=source.price ,
        stock_quantity=source.stock_quantity ,
        rating =source.rating,
        is_active=source.is_active, 
        created_at = source.created_at
    WHEN NOT MATCHED
    THEN INSERT(
      product_id ,
    name ,
    category ,
    brand ,
    price ,
    stock_quantity ,
    rating ,
    is_active, 
    created_at
    )
    VALUES(
    source.product_id ,
    source.name ,
    source.category ,
    source.brand ,
    source.price ,
    source.stock_quantity ,
    source.rating ,
    source.is_active, 
    source.created_at
    );
    RETURN 'PRODUCTS PRCESSING DONE';
    END;
    $$;

--CREATE A TASK TO MERGE PRODUCTS TABLE
CREATE OR REPLACE TASK PRODUCTS_MERGE_TASK
WAREHOUSE= COMPUTE_WH
SCHEDULE = 'USING CRON 0 21 * * * UTC'
AS
CALL PRODUCTS_TABLE_TRANSFORMATION();

--TO EXECUTE TASK
ALTER TASK PRODUCTS_MERGE_TASK RESUME;

EXECUTE TASK PRODUCTS_MERGE_TASK;
    
    