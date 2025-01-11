-- Product Profitability Analysis by Customer Type and Region
-- Which products generate the most revenue, and how does this vary by customer type and country?

CREATE OR REPLACE VIEW PRODUCT_CUSTOMER_ANALYSIS AS
SELECT 
    p.product_id,
    p.category,
    p.brand,
    c.customer_type,
    c.country,
    SUM(o.quantity * p.price) AS total_revenue,
    COUNT(DISTINCT o.customer_id) AS total_customers
FROM 
     CUSTOMERS c
JOIN 
    SILVER.ORDERS o  ON c.customer_id = o.customer_id
JOIN
    SILVER.PRODUCTS p ON o.product_id = p.product_id
GROUP BY
        p.product_id,
        p.category,
        p.brand,
        c.customer_type,
        c.country
ORDER BY 
    total_revenue;
