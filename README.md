# Retail-Data-Integration-with-Snowflake-and-Azure
Retail Data Integration with Snowflake and Azure

## Retail Data Integration with Snowflake and Azure

In this project, I integrated **Snowflake** with **Azure Data Lake Storage** to build a robust data pipeline for a retail data use case. The pipeline was designed to handle large volumes of retail data efficiently while enabling insightful analysis at multiple stages. Using **Snowflake SQL**, I performed a three-tier analysis process — **Bronze**, **Silver**, and **Gold** — to progressively refine and transform raw data into actionable insights.

### Key Highlights:
- **Bronze**: Raw data ingestion and initial cleansing from Azure Data Lake Storage.
- **Silver**: Data transformations and enrichment to create a clean, usable dataset.
- **Gold**: High-level aggregation and complex queries to generate valuable insights for business decision-making.

This project demonstrated my ability to leverage cloud technologies like **Snowflake** and **Azure** for data integration and transformation, enhancing the overall data flow and providing clear, actionable insights for retail operations.
# 📊 Data Sources

## 1️⃣ CUSTOMER Data Source  
**File Format:** CSV  

### Source Description:  
The Customer data source is a CSV file containing records about customers. This data serves as the foundation for understanding customer demographics, registration details, and engagement with the platform. The file captures customer-specific information, such as their location, age, and total purchases made.  

### Key Attributes:  
- **`customer_id` (INT):** Unique identifier for the customer.  
- **`name` (VARCHAR):** Full name of the customer.  
- **`email` (VARCHAR):** Customer's email address, useful for communication and marketing.  
- **`country` (STRING):** Customer's location (country) for geographic segmentation.  
- **`customer_type` (STRING):** Customer classification (e.g., "New", "Returning", "VIP").  
- **`registration_date` (DATE):** Date of customer registration, useful for tracking lifetime and engagement.  
- **`age` (INT):** Customer's age, useful for demographic analysis.  
- **`gender` (STRING):** Gender of the customer, used for segmentation analysis.  
- **`total_purchases` (INT):** Total number of purchases made by the customer, useful for customer value analysis.  

### Use Case:  
This data source can be leveraged for customer segmentation, customer lifetime value analysis, and personalized marketing campaigns.  

---

## 2️⃣ PRODUCT Data Source  
**File Format:** JSON  

### Source Description:  
The Product data source is a JSON file containing detailed product catalog information. This file captures essential product attributes such as name, category, brand, and stock availability. The flexible structure of JSON allows for hierarchical and nested attributes, making it ideal for product catalog data.  

### Key Attributes:  
- **`product_id` (INT):** Unique identifier for each product in the catalog.  
- **`name` (VARCHAR):** Name of the product for display in online stores or internal systems.  
- **`category` (STRING):** Product category (e.g., "Electronics", "Apparel"), supporting categorization for easier search and analysis.  
- **`brand` (STRING):** Brand name of the product, used for brand-level analysis and pricing strategy.  
- **`price` (FLOAT):** Retail price of the product, crucial for pricing analysis.  
- **`stock_quantity` (INT):** Number of units available in stock, important for inventory tracking.  
- **`rating` (FLOAT):** Average product rating (on a scale of 0 to 5), used for quality and review analysis.  
- **`is_active` (BOOLEAN):** Indicates if the product is active (available) or discontinued, used for filtering out obsolete products.  

### Use Case:  
This data source enables product performance tracking, inventory management, and price optimization. The hierarchical JSON structure is flattened as part of the data transformation process.  

---

## 3️⃣ ORDER Data Source  
**File Format:** Parquet  

### Source Description:  
The Order data source is a Parquet file that tracks customer transactions. Parquet is chosen for its ability to store large datasets efficiently with fast query performance. This file contains details of all customer purchases, such as transaction ID, payment method, and purchase amount, and is used to analyze sales trends and customer purchase behavior.  

### Key Attributes:  
- **`customer_id` (INT):** Customer who placed the order (foreign key to the CUSTOMER data source).  
- **`payment_method` (STRING):** Payment method used (e.g., "Credit Card", "PayPal"), used for payment analysis.  
- **`product_id` (INT):** Identifier of the purchased product (foreign key to the PRODUCT data source).  
- **`quantity` (INT):** Quantity of the product purchased in a single order.  
- **`store_type` (STRING):** Indicates the store type (e.g., "Online", "In-store") for channel-specific analysis.  
- **`total_amount` (DOUBLE):** Total cost of the purchase, useful for revenue calculation and forecasting.  
- **`transaction_date` (DATE):** Date of the purchase transaction, enabling time-series analysis of sales trends.  
- **`transaction_id` (STRING):** Unique identifier for each transaction.  

### Use Case:  
This data source enables revenue analysis, sales trend forecasting, and order tracking. Parquet provides the advantage of better performance and space efficiency when handling large volumes of transactional data.  
