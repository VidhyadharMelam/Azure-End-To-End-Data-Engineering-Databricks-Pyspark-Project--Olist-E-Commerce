# Databricks notebook source

spark.conf.set("fs.azure.account.auth.type.olistdatalake0.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.olistdatalake0.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.olistdatalake0.dfs.core.windows.net", "337998dd-c666-4fe7-85b6-8c1c5a7bf3c0")
spark.conf.set("fs.azure.account.oauth2.client.secret.olistdatalake0.dfs.core.windows.net", "B6F8Q~HtmJel-dvqF_JByXuNFMYQxpi.LjAl5beU")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.olistdatalake0.dfs.core.windows.net", "https://login.microsoftonline.com/26a6638c-cdb1-4861-8638-bb12bcd55165/oauth2/token")

# COMMAND ----------

# MAGIC %md
# MAGIC ## IMPORT LIBRARIES

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, regexp_replace, split, concat_ws, trim, upper, lower, avg, countDistinct
from pyspark.sql.types import IntegerType, DoubleType, StringType, DateType, TimestampType
from pyspark.sql import Window
from pyspark.sql.functions import row_number, lit

# COMMAND ----------

# MAGIC %md
# MAGIC ## SPARK SESSION

# COMMAND ----------

spark = SparkSession.builder.appName("SilverLayerProcessing").getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ## LOAD TABLES FROM BRONZE LAYER

# COMMAND ----------

df_customers = spark.read.format("delta").load("abfss://silver@olistdatalake0.dfs.core.windows.net/customers")
df_geolocation = spark.read.format("delta").load("abfss://silver@olistdatalake0.dfs.core.windows.net/geolocation")
df_order_items = spark.read.format("delta").load("abfss://silver@olistdatalake0.dfs.core.windows.net/order_items")
df_order_payments = spark.read.format("delta").load("abfss://silver@olistdatalake0.dfs.core.windows.net/order_payments")
df_order_reviews = spark.read.format("delta").load("abfss://silver@olistdatalake0.dfs.core.windows.net/order_reviews")
df_orders = spark.read.format("delta").load("abfss://silver@olistdatalake0.dfs.core.windows.net/orders")
df_products = spark.read.format("delta").load("abfss://silver@olistdatalake0.dfs.core.windows.net/products")
df_sellers = spark.read.format("delta").load("abfss://silver@olistdatalake0.dfs.core.windows.net/sellers")
df_product_category_name_translation = spark.read.format("delta").load("abfss://silver@olistdatalake0.dfs.core.windows.net/product_category_name_translation")

# COMMAND ----------

# MAGIC %md
# MAGIC # DIMENSIONS

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create DimCustomers Table

# COMMAND ----------

# Objective:
# The DimCustomer table stores customer-related attributes. This table will be used for joining with fact tables like FactSales.

# Transformations Applied:
# ✅ Generating Surrogate Keys → Unique customer_sk for efficient joins
# ✅ Joining Multiple Tables → Enriching customer data
# ✅ Handling Missing Values → Replacing NULLs with 'Unknown'
# ✅ Formatting Date Columns → Converting to standard formats
# ✅ Optimizing Query Performance → Storing as Delta Table

# COMMAND ----------

from pyspark.sql.functions import col, lit, monotonically_increasing_id

# Generate Surrogate Key
df_customers = df_customers.withColumn("customer_sk", monotonically_increasing_id())

# Handle Missing Values
df_customers = df_customers.fillna(
    {
        'customer_unique_id': 'Unknown', 
        'customer_id': 'Unknown',
        'customer_zip_code': 'Unknown',
        'customer_city': 'Unknown', 
        'customer_state': 'Unknown',
        'customer_full_address': 'Unknown'
        
    })

# Select Required Columns
df_customers_dim = df_customers.select(
    col("customer_sk"),
    col("customer_id").alias("natural_customer_key"),  # Natural Key
    col("customer_unique_id"),
    col("customer_zip_code"),
    col("customer_city"),
    col("customer_state"),
    col("customer_full_address")
)

# Save as Delta Table in Gold Layer
df_customers_dim.write.format("delta")\
                .mode("overwrite")\
                .option("path", "abfss://gold@olistdatalake0.dfs.core.windows.net/dim_customer")\
                .save()


# COMMAND ----------

# Explanation of Transformations
# 🔹 monotonically_increasing_id() → Generates a unique surrogate key (customer_sk).
# 🔹 fillna() → Replaces NULL values with 'Unknown' to prevent data loss.
# 🔹 Column Selection & Renaming → Keeps relevant columns and renames customer_id for clarity.
# 🔹 Saving as Delta Format → Ensures efficient query performance.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 - Read Products and Product Category Name Translation Tables

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create DimProduct Table

# COMMAND ----------

# Objective:
# The DimProduct table stores product-related attributes such as product category, weight, dimensions, and name translation. It will be used for joining with fact tables like FactSales.

# Transformations Applied:
# ✅ Generating Surrogate Keys → Unique product_sk for better joins
# ✅ Joining Multiple Tables → Enriching product data with category names
# ✅ Handling Missing Values → Replacing NULLs with 'Unknown'
# ✅ Data Formatting & Standardization → Ensuring consistent formats
# ✅ Optimizing Query Performance → Storing as Delta Table

# COMMAND ----------

from pyspark.sql.functions import col, monotonically_increasing_id

# Generate Surrogate Key
df_products = df_products.withColumn("product_sk", monotonically_increasing_id())

# Handle Missing Values
df_products = df_products.fillna(
    {
        'product_id': 'Unknown',
        'product_category_name': 'Unknown',
        'product_name_length': 0,
        'product_description_length': 0,
        'product_photos_quantity': 0,
        'product_weight_grams': 0,
        'product_length_centimeter': 0,
        'product_height_centimeter': 0,
        'product_width_centimeter': 0      
})

# Join with Product Category Name Translation Table
df_dim_product = df_products.join(
    df_product_category_name_translation, 
    on="product_category_name", 
    how="left"
)

# Select Required Columns
df_dim_product = df_dim_product.select(
    col("product_sk"),
    col("product_id").alias("natural_product_key"),  # Natural Key
    col("product_category_name").alias("category"),
    col("product_category_name_english").alias("category_english"),
    col("product_weight_grams"),
    col("product_length_centimeter"),
    col("product_height_centimeter"),
    col("product_width_centimeter")
)

# Save as Delta Table in Gold Layer
df_dim_product.write.format("delta")\
                .mode("overwrite")\
                .option("path", "abfss://gold@olistdatalake0.dfs.core.windows.net/dim_product")\
                .save()


# COMMAND ----------

# Explanation of Transformations
# 🔹 monotonically_increasing_id() → Creates a surrogate key (product_sk) for better joins.
# 🔹 fillna() → Replaces NULL values for missing weight, dimensions, and categories.
# 🔹 join() with product_category_df → Adds English product category names.
# 🔹 Column Selection & Renaming → Standardizes column names for clarity.
# 🔹 Saving as Delta Format → Ensures fast queries and optimization.



# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 - Read Sellers Table

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create DimSellers Table

# COMMAND ----------

# Objective:
# The DimSeller table will store seller-related attributes such as seller location (ZIP), seller unique ID, and additional information. It will be used for joining with fact tables like FactSales.

# Transformations Applied:
# ✅ Generating Surrogate Keys → Unique seller_sk for efficient joins
# ✅ Handling Missing Values → Filling NULLs with 'Unknown'
# ✅ Renaming & Standardizing Columns → Ensuring consistency
# ✅ Optimizing Query Performance → Storing as Delta Table

# COMMAND ----------

from pyspark.sql.functions import col, monotonically_increasing_id

# Generate Surrogate Key
df_sellers = df_sellers.withColumn("seller_sk", monotonically_increasing_id())

# Handle Missing Values
df_sellers = df_sellers.fillna({
    'seller_zip_code': 'Unknown',
    'seller_city': 'Unknown',
    'seller_id': 'Unknown',
    'seller_state': 'Unknown'
})

# Select Required Columns & Rename for Clarity
df_dim_seller = df_sellers.select(
    col("seller_sk"),
    col("seller_id").alias("natural_seller_key"),  # Natural Key
    col("seller_zip_code").alias("seller_zip_code"),
    col("seller_city").alias("seller_city"),
    col("seller_state").alias("seller_state")
)

# Save as Delta Table in Gold Layer
df_dim_seller.write.format("delta")\
                    .mode("overwrite")\
                    .option("path", "abfss://gold@olistdatalake0.dfs.core.windows.net/dim_seller")\
                    .save()


# COMMAND ----------

# Explanation of Transformations
# 🔹 monotonically_increasing_id() → Generates a surrogate key (seller_sk).
# 🔹 fillna() → Fills NULL values for missing ZIP codes.
# 🔹 Renaming Columns → Standardizes naming for business clarity.
# 🔹 Saving as Delta Format → Enables efficient storage & querying.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 - Read GeoLocation Table

# COMMAND ----------

df_geolocation = spark.read.format("delta").load("abfss://silver@olistdatalake0.dfs.core.windows.net/geolocation")

display(df_geolocation)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create DimGeolocation Table

# COMMAND ----------

# Objective:
# The DimGeolocation table will store geographical details related to customer and seller locations. This dimension table helps in analyzing sales patterns region-wise.

# Transformations Applied:
# ✅ Generating Surrogate Keys → Unique geolocation_sk for efficient joins
# ✅ Removing Duplicates → Ensuring each location is unique
# ✅ Renaming & Standardizing Columns → Consistent and business-friendly names
# ✅ Handling Missing Values → Replacing NULL values
# ✅ Optimizing Query Performance → Saving as Delta format

# COMMAND ----------

from pyspark.sql.functions import col, monotonically_increasing_id

# Remove Duplicates (Ensuring unique geolocation entries)
df_geolocation = df_geolocation.dropDuplicates(["geolocation_latitude", "geolocation_longitude"])

# Generate Surrogate Key
df_geolocation = df_geolocation.withColumn("geolocation_sk", monotonically_increasing_id())

# Handle Missing Values
df_geolocation = df_geolocation.fillna(
{
    'geolocation_city': 'Unknown',
    'geolocation_state': 'Unknown',
    'geolocation_zip_code': 'Unknown',
    'geolocation_latitude': 'Unknown',
    'geolocation_longitude': 'Unknown',
    'geolocation_full_address': 'Unknown'

})

# Select Required Columns & Rename for Clarity
df_dim_geolocation = df_geolocation.select(
    col("geolocation_sk"),
    col("geolocation_zip_code").alias("zip_code"),
    col("geolocation_latitude").alias("latitude"),
    col("geolocation_longitude").alias("longitude"),
    col("geolocation_city").alias("city"),
    col("geolocation_state").alias("state")
)

# Save as Delta Table in Gold Layer
df_dim_geolocation.write.format("delta")\
                     .mode("overwrite")\
                     .option("path", "abfss://gold@olistdatalake0.dfs.core.windows.net/dim_geolocation")\
                     .save()


# COMMAND ----------

# Explanation of Transformations
# 🔹 Removing Duplicates → Ensures that each location is stored only once.
# 🔹 Generating Surrogate Keys → geolocation_sk replaces natural keys for faster lookups.
# 🔹 Handling NULL Values → If city or state is missing, we set it to "Unknown".
# 🔹 Renaming Columns → Making names clear and standardized.
# 🔹 Saving as Delta Table → Efficient for querying and analysis.