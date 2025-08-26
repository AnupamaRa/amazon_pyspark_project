# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import window, avg, desc, col, length, count
spark = SparkSession.builder.appName("Amazon sales data").getOrCreate()
sales_data = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("/FileStore/tables/amazon.csv")
)
sales_data.display()


# COMMAND ----------

#Top product by rating
top_product = (
    sales_data.groupBy("product_id" , "product_name")
    .agg(avg("rating").alias("avg_rating"))
    .orderBy(desc("avg_rating"))
    .limit(10))

top_product.show()


# COMMAND ----------

#most reviwed product
Most_reviwed_pdt = (
    sales_data.groupBy("product_id" , "product_name")
    .count()
    .orderBy(desc('count'))
    .limit(10)
)
Most_reviwed_pdt.show()

# COMMAND ----------

#discount_analysis
Discount_analysis = sales_data.groupBy("category").agg(avg("discounted_price").alias("dis_analysis"))
Discount_analysis.display()

# COMMAND ----------

#User engagement 
user_engagement = (
    sales_data.groupBy("product_id", "product_name")
    .agg(
        avg("rating").alias("avg_rating"),
        count("rating").alias("review_count")
    )
    .orderBy(desc("avg_rating"))
    .limit(10)
)
user_engagement.show()