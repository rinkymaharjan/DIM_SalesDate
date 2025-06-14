# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType
from datetime import date, timedelta
from pyspark.sql.functions import (col, upper, hash, to_date, trim, date_format, year, month, quarter, expr, lit, when, concat)




# COMMAND ----------

spark = SparkSession.builder.appName("DIM-SalesDate").getOrCreate()

# COMMAND ----------

Schema = StructType([
    StructField("Region", StringType(), True),
    StructField("ProductCategory", StringType(), True),
    StructField("ProductSubCategory", StringType(), True),
    StructField("SalesChannel", StringType(), True),
    StructField("CustomerSegment", StringType(), True),
    StructField("SalesRep", StringType(), True),
    StructField("StoreType", StringType(), True),
    StructField("SalesDate", StringType(), True),
    StructField("UnitsSold", IntegerType(), True),
    StructField("Revenue", IntegerType(), True)
])

df_Fact_Sales = spark.read.option("header", True).schema(Schema).csv("/FileStore/tables/fact_sales.csv")

# COMMAND ----------


df_Fact_Sales = df_Fact_Sales.withColumn("SalesDate", to_date(trim("SalesDate"), "M/d/yyyy"))

# COMMAND ----------

df_min_max  = df_Fact_Sales.selectExpr("min(SalesDate) as min_date", "max(SalesDate) as max_date").first()
df_min_date = df_min_max["min_date"]
df_max_date = df_min_max["max_date"]

# COMMAND ----------

date_list = []
while df_min_date <= df_max_date:
    date_list.append((df_min_date,))
    df_min_date += timedelta(days=1)

df_Date  = spark.createDataFrame(date_list, ["Date"])

# COMMAND ----------

df_Date.display()

# COMMAND ----------

df_dim_date = df_Date.withColumn("DIM_DateID", hash(upper(date_format("Date", "yyyy-MM-dd"))).cast("bigint"))\
                    .withColumn("MonthName", date_format("Date", "MMMM")) \
                    .withColumn("Year", year("Date")) \
                    .withColumn("Semester", when(month("Date") <= 6, "H1").otherwise("H2")) \
                    .withColumn("Quarter", concat(lit("Q"), quarter("Date")))


# COMMAND ----------

df_dim_date.display()

# COMMAND ----------

dfBase = spark.createDataFrame([
    ("None",-1, "N/A", "None","N/A","N/A")
], ["Date", "DIM_DateID", "MonthName", "Year", "Semester", "Quarter" ])

dfDimFinal = df_dim_date.union(dfBase)

# COMMAND ----------

dfDimFinal.display()

# COMMAND ----------

dfDimFinal.write.format("delta").mode("overwrite").save("/FileStore/tables/DIM-SalesDate")

# COMMAND ----------

df_DIMSalesDate = spark.read.format("delta").load("/FileStore/tables/DIM-SalesDate")

# COMMAND ----------

df_DIMSalesDate.display()