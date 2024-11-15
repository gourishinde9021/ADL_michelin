# Databricks notebook source
# MAGIC %md
# MAGIC Create Table
# MAGIC 1. Pyspart (Dataframe)
# MAGIC 2. Spark SQL
# MAGIC 3. UI
# MAGIC

# COMMAND ----------

df=spark.read.csv("/Volumes/my_workspace/michelin/raw/circuits.csv",header=True,inferSchema=True)

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Transformation
# MAGIC ### Dataframe functions
# MAGIC
# MAGIC - .select
# MAGIC - .alias
# MAGIC - .withColumnRenamed
# MAGIC - .withColumn
# MAGIC
# MAGIC ### Functions
# MAGIC col
# MAGIC
# MAGIC
# MAGIC ### Spark is Lazy Evaluated.
# MAGIC

# COMMAND ----------

df.select("*").display()

# COMMAND ----------

from pyspark.sql.functions import col
df.select(col("circuitId").alias("Circuit_ID"),"circuitRef").display()

# COMMAND ----------

df.withColumnRenamed("circuitId","Circuit_ID")

# COMMAND ----------

df.withColumnsRenamed({"circuitId":"circuit_id","circuitRef":"circuit_ref"}).display()

# COMMAND ----------

from pyspark.sql.functions import *
df.withColumn("ingestion_date",current_date()).display()
df.withColumn("country",upper("country")).display()
