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

from pyspark.sql.functions import col
df1=df\
.withColumnsRenamed({"circuitId":"circuit_id","circuitRef":"circuit_ref","lat":"latitude","lng":"longitutde"})\
.withColumn("ingestion_date",current_timestamp())\
.drop("url")

# COMMAND ----------

df1.write.mode("overwrite").saveAsTable("michelin.circuits")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from michelin.circuits;

# COMMAND ----------


