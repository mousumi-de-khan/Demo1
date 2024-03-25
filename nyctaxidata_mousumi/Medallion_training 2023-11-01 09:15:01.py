# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT * FROM `samples`.`nyctaxi`.`trips`;

# COMMAND ----------

df=spark.table("samples.nyctaxi.trips")
display(df)


# COMMAND ----------

#Check the metadata of the table

df.printSchema()

