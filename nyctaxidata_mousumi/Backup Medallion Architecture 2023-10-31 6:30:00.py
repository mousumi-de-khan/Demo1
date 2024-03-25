# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT * FROM `samples`.`nyctaxi`.`trips`;

# COMMAND ----------

df=spark.table("samples.nyctaxi.trips")

display(df)

# COMMAND ----------

#Check the metadata of the table

df.printSchema()

# COMMAND ----------

# MAGIC %sql
# MAGIC --CREATE THE SCHEMA 
# MAGIC
# MAGIC CREATE SCHEMA nyctaxi_medallion;

# COMMAND ----------

# MAGIC %sql
# MAGIC --Creating the bronze table with ulaltered data from Nyc taxi trips
# MAGIC Create TABLE IF NOT EXISTS nyctaxi_medallion.trips_bronze(
# MAGIC tpep_pickup_datetime TIMESTAMP, 
# MAGIC tpep_dropoff_datetime TIMESTAMP,
# MAGIC trip_distance DOUBLE,
# MAGIC fare_amount DOUBLE,
# MAGIC pickup_zip INTEGER,
# MAGIC dropoff_zip INTEGER,
# MAGIC ingested_datetime TIMESTAMP,
# MAGIC ingested_date DATE
# MAGIC )
# MAGIC Partitioned by(ingested_date)
# MAGIC

# COMMAND ----------

#Populating the newly added ingested date and ingested datetime fields with current date and timestamp using functions imported from the pyspark.sql package


from pyspark.sql import functions as F

bronze = df.withColumn("ingested_datetime", F.current_timestamp()).withColumn("ingested_date", F.current_date())

display(bronze)



# COMMAND ----------

# MAGIC %sql
# MAGIC --creating the silver table with same specifications as the bronze table to do basic filtering of valid trips with trip distance > 0
# MAGIC CREATE TABLE IF NOT EXISTS nyctaxi_medallion.trips_silver(
# MAGIC   tpep_pickup_datetime  TIMESTAMP,
# MAGIC   tpep_dropoff_datetime TIMESTAMP,
# MAGIC   trip_distance         DOUBLE,
# MAGIC   fare_amount           DOUBLE,
# MAGIC   pickup_zip            INTEGER,
# MAGIC   dropoff_zip           INTEGER,
# MAGIC   ingested_datetime     TIMESTAMP,
# MAGIC   ingested_date         DATE
# MAGIC )
# MAGIC PARTITIONED BY (ingested_date);
# MAGIC
# MAGIC

# COMMAND ----------

from pyspark.sql import functions as F

silver = bronze.where("trip_distance > 0")

display(silver)

# COMMAND ----------

#This code will insert the data in the silver DataFrame into the trips_silver table in the nyctaxi_medallion namespace.

silver.write.insertInto("nyctaxi_medallion.trips_silver")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM nyctaxi_medallion.trips_silver 
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC --creating the gold table with agrregation of number of trips per day
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS nyctaxi_medallion.trips_gold (
# MAGIC   pickup_date DATE,
# MAGIC   trips INTEGER
# MAGIC )
# MAGIC
# MAGIC

# COMMAND ----------

#This code will create a gold table by grouping the silver DataFrame by pickup date, count the number of rows for each group, and rename the resulting count column to trips.


gold = gold = silver \
.select(F.to_date(F.col("tpep_pickup_datetime")).alias("pickup_date")) \
  .groupBy("pickup_date") \
  .count() \
  .withColumnRenamed("count", "trips")               

# COMMAND ----------



# COMMAND ----------

display(gold)
