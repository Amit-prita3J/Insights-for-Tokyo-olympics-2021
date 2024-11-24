# Databricks notebook source
df_Athletes=spark.read.csv("/mnt/stgamit/containeramit/Landing_zone/Athletes/*",header=True,inferSchema=True)
#display(df_Athletes)

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------


df_Athletes = df_Athletes.withColumn("process_date", date_format(current_date(), "yyyy-MM-dd"))
# Use the string column name in partitionBy
df_Athletes.write.mode('append').partitionBy("process_date").parquet("/mnt/stgamit/containeramit/BronzeZone/Athletes/")

# COMMAND ----------

df_Coaches=spark.read.csv("/mnt/stgamit/containeramit/Landing_zone/Coaches/*",header=True,inferSchema=True)
#display(df_Coaches)

# COMMAND ----------

df_Coaches = df_Coaches.withColumn("process_date", date_format(current_date(), "yyyy-MM-dd"))
# Use the string column name in partitionBy
df_Coaches.write.mode('append').partitionBy("process_date").parquet("/mnt/stgamit/containeramit/BronzeZone/Coaches/")

# COMMAND ----------

df_EntriesGender=spark.read.csv("/mnt/stgamit/containeramit/Landing_zone/EntriesGender/*",header=True,inferSchema=True)
#display(df_EntriesGender)

# COMMAND ----------

df_EntriesGender = df_EntriesGender.withColumn("process_date", date_format(current_date(), "yyyy-MM-dd"))
# Use the string column name in partitionBy
df_EntriesGender.write.mode('append').partitionBy("process_date").parquet("/mnt/stgamit/containeramit/BronzeZone/EntriesGender/")

# COMMAND ----------

df_Medals=spark.read.csv("/mnt/stgamit/containeramit/Landing_zone/Medals/*",header=True,inferSchema=True)
#display(df_Medals)

# COMMAND ----------

df_Medals = df_Medals.withColumn("process_date", date_format(current_date(), "yyyy-MM-dd"))
# Use the string column name in partitionBy
df_Medals.write.mode('append').partitionBy("process_date").parquet("/mnt/stgamit/containeramit/BronzeZone/Medals/")

# COMMAND ----------

df_Teams=spark.read.csv("/mnt/stgamit/containeramit/Landing_zone/Teams/*",header=True,inferSchema=True)
#display(df_Teams)

# COMMAND ----------

df_Teams = df_Teams.withColumn("process_date", date_format(current_date(), "yyyy-MM-dd"))
# Use the string column name in partitionBy
df_Teams.write.mode('append').partitionBy("process_date").parquet("/mnt/stgamit/containeramit/BronzeZone/Teams/")