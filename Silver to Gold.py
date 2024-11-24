# Databricks notebook source
from pyspark.sql.functions import lit,current_timestamp,concat

# COMMAND ----------

df=spark.read.format('delta').load("/mnt/stgamit/containeramit/SilverZone/Coaches")
display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver_dbnew.coaches

# COMMAND ----------

df=spark.sql('select * from silver_dbNew.Coaches')
display(df)

# COMMAND ----------

df=spark.read.table('silver_dbNew.Coaches')
display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver_dbnew.medals

# COMMAND ----------

dfMedal=spark.read.format('delta').load('/mnt/demo/SilverZone/Medals')
display(dfMedal)

# COMMAND ----------

#1.Find the top countries with the highest number of gold medals

df_TopContriesGold=dfMedal.orderBy('Gold',ascending=False).select('Team_Country','Gold')
display(df_TopContriesGold)

# COMMAND ----------

# MAGIC %sql
# MAGIC create database Gold_dbNew

# COMMAND ----------

df_TopContriesGold.write.format('delta').mode('append').option('path','/mnt/stgamit/containeramit/GoldZone/TopCountriesWithMedals').saveAsTable('Gold_dbNew.TopCountriesWithMedals')