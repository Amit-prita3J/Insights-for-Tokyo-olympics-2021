# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------

table_list = ["Athletes","Coaches","EntriesGender","Medals","Teams"]

# COMMAND ----------

bronze_zone_path = "/mnt/stgamit/containeramit/BronzeZone/"
silver_zone_parquet_path = "/mnt/stgamit/containeramit/SilverZone/"

# COMMAND ----------

for folder_name in table_list:
    input_path = bronze_zone_path + folder_name
    # Read data from input path
    df = spark.read.parquet(input_path)
    df.printSchema()
    display(df)

# COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS silver_dbNew")
spark.sql("USE silver_dbNew")

# COMMAND ----------

for folder_name in table_list:
    input_path = bronze_zone_path+"/"+folder_name
       
    # Read data from input path
    df = spark.read.parquet(input_path)

    #dropping ModifiedDate column
    df = df.drop('LastModifiedDate').drop('CreatedDate')

    # Apply NULL handling transformation
    for column, data_type in df.dtypes:
        if data_type == 'string':
            df = df.withColumn(column, F.when(df[column].isNull(), 'NA').otherwise(df[column]))
        elif data_type == 'int':
            df = df.withColumn(column, F.when(df[column].isNull(), -1).otherwise(df[column]))
        elif data_type.startswith('date') or data_type.startswith('timestamp'):
            df = df.withColumn(column, F.when(df[column].isNull(), '1900-01-01').otherwise(df[column]))
    #df=df.dropDuplicates()
    # Add audit columns - createdby,createddate,modifieddate
    #df = df.withColumn("CreatedID", F.lit("Pranab"))  
    df = df.withColumn("CreatedDate", F.date_format(F.current_timestamp(),"yyyy-MM-dd HH:mm:ss"))
    df = df.withColumn("ModifiedDate", F.date_format(F.current_timestamp(),"yyyy-MM-dd HH:mm:ss"))
    df.write.mode("append").format("delta").option("path",silver_zone_parquet_path+folder_name).saveAsTable("silver_dbNew."+folder_name)

# COMMAND ----------

# for table_name in table_list:
#     input_path = silver_zone_parquet_path + "/" + table_name
#     # Read data from input path
#     df = spark.read.parquet(input_path,mode="DROPMAL FORMED")
#     # df.printSchema()
#     display(df)