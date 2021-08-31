# Databricks notebook source
# MAGIC %run "Users/mblahay@gmail.com/Demo Credentials"

# COMMAND ----------

#Setting up snowflake authentication
snowflake_options = {
  "sfUrl": "https://op82353.east-us-2.azure.snowflakecomputing.com",
  "sfUser": sfUser,
  "sfPassword": sfPassword,
  "sfDatabase": "NORTHWOODS",
  "sfSchema": "POV_REPORTING",
  "sfWarehouse": "SF_TUTS_WH"
}

# COMMAND ----------

spark.conf.set("fs.azure.account.key.northwoods40954fb03.dfs.core.windows.net", adls_key)

# COMMAND ----------

#Loading the Flights table
(spark.read
  .format("csv")
  .option("path","abfss://landing@northwoods40954fb03.dfs.core.windows.net/flights")
  .option("header",True)
  .load()
  .write
  .format("snowflake")
  .mode("overwrite")
  .options(**snowflake_options)
  .option("dbtable","flights")
  .save()
)

# COMMAND ----------

#Loading the Airline table
(spark.read
  .format("csv")
  .option("path","abfss://landing@northwoods40954fb03.dfs.core.windows.net/airlines")
  .option("header",True)
  .load()
  .write
  .format("snowflake")
  .mode("overwrite")
  .options(**snowflake_options)
  .option("dbtable","airlines")
  .save()
)

# COMMAND ----------

#Loading the airport table
(spark.read
  .format("csv")
  .option("path","abfss://landing@northwoods40954fb03.dfs.core.windows.net/airports")
  .option("header",True)
  .load()
  .write
  .format("snowflake")
  .mode("overwrite")
  .options(**snowflake_options)
  .option("dbtable","airports")
  .save()
)
