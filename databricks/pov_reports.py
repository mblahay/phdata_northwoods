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

def get_report(view_name):
  (spark.read
  .format("snowflake")
  .options(**snowflake_options)
  .option("query",  f"select * FROM {view_name}")
  .load()
  .display())

# COMMAND ----------

get_report("num_flights_by_airline")

# COMMAND ----------

# MAGIC %md
# MAGIC # Number of Flights by Airline/Airport by month

# COMMAND ----------

get_report("num_flights_by_airline_airport")

# COMMAND ----------

get_report("num_flights_by_airline")

# COMMAND ----------

# MAGIC %md
# MAGIC # On-time Percentage by Airline

# COMMAND ----------

get_report("on_time_percentage_by_airline")

# COMMAND ----------

get_report("on_time_percentage_by_airline")

# COMMAND ----------

# MAGIC %md
# MAGIC #Largest Number of Delays by Airline

# COMMAND ----------

get_report("largest_num_of_delays_by_airline")

# COMMAND ----------

get_report("largest_num_of_delays_by_airline")

# COMMAND ----------

# MAGIC %md
# MAGIC #Cancellation Reasons by Airport

# COMMAND ----------

get_report("cancellation_reasons_by_airport")

# COMMAND ----------

get_report("cancellations_by_reasons_and_airports")

# COMMAND ----------

# MAGIC %md
# MAGIC #Delay Reasons by Airport

# COMMAND ----------

get_report("delay_reasons_by_airport")

# COMMAND ----------

get_report("delay_reason_counts_by_airport")

# COMMAND ----------

# MAGIC %md
# MAGIC #Airline with the Most Unique Routes

# COMMAND ----------

get_report("airline_most_unique_routes")

# COMMAND ----------

get_report("airline_unique_routes")
